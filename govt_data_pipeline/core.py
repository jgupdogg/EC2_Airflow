"""
Core classes for the government data pipeline.

This module defines the object-oriented architecture for scraping,
processing, and storing government website data with Supabase integration.
"""

import json
import logging
import os
import hashlib
import traceback
from datetime import datetime
from typing import List, Dict, Any, Optional, Union, Type
from abc import ABC, abstractmethod
import requests
from bs4 import BeautifulSoup
from langchain.schema import Document as LCDocument
from supabase import create_client, Client
from web_scraper.airflow_web_scraper import AirflowWebScraper
from selenium.webdriver.common.by import By
from contextlib import contextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



class ScraperAdapter:
    """
    Adapter class that manages AirflowWebScraper and provides an interface
    compatible with the existing government data pipeline.
    """
    
    _instance = None  # Singleton instance
    
    @classmethod
    def get_instance(cls, **kwargs):
        """
        Get or create a singleton instance of the ScraperAdapter.
        This helps prevent creating multiple browser instances.
        
        Args:
            **kwargs: Arguments to pass to AirflowWebScraper constructor if creating new instance
            
        Returns:
            ScraperAdapter: The singleton instance
        """
        if cls._instance is None:
            cls._instance = cls(**kwargs)
        return cls._instance
    
    def __init__(self, headless=False, use_virtual_display=True):
        """
        Initialize the adapter with an AirflowWebScraper instance.
        
        Args:
            headless: Run browser in headless mode
            use_virtual_display: Use virtual display for X server
        """
        self.scraper = None
        self.headless = headless
        self.use_virtual_display = use_virtual_display
        self._init_count = 0
        
    def _initialize(self):
        """Initialize the AirflowWebScraper if not already initialized."""
        if self.scraper is None:
            logger.info("Initializing AirflowWebScraper")
            self.scraper = AirflowWebScraper(
                headless=self.headless,
                use_virtual_display=self.use_virtual_display,
                page_load_timeout=30
            )
        self._init_count += 1
        
    def _cleanup(self, force=False):
        """
        Clean up resources if no more users.
        
        Args:
            force: Force cleanup regardless of reference count
        """
        self._init_count -= 1
        if force or self._init_count <= 0:
            if self.scraper:
                logger.info("Cleaning up AirflowWebScraper")
                self.scraper.quit()
                self.scraper = None
                self._init_count = 0
    
    @contextmanager
    def managed_scraper(self):
        """
        Context manager for automatically managing the lifecycle of the scraper.
        
        Usage:
            with adapter.managed_scraper() as scraper:
                scraper.navigate(url)
        """
        self._initialize()
        try:
            yield self.scraper
        finally:
            self._cleanup()
    
    def fetch_page(self, url, wait_time=None):
        """
        Fetch a page using AirflowWebScraper.
        
        Args:
            url: URL to fetch
            wait_time: Optional wait time after page load
            
        Returns:
            str: HTML content of the page, or empty string if failed
        """
        with self.managed_scraper() as scraper:
            success = scraper.navigate(url, wait_time=wait_time)
            if success:
                return scraper.get_page_source()
            return ""
    
    def fetch_content_with_extractor(self, url, extractor_type, selector, timeout=10):
        """
        Fetch content from a page using a specific extractor type.
        
        Args:
            url: URL to fetch
            extractor_type: 'css' or 'xpath'
            selector: CSS selector or XPath expression
            timeout: Timeout for finding elements
            
        Returns:
            str: Extracted content, or empty string if failed
        """
        with self.managed_scraper() as scraper:
            success = scraper.navigate(url)
            if not success:
                return ""
            
            if extractor_type.lower() == 'xpath':
                # Convert to By.XPATH for Selenium
                locator = (By.XPATH, selector)
            else:
                # Default to CSS selector
                locator = (By.CSS_SELECTOR, selector)
            
            elements = scraper.find_elements(locator, timeout=timeout)
            if not elements:
                # Fall back to page source
                logger.warning(f"No elements found with {extractor_type}: {selector}. Returning full page.")
                return scraper.get_page_source()
            
            # Extract text from all matching elements
            content = ' '.join([elem.text for elem in elements if elem.text.strip()])
            return content
    
    def extract_document_links(self, url, extractor_type, selector, timeout=10):
        """
        Extract document links from a page.
        
        Args:
            url: URL to fetch
            extractor_type: 'css' or 'xpath'
            selector: CSS selector or XPath expression
            timeout: Timeout for finding elements
            
        Returns:
            List[Dict]: List of document links (url and title)
        """
        links = []
        
        with self.managed_scraper() as scraper:
            success = scraper.navigate(url)
            if not success:
                return links
            
            if extractor_type.lower() == 'xpath':
                # Convert to By.XPATH for Selenium
                locator = (By.XPATH, selector)
            else:
                # Default to CSS selector
                locator = (By.CSS_SELECTOR, selector)
            
            elements = scraper.find_elements(locator, timeout=timeout)
            
            for elem in elements:
                try:
                    href = elem.get_attribute('href')
                    if href:
                        title = elem.text.strip() or elem.get_attribute('title') or "Untitled"
                        links.append({
                            'url': href,
                            'title': title
                        })
                except Exception as e:
                    logger.error(f"Error extracting link: {e}")
                    continue
            
        return links
    
    def close(self):
        """Force close the scraper."""
        self._cleanup(force=True)
        
    def __del__(self):
        """Ensure scraper is closed when adapter is garbage collected."""
        self.close()
        

class ContentExtractor(ABC):
    """Base class for extracting content from web pages."""
    
    def __init__(self):
        """Initialize with a default scraper adapter."""
        self.scraper_adapter = ScraperAdapter.get_instance()
    
    @abstractmethod
    def extract_document_links(self, html: str, url: str) -> List[Dict[str, str]]:
        """
        Extract document links from an HTML page.
        
        Args:
            html: HTML content
            url: Base URL for resolving relative links
            
        Returns:
            List of dictionaries with 'url' and 'title' keys
        """
        pass
    
    @abstractmethod
    def extract_content(self, html: str, url: str) -> str:
        """
        Extract main content from an HTML page.
        
        Args:
            html: HTML content
            url: URL of the page
            
        Returns:
            Extracted text content
        """
        pass
    
    def extract_links_with_selenium(self, url: str) -> List[Dict[str, str]]:
        """
        Extract document links directly using Selenium.
        
        Args:
            url: URL to scrape
            
        Returns:
            List of dictionaries with 'url' and 'title' keys
        """
        pass


class XPathExtractor(ContentExtractor):
    """Content extractor using XPath selectors."""
    
    def __init__(self, document_links_xpath: str, content_xpath: str = None):
        """
        Initialize with XPath expressions.
        
        Args:
            document_links_xpath: XPath for finding document links
            content_xpath: Optional XPath for extracting main content
        """
        super().__init__()
        self.document_links_xpath = document_links_xpath
        self.content_xpath = content_xpath
    
    def extract_document_links(self, html: str, url: str) -> List[Dict[str, str]]:
        """Extract document links using XPath."""
        # Use the legacy method for compatibility
        from lxml import html as lxml_html
        
        tree = lxml_html.fromstring(html)
        links = []
        
        for element in tree.xpath(self.document_links_xpath):
            href = element.get('href')
            if href:
                # Resolve relative URLs
                if not href.startswith(('http://', 'https://')):
                    href = requests.compat.urljoin(url, href)
                
                title = element.text_content().strip() if element.text_content() else "Untitled"
                links.append({
                    'url': href,
                    'title': title
                })
        
        return links
    
    def extract_content(self, html: str, url: str) -> str:
        """Extract content using XPath."""
        from lxml import html as lxml_html
        
        tree = lxml_html.fromstring(html)
        
        if self.content_xpath:
            elements = tree.xpath(self.content_xpath)
            content = ' '.join([elem.text_content().strip() for elem in elements if elem.text_content()])
            return content
        
        # Fallback to extracting main content
        soup = BeautifulSoup(html, 'html.parser')
        main_content = soup.find('main') or soup.find('article') or soup.find('div', {'id': 'content'})
        
        if main_content:
            return main_content.get_text(' ', strip=True)
        
        # Last resort, get body text
        body = soup.find('body')
        return body.get_text(' ', strip=True) if body else ''
    
    def extract_links_with_selenium(self, url: str) -> List[Dict[str, str]]:
        """Extract document links directly using Selenium."""
        return self.scraper_adapter.extract_document_links(
            url=url,
            extractor_type='xpath',
            selector=self.document_links_xpath
        )
    
    def extract_content_with_selenium(self, url: str) -> str:
        """Extract content directly using Selenium."""
        if self.content_xpath:
            return self.scraper_adapter.fetch_content_with_extractor(
                url=url,
                extractor_type='xpath',
                selector=self.content_xpath
            )
        
        # If no content XPath is specified, fetch the page and use the regular extraction
        html = self.scraper_adapter.fetch_page(url)
        return self.extract_content(html, url)



class CSSExtractor(ContentExtractor):
    """Content extractor using CSS selectors."""
    
    def __init__(self, document_links_css: str, content_css: str = None):
        """
        Initialize with CSS selectors.
        
        Args:
            document_links_css: CSS selector for finding document links
            content_css: Optional CSS selector for extracting main content
        """
        super().__init__()
        self.document_links_css = document_links_css
        self.content_css = content_css
    
    def extract_document_links(self, html: str, url: str) -> List[Dict[str, str]]:
        """Extract document links using CSS selectors."""
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        
        for element in soup.select(self.document_links_css):
            href = element.get('href')
            if href:
                # Resolve relative URLs
                if not href.startswith(('http://', 'https://')):
                    href = requests.compat.urljoin(url, href)
                
                title = element.get_text().strip() if element.get_text() else "Untitled"
                links.append({
                    'url': href,
                    'title': title
                })
        
        return links
    
    def extract_content(self, html: str, url: str) -> str:
        """Extract content using CSS selectors."""
        soup = BeautifulSoup(html, 'html.parser')
        
        if self.content_css:
            elements = soup.select(self.content_css)
            content = ' '.join([elem.get_text(' ', strip=True) for elem in elements if elem.get_text()])
            return content
        
        # Fallback to extracting main content
        main_content = soup.find('main') or soup.find('article') or soup.find('div', id='content')
        
        if main_content:
            return main_content.get_text(' ', strip=True)
        
        # Last resort, get body text
        body = soup.find('body')
        return body.get_text(' ', strip=True) if body else ''
    
    def extract_links_with_selenium(self, url: str) -> List[Dict[str, str]]:
        """Extract document links directly using Selenium."""
        return self.scraper_adapter.extract_document_links(
            url=url,
            extractor_type='css',
            selector=self.document_links_css
        )
    
    def extract_content_with_selenium(self, url: str) -> str:
        """Extract content directly using Selenium."""
        if self.content_css:
            return self.scraper_adapter.fetch_content_with_extractor(
                url=url,
                extractor_type='css',
                selector=self.content_css
            )
        
        # If no content CSS is specified, fetch the page and use the regular extraction
        html = self.scraper_adapter.fetch_page(url)
        return self.extract_content(html, url)
    

class SubSource:
    """
    Represents a specific section or page within a government website.
    Contains rules for finding and extracting documents.
    
    Updated to use AirflowWebScraper for handling JavaScript-heavy sites.
    """
    
    def __init__(self, subsource_config: Dict[str, Any], parent_source=None):
        """
        Initialize a SubSource from configuration.
        
        Args:
            subsource_config: Dictionary with subsource configuration
            parent_source: Parent ScrapeSource object
        """
        self.name = subsource_config.get('name', 'Unnamed Subsource')
        self.url_pattern = subsource_config.get('url_pattern', '')
        self.parent = parent_source
        
        # Initialize the appropriate extractor
        extraction_config = subsource_config.get('extraction', {})
        extractor_type = extraction_config.get('type', 'css')
                
        if extractor_type.lower() == 'xpath':
            self.extractor = XPathExtractor(
                document_links_xpath=extraction_config.get('document_links', '//a'),
                content_xpath=extraction_config.get('content', None)
            )
        else:  # Default to CSS
            self.extractor = CSSExtractor(
                document_links_css=extraction_config.get('document_links', 'a'),
                content_css=extraction_config.get('content', None)
            )
        
        # Additional configuration
        self.pagination = extraction_config.get('pagination', None)
        self.max_pages = subsource_config.get('max_pages', 1)
        self.max_documents = subsource_config.get('max_documents', 100)
        
        # Use JavaScript rendering flag
        self.use_javascript = subsource_config.get('use_javascript', False)
        
        # Initialize scraper adapter
        self.scraper_adapter = ScraperAdapter.get_instance()
    
    def get_full_url(self) -> str:
        """Get the full URL for this subsource."""
        if self.parent:
            return requests.compat.urljoin(self.parent.base_url, self.url_pattern)
        return self.url_pattern
    
    def fetch_page(self, page_num: int = 1) -> str:
        """
        Fetch a page from this subsource.
        
        Args:
            page_num: Page number for pagination
            
        Returns:
            HTML content of the page
        """
        url = self.get_full_url()
        
        # Apply pagination if needed
        if page_num > 1 and '?' in url:
            url += f"&page={page_num}"
        elif page_num > 1:
            url += f"?page={page_num}"
        
        # If site requires JavaScript, use AirflowWebScraper
        if self.use_javascript:
            logger.info(f"Using AirflowWebScraper to fetch {url} (page {page_num})")
            html = self.scraper_adapter.fetch_page(url)
            return html
        else:
            # Use traditional requests for non-JavaScript sites
            try:
                logger.info(f"Using requests to fetch {url} (page {page_num})")
                response = requests.get(url)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                logger.error(f"Error fetching page {url}: {e}")
                return ""
    
    def get_document_links(self) -> List[Dict[str, str]]:
        """
        Get all document links from this subsource.
        
        Returns:
            List of dictionaries with 'url' and 'title' keys
        """
        all_links = []
        page_num = 1
        
        # If using JavaScript rendering, use Selenium directly
        if self.use_javascript:
            while page_num <= self.max_pages:
                url = self.get_full_url()
                
                # Apply pagination if needed
                if page_num > 1 and '?' in url:
                    url += f"&page={page_num}"
                elif page_num > 1:
                    url += f"?page={page_num}"
                
                # Use Selenium-based extraction
                links = self.extractor.extract_links_with_selenium(url)
                
                all_links.extend(links)
                
                if not self.pagination or len(links) == 0:
                    break
                
                page_num += 1
        else:
            # Use traditional HTML parsing
            while page_num <= self.max_pages:
                html = self.fetch_page(page_num)
                if not html:
                    break
                
                links = self.extractor.extract_document_links(html, self.get_full_url())
                all_links.extend(links)
                
                if not self.pagination or len(links) == 0:
                    break
                
                page_num += 1
        
        # Limit the number of documents and transform URLs
        limited_links = all_links[:self.max_documents]
        
        # Transform URLs - replace .toc.htm with .htm in URLs
        transformed_links = []
        for link in limited_links:
            url = link['url']
            # Check if URL contains .toc.htm
            if '.toc.htm' in url:
                # Replace .toc.htm with .htm
                url = url.replace('.toc.htm', '.htm')
                link['url'] = url
            transformed_links.append(link)
        
        # Return transformed links
        return transformed_links

class ScrapeSource:
    """
    Represents a government website to be scraped.
    Contains configuration and subsources.
    """
    
    def __init__(self, source_config: Dict[str, Any]):
        """
        Initialize a ScrapeSource from configuration.
        
        Args:
            source_config: Dictionary with source configuration
        """
        self.name = source_config.get('name', 'Unnamed Source')
        self.base_url = source_config.get('url', '')
        self.subsources = [SubSource(sub, self) for sub in source_config.get('pages', [])]
    
    def get_subsources(self) -> List[SubSource]:
        """Get all subsources for this source."""
        return self.subsources




class Document:
    """
    Represents a document scraped from a government website.
    Tracks state and processing.
    
    Updated to use AirflowWebScraper for JavaScript-heavy sites.
    """
    
    def __init__(self, url: str, title: str, source_name: str, subsource_name: str):
        """
        Initialize a Document.
        
        Args:
            url: URL of the document
            title: Title of the document
            source_name: Name of the source website
            subsource_name: Name of the subsource
        """
        self.url = url
        self.title = title or "Untitled Document"  # Ensure title is never None
        self.source_name = source_name
        self.subsource_name = subsource_name
        self.content = None
        self.content_hash = None  # Hash to identify unique content
        self.summary = None
        self.embedding_id = None
        self.status = "new"
        self.scrape_time = None
        self.process_time = None
        self.last_checked = None  # When we last verified this document
        self.doc_id = None  # Database ID
        self.use_javascript = False  # Flag to indicate if the document needs JavaScript
    
    def fetch_content(self, extractor, use_javascript: Optional[bool] = True) -> bool:
        """
        Fetch and extract content for this document.
        
        Args:
            extractor: ContentExtractor to use
            use_javascript: Override the document's use_javascript flag
            
        Returns:
            bool: True if successful
        """
        # Determine whether to use JavaScript
        js_enabled = use_javascript if use_javascript is not None else self.use_javascript
        
        try:
            logger.info(f"Fetching content from URL: {self.url} (JavaScript: {js_enabled})")
            
            if js_enabled:
                # Use Selenium-based extraction
                logger.debug(f"Using Selenium to extract content from {self.url}")
                
                # Get a scraper adapter instance
                scraper_adapter = ScraperAdapter.get_instance()
                
                # Try to use the direct Selenium extraction
                if hasattr(extractor, 'extract_content_with_selenium'):
                    self.content = extractor.extract_content_with_selenium(self.url)
                else:
                    # Fallback: get page and then extract
                    html = scraper_adapter.fetch_page(self.url)
                    
                    if not html:
                        logger.error(f"Failed to fetch page using Selenium: {self.url}")
                        self.status = "error_page_fetch"
                        return False
                    
                    # Extract content using the provided extractor
                    self.content = extractor.extract_content(html, self.url)
            else:
                # Use traditional requests for non-JavaScript sites
                import requests
                
                # Set a timeout and user agent for better scraping
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                response = requests.get(self.url, headers=headers, timeout=10)
                
                # Check status code
                if response.status_code != 200:
                    logger.error(f"HTTP error {response.status_code} when fetching {self.url}")
                    self.status = f"error_http_{response.status_code}"
                    return False
                    
                # Check content type
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' not in content_type and 'application/xhtml+xml' not in content_type:
                    logger.error(f"Unsupported content type: {content_type} for {self.url}")
                    self.status = "error_content_type"
                    return False
                
                # Extract content
                logger.debug(f"Extracting content using extractor type: {type(extractor).__name__}")
                self.content = extractor.extract_content(response.text, self.url)
            
            # Check if content was extracted
            if not self.content or len(self.content.strip()) == 0:
                logger.warning(f"No content extracted from {self.url}")
                self.status = "error_no_content"
                return False
                
            # Log content length
            logger.debug(f"Extracted content length: {len(self.content)} characters")
            
            # Update document timestamps
            self.scrape_time = datetime.now()
            self.last_checked = datetime.now()
            
            # Generate a hash of the content for checking duplication
            self.content_hash = hashlib.md5(self.content.encode('utf-8')).hexdigest()
            self.status = "scraped"
            
            logger.info(f"Successfully fetched and extracted content from {self.url}")
            return True
        
        except Exception as e:
            logger.error(f"Unexpected error when fetching document {self.url}: {str(e)}", exc_info=True)
            logger.error(f"Traceback: {traceback.format_exc()}")
            self.status = "error_unknown"
            return False
    
    def to_dict(self) -> dict:
        """Convert document to dictionary for storage."""
        return {
            "url": self.url,
            "title": self.title,
            "source_name": self.source_name,
            "subsource_name": self.subsource_name,
            "content": self.content,
            "content_hash": self.content_hash,
            "summary": self.summary,
            "embedding_id": self.embedding_id,
            "status": self.status,
            "scrape_time": self.scrape_time.isoformat() if self.scrape_time else None,
            "process_time": self.process_time.isoformat() if self.process_time else None,
            "last_checked": self.last_checked.isoformat() if self.last_checked else None,
            "id": self.doc_id  # Include ID for completeness
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Document':
        """Create a Document from a dictionary."""
        doc = cls(
            url=data.get("url", ""),
            title=data.get("title", "Untitled"),
            source_name=data.get("source_name", "Unknown Source"),
            subsource_name=data.get("subsource_name", "Unknown Subsource")
        )
        
        # Safely assign properties
        doc.content = data.get("content")
        doc.content_hash = data.get("content_hash")
        doc.summary = data.get("summary")
        doc.embedding_id = data.get("embedding_id")
        doc.status = data.get("status", "new")
        doc.use_javascript = data.get("use_javascript", False)
        
        # Safely parse date fields, handling None values
        try:
            if data.get("scrape_time") and isinstance(data.get("scrape_time"), str):
                doc.scrape_time = datetime.fromisoformat(data["scrape_time"])
            elif data.get("scrape_time"):  # Handle datetime objects from database
                doc.scrape_time = data["scrape_time"]
        except ValueError as e:
            logger.warning(f"Invalid scrape_time format: {data.get('scrape_time')}: {str(e)}")
        
        try:
            if data.get("process_time") and isinstance(data.get("process_time"), str):
                doc.process_time = datetime.fromisoformat(data["process_time"])
            elif data.get("process_time"):
                doc.process_time = data["process_time"]
        except ValueError as e:
            logger.warning(f"Invalid process_time format: {data.get('process_time')}: {str(e)}")
            
        try:
            if data.get("last_checked") and isinstance(data.get("last_checked"), str):
                doc.last_checked = datetime.fromisoformat(data["last_checked"])
            elif data.get("last_checked"):
                doc.last_checked = data["last_checked"]
        except ValueError as e:
            logger.warning(f"Invalid last_checked format: {data.get('last_checked')}: {str(e)}")
        
        # Set ID if available
        doc.doc_id = data.get("id")
        
        return doc
    
    def __str__(self) -> str:
        """String representation of Document."""
        return f"Document(id={self.doc_id}, url='{self.url}', status='{self.status}')"
        
    def __repr__(self) -> str:
        """Detailed representation of Document."""
        return f"Document(id={self.doc_id}, url='{self.url}', title='{self.title}', status='{self.status}', " \
               f"scrape_time={self.scrape_time}, content_length={len(self.content) if self.content else 0})"
               
               
class Processor:
    """
    Handles AI processing of documents:
    - Generating summaries
    - Creating embeddings
    - Storing in vector database
    """
    
    def __init__(
        self, 
        anthropic_api_key: str = None, 
        openai_api_key: str = None,
        pinecone_api_key: str = None,
        pinecone_index: str = "govt-scrape-index",
        pinecone_namespace: str = "govt-content"
    ):
        """
        Initialize the processor with API keys.
        
        Args:
            anthropic_api_key: API key for Anthropic (Claude)
            openai_api_key: API key for OpenAI (embeddings)
            pinecone_api_key: API key for Pinecone
            pinecone_index: Pinecone index name
            pinecone_namespace: Pinecone namespace
        """
        # Store configuration
        self.anthropic_api_key = anthropic_api_key or os.getenv("ANTHROPIC_API_KEY")
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        self.pinecone_api_key = pinecone_api_key or os.getenv("PINECONE_API_KEY")
        self.pinecone_index = pinecone_index
        self.pinecone_namespace = pinecone_namespace
        
        # Initialize models lazily when needed
        self._llm = None
        self._embedding_model = None
        self._vector_store = None
    
    def _init_llm(self):
        """Initialize LLM if not already done."""
        if self._llm is None:
            from langchain_anthropic import ChatAnthropic
            
            self._llm = ChatAnthropic(
                model="claude-3-haiku-20240307",
                anthropic_api_key=self.anthropic_api_key,
                temperature=0.3
            )
            logger.info("Initialized Anthropic Claude model")
    
    def _init_embedding_model(self):
        """Initialize embedding model if not already done."""
        if self._embedding_model is None:
            from langchain_openai import OpenAIEmbeddings
            
            self._embedding_model = OpenAIEmbeddings(
                model="text-embedding-3-small",
                openai_api_key=self.openai_api_key
            )
            logger.info("Initialized OpenAI embedding model")
    
    def _init_vector_store(self):
        """Initialize Pinecone vector store if not already done."""
        if self._vector_store is None:
            try:
                # Initialize embedding model first if needed
                self._init_embedding_model()
                
                # Import the recommended langchain_pinecone package
                from langchain_pinecone import PineconeVectorStore
                
                # Set up Pinecone environment
                import pinecone
                pc = pinecone.Pinecone(api_key=self.pinecone_api_key)
                
                # Check if index exists, create if it doesn't
                if self.pinecone_index not in [idx.name for idx in pc.list_indexes()]:
                    logger.warning(f"Index {self.pinecone_index} not found, creating...")
                    # Create the index with appropriate dimensions for the embedding model
                    pc.create_index(
                        name=self.pinecone_index,
                        dimension=1536,  # Dimension for text-embedding-3-small
                        metric="cosine"
                    )
                
                # Initialize the vector store with LangChain
                self._vector_store = PineconeVectorStore(
                    index_name=self.pinecone_index,
                    embedding=self._embedding_model,
                    text_key="content",  # The key in metadata containing the text to embed
                    namespace=self.pinecone_namespace
                )
                
                logger.info(f"Successfully initialized Pinecone vector store with index: {self.pinecone_index}, namespace: {self.pinecone_namespace}")
            
            except ImportError as ie:
                logger.error(f"Import error: {ie}. Make sure you have langchain-pinecone package installed.")
                raise
            except Exception as e:
                logger.error(f"Error initializing vector store: {e}", exc_info=True)
                raise
    
    def summarize(self, document):
        """
        Generate a summary for a document.
        
        Args:
            document: Document to summarize
            
        Returns:
            Summary text
        """
        if not document.content:
            raise ValueError("Document has no content to summarize")
        
        self._init_llm()
        
        prompt = f"""Generate a structured summary of the following government website content.

Input:
Title: {document.title}
Source: {document.source_name} - {document.subsource_name}
URL: {document.url}

Content:
{document.content[:8000]}

Output Format:
1. TITLE: A clear, direct title that captures the main topic (no more than 10 words)
2. FACTS: 3-5 bullet points with the most important and relevant information 
3. SENTIMENT: One bullet point expressing the overall sentiment (positive, negative, or neutral) of the content
4. TAGS: 5-7 relevant keywords/tags separated by commas

Example:
TITLE: Federal Grant Program Launches for Rural Communities

- $50 million in federal funding allocated to support infrastructure in rural communities
- Applications will be accepted from April 1 to June 30, 2025
- Eligible counties must have populations under 50,000 residents
- Priority given to projects addressing water quality and broadband access
- Overall sentiment is positive, with program expected to benefit approximately 200 rural counties nationwide

TAGS: rural infrastructure, federal grants, funding opportunity, application deadline, eligibility requirements, water quality, broadband
"""

        response = self._llm.invoke(prompt)
        summary = response.content.strip()
        
        return summary
    
    def store_embedding(self, document) -> str:
        """
        Store document embedding in Pinecone using LangChain.
        
        Args:
            document: Document with summary
            
        Returns:
            Embedding ID
        """
        if not document.summary:
            raise ValueError("Document has no summary to embed")
        
        self._init_vector_store()
        
        # Create a unique ID
        embedding_id = f"gov-{hashlib.md5(document.url.encode()).hexdigest()[:12]}"
        
        try:
            # Create LangChain Document
            lc_doc = LCDocument(
                page_content=document.summary,
                metadata={
                    "url": document.url,
                    "title": document.title,
                    "source": document.source_name, 
                    "subsource": document.subsource_name,
                    "content": document.summary,  # This is used as text_key for embedding
                    "processed_at": datetime.now().isoformat()
                }
            )
            
            # Store in vector store
            ids = self._vector_store.add_documents([lc_doc], ids=[embedding_id])
            
            logger.info(f"Successfully stored document in vector store with ID: {ids[0]}")
            return ids[0]
        
        except Exception as e:
            logger.error(f"Error storing embedding: {e}", exc_info=True)
            raise
    
    def process_document(self, document) -> bool:
        """
        Process a document end-to-end:
        1. Generate summary
        2. Create embedding
        3. Store in vector database
        
        Args:
            document: Document to process
            
        Returns:
            bool: True if successful
        """
        try:
            # Skip if already processed unless forced
            if document.embedding_id and document.status == "processed":
                logger.info(f"Document already processed with embedding_id: {document.embedding_id}")
                return True
                
            # Generate summary if needed
            if not document.summary:
                document.summary = self.summarize(document)
                logger.info(f"Generated summary for document: {document.url}")
            
            # Create and store embedding
            document.embedding_id = self.store_embedding(document)
            
            # Update status
            document.status = "processed"
            document.process_time = datetime.now()
            
            logger.info(f"Successfully processed document: {document.url}")
            return True
        
        except Exception as e:
            logger.error(f"Error processing document {document.url}: {e}", exc_info=True)
            document.status = "error_processing"
            return False

    def search_similar_documents(self, query: str, k: int = 5) -> List[Dict[str, Any]]:
        """
        Search for documents similar to the query.
        
        Args:
            query: Text query
            k: Number of results to return
            
        Returns:
            List of document dictionaries with similarity scores
        """
        self._init_vector_store()
        
        try:
            results = self._vector_store.similarity_search_with_score(query, k=k)
            
            # Format results
            formatted_results = []
            for doc, score in results:
                formatted_results.append({
                    "title": doc.metadata.get("title", "Untitled"),
                    "url": doc.metadata.get("url", ""),
                    "source": doc.metadata.get("source", ""),
                    "subsource": doc.metadata.get("subsource", ""),
                    "summary": doc.page_content,
                    "similarity_score": score
                })
            
            return formatted_results
            
        except Exception as e:
            logger.error(f"Error searching similar documents: {e}", exc_info=True)
            return []



class SupabaseManager:
    """
    Manages all database interactions using Supabase.
    Handles document storage, retrieval, and status updates.
    """
    
    def __init__(self, supabase_url: str = None, supabase_key: str = None):
        """
        Initialize the Supabase client.
        
        Args:
            supabase_url: Supabase project URL
            supabase_key: Supabase API key
        """
        self.supabase_url = supabase_url or os.getenv("SUPABASE_URL")
        self.supabase_key = supabase_key or os.getenv("SUPABASE_KEY")
        self.supabase = None
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Supabase URL and API key are required. Set SUPABASE_URL and SUPABASE_KEY environment variables.")
        
        try:
            self.supabase = create_client(self.supabase_url, self.supabase_key)
            logger.info("Successfully initialized Supabase client")
        except Exception as e:
            logger.error(f"Error initializing Supabase client: {e}")
            raise
    
    def setup_tables(self) -> bool:
        """
        Verify that the necessary tables exist in Supabase.
        The tables should be created manually in the Supabase SQL editor.
        
        Returns:
            bool: True if tables exist
        """
        try:
            # Verify the tables exist by querying them
            logger.info("Verifying Supabase tables exist")
            
            try:
                # Check if govt_documents table exists
                self.supabase.table("govt_documents").select("id").limit(1).execute()
                logger.info("govt_documents table exists")
                
                # Check if govt_sources table exists
                self.supabase.table("govt_sources").select("id").limit(1).execute()
                logger.info("govt_sources table exists")
                
                # Check if govt_subsources table exists
                self.supabase.table("govt_subsources").select("id").limit(1).execute()
                logger.info("govt_subsources table exists")
                
                return True
            except Exception as e:
                logger.error(f"Error verifying tables: {e}")
                logger.info("Please run the table creation SQL in the Supabase SQL editor")
                return False
            
        except Exception as e:
            logger.error(f"Error setting up Supabase tables: {e}")
            return False
    
    def store_source(self, name: str, base_url: str) -> int:
        """
        Store a source in the database.
        
        Args:
            name: Name of the source
            base_url: Base URL of the source
            
        Returns:
            int: Source ID if successful, 0 otherwise
        """
        try:
            # Check if source exists
            result = self.supabase.table("govt_sources").select("id").eq("name", name).execute()
            
            if result.data and len(result.data) > 0:
                # Source exists, return ID
                source_id = result.data[0]["id"]
                logger.info(f"Source {name} already exists with ID {source_id}")
                return source_id
            
            # Insert new source
            now = datetime.now().isoformat()
            result = self.supabase.table("govt_sources").insert({
                "name": name,
                "base_url": base_url,
                "created_at": now,
                "updated_at": now
            }).execute()
            
            if result.data and len(result.data) > 0:
                source_id = result.data[0]["id"]
                logger.info(f"Source {name} stored with ID {source_id}")
                return source_id
            
            return 0
            
        except Exception as e:
            logger.error(f"Error storing source {name}: {e}")
            return 0
    
    def store_subsource(self, source_id: int, name: str, url_pattern: str) -> int:
        """
        Store a subsource in the database.
        
        Args:
            source_id: Parent source ID
            name: Name of the subsource
            url_pattern: URL pattern for the subsource
            
        Returns:
            int: Subsource ID if successful, 0 otherwise
        """
        try:
            # Check if subsource exists
            result = self.supabase.table("govt_subsources").select("id").eq("source_id", source_id).eq("name", name).execute()
            
            if result.data and len(result.data) > 0:
                # Subsource exists, return ID
                subsource_id = result.data[0]["id"]
                logger.info(f"Subsource {name} already exists with ID {subsource_id}")
                return subsource_id
            
            # Insert new subsource
            now = datetime.now().isoformat()
            result = self.supabase.table("govt_subsources").insert({
                "source_id": source_id,
                "name": name,
                "url_pattern": url_pattern,
                "created_at": now,
                "updated_at": now
            }).execute()
            
            if result.data and len(result.data) > 0:
                subsource_id = result.data[0]["id"]
                logger.info(f"Subsource {name} stored with ID {subsource_id}")
                return subsource_id
            
            return 0
            
        except Exception as e:
            logger.error(f"Error storing subsource {name}: {e}")
            return 0
    
    def get_document_by_url(self, url: str) -> Optional[Document]:
        """
        Get a document by URL.
        
        Args:
            url: URL of the document
            
        Returns:
            Document object if found, None otherwise
        """
        try:
            result = self.supabase.table("govt_documents").select("*").eq("url", url).execute()
            
            if result.data and len(result.data) > 0:
                return Document.from_dict(result.data[0])
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting document by URL {url}: {e}")
            return None
    
    
    def get_documents_by_ids(self, doc_ids):
        """
        Get multiple documents by their IDs.
        
        Args:
            doc_ids: List of document IDs to retrieve
            
        Returns:
            List of document dictionaries
        """
        try:
            if not doc_ids:
                return []
            
            # Convert any non-string IDs to strings
            str_ids = [str(doc_id) for doc_id in doc_ids]
            
            # Query Supabase for documents with these IDs
            result = self.supabase.table("govt_documents").select(
                "id", "title", "url", "summary", "source_name", "subsource_name", "content_hash"
            ).in_("id", str_ids).execute()
            
            if result.data:
                logger.info(f"Retrieved {len(result.data)} documents from Supabase by IDs")
                return result.data
            else:
                logger.warning(f"No documents found in Supabase for the provided IDs")
                return []
                
        except Exception as e:
            logger.error(f"Error getting documents by IDs: {e}")
            return []
    
    def store_document(self, document: Document) -> int:
        """
        Store a document in Supabase.
        
        Args:
            document: Document to store
            
        Returns:
            int: Document ID if successful, 0 otherwise
        """
        try:
            # Convert document to dictionary
            doc_dict = document.to_dict()
            
            # Remove ID from dict for insert, we'll use it for check and update
            doc_id = doc_dict.pop("id", None)
            
            # Handle datetime objects for JSON serialization
            for key, value in doc_dict.items():
                if isinstance(value, datetime):
                    doc_dict[key] = value.isoformat()
            
            # Add updated_at timestamp
            doc_dict["updated_at"] = datetime.now().isoformat()
            
            if doc_id:
                # Document exists, update it
                result = self.supabase.table("govt_documents").update(doc_dict).eq("id", doc_id).execute()
                
                if result.data and len(result.data) > 0:
                    logger.info(f"Updated document with ID {doc_id}")
                    return doc_id
                else:
                    logger.error(f"Failed to update document with ID {doc_id}")
                    return 0
            else:
                # Check if document with this URL exists
                existing = self.get_document_by_url(document.url)
                
                if existing:
                    # Document exists, update with existing ID
                    doc_dict["updated_at"] = datetime.now().isoformat()
                    result = self.supabase.table("govt_documents").update(doc_dict).eq("id", existing.doc_id).execute()
                    
                    if result.data and len(result.data) > 0:
                        document.doc_id = existing.doc_id
                        logger.info(f"Updated existing document with ID {existing.doc_id}")
                        return existing.doc_id
                    else:
                        logger.error(f"Failed to update existing document with URL {document.url}")
                        return 0
                else:
                    # New document, insert it
                    # Add created_at timestamp
                    doc_dict["created_at"] = datetime.now().isoformat()
                    
                    result = self.supabase.table("govt_documents").insert(doc_dict).execute()
                    
                    if result.data and len(result.data) > 0:
                        doc_id = result.data[0]["id"]
                        document.doc_id = doc_id
                        logger.info(f"Inserted new document with ID {doc_id}")
                        return doc_id
                    else:
                        logger.error(f"Failed to insert document with URL {document.url}")
                        return 0
                        
        except Exception as e:
            logger.error(f"Error storing document {document.url}: {e}")
            return 0
    
    def get_unprocessed_documents(self, limit: int = 100) -> List[Document]:
        """
        Get documents that need processing.
        
        Args:
            limit: Maximum number of documents to return
            
        Returns:
            List of Document objects
        """
        try:
            result = self.supabase.table("govt_documents").select("*").eq(
                "status", "scraped"
            ).order("scrape_time", {"ascending": True}).limit(limit).execute()
            
            if result.data:
                return [Document.from_dict(doc) for doc in result.data]
            
            return []
            
        except Exception as e:
            logger.error(f"Error getting unprocessed documents: {e}")
            return []
    
    def search_full_text(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Perform full-text search on document content using Supabase's PostgreSQL.
        
        Args:
            query: Search query
            limit: Maximum number of results
            
        Returns:
            List of matching documents with search metadata
        """
        try:
            # Try to use stored procedure if it exists
            try:
                # Call the stored procedure (must be created in SQL editor first)
                result = self.supabase.rpc(
                    "search_documents", 
                    {"search_query": query, "max_results": limit}
                ).execute()
                
                if result.data:
                    return result.data
            except Exception as e:
                logger.warning(f"Could not use search_documents function: {e}")
                logger.info("Falling back to basic search")
                
                # Fall back to basic search using filter
                # Note: This is less powerful than the full-text search function
                result = self.supabase.table("govt_documents").select(
                    "id", "url", "title", "source_name", "subsource_name", "summary"
                ).ilike("content", f"%{query}%").limit(limit).execute()
                
                if result.data:
                    # Add placeholder rank and highlight
                    for item in result.data:
                        item["rank"] = 1.0
                        item["highlight"] = "..."  # No highlight in basic search
                    
                    return result.data
            
            return []
            
        except Exception as e:
            logger.error(f"Error performing full-text search: {e}")
            return []
    
    def upload_file_to_storage(self, file_path: str, bucket: str = 'documents', 
                            remote_path: str = None) -> Optional[str]:
        """
        Upload a file to Supabase Storage.
        
        Args:
            file_path: Local path to file
            bucket: Storage bucket name
            remote_path: Path/filename in storage
            
        Returns:
            str: Public URL of uploaded file if successful, None otherwise
        """
        try:
            # Check if bucket exists, create if not
            buckets = self.supabase.storage.list_buckets()
            bucket_exists = any(b['name'] == bucket for b in buckets)
            
            if not bucket_exists:
                logger.info(f"Creating bucket: {bucket}")
                self.supabase.storage.create_bucket(bucket)
            
            # Determine remote path if not provided
            if not remote_path:
                file_name = os.path.basename(file_path)
                remote_path = file_name
            
            # Read file content
            with open(file_path, 'rb') as f:
                file_content = f.read()
            
            # Upload file
            result = self.supabase.storage.from_(bucket).upload(
                remote_path, 
                file_content,
                file_options={"contentType": "application/octet-stream"}
            )
            
            # Get public URL
            public_url = self.supabase.storage.from_(bucket).get_public_url(remote_path)
            
            logger.info(f"Successfully uploaded file to {public_url}")
            return public_url
            
        except Exception as e:
            logger.error(f"Error uploading file to Supabase Storage: {e}")
            return None


class ScrapeConfig:
    """
    Main configuration class for the scraping pipeline.
    Loads and parses the JSON configuration file.
    """
    
    def __init__(self, config_path: str):
        """
        Initialize from a configuration file.
        
        Args:
            config_path: Path to JSON configuration file
        """
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.sources = [ScrapeSource(src) for src in self.config.get('sources', [])]
    
    def get_sources(self) -> List[ScrapeSource]:
        """Get all sources from the configuration."""
        return self.sources