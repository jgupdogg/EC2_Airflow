"""
Core classes for the government data pipeline.

This module defines the object-oriented architecture for scraping,
processing, and storing government website data.
"""

import json
import logging
import os
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional, Union, Type
from abc import ABC, abstractmethod
import requests
from bs4 import BeautifulSoup
from langchain.schema import Document as LCDocument

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ContentExtractor(ABC):
    """Base class for extracting content from web pages."""
    
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


class XPathExtractor(ContentExtractor):
    """Content extractor using XPath selectors."""
    
    def __init__(self, document_links_xpath: str, content_xpath: str = None):
        """
        Initialize with XPath expressions.
        
        Args:
            document_links_xpath: XPath for finding document links
            content_xpath: Optional XPath for extracting main content
        """
        self.document_links_xpath = document_links_xpath
        self.content_xpath = content_xpath
    
    def extract_document_links(self, html: str, url: str) -> List[Dict[str, str]]:
        """Extract document links using XPath."""
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


class CSSExtractor(ContentExtractor):
    """Content extractor using CSS selectors."""
    
    def __init__(self, document_links_css: str, content_css: str = None):
        """
        Initialize with CSS selectors.
        
        Args:
            document_links_css: CSS selector for finding document links
            content_css: Optional CSS selector for extracting main content
        """
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


class SubSource:
    """
    Represents a specific section or page within a government website.
    Contains rules for finding and extracting documents.
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
        
        try:
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
        
        while page_num <= self.max_pages:
            html = self.fetch_page(page_num)
            if not html:
                break
            
            links = self.extractor.extract_document_links(html, self.get_full_url())
            all_links.extend(links)
            
            if not self.pagination or len(links) == 0:
                break
            
            page_num += 1
        
        # Limit the number of documents
        return all_links[:self.max_documents]


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
    
    def fetch_content(self, extractor) -> bool:
        """
        Fetch and extract content for this document.
        
        Args:
            extractor: ContentExtractor to use
            
        Returns:
            bool: True if successful
        """
        try:
            logger.info(f"Fetching content from URL: {self.url}")
            
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
        
        except requests.Timeout:
            logger.error(f"Timeout when fetching {self.url}")
            self.status = "error_timeout"
            return False
        except requests.ConnectionError:
            logger.error(f"Connection error when fetching {self.url}")
            self.status = "error_connection"
            return False
        except requests.RequestException as e:
            logger.error(f"Request error when fetching {self.url}: {str(e)}")
            self.status = "error_request"
            return False
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
            url=data["url"],
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
        
        prompt = f"""Please provide a concise summary of the following government website content.
Focus on the key information, main services offered, and important points for citizens.
Keep your summary informative and factual, between 3-5 sentences.

Title: {document.title}
Source: {document.source_name} - {document.subsource_name}
URL: {document.url}

Content:
{document.content[:8000]}  # Limit content length

Summary:"""

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
        
        


class StorageManager:
    """
    Handles database operations for storing documents and tracking state.
    """
    
    def __init__(self, db_url: str = None):
        """
        Initialize the storage manager.
        
        Args:
            db_url: Database connection string
        """
        self.db_url = db_url or os.getenv("DATABASE_URL")
        self.conn = None
        self.cursor = None
        self._schema_cache = {}  # Cache schema information to reduce DB queries
    
    def connect(self) -> bool:
        """
        Connect to the database.
        
        Returns:
            bool: True if successful
        """
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            # Log connection attempt without exposing credentials
            if self.db_url:
                parts = self.db_url.split('@')
                if len(parts) > 1:
                    prefix = parts[0].split(':')[0]
                    suffix = parts[1]
                    logger.info(f"Connecting to database with URL pattern: {prefix}:***@{suffix}")
                else:
                    # Handle case where URL doesn't contain '@'
                    logger.info("Connecting to database (URL format not recognized)")
            else:
                logger.error("Database URL is None or empty")
                return False
            
            # Convert SQLAlchemy connection string to psycopg2 format if needed
            if self.db_url and self.db_url.startswith('postgresql+psycopg2://'):
                db_url = self.db_url.replace('postgresql+psycopg2://', 'postgresql://')
            else:
                db_url = self.db_url
            
            logger.debug(f"Connecting with URL: {db_url[:10]}...{db_url[-10:] if len(db_url) > 20 else db_url}")
            self.conn = psycopg2.connect(db_url)
            
            if self.conn is None:
                logger.error("Connection object is None after psycopg2.connect call")
                return False
                
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
            # Test connection
            self.cursor.execute("SELECT 1 as test")
            result = self.cursor.fetchone()
            
            if result and result['test'] == 1:
                logger.info("Successfully connected to database")
                return True
            else:
                logger.error("Connection test failed: result does not contain expected 'test' value of 1")
                return False
        
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}", exc_info=True)
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # More detailed error messages for common issues
            error_msg = str(e).lower()
            if "does not exist" in error_msg:
                logger.error("Database does not exist. Please create it first.")
            elif "password authentication failed" in error_msg:
                logger.error("Database authentication failed. Check credentials.")
            elif "could not connect to server" in error_msg:
                logger.error("Could not connect to database server. Check if it's running.")
            
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed")
    
    def setup_tables(self) -> bool:
        """
        Create database tables if they don't exist.
        
        Returns:
            bool: True if successful
        """
        try:
            if not self.conn or self.conn.closed:
                logger.info("Connection not established or closed, attempting to connect...")
                if not self.connect():
                    logger.error("Failed to connect to database in setup_tables")
                    return False
            
            logger.info("Setting up database tables...")
            
            # Log the SQL statements for debugging
            create_sources_sql = """
                CREATE TABLE IF NOT EXISTS govt_sources (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    base_url VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            logger.debug(f"Executing SQL: {create_sources_sql}")
            self.cursor.execute(create_sources_sql)
            logger.info("govt_sources table created or verified")
            
            create_subsources_sql = """
                CREATE TABLE IF NOT EXISTS govt_subsources (
                    id SERIAL PRIMARY KEY,
                    source_id INTEGER REFERENCES govt_sources(id),
                    name VARCHAR(255) NOT NULL,
                    url_pattern VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            logger.debug(f"Executing SQL: {create_subsources_sql}")
            self.cursor.execute(create_subsources_sql)
            logger.info("govt_subsources table created or verified")
            
            create_documents_sql = """
                CREATE TABLE IF NOT EXISTS govt_documents (
                    id SERIAL PRIMARY KEY,
                    url VARCHAR(255) NOT NULL UNIQUE,
                    title VARCHAR(255),
                    source_name VARCHAR(255),
                    subsource_name VARCHAR(255),
                    content TEXT,
                    content_hash VARCHAR(32),
                    summary TEXT,
                    embedding_id VARCHAR(255),
                    status VARCHAR(50) DEFAULT 'new',
                    scrape_time TIMESTAMP,
                    process_time TIMESTAMP,
                    last_checked TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            logger.debug(f"Executing SQL: {create_documents_sql}")
            self.cursor.execute(create_documents_sql)
            logger.info("govt_documents table created or verified")
            
            create_index_sql = """
                CREATE INDEX IF NOT EXISTS idx_govt_documents_content_hash
                ON govt_documents(content_hash)
            """
            logger.debug(f"Executing SQL: {create_index_sql}")
            self.cursor.execute(create_index_sql)
            logger.info("index on content_hash created or verified")
            
            create_status_index_sql = """
                CREATE INDEX IF NOT EXISTS idx_govt_documents_status
                ON govt_documents(status)
            """
            logger.debug(f"Executing SQL: {create_status_index_sql}")
            self.cursor.execute(create_status_index_sql)
            logger.info("index on status created or verified")
            
            self.conn.commit()
            logger.info("All tables created or verified successfully")
            
            # Cache schema information
            self._cache_schema_info()
            
            return True
        
        except Exception as e:
            logger.error(f"Error setting up tables: {str(e)}", exc_info=True)
            logger.error(f"Traceback: {traceback.format_exc()}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def _cache_schema_info(self):
        """Cache schema information to avoid repeated queries."""
        try:
            # Get column information for govt_documents table
            self.cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'govt_documents'
            """)
            
            columns = [row['column_name'] for row in self.cursor.fetchall()]
            self._schema_cache['govt_documents_columns'] = set(columns)
            
            logger.debug(f"Cached schema info: {self._schema_cache}")
        
        except Exception as e:
            logger.error(f"Error caching schema info: {str(e)}", exc_info=True)
            # If we can't cache, just continue with empty cache
            self._schema_cache = {}
    
    def _has_column(self, table: str, column: str) -> bool:
        """Check if a column exists in a table."""
        # Use cached schema if available
        if f"{table}_columns" in self._schema_cache:
            return column in self._schema_cache[f"{table}_columns"]
        
        # Otherwise query the database
        try:
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = %s AND column_name = %s
                )
            """, (table, column))
            
            result = self.cursor.fetchone()
            return result[0] if result else False
        
        except Exception as e:
            logger.error(f"Error checking column existence: {str(e)}", exc_info=True)
            return False
    
    def store_document(self, document) -> int:
        """
        Store a document in the database.
        
        Args:
            document: Document to store
            
        Returns:
            int: Document ID if successful, 0 otherwise
        """
        try:
            # Ensure we have a valid document
            if document is None:
                logger.error("Cannot store None document")
                return 0
                
            # Log document details
            logger.debug(f"Attempting to store document: {document.url}")
            logger.debug(f"Document title: {document.title}")
            logger.debug(f"Document status: {document.status}")
            logger.debug(f"Document has content: {document.content is not None}")
            logger.debug(f"Document content length: {len(document.content) if document.content else 0}")
                
            # Check database connection
            if not self.conn or self.conn.closed:
                logger.info("Database connection not established or closed, attempting to connect...")
                if not self.connect():
                    logger.error("Failed to connect to database in store_document")
                    return 0
            
            # Check for duplicate content if hash is available
            if document.content_hash and document.content and self._has_column('govt_documents', 'content_hash'):
                check_duplicate_sql = "SELECT id, url FROM govt_documents WHERE content_hash = %s AND url != %s"
                logger.debug(f"Executing SQL: {check_duplicate_sql} with params: [{document.content_hash}, {document.url}]")
                self.cursor.execute(check_duplicate_sql, (document.content_hash, document.url))
                duplicate = self.cursor.fetchone()
                if duplicate:
                    logger.info(f"Content duplicate found: {document.url} has same content as {duplicate['url']}")
            
            # Check if document with this URL already exists
            check_exists_sql = "SELECT id FROM govt_documents WHERE url = %s"
            logger.debug(f"Executing SQL: {check_exists_sql} with param: [{document.url}]")
            self.cursor.execute(check_exists_sql, (document.url,))
            
            result = self.cursor.fetchone()
            
            # Prepare values for database operations
            values = {
                "url": document.url,
                "title": document.title,
                "source_name": document.source_name,
                "subsource_name": document.subsource_name,
                "content": document.content,
                "content_hash": document.content_hash,
                "summary": document.summary,
                "embedding_id": document.embedding_id,
                "status": document.status,
                "scrape_time": document.scrape_time,
                "process_time": document.process_time,
                "last_checked": document.last_checked
            }
            
            # Log the values being stored
            safe_values = {k: (v[:100] + '...' if isinstance(v, str) and len(v) > 100 else v) for k, v in values.items()}
            logger.debug(f"Document values to store: {safe_values}")
            
            if result:
                # Update existing document
                doc_id = result['id']
                logger.info(f"Updating existing document with ID: {doc_id}")
                
                # Build update SQL based on existing columns
                update_parts = []
                update_values = []
                
                for col, val in values.items():
                    if col != 'url' and self._has_column('govt_documents', col):
                        update_parts.append(f"{col} = %s")
                        update_values.append(val)
                
                # Add updated_at timestamp
                update_parts.append("updated_at = CURRENT_TIMESTAMP")
                
                # Add the document ID at the end
                update_values.append(doc_id)
                
                # Execute update
                update_sql = f"""
                    UPDATE govt_documents 
                    SET {', '.join(update_parts)}
                    WHERE id = %s
                """
                
                logger.debug(f"Executing SQL: {update_sql}")
                logger.debug(f"SQL Parameters: {update_values}")
                
                self.cursor.execute(update_sql, update_values)
                self.conn.commit()
                document.doc_id = doc_id
                logger.info(f"Successfully updated document with ID: {doc_id}")
                
                return doc_id
            
            else:
                # Insert new document
                logger.info(f"Inserting new document: {document.url}")
                
                # Filter columns that exist in the table
                insert_cols = []
                insert_vals = []
                placeholders = []
                
                for col, val in values.items():
                    if self._has_column('govt_documents', col):
                        insert_cols.append(col)
                        insert_vals.append(val)
                        placeholders.append("%s")
                
                # Execute insert
                insert_sql = f"""
                    INSERT INTO govt_documents 
                    ({', '.join(insert_cols)})
                    VALUES ({', '.join(placeholders)})
                    RETURNING id
                """
                
                logger.debug(f"Executing SQL: {insert_sql}")
                logger.debug(f"SQL Parameters: {[val[:100] + '...' if isinstance(val, str) and len(val) > 100 else val for val in insert_vals]}")
                
                self.cursor.execute(insert_sql, insert_vals)
                result = self.cursor.fetchone()
                
                if not result:
                    logger.error("Insert succeeded but no ID was returned")
                    self.conn.rollback()
                    return 0
                    
                doc_id = result['id']
                self.conn.commit()
                document.doc_id = doc_id
                logger.info(f"Successfully inserted document with ID: {doc_id}")
                
                return doc_id
        
        except Exception as e:
            logger.error(f"Error storing document: {str(e)}", exc_info=True)
            logger.error(f"Traceback: {traceback.format_exc()}")
            if self.conn:
                self.conn.rollback()
            return 0
    
    def get_document_by_url(self, url: str) -> Optional[Any]:
        """Get a document by URL."""
        try:
            if not self.conn or self.conn.closed:
                if not self.connect():
                    return None
            
            query = "SELECT * FROM govt_documents WHERE url = %s"
            logger.debug(f"Executing SQL: {query} with param: [{url}]")
            self.cursor.execute(query, (url,))
            
            result = self.cursor.fetchone()
            
            if result:
                from core import Document
                return Document.from_dict(result)
            
            return None
        
        except Exception as e:
            logger.error(f"Error fetching document: {str(e)}", exc_info=True)
            return None
    
    def get_unprocessed_documents(self, limit: int = 100) -> List[Any]:
        """Get documents that need processing."""
        try:
            if not self.conn or self.conn.closed:
                if not self.connect():
                    return []
            
            query = """
                SELECT * FROM govt_documents
                WHERE status = 'scraped'
                ORDER BY scrape_time ASC
                LIMIT %s
            """
            logger.debug(f"Executing SQL: {query} with param: [{limit}]")
            self.cursor.execute(query, (limit,))
            
            results = self.cursor.fetchall()
            
            from core import Document
            return [Document.from_dict(row) for row in results]
        
        except Exception as e:
            logger.error(f"Error fetching unprocessed documents: {str(e)}", exc_info=True)
            return []
        

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