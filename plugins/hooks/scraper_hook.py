# plugins/hooks/scraper_hook.py
from airflow.hooks.base import BaseHook
from contextlib import contextmanager
import logging

class ScraperHook(BaseHook):
    """
    Hook for interacting with AirflowWebScraper.
    
    :param scraper_conn_id: Connection ID for web scraper (optional)
    :type scraper_conn_id: str
    """
    
    def __init__(self, scraper_conn_id=None):
        super().__init__()
        self.scraper_conn_id = scraper_conn_id
        self.scraper = None
        self.logger = logging.getLogger(__name__)
    
    def get_conn(self, headless=True, use_virtual_display=True, display_visible=False, page_load_timeout=30):
        """
        Returns an AirflowWebScraper instance.
        
        :param headless: Run browser in headless mode
        :type headless: bool
        :param use_virtual_display: Use virtual display for X server
        :type use_virtual_display: bool
        :param display_visible: Make virtual display visible
        :type display_visible: bool
        :param page_load_timeout: Maximum wait time for page loads in seconds
        :type page_load_timeout: int
        :return: AirflowWebScraper instance
        """
        from utils.airflow_web_scraper import AirflowWebScraper
        
        if self.scraper is None:
            self.scraper = AirflowWebScraper(
                headless=headless,
                use_virtual_display=use_virtual_display,
                display_visible=display_visible,
                page_load_timeout=page_load_timeout
            )
            self.logger.info("Initialized AirflowWebScraper")
        
        return self.scraper
    
    @contextmanager
    def get_scraper(self, **kwargs):
        """
        Context manager for AirflowWebScraper.
        
        Usage:
            with hook.get_scraper() as scraper:
                scraper.navigate(url)
        """
        scraper = None
        try:
            scraper = self.get_conn(**kwargs)
            yield scraper
        finally:
            # Don't quit the scraper here, as we're managing it through the hook
            pass
    
    def navigate(self, url, wait_time=None, **kwargs):
        """
        Navigate to a URL.
        
        :param url: URL to navigate to
        :type url: str
        :param wait_time: Optional specific wait time
        :type wait_time: float
        :return: bool: True if navigation succeeded
        """
        scraper = self.get_conn(**kwargs)
        return scraper.navigate(url, wait_time)
    
    def get_page_source(self, **kwargs):
        """
        Get the full HTML source of the current page.
        
        :return: String containing page HTML
        """
        scraper = self.get_conn(**kwargs)
        return scraper.get_page_source()
    
    def find_elements(self, locator, timeout=10, **kwargs):
        """
        Find all elements matching the locator.
        
        :param locator: Tuple of (By.TYPE, "locator_string")
        :type locator: tuple
        :param timeout: Maximum time to wait for elements
        :type timeout: int
        :return: List of WebElements
        """
        scraper = self.get_conn(**kwargs)
        return scraper.find_elements(locator, timeout)
    
    def extract_document_links(self, url, extractor_type, selector, timeout=10, **kwargs):
        """
        Extract document links from a page.
        
        :param url: URL to navigate to
        :type url: str
        :param extractor_type: 'css' or 'xpath'
        :type extractor_type: str
        :param selector: CSS selector or XPath expression
        :type selector: str
        :param timeout: Timeout for finding elements
        :type timeout: int
        :return: List of document links (url and title)
        """
        from selenium.webdriver.common.by import By
        
        scraper = self.get_conn(**kwargs)
        
        # Navigate to the URL
        if not scraper.navigate(url):
            self.logger.error(f"Failed to navigate to {url}")
            return []
        
        # Determine locator type
        locator_type = By.CSS_SELECTOR if extractor_type.lower() == 'css' else By.XPATH
        locator = (locator_type, selector)
        
        # Find elements
        elements = scraper.find_elements(locator, timeout)
        
        # Extract links
        links = []
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
                self.logger.error(f"Error extracting link: {e}")
                continue
        
        return links
    
    # Add this to plugins/hooks/scraper_hook.py
    def fetch_document_content(self, document, extractor, use_javascript=True, **kwargs):
        """
        Fetch and extract content for a document.
        
        :param document: Document object to fetch content for
        :type document: Document
        :param extractor: ContentExtractor to use
        :type extractor: ContentExtractor
        :param use_javascript: Use JavaScript-enabled browser for rendering
        :type use_javascript: bool
        :return: bool: True if content was successfully fetched
        """
        try:
            self.logger.info(f"Fetching content from URL: {document.url}")
            
            # Get browser instance
            scraper = self.get_conn(headless=True, use_virtual_display=True, **kwargs)
            
            # Navigate to the URL
            if not scraper.navigate(document.url):
                self.logger.error(f"Failed to navigate to {document.url}")
                document.status = "error_navigation"
                return False
            
            # Get page content
            html = scraper.get_page_source()
            
            if not html:
                self.logger.error(f"No HTML content retrieved from {document.url}")
                document.status = "error_no_html"
                return False
            
            # Extract content using BeautifulSoup as a fallback
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            
            # Try to find main content with common selectors
            main_content = soup.find('main') or soup.find('article') or soup.find('div', {'id': 'content'})
            
            if main_content:
                document.content = main_content.get_text(' ', strip=True)
            else:
                # Use body as fallback
                body = soup.find('body')
                document.content = body.get_text(' ', strip=True) if body else ''
            
            # Check if content was extracted
            if not document.content or len(document.content.strip()) == 0:
                self.logger.warning(f"No content extracted from {document.url}")
                document.status = "error_no_content"
                return False
            
            # Update document metadata
            from datetime import datetime
            import hashlib
            
            document.scrape_time = datetime.now()
            document.last_checked = datetime.now()
            document.content_hash = hashlib.md5(document.content.encode('utf-8')).hexdigest()
            document.status = "scraped"
            
            self.logger.info(f"Successfully fetched content from {document.url}")
            return True
        
        except Exception as e:
            self.logger.error(f"Error fetching content for {document.url}: {str(e)}", exc_info=True)
            document.status = "error_exception"
            return False
    
    def close(self):
        """Close the scraper."""
        if self.scraper:
            self.scraper.quit()
            self.scraper = None
            self.logger.info("AirflowWebScraper closed")
    
    def __del__(self):
        """Ensure scraper is closed when hook is garbage collected."""
        self.close()