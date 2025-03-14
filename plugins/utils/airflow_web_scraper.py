"""
Web scraper utility optimized for Airflow environments.
Provides a lightweight interface with virtual display support.
"""

import logging
import os
import random
import time
from typing import Dict, List, Optional, Union, Any

from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException, 
    ElementNotInteractableException, 
    WebDriverException,
    TimeoutException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from pyvirtualdisplay import Display
from seleniumbase import Driver

logger = logging.getLogger(__name__)

# Default paths - can be overridden via environment variables
DEFAULT_CHROME_PATH = os.getenv("CHROME_PATH", "/usr/bin/google-chrome")
DEFAULT_CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH", "/usr/local/bin/chromedriver")
DEFAULT_USER_DATA_DIR = os.getenv("CHROME_PROFILE_PATH", "/tmp/chrome_profile")

class AirflowWebScraper:
    """
    A lightweight web scraper optimized for Airflow tasks.
    Features stealth capabilities and virtual display support.
    
    Designed to be used within Airflow tasks with minimal resource footprint.
    """
    
    def __init__(
        self,
        headless: bool = True,
        display_visible: bool = False,
        use_virtual_display: bool = True,
        user_agent: Optional[str] = None,
        use_stealth: bool = True,
        chrome_path: Optional[str] = None,
        chromedriver_path: Optional[str] = None,
        page_load_timeout: int = 30,
        display_size: tuple = (1280, 1024)  # Smaller default size to save resources
    ):
        """
        Initialize the web scraper with Airflow-friendly defaults.
        
        Args:
            headless: Run browser in headless mode (default True for AWS environments)
            display_visible: Make virtual display visible (only relevant if use_virtual_display=True)
            use_virtual_display: Use pyvirtualdisplay for X server
            user_agent: Custom user agent string
            use_stealth: Apply selenium-stealth to avoid detection
            chrome_path: Path to Chrome binary
            chromedriver_path: Path to chromedriver binary
            page_load_timeout: Maximum wait time for page loads in seconds
            display_size: Virtual display resolution
        """
        self.chrome_path = chrome_path or DEFAULT_CHROME_PATH
        self.chromedriver_path = chromedriver_path or DEFAULT_CHROMEDRIVER_PATH
        self.use_virtual_display = use_virtual_display
        self.display_visible = display_visible
        self.headless = headless
        self.page_load_timeout = page_load_timeout
        
        logger.info(
            f"Initializing AirflowWebScraper with: headless={headless}, "
            f"use_virtual_display={use_virtual_display}, display_visible={display_visible}"
        )
        
        # Initialize virtual display if requested
        self.display = None
        if use_virtual_display:
            try:
                self.display = Display(visible=1 if display_visible else 0, size=display_size)
                self.display.start()
                logger.info(f"Virtual display started with size {display_size}")
            except Exception as e:
                logger.error(f"Failed to start virtual display: {e}")
                raise
        
        # Initialize the browser driver
        try:
            # Configure driver options
            driver_options = {
                "uc": True,  # Use undetected-chromedriver mode
                "headless": headless
            }
            
            # Add custom user agent if provided
            if user_agent:
                driver_options["user_agent"] = user_agent
            
            # Initialize the driver with seleniumbase for undetected chrome
            self.driver = Driver(**driver_options)
            self.driver.set_page_load_timeout(self.page_load_timeout)
            
            logger.info("Browser driver initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize browser driver: {e}")
            if self.display:
                self.display.stop()
            raise
    
    def navigate(self, url: str, wait_time: Optional[float] = None) -> bool:
        """
        Navigate to a URL with automatic waiting and randomized delay.
        
        Args:
            url: The URL to navigate to
            wait_time: Optional specific wait time (otherwise uses random delay)
            
        Returns:
            bool: True if navigation succeeded, False otherwise
        """
        try:
            logger.info(f"Navigating to: {url}")
            self.driver.get(url)
            
            # Add a small random delay to appear more human-like
            if wait_time is None:
                wait_time = random.uniform(1.0, 2.0)  # Shorter delay for Airflow tasks
                
            time.sleep(wait_time)
            return True
            
        except TimeoutException:
            logger.warning(f"Timeout while loading {url}")
            return False
        except WebDriverException as e:
            logger.error(f"Error navigating to {url}: {e}")
            return False
    
    def find_element(self, locator: tuple, timeout: int = 10):
        """
        Find an element with explicit wait.
        
        Args:
            locator: Tuple of (By.TYPE, "locator_string")
            timeout: Maximum time to wait for element
            
        Returns:
            WebElement or None if not found
        """
        try:
            return WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located(locator)
            )
        except (TimeoutException, NoSuchElementException):
            logger.warning(f"Element not found: {locator}")
            return None
    
    def find_elements(self, locator: tuple, timeout: int = 10) -> List:
        """
        Find all elements matching the locator with explicit wait.
        
        Args:
            locator: Tuple of (By.TYPE, "locator_string")
            timeout: Maximum time to wait for elements
            
        Returns:
            List of WebElements or empty list if none found
        """
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located(locator)
            )
            return self.driver.find_elements(*locator)
        except (TimeoutException, NoSuchElementException):
            logger.warning(f"No elements found: {locator}")
            return []
    
    def get_text(self, locator: tuple, timeout: int = 10) -> Optional[str]:
        """
        Get text from an element.
        
        Args:
            locator: Tuple of (By.TYPE, "locator_string")
            timeout: Maximum time to wait for element
            
        Returns:
            String content or None if element not found
        """
        element = self.find_element(locator, timeout)
        if element:
            return element.text
        return None
    
    def get_attribute(self, locator: tuple, attribute: str, timeout: int = 10) -> Optional[str]:
        """
        Get attribute value from an element.
        
        Args:
            locator: Tuple of (By.TYPE, "locator_string")
            attribute: Name of the attribute to retrieve
            timeout: Maximum time to wait for element
            
        Returns:
            Attribute value or None if element not found
        """
        element = self.find_element(locator, timeout)
        if element:
            return element.get_attribute(attribute)
        return None
    
    def get_page_source(self) -> str:
        """
        Get the full HTML source of the current page.
        
        Returns:
            String containing page HTML
        """
        return self.driver.page_source
    
    def get_title(self) -> str:
        """
        Get the title of the current page.
        
        Returns:
            String containing page title
        """
        return self.driver.title
    
    def get_current_url(self) -> str:
        """
        Get the URL of the current page.
        
        Returns:
            String containing current URL
        """
        return self.driver.current_url
    
    def scroll_to_bottom(self):
        """Scroll to the bottom of the page."""
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.5)  # Brief pause to allow content to load
    
    def take_screenshot(self, filename: str) -> bool:
        """
        Save a screenshot of the current browser window.
        
        Args:
            filename: Path where the screenshot should be saved
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.driver.save_screenshot(filename)
            logger.info(f"Screenshot saved to {filename}")
            return True
        except Exception as e:
            logger.error(f"Failed to save screenshot: {e}")
            return False
    
    def quit(self):
        """
        Clean up resources by quitting the driver and stopping the display.
        """
        try:
            if hasattr(self, 'driver') and self.driver:
                self.driver.quit()
                logger.info("Browser driver closed")
        except Exception as e:
            logger.error(f"Error closing browser driver: {e}")
        
        try:
            if self.display:
                self.display.stop()
                logger.info("Virtual display stopped")
        except Exception as e:
            logger.error(f"Error stopping virtual display: {e}")
    
    def __enter__(self):
        """Support using the scraper with 'with' statements."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure resources are cleaned up when exiting a 'with' block."""
        self.quit()