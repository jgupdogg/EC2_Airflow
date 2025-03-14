#!/usr/bin/env python3
"""
Standalone script to scrape government websites and store data in PostgreSQL.
Can be run directly or used as a module within an Airflow task.
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional
import psycopg2
from bs4 import BeautifulSoup

# Add the parent directory to sys.path to import the scraper module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from plugins.utils.web_scraper import AirflowWebScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection string from environment variable or default
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://postgres:St0ck!adePG@localhost:5432/pipeline_db')


def setup_database(conn_string: str) -> bool:
    """
    Set up the database table for storing scraped data.
    
    Args:
        conn_string: PostgreSQL connection string
        
    Returns:
        bool: True if setup successful, False otherwise
    """
    try:
        # Convert SQLAlchemy connection string to psycopg2 format if needed
        if conn_string.startswith('postgresql+psycopg2://'):
            conn_string = conn_string.replace('postgresql+psycopg2://', 'postgresql://')
        
        # Connect to database
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS govt_scrape (
                id SERIAL PRIMARY KEY,
                url VARCHAR(255) NOT NULL,
                title VARCHAR(255),
                snippet TEXT,
                scrape_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Database setup completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return False


def scrape_website(url: str, scraper: AirflowWebScraper) -> Optional[Dict[str, Any]]:
    """
    Scrape a website and return the extracted data.
    
    Args:
        url: Website URL to scrape
        scraper: Initialized web scraper instance
        
    Returns:
        Dict with extracted data or None on failure
    """
    try:
        # Navigate to the website
        if not scraper.navigate(url):
            logger.error(f"Failed to navigate to {url}")
            return None
        
        # Get the page title
        title = scraper.get_title()
        
        # Get the page HTML and parse with BeautifulSoup
        html = scraper.get_page_source()
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract a snippet (first paragraph or relevant content)
        snippet = ""
        for p in soup.find_all('p'):
            if p.text and len(p.text.strip()) > 50:  # Find a substantial paragraph
                snippet = p.text.strip()
                break
        
        if not snippet and soup.main:
            # Try to get text from main content area
            snippet = soup.main.get_text(' ', strip=True)[:500]  # First 500 chars
        
        # Return extracted data
        return {
            'url': url,
            'title': title,
            'snippet': snippet,
            'scrape_time': datetime.now()
        }
        
    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return None


def store_data(data: Dict[str, Any], conn_string: str) -> bool:
    """
    Store scraped data in the database.
    
    Args:
        data: Dictionary of scraped data
        conn_string: PostgreSQL connection string
        
    Returns:
        bool: True if data stored successfully, False otherwise
    """
    try:
        # Convert SQLAlchemy connection string to psycopg2 format if needed
        if conn_string.startswith('postgresql+psycopg2://'):
            conn_string = conn_string.replace('postgresql+psycopg2://', 'postgresql://')
        
        # Connect to database
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        
        # Insert data
        cursor.execute(
            """
            INSERT INTO govt_scrape (url, title, snippet, scrape_time)
            VALUES (%s, %s, %s, %s)
            """,
            (data['url'], data['title'], data['snippet'], data['scrape_time'])
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Data for {data['url']} stored successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to store data: {e}")
        return False


def scrape_and_store(urls: List[str], db_conn_string: str) -> List[Dict[str, Any]]:
    """
    Scrape multiple websites and store data in PostgreSQL.
    
    Args:
        urls: List of website URLs to scrape
        db_conn_string: PostgreSQL connection string
        
    Returns:
        List of results dictionaries
    """
    results = []
    
    # Ensure database is set up
    if not setup_database(db_conn_string):
        logger.error("Database setup failed, aborting scraping")
        return results
    
    # Initialize scraper
    with AirflowWebScraper(
        headless=True,
        use_virtual_display=True,
        display_visible=False,
        page_load_timeout=30
    ) as scraper:
        # Scrape each website
        for url in urls:
            try:
                data = scrape_website(url, scraper)
                if data:
                    if store_data(data, db_conn_string):
                        results.append({
                            'url': url,
                            'title': data['title'],
                            'success': True
                        })
                    else:
                        results.append({
                            'url': url,
                            'success': False,
                            'error': 'Failed to store data'
                        })
                else:
                    results.append({
                        'url': url,
                        'success': False,
                        'error': 'Failed to scrape website'
                    })
            except Exception as e:
                logger.error(f"Error processing {url}: {e}")
                results.append({
                    'url': url,
                    'success': False,
                    'error': str(e)
                })
    
    return results


def main():
    """Main entry point for standalone script."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Scrape government websites and store data in PostgreSQL')
    parser.add_argument('--urls', nargs='+', default=[
        "https://www.usa.gov",
        "https://www.whitehouse.gov",
        "https://www.irs.gov"
    ], help='List of URLs to scrape')
    parser.add_argument('--db-url', default=DATABASE_URL, help='Database connection string')
    parser.add_argument('--headless', action='store_true', default=True, help='Run browser in headless mode')
    parser.add_argument('--display-visible', action='store_true', default=False, help='Make virtual display visible')
    
    args = parser.parse_args()
    
    # Run scraper
    results = scrape_and_store(args.urls, args.db_url)
    
    # Print results
    print("\nScraping Results:")
    print("----------------")
    for result in results:
        status = "✓ Success" if result.get('success', False) else f"✗ Failed: {result.get('error', 'Unknown error')}"
        print(f"{result['url']}: {status}")
    
    # Print summary
    success_count = sum(1 for r in results if r.get('success', False))
    print(f"\nSummary: {success_count}/{len(results)} websites scraped successfully")


if __name__ == "__main__":
    main()