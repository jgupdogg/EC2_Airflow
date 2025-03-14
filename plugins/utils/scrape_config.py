# plugins/utils/scrape_config.py
import json
from typing import List, Dict, Any

class Extractor:
    """Represents document extraction configuration"""
    def __init__(self, data: Dict[str, Any]):
        self.type = data.get('type', 'css')
        # Store document link selectors based on type
        if self.type == 'css':
            self.document_links_css = data.get('document_links')
        else:
            self.document_links_xpath = data.get('document_links')
        self.content = data.get('content')
        self.pagination = data.get('pagination')

class Subsource:
    """Represents a subsource/page within a source"""
    def __init__(self, data: Dict[str, Any], base_url: str):
        self.name = data.get('name', 'Unnamed')
        self.url_pattern = data.get('url_pattern', '')
        self.max_pages = data.get('max_pages', 1)
        self.max_documents = data.get('max_documents', 10)
        self.use_javascript = data.get('use_javascript', False)
        self.base_url = base_url
        self.extractor = Extractor(data.get('extraction', {}))
    
    def get_full_url(self) -> str:
        """Get complete URL including base URL"""
        if self.url_pattern.startswith('http'):
            return self.url_pattern
        return f"{self.base_url.rstrip('/')}/{self.url_pattern.lstrip('/')}"

class Source:
    """Represents a source website"""
    def __init__(self, data: Dict[str, Any]):
        self.name = data.get('name', 'Unnamed Source')
        self.base_url = data.get('url', '')
        self.pages = data.get('pages', [])
    
    def get_subsources(self) -> List[Subsource]:
        """Get list of subsources from this source"""
        return [Subsource(page, self.base_url) for page in self.pages]

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
        
        # Convert JSON dictionaries to proper objects
        self.sources = [Source(source) for source in self.config.get('sources', [])]
    
    def get_sources(self) -> List[Source]:
        """Get all sources from the configuration."""
        return self.sources