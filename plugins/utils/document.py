## 6. Document Class in Utils

# plugins/utils/document.py
import hashlib
from datetime import datetime
from typing import Optional

class Document:
    """
    Simplified Document class for Airflow operators.
    Represents a document scraped from a government website.
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
        self.title = title or "Untitled Document"
        self.source_name = source_name
        self.subsource_name = subsource_name
        self.content = None
        self.content_hash = None
        self.summary = None
        self.embedding_id = None
        self.status = "new"
        self.scrape_time = None
        self.process_time = None
        self.last_checked = None
        self.doc_id = None
        self.use_javascript = False
    
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
            "id": self.doc_id
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
        
        # Assign properties
        doc.content = data.get("content")
        doc.content_hash = data.get("content_hash")
        doc.summary = data.get("summary")
        doc.embedding_id = data.get("embedding_id")
        doc.status = data.get("status", "new")
        doc.use_javascript = data.get("use_javascript", False)
        
        # Parse date fields
        try:
            if data.get("scrape_time") and isinstance(data.get("scrape_time"), str):
                doc.scrape_time = datetime.fromisoformat(data["scrape_time"])
            elif data.get("scrape_time"):
                doc.scrape_time = data["scrape_time"]
        except ValueError:
            pass
        
        try:
            if data.get("process_time") and isinstance(data.get("process_time"), str):
                doc.process_time = datetime.fromisoformat(data["process_time"])
            elif data.get("process_time"):
                doc.process_time = data["process_time"]
        except ValueError:
            pass
            
        try:
            if data.get("last_checked") and isinstance(data.get("last_checked"), str):
                doc.last_checked = datetime.fromisoformat(data["last_checked"])
            elif data.get("last_checked"):
                doc.last_checked = data["last_checked"]
        except ValueError:
            pass
        
        # Set ID if available
        doc.doc_id = data.get("id")
        
        return doc
