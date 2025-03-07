#!/usr/bin/env python3
"""
Search utility for retrieving information from the government data pipeline.
Performs semantic search against the Pinecone vector database.
"""

import os
import sys
import argparse
import logging
from typing import List, Dict, Any
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class SearchEngine:
    """
    Search engine for government data using vector similarity.
    """
    
    def __init__(
        self,
        openai_api_key: str = None,
        pinecone_api_key: str = None,
        pinecone_index: str = "govt-scrape-index",
        pinecone_namespace: str = "govt-content"
    ):
        """
        Initialize the search engine.
        
        Args:
            openai_api_key: API key for OpenAI (embeddings)
            pinecone_api_key: API key for Pinecone
            pinecone_index: Pinecone index name
            pinecone_namespace: Pinecone namespace
        """
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        self.pinecone_api_key = pinecone_api_key or os.getenv("PINECONE_API_KEY")
        self.pinecone_index = pinecone_index
        self.pinecone_namespace = pinecone_namespace
        
        # Initialize models lazily
        self._embedding_model = None
        self._pinecone = None
        self._index = None
    
    def _init_embedding_model(self):
        """Initialize embedding model if not already done."""
        if self._embedding_model is None:
            from langchain_openai import OpenAIEmbeddings
            
            self._embedding_model = OpenAIEmbeddings(
                model="text-embedding-3-small",
                openai_api_key=self.openai_api_key
            )
    
    def _init_pinecone(self):
        """Initialize Pinecone if not already done."""
        if self._pinecone is None:
            try:
                import pinecone  # Using the new package name
                
                self._pinecone = pinecone.Pinecone(api_key=self.pinecone_api_key)
                self._index = self._pinecone.Index(name=self.pinecone_index)
                logger.info(f"Pinecone initialized with index: {self.pinecone_index}")
            except ImportError:
                logger.error("Failed to import pinecone. Make sure you have the 'pinecone' package installed (not pinecone-client).")
                raise
    
    def create_query_embedding(self, query: str) -> List[float]:
        """
        Create embedding for a search query.
        
        Args:
            query: Search query text
            
        Returns:
            Embedding vector
        """
        self._init_embedding_model()
        
        embedding = self._embedding_model.embed_query(query)
        return embedding
    
    def search(
        self, 
        query: str, 
        top_k: int = 5, 
        filter: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for similar documents.
        
        Args:
            query: Search query
            top_k: Number of results to return
            filter: Optional filter for metadata
            
        Returns:
            List of search results with metadata
        """
        self._init_embedding_model()
        self._init_pinecone()
        
        # Create query embedding
        query_embedding = self.create_query_embedding(query)
        
        # Search in Pinecone
        results = self._index.query(
            vector=query_embedding,
            namespace=self.pinecone_namespace,
            top_k=top_k,
            include_metadata=True,
            filter=filter
        )
        
        # Format results
        formatted_results = []
        for match in results.get("matches", []):
            formatted_results.append({
                "score": match.get("score", 0),
                "id": match.get("id", ""),
                "title": match.get("metadata", {}).get("title", ""),
                "url": match.get("metadata", {}).get("url", ""),
                "source": match.get("metadata", {}).get("source", ""),
                "subsource": match.get("metadata", {}).get("subsource", ""),
                "summary": match.get("metadata", {}).get("summary", "")
            })
        
        return formatted_results


def main():
    """Main entry point for search utility."""
    parser = argparse.ArgumentParser(description='Government data search engine')
    parser.add_argument('query', help='Search query')
    parser.add_argument('--top-k', type=int, default=3, help='Number of results to return')
    parser.add_argument('--source', help='Filter by source name')
    parser.add_argument('--index', default=os.getenv('PINECONE_INDEX_NAME', 'govt-scrape-index'), help='Pinecone index name')
    parser.add_argument('--namespace', default=os.getenv('PINECONE_NAMESPACE', 'govt-content'), help='Pinecone namespace')
    
    args = parser.parse_args()
    
    # Check if API keys are set
    if not os.getenv("OPENAI_API_KEY"):
        logger.error("OPENAI_API_KEY environment variable not set")
        sys.exit(1)
    
    if not os.getenv("PINECONE_API_KEY"):
        logger.error("PINECONE_API_KEY environment variable not set")
        sys.exit(1)
    
    # Initialize search engine
    search_engine = SearchEngine(
        pinecone_index=args.index,
        pinecone_namespace=args.namespace
    )
    
    # Build filter if needed
    filter_dict = {}
    if args.source:
        filter_dict["source"] = {"$eq": args.source}
    
    # Perform search
    results = search_engine.search(
        query=args.query,
        top_k=args.top_k,
        filter=filter_dict if filter_dict else None
    )
    
    # Display results
    print(f"\nSearch Results for: '{args.query}'")
    print("=" * 50)
    
    if not results:
        print("No results found.")
    
    for i, result in enumerate(results, 1):
        print(f"\n{i}. {result['title']} (Score: {result['score']:.2f})")
        print(f"   Source: {result['source']} - {result['subsource']}")
        print(f"   URL: {result['url']}")
        print(f"   Summary: {result['summary']}")
    
    print("\n" + "=" * 50)


if __name__ == "__main__":
    main()