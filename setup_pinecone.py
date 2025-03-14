#!/usr/bin/env python3
"""
Setup script for creating a Pinecone index for government website summaries.
Includes dotenv support for loading environment variables from .env file.
"""

import os
import sys
import argparse
import logging
import subprocess

# Import dotenv for loading environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()  # Load environment variables from .env file
except ImportError:
    print("python-dotenv not installed. Installing now...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "python-dotenv"])
    from dotenv import load_dotenv
    load_dotenv()

# Check if we need to fix Pinecone installation
try:
    subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", "pinecone-client"])
    print("Removed old pinecone-client package")
except:
    print("pinecone-client not found, continuing...")

try:
    import pinecone
    print(f"Pinecone version: {pinecone.__version__}")
except ImportError:
    print("Installing pinecone package...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pinecone-client"])

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_pinecone_index(
    api_key: str,
    index_name: str = "govt-scrape-index",
    dimension: int = 1536,  # Default for OpenAI text-embedding-3-small
    metric: str = "cosine"
):
    """
    Create a Pinecone index for storing government website summary embeddings.
    
    Args:
        api_key: Pinecone API key
        index_name: Name of the index to create
        dimension: Dimensionality of the embeddings (depends on the model used)
        metric: Distance metric for similarity search (cosine, euclidean, dotproduct)
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Import Pinecone after ensuring it's properly installed
        from pinecone import Pinecone
        
        # Initialize Pinecone
        pc = Pinecone(api_key=api_key)
        
        # Check if index already exists
        existing_indexes = [idx["name"] for idx in pc.list_indexes()]
        
        if index_name in existing_indexes:
            logger.info(f"Index '{index_name}' already exists")
            return True
        
        # Create index - using updated API based on Pinecone documentation
        pc.create_index(
            name=index_name,
            dimension=dimension,
            metric=metric,
            spec={
                "serverless": {
                    "cloud": "aws",
                    "region": "us-east-1"
                }
            }
        )
        
        logger.info(f"Successfully created index '{index_name}' with {dimension} dimensions")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create Pinecone index: {e}")
        return False

def main():
    """Main entry point for script."""
    parser = argparse.ArgumentParser(description='Set up Pinecone index for government website summaries')
    parser.add_argument('--api-key', default=os.getenv('PINECONE_API_KEY'), help='Pinecone API key')
    parser.add_argument('--index-name', default="govt-scrape-index", help='Name of the index to create')
    parser.add_argument('--dimension', type=int, default=1536, help='Dimensionality of the embeddings')
    parser.add_argument('--metric', default="cosine", choices=["cosine", "euclidean", "dotproduct"], help='Distance metric')
    
    args = parser.parse_args()
    
    if not args.api_key:
        logger.error("Pinecone API key is required. Set it in your .env file as PINECONE_API_KEY or provide with --api-key")
        sys.exit(1)
    
    if create_pinecone_index(args.api_key, args.index_name, args.dimension, args.metric):
        logger.info("Setup complete.")
    else:
        logger.error("Setup failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()