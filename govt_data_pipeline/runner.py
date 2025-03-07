#!/usr/bin/env python3
"""
Main script for running the government data pipeline.
Handles the full workflow from scraping to processing to storage.
"""

import os
import sys
import time
import argparse
import logging
import traceback
from typing import List, Dict, Any
import concurrent.futures
from datetime import datetime
from dotenv import load_dotenv

# Import setup_logging from the new module
from setup_logging import setup_logging, generate_log_file_name

# Import core components
from core import (
    ScrapeConfig, 
    Document, 
    Processor, 
    StorageManager,
    ContentExtractor
)

# Get logger (will be properly configured after setup_logging is called)
logger = logging.getLogger(__name__)

# Load environment variables
# Look for .env in parent directory and current directory
dotenv_paths = [
    os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'),
    os.path.join(os.path.dirname(__file__), '.env')
]

for dotenv_path in dotenv_paths:
    if os.path.exists(dotenv_path):
        logger.debug(f"Loading environment variables from {dotenv_path}")
        load_dotenv(dotenv_path)
        break

def scrape_documents(config_path: str, db_manager: StorageManager) -> List[Document]:
    """
    Scrape documents based on configuration.
    
    Args:
        config_path: Path to configuration file
        db_manager: Storage manager for database operations
        
    Returns:
        List of scraped documents
    """
    try:
        # Load configuration
        logger.info(f"Loading configuration from {config_path}")
        config = ScrapeConfig(config_path)
        
        # Set up database tables
        logger.info("Setting up database tables")
        if not db_manager.setup_tables():
            logger.error("Failed to set up database tables, aborting scrape")
            return []
        
        scraped_docs = []
        
        # Process each source
        sources = config.get_sources()
        logger.info(f"Processing {len(sources)} sources")
        
        for source_idx, source in enumerate(sources, 1):
            logger.info(f"Processing source {source_idx}/{len(sources)}: {source.name}")
            
            # Process each subsource
            subsources = source.get_subsources()
            logger.info(f"Found {len(subsources)} subsources for {source.name}")
            
            for subsource_idx, subsource in enumerate(subsources, 1):
                logger.info(f"Processing subsource {subsource_idx}/{len(subsources)}: {subsource.name}")
                
                try:
                    # Get document links
                    logger.info(f"Fetching document links from {subsource.get_full_url()}")
                    links = subsource.get_document_links()
                    logger.info(f"Found {len(links)} document links")
                    
                    # Process each document link
                    for link_idx, link in enumerate(links, 1):
                        url = link['url']
                        title = link['title']
                        
                        logger.info(f"Processing document {link_idx}/{len(links)}: {title}")
                        logger.debug(f"Document URL: {url}")
                        
                        try:
                            # Check if already in database
                            existing_doc = db_manager.get_document_by_url(url)
                            if existing_doc and existing_doc.status != "error":
                                logger.info(f"Document already exists: {url}")
                                if existing_doc.status == "scraped":
                                    # Add to list for processing
                                    scraped_docs.append(existing_doc)
                                continue
                            
                            # Create new document
                            doc = Document(
                                url=url,
                                title=title,
                                source_name=source.name,
                                subsource_name=subsource.name
                            )
                            
                            # Fetch content
                            logger.info(f"Fetching document: {url}")
                            if doc.fetch_content(subsource.extractor):
                                # Store in database
                                doc_id = db_manager.store_document(doc)
                                
                                if doc_id:
                                    logger.info(f"Document stored with ID: {doc_id}")
                                    scraped_docs.append(doc)
                                else:
                                    logger.error(f"Failed to store document in database: {url}")
                                
                                # Add a small delay to avoid overwhelming the server
                                time.sleep(1)
                            else:
                                logger.error(f"Failed to fetch content for: {url}, status: {doc.status}")
                                
                        except Exception as e:
                            logger.error(f"Error processing document {url}: {str(e)}", exc_info=True)
                            continue
                
                except Exception as e:
                    logger.error(f"Error processing subsource {subsource.name}: {str(e)}", exc_info=True)
                    continue
        
        logger.info(f"Successfully scraped {len(scraped_docs)} documents")
        return scraped_docs
    
    except Exception as e:
        logger.error(f"Error in scrape_documents: {str(e)}", exc_info=True)
        return []


def process_documents(documents: List[Document], db_manager: StorageManager, parallel: bool = False) -> List[Document]:
    """
    Process documents with AI:
    1. Generate summaries
    2. Create embeddings
    3. Store in vector database
    
    Args:
        documents: List of documents to process
        db_manager: Storage manager for database operations
        parallel: Whether to process in parallel
        
    Returns:
        List of processed documents
    """
    if not documents:
        logger.info("No documents to process")
        return []
    
    try:
        # Initialize processor
        processor = Processor()
        processed_docs = []
        
        logger.info(f"Processing {len(documents)} documents (parallel={parallel})")
        
        # Process documents
        if parallel and len(documents) > 1:
            # Process in parallel
            def process_doc(doc):
                try:
                    logger.info(f"Processing document (parallel): {doc.url}")
                    if processor.process_document(doc):
                        doc_id = db_manager.store_document(doc)
                        if doc_id:
                            logger.info(f"Successfully processed and stored document with ID: {doc_id}")
                            return doc
                        else:
                            logger.error(f"Failed to store processed document: {doc.url}")
                            return None
                    else:
                        logger.error(f"Failed to process document: {doc.url}")
                        return None
                except Exception as e:
                    logger.error(f"Error in process_doc worker: {str(e)}", exc_info=True)
                    return None
            
            logger.info(f"Starting parallel processing with ThreadPoolExecutor")
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(process_doc, doc) for doc in documents]
                
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    logger.debug(f"Completed task {i+1}/{len(futures)}")
                    try:
                        result = future.result()
                        if result:
                            processed_docs.append(result)
                    except Exception as e:
                        logger.error(f"Error in parallel processing: {str(e)}", exc_info=True)
            
            logger.info(f"Parallel processing complete: {len(processed_docs)}/{len(documents)} documents processed")
        else:
            # Process sequentially
            logger.info("Starting sequential processing")
            for idx, doc in enumerate(documents, 1):
                logger.info(f"Processing document {idx}/{len(documents)}: {doc.url}")
                try:
                    if processor.process_document(doc):
                        doc_id = db_manager.store_document(doc)
                        if doc_id:
                            logger.info(f"Successfully processed and stored document with ID: {doc_id}")
                            processed_docs.append(doc)
                        else:
                            logger.error(f"Failed to store processed document: {doc.url}")
                    else:
                        logger.error(f"Failed to process document: {doc.url}")
                except Exception as e:
                    logger.error(f"Error processing document {doc.url}: {str(e)}", exc_info=True)
            
            logger.info(f"Sequential processing complete: {len(processed_docs)}/{len(documents)} documents processed")
        
        return processed_docs
    
    except Exception as e:
        logger.error(f"Error in process_documents: {str(e)}", exc_info=True)
        return []


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Government data pipeline runner')
    parser.add_argument('--config', default='config.json', help='Path to configuration file')
    parser.add_argument('--db-url', default=os.getenv('DATABASE_URL'), help='Database connection string')
    parser.add_argument('--scrape-only', action='store_true', help='Only scrape, don\'t process')
    parser.add_argument('--process-only', action='store_true', help='Only process existing documents')
    parser.add_argument('--parallel', action='store_true', help='Process documents in parallel')
    parser.add_argument('--limit', type=int, default=10, help='Limit the number of documents to process')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--log-file', default=None, help='Path to log file (default: auto-generated)')
    
    args = parser.parse_args()
    
    # Set up logging
    log_file = args.log_file or generate_log_file_name()
    setup_logging(log_level=logging.DEBUG if args.debug else logging.INFO, 
                 log_file=log_file, 
                 debug_mode=args.debug)
    
    logger.info("Starting government data pipeline")
    logger.info(f"Arguments: {vars(args)}")
    
    # Check database URL
    if not args.db_url:
        logger.error("Database URL not provided. Set DATABASE_URL environment variable or use --db-url")
        sys.exit(1)
    
    try:
        # Initialize storage manager
        logger.info(f"Initializing storage manager with database URL: {args.db_url[:10]}...(hidden)")
        db_manager = StorageManager(args.db_url)
        
        # Test database connection
        if not db_manager.connect():
            logger.error("Failed to connect to database, exiting")
            sys.exit(1)
        
        # Process based on mode
        if args.process_only:
            # Only process existing documents
            logger.info("Running in process-only mode")
            db_manager.setup_tables()
            docs = db_manager.get_unprocessed_documents(args.limit)
            logger.info(f"Found {len(docs)} unprocessed documents")
            
            processed = process_documents(docs, db_manager, args.parallel)
            logger.info(f"Successfully processed {len(processed)}/{len(docs)} documents")
        
        elif args.scrape_only:
            # Only scrape documents
            logger.info("Running in scrape-only mode")
            scraped = scrape_documents(args.config, db_manager)
            logger.info(f"Successfully scraped {len(scraped)} documents")
        
        else:
            # Full pipeline: scrape and process
            logger.info("Running full pipeline: scrape and process")
            
            # Scrape documents
            scraped = scrape_documents(args.config, db_manager)
            logger.info(f"Successfully scraped {len(scraped)} documents")
            
            # Limit the number of documents to process if needed
            if args.limit and len(scraped) > args.limit:
                logger.info(f"Limiting processing to {args.limit} of {len(scraped)} scraped documents")
                scraped = scraped[:args.limit]
            
            # Process documents
            processed = process_documents(scraped, db_manager, args.parallel)
            logger.info(f"Successfully processed {len(processed)}/{len(scraped)} documents")
        
        # Clean up
        db_manager.disconnect()
        logger.info("Pipeline execution completed successfully")
    
    except Exception as e:
        logger.error(f"Unhandled exception in main: {str(e)}", exc_info=True)
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()