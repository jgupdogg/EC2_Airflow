#!/usr/bin/env python3
"""
Updated main script for running the government data pipeline with Supabase and Neo4j integration.
Uses the unified EnhancedProcessor to handle all processing.
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

# Import core components and managers
from core import (
    ScrapeConfig,
    Document,
    SupabaseManager,
    ScraperAdapter
)

# Import unified enhanced processor
from enhanced_processor import EnhancedProcessor

GLOBAL_SCRAPER = None

# Get logger (will be properly configured after setup_logging is called)
logger = logging.getLogger(__name__)

# Load environment variables from multiple possible locations
current_dir = os.path.abspath(os.getcwd())
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)

dotenv_paths = [
    # Current working directory
    os.path.join(current_dir, '.env'),
    # Script directory
    os.path.join(script_dir, '.env'),
    # Parent directory
    os.path.join(parent_dir, '.env'),
    # govt_data_pipeline folder (if that's the project root)
    os.path.join(current_dir, 'govt_data_pipeline', '.env'),
    # Home directory
    os.path.join(os.path.expanduser('~'), '.env'),
]

# Debug info about where we're looking
print("Looking for .env file in:")
for path in dotenv_paths:
    print(f" - {path}")

env_file_found = False
for dotenv_path in dotenv_paths:
    if os.path.exists(dotenv_path):
        print(f"Found and loading .env from: {dotenv_path}")
        load_dotenv(dotenv_path)
        env_file_found = True
        break

def create_sample_env_file(path):
    """Create a sample .env file with placeholders."""
    sample_content = """# Supabase credentials for the "Agent Alpha" project
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_KEY=your-supabase-key

# API keys for AI models
ANTHROPIC_API_KEY=your_anthropic_key
OPENAI_API_KEY=your_openai_key

# Pinecone vector database configuration
PINECONE_API_KEY=your_pinecone_key
PINECONE_INDEX_NAME=govt-scrape-index
PINECONE_NAMESPACE=govt-content

# Neo4j knowledge graph configuration
NEO4J_URI=neo4j+s://your-instance-id.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your-password

# Optional logging configuration
LOG_LEVEL=INFO
LOG_FILE=govt_scraper.log
"""
    try:
        with open(path, 'w') as f:
            f.write(sample_content)
        return True
    except Exception as e:
        print(f"Error creating sample .env file: {e}")
        return False

if not env_file_found:
    print("Warning: No .env file found in any of the expected locations.")
    create_env = input("Would you like to create a sample .env file in the current directory? (y/n): ")
    if create_env.lower() == 'y':
        env_path = os.path.join(current_dir, '.env')
        if create_sample_env_file(env_path):
            print(f"Created sample .env file at {env_path}")
            print("Please edit this file with your actual credentials and run the script again.")
        else:
            print("Failed to create sample .env file.")
        sys.exit(1)

def scrape_documents(config_path: str, db_manager: SupabaseManager) -> List[Document]:
    """
    Scrape documents based on configuration.
    Optimized to skip older documents when a newer one is already in the database.
    
    Args:
        config_path: Path to configuration file
        db_manager: Supabase manager for database operations
        
    Returns:
        List of scraped documents
    """
    try:
        global GLOBAL_SCRAPER
        if GLOBAL_SCRAPER is None:
            logger.info("Initializing global browser for the session")
            GLOBAL_SCRAPER = ScraperAdapter.get_instance(headless=False, use_virtual_display=True)
            
        
        # Load configuration
        logger.info(f"Loading configuration from {config_path}")
        config = ScrapeConfig(config_path)
        
        # Verify database tables exist
        logger.info("Verifying Supabase tables exist")
        if not db_manager.setup_tables():
            logger.error("Required tables don't exist in Supabase, aborting scrape")
            logger.error("Please run setup_supabase.py --print-sql-only to generate SQL")
            logger.error("Then run that SQL in the Supabase SQL Editor")
            return []
        
        scraped_docs = []
        
        # Process each source
        sources = config.get_sources()
        logger.info(f"Processing {len(sources)} sources")
        
        for source_idx, source in enumerate(sources, 1):
            logger.info(f"Processing source {source_idx}/{len(sources)}: {source.name}")
            
            # Store source in Supabase
            source_id = db_manager.store_source(source.name, source.base_url)
            if not source_id:
                logger.error(f"Failed to store source {source.name} in database")
                continue
            
            # Process each subsource
            subsources = source.get_subsources()
            logger.info(f"Found {len(subsources)} subsources for {source.name}")
            
            for subsource_idx, subsource in enumerate(subsources, 1):
                logger.info(f"Processing subsource {subsource_idx}/{len(subsources)}: {subsource.name}")
                
                # Store subsource in Supabase
                subsource_id = db_manager.store_subsource(source_id, subsource.name, subsource.url_pattern)
                if not subsource_id:
                    logger.error(f"Failed to store subsource {subsource.name} in database")
                    continue
                
                try:
                    # Get document links
                    logger.info(f"Fetching document links from {subsource.get_full_url()}")
                    links = subsource.get_document_links()
                    logger.info(f"Found {len(links)} document links")
                    
                    # Process each document link
                    found_existing_document = False
                    new_docs_count = 0
                    
                    for link_idx, link in enumerate(links, 1):
                        url = link['url']
                        title = link['title']
                        
                        logger.info(f"Processing document {link_idx}/{len(links)}: {title}")
                        logger.debug(f"Document URL: {url}")
                        
                        try:
                            # Check if already in database
                            existing_doc = db_manager.get_document_by_url(url)
                            if existing_doc:
                                # If document exists and is not an error, we've hit older content
                                if existing_doc.status != "error":
                                    logger.info(f"Document already exists: {url}")
                                    if existing_doc.status == "scraped":
                                        # Add to list for processing
                                        scraped_docs.append(existing_doc)
                                    
                                    # If this is a successfully processed document,
                                    # stop processing this subsource as we've reached existing content
                                    if existing_doc.status in ["scraped", "processed"]:
                                        logger.info(f"Found existing document that's already processed. " +
                                                   f"Skipping remaining {len(links) - link_idx} documents " +
                                                   f"in this subsource (chronological optimization).")
                                        found_existing_document = True
                                        break
                                        
                                    # Otherwise continue with next link (e.g., if document had errors)
                                    continue
                                else:
                                    # If it was an error, try to process it again
                                    logger.info(f"Re-processing previously errored document: {url}")
                            
                            # Create new document
                            doc = Document(
                                url=url,
                                title=title,
                                source_name=source.name,
                                subsource_name=subsource.name
                            )
                            doc.use_javascript = True  # Force JavaScript mode for all documents

                            
                            # Fetch content
                            logger.info(f"Fetching document: {url}")
                            if doc.fetch_content(subsource.extractor):
                                # Store in database
                                doc_id = db_manager.store_document(doc)
                                
                                if doc_id:
                                    logger.info(f"Document stored with ID: {doc_id}")
                                    doc.doc_id = doc_id
                                    scraped_docs.append(doc)
                                    new_docs_count += 1
                                else:
                                    logger.error(f"Failed to store document in database: {url}")
                                
                                # Add a small delay to avoid overwhelming the server
                                time.sleep(1)
                            else:
                                logger.error(f"Failed to fetch content for: {url}, status: {doc.status}")
                                
                        except Exception as e:
                            logger.error(f"Error processing document {url}: {str(e)}", exc_info=True)
                            continue
                    
                    # Log summary for this subsource
                    if found_existing_document:
                        logger.info(f"Completed subsource with {new_docs_count} new documents (stopped at existing document)")
                    else:
                        logger.info(f"Completed subsource with {new_docs_count} new documents (processed all links)")
                
                except Exception as e:
                    logger.error(f"Error processing subsource {subsource.name}: {str(e)}", exc_info=True)
                    continue
        
        logger.info(f"Successfully scraped {len(scraped_docs)} documents")
        return scraped_docs
    
    except Exception as e:
        logger.error(f"Error in scrape_documents: {str(e)}", exc_info=True)
        return []


def process_documents(documents: List[Document], db_manager: SupabaseManager, parallel: bool = False) -> List[Document]:
    """
    Process documents with AI:
    1. Generate summaries
    2. Create embeddings
    3. Store in vector database
    4. Extract entities and add to knowledge graph
    
    Args:
        documents: List of documents to process
        db_manager: Supabase manager for database operations
        parallel: Whether to process in parallel
        
    Returns:
        List of processed documents
    """
    if not documents:
        logger.info("No documents to process")
        return []
    
    try:
        # Initialize processor
        processor = EnhancedProcessor()
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
        
        # If knowledge graph manager is available, print statistics
        if processor._kg_manager:
            try:
                kg_stats = processor._kg_manager.get_statistics()
                logger.info("Knowledge Graph Statistics:")
                logger.info(f"  Documents: {kg_stats.get('document_count', 0)}")
                logger.info(f"  Entities: {kg_stats.get('entity_count', 0)}")
                logger.info(f"  Entity types: {kg_stats.get('entity_types', {})}")
                logger.info(f"  Relationships: {kg_stats.get('relationship_count', 0)}")
            except Exception as e:
                logger.warning(f"Could not retrieve knowledge graph statistics: {e}")
        
        return processed_docs
    
    except Exception as e:
        logger.error(f"Error in process_documents: {str(e)}", exc_info=True)
        return []


def reset_knowledge_graph():
    """Reset the Neo4j knowledge graph database."""
    neo4j_uri = os.getenv("NEO4J_URI")
    neo4j_username = os.getenv("NEO4J_USERNAME")
    neo4j_password = os.getenv("NEO4J_PASSWORD")
    
    if not all([neo4j_uri, neo4j_username, neo4j_password]):
        logger.error("Neo4j credentials not found. Cannot reset knowledge graph.")
        return False
    
    try:
        # Import here to avoid requiring Neo4j for users who don't need it
        from knowledge_graph_manager import KnowledgeGraphManager
        
        # Initialize knowledge graph manager and reset database
        logger.info("Initializing KnowledgeGraphManager for reset")
        kg_manager = KnowledgeGraphManager(
            uri=neo4j_uri,
            username=neo4j_username,
            password=neo4j_password
        )
        
        logger.info("Resetting Neo4j knowledge graph database")
        kg_manager.reset_database()
        logger.info("Knowledge graph reset complete")
        return True
    
    except Exception as e:
        logger.error(f"Error resetting knowledge graph: {e}")
        return False


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Government data pipeline runner with Supabase and Neo4j integration')
    parser.add_argument('--config', default='config.json', help='Path to configuration file')
    parser.add_argument('--env-file', help='Path to .env file with credentials')
    parser.add_argument('--supabase-url', default=os.getenv('SUPABASE_URL'), help='Supabase project URL')
    parser.add_argument('--supabase-key', default=os.getenv('SUPABASE_KEY'), help='Supabase API key')
    parser.add_argument('--scrape-only', action='store_true', help='Only scrape, don\'t process')
    parser.add_argument('--process-only', action='store_true', help='Only process existing documents')
    parser.add_argument('--parallel', action='store_true', help='Process documents in parallel')
    parser.add_argument('--limit', type=int, default=20, help='Limit the number of documents to process')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--log-file', default=None, help='Path to log file (default: auto-generated)')
    parser.add_argument('--verify-tables', action='store_true', help='Only verify that Supabase tables exist')
    
    # Knowledge graph related arguments
    parser.add_argument('--reset-kg', action='store_true', help='Reset the knowledge graph database before processing')
    parser.add_argument('--kg-stats', action='store_true', help='Print knowledge graph statistics and exit')
    
    args = parser.parse_args()
    
    # If env-file is specified, load it directly
    if args.env_file:
        if os.path.exists(args.env_file):
            print(f"Loading environment variables from specified file: {args.env_file}")
            load_dotenv(args.env_file)
        else:
            print(f"Error: Specified .env file does not exist: {args.env_file}")
            sys.exit(1)
    
    # Set up logging
    log_file = args.log_file or generate_log_file_name()
    setup_logging(log_level=logging.DEBUG if args.debug else logging.INFO, 
                 log_file=log_file, 
                 debug_mode=args.debug)
    
    logger.info("Starting government data pipeline with unified processor")
    logger.info(f"Arguments: {vars(args)}")
    
    # Check Supabase credentials
    if not args.supabase_url or not args.supabase_key:
        logger.error("Supabase URL and key not provided.")
        print("\nMissing Supabase credentials. You can provide them in several ways:")
        print("1. Create a .env file with SUPABASE_URL and SUPABASE_KEY variables")
        print("2. Set environment variables: export SUPABASE_URL=your-url SUPABASE_KEY=your-key")
        print("3. Pass them as command line arguments: --supabase-url your-url --supabase-key your-key")
        print("\nCurrent environment variables:")
        print(f"SUPABASE_URL: {'Found' if os.getenv('SUPABASE_URL') else 'Not found'}")
        print(f"SUPABASE_KEY: {'Found' if os.getenv('SUPABASE_KEY') else 'Not found'}")
        print("\nCurrent working directory:", os.getcwd())
        sys.exit(1)
    
    # If only getting knowledge graph statistics
    if args.kg_stats:
        try:
            from knowledge_graph_manager import KnowledgeGraphManager
            
            neo4j_uri = os.getenv("NEO4J_URI")
            neo4j_username = os.getenv("NEO4J_USERNAME")
            neo4j_password = os.getenv("NEO4J_PASSWORD")
            
            if not all([neo4j_uri, neo4j_username, neo4j_password]):
                logger.error("Neo4j credentials not found. Cannot get knowledge graph statistics.")
                sys.exit(1)
                
            kg_manager = KnowledgeGraphManager(
                uri=neo4j_uri,
                username=neo4j_username,
                password=neo4j_password
            )
            
            stats = kg_manager.get_statistics()
            print("\nKnowledge Graph Statistics:")
            print(f"Documents: {stats.get('document_count', 0)}")
            print(f"Entities: {stats.get('entity_count', 0)}")
            print("Entity types:")
            for entity_type, count in stats.get('entity_types', {}).items():
                print(f"  - {entity_type}: {count}")
            print(f"Mentions: {stats.get('mention_count', 0)}")
            print(f"Relationships: {stats.get('relationship_count', 0)}")
            print("Relationship types:")
            for rel_type, count in stats.get('relationship_types', {}).items():
                print(f"  - {rel_type}: {count}")
            
            sys.exit(0)
        except Exception as e:
            logger.error(f"Error getting knowledge graph statistics: {e}")
            sys.exit(1)
    
    try:
        # Initialize Supabase manager
        logger.info(f"Initializing Supabase manager with URL: {args.supabase_url[:20]}...(hidden)")
        db_manager = SupabaseManager(args.supabase_url, args.supabase_key)
        
        # If only verifying tables, do that and exit
        if args.verify_tables:
            logger.info("Verifying Supabase tables exist")
            if db_manager.setup_tables():
                logger.info("All required Supabase tables exist")
                sys.exit(0)
            else:
                logger.error("Required tables don't exist in Supabase")
                logger.error("Please run setup_supabase.py --print-sql-only to generate SQL")
                logger.error("Then run that SQL in the Supabase SQL Editor")
                sys.exit(1)
        
        # Reset knowledge graph if requested
        if args.reset_kg:
            logger.info("Resetting knowledge graph database")
            if not reset_knowledge_graph():
                logger.error("Failed to reset knowledge graph")
                sys.exit(1)
            
            if args.process_only or not args.scrape_only:
                logger.info("Continuing with document processing...")
            else:
                logger.info("Knowledge graph reset complete")
                sys.exit(0)
        
        # Process based on mode
        if args.process_only:
            # Verify tables exist first
            if not db_manager.setup_tables():
                logger.error("Required tables don't exist in Supabase, aborting")
                logger.error("Please run setup_supabase.py --print-sql-only to generate SQL")
                logger.error("Then run that SQL in the Supabase SQL Editor")
                sys.exit(1)
                
            # Only process existing documents
            logger.info("Running in process-only mode")
            docs = db_manager.get_unprocessed_documents(args.limit)
            logger.info(f"Found {len(docs)} unprocessed documents")
            
            processed = process_documents(docs, db_manager, args.parallel)
            logger.info(f"Successfully processed {len(processed)}/{len(docs)} documents")
        
        elif args.scrape_only:
            # Only scrape documents
            logger.info("Running in scrape-only mode")
            scraped = scrape_documents(args.config, db_manager)
            if not scraped:
                logger.error("No documents were scraped, check logs for errors")
                sys.exit(1)
            logger.info(f"Successfully scraped {len(scraped)} documents")
        
        else:
            # Full pipeline: scrape and process
            logger.info("Running full pipeline: scrape and process")
            
            # Scrape documents
            scraped = scrape_documents(args.config, db_manager)
            if not scraped:
                logger.error("No documents were scraped, check logs for errors")
                sys.exit(1)
            logger.info(f"Successfully scraped {len(scraped)} documents")
            
            # Limit the number of documents to process if needed
            if args.limit and len(scraped) > args.limit:
                logger.info(f"Limiting processing to {args.limit} of {len(scraped)} scraped documents")
                scraped = scraped[:args.limit]
            
            # Process documents
            processed = process_documents(scraped, db_manager, args.parallel)
            logger.info(f"Successfully processed {len(processed)}/{len(scraped)} documents")
        
        logger.info("Pipeline execution completed successfully")
    
    except Exception as e:
        logger.error(f"Unhandled exception in main: {str(e)}", exc_info=True)
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        # Close the global scraper
        global GLOBAL_SCRAPER
        if GLOBAL_SCRAPER is not None:
            logger.info("Closing global browser session")
            GLOBAL_SCRAPER.close()

if __name__ == "__main__":
    main()