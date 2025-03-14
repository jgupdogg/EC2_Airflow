#!/usr/bin/env python3
"""
Process scraped content from PostgreSQL database:
1. Retrieve entries from govt_scrape table
2. Generate AI summaries using LangChain and Anthropic
3. Convert summaries to embeddings
4. Store embeddings in Pinecone vector database
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import hashlib
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# LangChain imports
from langchain_anthropic import ChatAnthropic
from langchain.chains.summarize import load_summarize_chain
from langchain.docstore.document import Document
from langchain_openai import OpenAIEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter

# Pinecone import
from pinecone import Pinecone, Index

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+psycopg2://postgres:St0ck!adePG@localhost:5432/pipeline_db')
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
PINECONE_API_KEY = os.getenv('PINECONE_API_KEY')
PINECONE_INDEX_NAME = os.getenv('PINECONE_INDEX_NAME', 'govt-scrape-index')
PINECONE_NAMESPACE = os.getenv('PINECONE_NAMESPACE', 'govt-content')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')


def fetch_scraped_data(conn_string: str, days_ago: int = 30) -> List[Dict[str, Any]]:
    """
    Fetch scraped data from PostgreSQL database.
    
    Args:
        conn_string: PostgreSQL connection string
        days_ago: Only fetch records from the last N days
        
    Returns:
        List of dictionaries containing scraped data
    """
    try:
        # Convert SQLAlchemy connection string to psycopg2 format if needed
        if conn_string.startswith('postgresql+psycopg2://'):
            conn_string = conn_string.replace('postgresql+psycopg2://', 'postgresql://')
        
        # Connect to database
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Fetch recent records
        cutoff_date = datetime.now() - timedelta(days=days_ago)
        
        cursor.execute("""
            SELECT id, url, title, snippet, scrape_time 
            FROM govt_scrape
            WHERE scrape_time >= %s
            ORDER BY scrape_time DESC
        """, (cutoff_date,))
        
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Fetched {len(results)} records from database")
        return [dict(r) for r in results]
        
    except Exception as e:
        logger.error(f"Failed to fetch data from database: {e}")
        return []


def generate_summary(content: str, title: str = "", url: str = "") -> str:
    """
    Generate a summary of content using LangChain and Anthropic.
    
    Args:
        content: The text content to summarize
        title: Optional title to provide context
        url: Optional URL to provide source context
        
    Returns:
        Summarized text
    """
    try:
        if not ANTHROPIC_API_KEY:
            raise ValueError("ANTHROPIC_API_KEY environment variable not set")
        
        # Initialize Anthropic model
        model = ChatAnthropic(
            model="claude-3-haiku-20240307",
            anthropic_api_key=ANTHROPIC_API_KEY,
            temperature=0.3
        )
        
        # Prepare context with title and URL if available
        context = ""
        if title:
            context += f"Title: {title}\n"
        if url:
            context += f"Source: {url}\n"
        
        full_content = f"{context}\n{content}" if context else content
        
        # Use a simple prompt-based approach rather than the chain
        prompt = f"""Please provide a concise summary of the following government website content.
Focus on the key information, main services offered, and important points for citizens.
Keep your summary informative and factual, between 3-5 sentences.

Content:
{full_content}

Summary:"""

        # Generate summary
        response = model.invoke(prompt)
        summary = response.content.strip()
        
        logger.info(f"Generated summary: {summary[:100]}...")
        return summary
        
    except Exception as e:
        logger.error(f"Failed to generate summary: {e}")
        return f"Error generating summary: {str(e)}"


def create_embeddings(text: str) -> List[float]:
    """
    Create embeddings from text using OpenAI's embedding model.
    
    Args:
        text: Text to convert to embeddings
        
    Returns:
        List of float values representing the embedding vector
    """
    try:
        if not OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        
        # Initialize OpenAI embeddings
        embedding_model = OpenAIEmbeddings(
            model="text-embedding-3-small",
            openai_api_key=OPENAI_API_KEY
        )
        
        # Generate embeddings
        embedding = embedding_model.embed_query(text)
        
        logger.info(f"Generated embedding vector of length {len(embedding)}")
        return embedding
        
    except Exception as e:
        logger.error(f"Failed to create embeddings: {e}")
        raise


def store_in_pinecone(
    embeddings_data: List[Dict[str, Any]], 
    index_name: str, 
    namespace: str
) -> bool:
    """
    Store embeddings in Pinecone vector database.
    
    Args:
        embeddings_data: List of dictionaries with id, values, and metadata
        index_name: Pinecone index name
        namespace: Pinecone namespace
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if not PINECONE_API_KEY:
            raise ValueError("PINECONE_API_KEY environment variable not set")
        
        # Initialize Pinecone
        from pinecone import Pinecone
        pc = Pinecone(api_key=PINECONE_API_KEY)
        
        # Connect to index
        index = pc.Index(name=index_name)
        
        # Upsert vectors
        # Transforming data to match latest Pinecone API format if needed
        vectors_to_upsert = []
        for item in embeddings_data:
            vectors_to_upsert.append({
                "id": item["id"],
                "values": item["values"],
                "metadata": item["metadata"]
            })
        
        # Upsert vectors
        index.upsert(
            vectors=vectors_to_upsert,
            namespace=namespace
        )
        
        logger.info(f"Successfully stored {len(embeddings_data)} embeddings in Pinecone index '{index_name}', namespace '{namespace}'")
        return True
        
    except Exception as e:
        logger.error(f"Failed to store embeddings in Pinecone: {e}")
        return False


def create_summary_table(conn_string: str) -> bool:
    """
    Create a table to store summaries if it doesn't exist.
    
    Args:
        conn_string: PostgreSQL connection string
        
    Returns:
        bool: True if successful, False otherwise
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
            CREATE TABLE IF NOT EXISTS govt_summaries (
                id SERIAL PRIMARY KEY,
                scrape_id INTEGER REFERENCES govt_scrape(id),
                summary TEXT NOT NULL,
                embedding_id VARCHAR(255),
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_scrape_id UNIQUE (scrape_id)
            )
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Summary table created or verified")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create summary table: {e}")
        return False


def store_summary(conn_string: str, scrape_id: int, summary: str, embedding_id: str) -> bool:
    """
    Store summary in the database.
    
    Args:
        conn_string: PostgreSQL connection string
        scrape_id: ID of the scraped content
        summary: Generated summary text
        embedding_id: ID of the embedding in Pinecone
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Convert SQLAlchemy connection string to psycopg2 format if needed
        if conn_string.startswith('postgresql+psycopg2://'):
            conn_string = conn_string.replace('postgresql+psycopg2://', 'postgresql://')
        
        # Connect to database
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        
        # Check if entry exists
        cursor.execute(
            "SELECT id FROM govt_summaries WHERE scrape_id = %s",
            (scrape_id,)
        )
        existing = cursor.fetchone()
        
        if existing:
            # Update existing record
            cursor.execute(
                """
                UPDATE govt_summaries
                SET summary = %s, embedding_id = %s, processed_at = CURRENT_TIMESTAMP
                WHERE scrape_id = %s
                """,
                (summary, embedding_id, scrape_id)
            )
        else:
            # Insert new record
            cursor.execute(
                """
                INSERT INTO govt_summaries (scrape_id, summary, embedding_id)
                VALUES (%s, %s, %s)
                """,
                (scrape_id, summary, embedding_id)
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Stored summary for scrape_id {scrape_id}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to store summary: {e}")
        return False


def process_scrape_data(
    db_conn_string: str,
    days_ago: int = 30,
    index_name: str = PINECONE_INDEX_NAME,
    namespace: str = PINECONE_NAMESPACE,
    batch_size: int = 10
) -> List[Dict[str, Any]]:
    """
    Process scraped data:
    1. Generate summaries
    2. Create embeddings
    3. Store in Pinecone
    4. Record results in database
    
    Args:
        db_conn_string: PostgreSQL connection string
        days_ago: Only process records from the last N days
        index_name: Pinecone index name
        namespace: Pinecone namespace
        batch_size: Number of records to process in one batch for Pinecone
        
    Returns:
        List of results
    """
    results = []
    
    # Ensure summary table exists
    if not create_summary_table(db_conn_string):
        logger.error("Failed to create summary table, aborting")
        return results
    
    # Fetch scraped data
    scraped_data = fetch_scraped_data(db_conn_string, days_ago)
    if not scraped_data:
        logger.warning("No scraped data to process")
        return results
    
    # Process in batches to avoid overwhelming Pinecone
    embeddings_batch = []
    
    for item in scraped_data:
        try:
            scrape_id = item['id']
            
            # Create combined content for summarization
            title = item.get('title', '')
            url = item.get('url', '')
            content = item.get('snippet', '')
            
            if not content:
                logger.warning(f"Empty content for scrape_id {scrape_id}, skipping")
                continue
            
            # Generate summary
            summary = generate_summary(content, title, url)
            
            # Create embeddings
            embedding_vector = create_embeddings(summary)
            
            # Create a unique ID for the embedding
            embedding_id = f"gov-{scrape_id}-{hashlib.md5(summary.encode()).hexdigest()[:8]}"
            
            # Add to batch for Pinecone
            embeddings_batch.append({
                "id": embedding_id,
                "values": embedding_vector,
                "metadata": {
                    "source": url,
                    "title": title,
                    "summary": summary,
                    "scrape_id": scrape_id,
                    "processed_at": datetime.now().isoformat()
                }
            })
            
            # Store summary in database
            store_summary(db_conn_string, scrape_id, summary, embedding_id)
            
            # Add to results
            results.append({
                "scrape_id": scrape_id,
                "url": url,
                "summary": summary,
                "embedding_id": embedding_id,
                "success": True
            })
            
            # Process batch if it reaches the batch size
            if len(embeddings_batch) >= batch_size:
                store_in_pinecone(embeddings_batch, index_name, namespace)
                embeddings_batch = []
                
        except Exception as e:
            logger.error(f"Error processing scrape_id {item.get('id')}: {e}")
            results.append({
                "scrape_id": item.get('id'),
                "url": item.get('url'),
                "success": False,
                "error": str(e)
            })
    
    # Process any remaining items in the batch
    if embeddings_batch:
        store_in_pinecone(embeddings_batch, index_name, namespace)
    
    return results


def main():
    """Main entry point for standalone script."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process scraped content to summaries and embeddings')
    parser.add_argument('--db-url', default=DATABASE_URL, help='Database connection string')
    parser.add_argument('--days', type=int, default=30, help='Process records from the last N days')
    parser.add_argument('--index', default=PINECONE_INDEX_NAME, help='Pinecone index name')
    parser.add_argument('--namespace', default=PINECONE_NAMESPACE, help='Pinecone namespace')
    parser.add_argument('--batch-size', type=int, default=10, help='Batch size for Pinecone upserts')
    
    args = parser.parse_args()
    
    # Check for required environment variables
    if not ANTHROPIC_API_KEY:
        logger.error("ANTHROPIC_API_KEY environment variable not set")
        sys.exit(1)
    
    if not OPENAI_API_KEY:
        logger.error("OPENAI_API_KEY environment variable not set")
        sys.exit(1)
        
    if not PINECONE_API_KEY:
        logger.error("PINECONE_API_KEY environment variable not set")
        sys.exit(1)
    
    # Process data
    results = process_scrape_data(
        args.db_url, 
        args.days, 
        args.index, 
        args.namespace,
        args.batch_size
    )
    
    # Print results
    print("\nProcessing Results:")
    print("-----------------")
    for result in results:
        status = "✓ Success" if result.get('success', False) else f"✗ Failed: {result.get('error', 'Unknown error')}"
        print(f"{result.get('url', 'Unknown URL')}: {status}")
    
    # Print summary
    success_count = sum(1 for r in results if r.get('success', False))
    print(f"\nSummary: {success_count}/{len(results)} items processed successfully")


if __name__ == "__main__":
    main()