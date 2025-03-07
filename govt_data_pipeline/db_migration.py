#!/usr/bin/env python3
"""
Database migration script to update the schema with new columns.
Run this before using the updated scraper code.
"""

import os
import sys
import argparse
import logging
from dotenv import load_dotenv
import psycopg2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def migrate_database(db_url: str = None):
    """
    Update the database schema to add new columns.
    
    Args:
        db_url: Database connection string
    """
    db_url = db_url or os.getenv("DATABASE_URL")
    
    if not db_url:
        logger.error("Database URL not provided. Set DATABASE_URL environment variable or use --db-url")
        sys.exit(1)
    
    try:
        # Convert SQLAlchemy connection string to psycopg2 format if needed
        if db_url.startswith('postgresql+psycopg2://'):
            db_url = db_url.replace('postgresql+psycopg2://', 'postgresql://')
        
        # Connect to database
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        # Check if govt_documents table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'govt_documents'
            )
        """)
        
        if not cursor.fetchone()[0]:
            logger.info("Table govt_documents doesn't exist yet, no migration needed")
            conn.close()
            return
        
        # Check if content_hash column exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = 'govt_documents' AND column_name = 'content_hash'
            )
        """)
        
        if not cursor.fetchone()[0]:
            logger.info("Adding content_hash column")
            cursor.execute("""
                ALTER TABLE govt_documents 
                ADD COLUMN content_hash VARCHAR(32)
            """)
            
            # Add index on content_hash
            cursor.execute("""
                CREATE INDEX idx_govt_documents_content_hash
                ON govt_documents(content_hash)
            """)
        
        # Check if last_checked column exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = 'govt_documents' AND column_name = 'last_checked'
            )
        """)
        
        if not cursor.fetchone()[0]:
            logger.info("Adding last_checked column")
            cursor.execute("""
                ALTER TABLE govt_documents 
                ADD COLUMN last_checked TIMESTAMP
            """)
        
        # Commit changes
        conn.commit()
        logger.info("Migration completed successfully")
        
        # Close connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error migrating database: {e}")
        sys.exit(1)

def main():
    """Main entry point for migration script."""
    parser = argparse.ArgumentParser(description='Migrate database schema')
    parser.add_argument('--db-url', default=os.getenv('DATABASE_URL'), help='Database connection string')
    parser.add_argument('--fix-pinecone', action='store_true', help='Fix pinecone package issue')
    
    args = parser.parse_args()
    
    if args.fix_pinecone:
        logger.info("Fixing Pinecone package...")
        try:
            # Remove old package
            subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", "pinecone-client"])
            logger.info("Removed old pinecone-client package")
            
            # Install new package
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pinecone>=5.1.0"])
            logger.info("Installed new pinecone package")
        except Exception as e:
            logger.error(f"Error fixing Pinecone package: {e}")
    
    migrate_database(args.db_url)
    
    print("Migration completed. You can now run the scraper.")

if __name__ == "__main__":
    main()