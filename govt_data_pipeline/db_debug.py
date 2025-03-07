#!/usr/bin/env python3
"""
Debug script to diagnose database connection and table creation issues.
"""

import os
import sys
import logging
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def debug_database_connection(db_url=None):
    """Test database connection and schema creation with detailed logging."""
    db_url = db_url or os.getenv("DATABASE_URL")
    
    if not db_url:
        logger.error("No DATABASE_URL provided. Check your .env file or set it manually.")
        return False
    
    logger.info(f"Attempting to connect to database with URL pattern: {db_url.split('@')[0].split(':')[0]}:***@{db_url.split('@')[1]}")
    
    try:
        # Convert SQLAlchemy connection string to psycopg2 format if needed
        if db_url.startswith('postgresql+psycopg2://'):
            psycopg_url = db_url.replace('postgresql+psycopg2://', 'postgresql://')
        else:
            psycopg_url = db_url
        
        # Parse database name from connection string
        db_name = psycopg_url.split('/')[-1]
        logger.info(f"Database name from connection string: {db_name}")
        
        # First try connecting to the specified database
        try:
            logger.info("Attempting to connect to the specified database...")
            conn = psycopg2.connect(psycopg_url)
            logger.info("Connection successful!")
            
            # Try creating a test table
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            logger.info("Testing table creation...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_connection (
                    id SERIAL PRIMARY KEY,
                    test_value TEXT
                )
            """)
            
            # Insert a test value
            cursor.execute("""
                INSERT INTO test_connection (test_value) 
                VALUES ('Test connection successful')
            """)
            
            # Query to verify
            cursor.execute("SELECT * FROM test_connection")
            result = cursor.fetchone()
            logger.info(f"Test query result: {result}")
            
            # Clean up
            cursor.execute("DROP TABLE test_connection")
            
            logger.info("Database connection and table operations successful!")
            conn.close()
            return True
            
        except psycopg2.OperationalError as e:
            logger.warning(f"Could not connect to database: {e}")
            
            # If connection failed, try connecting to postgres to check if database exists
            try:
                logger.info("Trying to connect to postgres database to check if target database exists...")
                # Extract connection details without the database name
                if '@' in psycopg_url:
                    base_url = psycopg_url.rsplit('/', 1)[0] + '/postgres'
                else:
                    logger.error("Invalid connection string format")
                    return False
                
                conn = psycopg2.connect(base_url)
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cursor = conn.cursor()
                
                # Check if database exists
                cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
                exists = cursor.fetchone()
                
                if not exists:
                    logger.info(f"Database '{db_name}' does not exist. Creating it...")
                    cursor.execute(f"CREATE DATABASE {db_name}")
                    logger.info(f"Database '{db_name}' created successfully!")
                else:
                    logger.info(f"Database '{db_name}' already exists.")
                
                cursor.close()
                conn.close()
                
                # Try connecting to the database again
                logger.info("Trying to connect to the database again...")
                conn = psycopg2.connect(psycopg_url)
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cursor = conn.cursor()
                
                # Try creating a test table
                logger.info("Testing table creation...")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS test_connection (
                        id SERIAL PRIMARY KEY,
                        test_value TEXT
                    )
                """)
                
                # Insert a test value
                cursor.execute("""
                    INSERT INTO test_connection (test_value) 
                    VALUES ('Test connection successful')
                """)
                
                # Query to verify
                cursor.execute("SELECT * FROM test_connection")
                result = cursor.fetchone()
                logger.info(f"Test query result: {result}")
                
                # Clean up
                cursor.execute("DROP TABLE test_connection")
                
                logger.info("Database connection and table operations successful!")
                conn.close()
                return True
                
            except Exception as inner_e:
                logger.error(f"Error while trying to create/connect to database: {inner_e}")
                return False
    
    except Exception as e:
        logger.error(f"Unexpected error during database debugging: {e}")
        return False


def setup_database_tables(db_url=None):
    """Create the required tables for the govt_data_pipeline."""
    db_url = db_url or os.getenv("DATABASE_URL")
    
    if not db_url:
        logger.error("No DATABASE_URL provided. Check your .env file or set it manually.")
        return False
    
    try:
        # Convert SQLAlchemy connection string to psycopg2 format if needed
        if db_url.startswith('postgresql+psycopg2://'):
            psycopg_url = db_url.replace('postgresql+psycopg2://', 'postgresql://')
        else:
            psycopg_url = db_url
        
        # Connect to the database
        conn = psycopg2.connect(psycopg_url)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Create table for sources
        logger.info("Creating govt_sources table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS govt_sources (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                base_url VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create table for subsources
        logger.info("Creating govt_subsources table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS govt_subsources (
                id SERIAL PRIMARY KEY,
                source_id INTEGER REFERENCES govt_sources(id),
                name VARCHAR(255) NOT NULL,
                url_pattern VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create table for documents
        logger.info("Creating govt_documents table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS govt_documents (
                id SERIAL PRIMARY KEY,
                url VARCHAR(255) NOT NULL UNIQUE,
                title VARCHAR(255),
                source_name VARCHAR(255),
                subsource_name VARCHAR(255),
                content TEXT,
                content_hash VARCHAR(32),
                summary TEXT,
                embedding_id VARCHAR(255),
                status VARCHAR(50) DEFAULT 'new',
                scrape_time TIMESTAMP,
                process_time TIMESTAMP,
                last_checked TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create index on content_hash
        logger.info("Creating index on content_hash...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_govt_documents_content_hash
            ON govt_documents(content_hash)
        """)
        
        logger.info("All tables created successfully!")
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error creating database tables: {e}")
        return False


def main():
    """Main entry point for the debug script."""
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        print("ERROR: No DATABASE_URL found in environment variables.")
        print("Please set the DATABASE_URL environment variable with your database connection string.")
        print("Example: postgresql+psycopg2://username:password@localhost:5432/database_name")
        sys.exit(1)
    
    print("\n==== TESTING DATABASE CONNECTION ====")
    connection_ok = debug_database_connection(db_url)
    
    if connection_ok:
        print("\n==== SETTING UP DATABASE TABLES ====")
        tables_ok = setup_database_tables(db_url)
        
        if tables_ok:
            print("\n==== SUCCESS ====")
            print("Database connection and table setup successful!")
            print("You can now run the scraper with:")
            print("python runner.py")
        else:
            print("\n==== ERROR ====")
            print("Failed to set up database tables.")
    else:
        print("\n==== ERROR ====")
        print("Failed to connect to the database.")
        print("Please check your DATABASE_URL and ensure the PostgreSQL server is running.")
        print("Also make sure the user has appropriate permissions.")


if __name__ == "__main__":
    main()