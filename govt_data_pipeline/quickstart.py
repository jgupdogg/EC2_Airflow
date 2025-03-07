#!/usr/bin/env python3
"""
Quickstart script for the govt_data_pipeline.
Checks and sets up everything needed to run the pipeline.
"""

import os
import sys
import argparse
import logging
import subprocess
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def check_environment():
    """Check if all required environment variables are set."""
    required_vars = [
        "DATABASE_URL",
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "PINECONE_API_KEY"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these in your .env file or environment.")
        return False
    
    logger.info("All required environment variables are set.")
    return True

def check_dependencies():
    """Check if all required Python packages are installed."""
    required_packages = [
        "psycopg2-binary",
        "requests",
        "beautifulsoup4",
        "lxml",
        "python-dotenv",
        "pinecone>=5.1.0",  # Updated to use the new package name
        "langchain-anthropic",
        "langchain-openai"
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            package_name = package.replace('-', '_').split('>=')[0].split('==')[0]
            __import__(package_name)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        logger.error(f"Missing Python packages: {', '.join(missing_packages)}")
        logger.info("Installing missing packages...")
        
        # Remove pinecone-client if it exists
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", "pinecone-client"])
            logger.info("Removed old pinecone-client package")
        except:
            pass  # It's okay if it wasn't installed
        
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing_packages)
            logger.info("All packages installed successfully.")
        except subprocess.CalledProcessError:
            logger.error("Failed to install packages. Please install them manually.")
            return False
    
    logger.info("All required Python packages are installed.")
    return True

def setup_database():
    """Set up the database for the pipeline."""
    logger.info("Setting up database...")
    
    try:
        # Run the database debug script
        result = subprocess.run(
            [sys.executable, "db_debug.py"],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error("Database setup failed!")
            logger.error(result.stderr)
            return False
        
        logger.info("Database setup completed.")
        return True
    
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        return False

def run_scraper(args):
    """Run the scraper with the specified arguments."""
    logger.info("Running scraper...")
    
    cmd = [sys.executable, "runner.py"]
    
    if args.config:
        cmd.extend(["--config", args.config])
    
    if args.scrape_only:
        cmd.append("--scrape-only")
    
    if args.process_only:
        cmd.append("--process-only")
    
    if args.parallel:
        cmd.append("--parallel")
    
    if args.limit:
        cmd.extend(["--limit", str(args.limit)])
    
    try:
        subprocess.run(cmd)
        return True
    
    except Exception as e:
        logger.error(f"Error running scraper: {e}")
        return False

def main():
    """Main entry point for the quickstart script."""
    parser = argparse.ArgumentParser(description='Quickstart for the govt_data_pipeline')
    parser.add_argument('--config', default='config.json', help='Path to configuration file')
    parser.add_argument('--scrape-only', action='store_true', help='Only scrape, don\'t process')
    parser.add_argument('--process-only', action='store_true', help='Only process existing documents')
    parser.add_argument('--parallel', action='store_true', help='Process documents in parallel')
    parser.add_argument('--limit', type=int, default=10, help='Limit the number of documents to process')
    parser.add_argument('--skip-checks', action='store_true', help='Skip environment and dependency checks')
    parser.add_argument('--skip-db-setup', action='store_true', help='Skip database setup')
    
    args = parser.parse_args()
    
    # Display welcome message
    print("\n" + "=" * 50)
    print("Government Data Pipeline Quickstart")
    print("=" * 50 + "\n")
    
    # Check environment and dependencies
    if not args.skip_checks:
        if not check_environment():
            print("\nPlease fix the environment issues and try again.")
            sys.exit(1)
        
        if not check_dependencies():
            print("\nPlease fix the dependency issues and try again.")
            sys.exit(1)
    
    # Set up database
    if not args.skip_db_setup:
        if not setup_database():
            print("\nPlease fix the database issues and try again.")
            sys.exit(1)
    
    # Run the scraper
    if not run_scraper(args):
        print("\nScraper encountered errors. Please check the logs.")
        sys.exit(1)
    
    print("\n" + "=" * 50)
    print("Government Data Pipeline completed successfully!")
    print("=" * 50 + "\n")

if __name__ == "__main__":
    main()