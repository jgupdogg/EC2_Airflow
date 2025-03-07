#!/usr/bin/env python3
"""
Fix script for the Pinecone package issue.
Removes the old pinecone-client package and installs the new pinecone package.
"""

import os
import sys
import subprocess
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fix_pinecone_package():
    """Fix the Pinecone package issue."""
    try:
        # Check if old package is installed
        try:
            import pinecone_client
            logger.info("Found old pinecone-client package. Removing...")
            
            # Remove old package
            subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", "pinecone-client"])
            logger.info("Removed old pinecone-client package")
        except ImportError:
            logger.info("Old pinecone-client package not found.")
        
        # Check if new package is installed
        try:
            import pinecone
            logger.info(f"New pinecone package is already installed (version: {pinecone.__version__}).")
        except ImportError:
            logger.info("Installing new pinecone package...")
            
            # Install new package
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pinecone>=5.1.0"])
            logger.info("Installed new pinecone package")
        
        return True
        
    except Exception as e:
        logger.error(f"Error fixing Pinecone package: {e}")
        return False

def main():
    """Main entry point for the fix script."""
    print("\n==== FIXING PINECONE PACKAGE ====")
    
    if fix_pinecone_package():
        print("\n==== SUCCESS ====")
        print("Pinecone package issue fixed. You can now run the scraper.")
    else:
        print("\n==== ERROR ====")
        print("Failed to fix Pinecone package issue.")
        print("Please run these commands manually:")
        print("  pip uninstall -y pinecone-client")
        print("  pip install pinecone>=5.1.0")
        sys.exit(1)

if __name__ == "__main__":
    main()