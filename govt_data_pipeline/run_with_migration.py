#!/usr/bin/env python3
"""
Wrapper script to run the database migration and then the scraper.
"""

import os
import sys
import subprocess
import argparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def run_command(command):
    """Run a command and print output in real-time."""
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        shell=True
    )
    
    for line in iter(process.stdout.readline, ""):
        print(line, end="")
    
    process.stdout.close()
    return_code = process.wait()
    
    if return_code:
        print(f"Command failed with return code {return_code}")
        sys.exit(return_code)


def main():
    """Run the database migration and then the scraper."""
    parser = argparse.ArgumentParser(description='Run the govt data pipeline with database migration')
    parser.add_argument('--config', default='config.json', help='Path to configuration file')
    parser.add_argument('--db-url', default=os.getenv('DATABASE_URL'), help='Database connection string')
    parser.add_argument('--scrape-only', action='store_true', help='Only scrape, don\'t process')
    parser.add_argument('--process-only', action='store_true', help='Only process existing documents')
    parser.add_argument('--parallel', action='store_true', help='Process documents in parallel')
    parser.add_argument('--limit', type=int, default=10, help='Limit the number of documents to process')
    parser.add_argument('--skip-migration', action='store_true', help='Skip database migration')
    parser.add_argument('--skip-pinecone-fix', action='store_true', help='Skip fixing Pinecone package')
    
    args = parser.parse_args()
    
    # Fix Pinecone package issue first
    if not args.skip_pinecone_fix:
        print("Fixing Pinecone package issue...")
        run_command("python fix_pinecone.py")
    
    # Run migration first unless skipped
    if not args.skip_migration:
        print("Running database migration...")
        run_command(f"python db_migration.py --db-url '{args.db_url}'")
    
    # Build command for runner.py
    cmd = ["python", "runner.py"]
    
    if args.config:
        cmd.append(f"--config {args.config}")
    
    if args.db_url:
        cmd.append(f"--db-url '{args.db_url}'")
    
    if args.scrape_only:
        cmd.append("--scrape-only")
    
    if args.process_only:
        cmd.append("--process-only")
    
    if args.parallel:
        cmd.append("--parallel")
    
    if args.limit:
        cmd.append(f"--limit {args.limit}")
    
    # Run the scraper
    print("\nRunning scraper...")
    run_command(" ".join(cmd))


if __name__ == "__main__":
    main()