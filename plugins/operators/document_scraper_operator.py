from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

class DocumentScraperOperator(BaseOperator):
    """
    Operator that scrapes documents from configured sources.
    
    :param config_path: Path to configuration file
    :type config_path: str
    :param supabase_conn_id: Connection ID for Supabase
    :type supabase_conn_id: str
    :param scraper_conn_id: Connection ID for web scraper
    :type scraper_conn_id: str
    :param headless: Run browser in headless mode
    :type headless: bool
    :param use_virtual_display: Use virtual display for X server
    :type use_virtual_display: bool
    :param limit: Maximum number of documents to scrape per source
    :type limit: int
    """
    template_fields = ['config_path', 'headless', 'limit'] 
    
    @apply_defaults
    def __init__(
        self,
        config_path: str,
        supabase_conn_id: str = 'supabase_default',
        scraper_conn_id: str = None,
        headless: bool = True,
        use_virtual_display: bool = False,
        limit: int = 2,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.config_path = config_path
        self.supabase_conn_id = supabase_conn_id
        self.scraper_conn_id = scraper_conn_id
        self.headless = headless
        self.use_virtual_display = use_virtual_display
        self.limit = limit
        self.logger = logging.getLogger(__name__)
        
    def execute(self, context):
        """
        Scrape documents from configured sources.
        
        Returns:
            List of scraped document IDs
        """
        from hooks.supabase_hook import SupabaseHook
        from hooks.scraper_hook import ScraperHook
        from utils.scrape_config import ScrapeConfig
        from utils.document import Document
        
        # convert limit to int or None
        limit = int(self.limit) if self.limit else None

        # Initialize hooks
        supabase_hook = SupabaseHook(self.supabase_conn_id)
        scraper_hook = ScraperHook(self.scraper_conn_id)
        
        # Verify tables exist
        if not supabase_hook.setup_tables():
            raise ValueError("Required Supabase tables don't exist")
        
        # Load configuration
        self.logger.info(f"Loading configuration from {self.config_path}")
        config = ScrapeConfig(self.config_path)
        
        scraped_doc_ids = []
        
        try:
            # Process each source
            sources = config.get_sources()
            self.logger.info(f"Processing {len(sources)} sources")
            
            for source_idx, source in enumerate(sources, 1):
                self.logger.info(f"Processing source {source_idx}/{len(sources)}: {source.name}")
                
                # Store source in Supabase
                supabase = supabase_hook.get_conn()
                source_result = supabase.table("govt_sources").select("id").eq("name", source.name).execute()
                
                if source_result.data and len(source_result.data) > 0:
                    source_id = source_result.data[0]["id"]
                    self.logger.info(f"Source {source.name} already exists with ID {source_id}")
                else:
                    # Insert new source
                    now = datetime.now().isoformat()
                    source_result = supabase.table("govt_sources").insert({
                        "name": source.name,
                        "base_url": source.base_url,
                        "created_at": now,
                        "updated_at": now
                    }).execute()
                    
                    source_id = source_result.data[0]["id"] if source_result.data else None
                
                if not source_id:
                    self.logger.error(f"Failed to store source {source.name} in database")
                    continue
                
                # Process each subsource
                subsources = source.get_subsources()
                self.logger.info(f"Found {len(subsources)} subsources for {source.name}")
                
                for subsource_idx, subsource in enumerate(subsources, 1):
                    self.logger.info(f"Processing subsource {subsource_idx}/{len(subsources)}: {subsource.name}")
                    
                    # Store subsource in Supabase
                    subsource_result = supabase.table("govt_subsources").select("id").eq("source_id", source_id).eq("name", subsource.name).execute()
                    
                    if subsource_result.data and len(subsource_result.data) > 0:
                        subsource_id = subsource_result.data[0]["id"]
                        self.logger.info(f"Subsource {subsource.name} already exists with ID {subsource_id}")
                    else:
                        # Insert new subsource
                        now = datetime.now().isoformat()
                        subsource_result = supabase.table("govt_subsources").insert({
                            "source_id": source_id,
                            "name": subsource.name,
                            "url_pattern": subsource.url_pattern,
                            "created_at": now,
                            "updated_at": now
                        }).execute()
                        
                        subsource_id = subsource_result.data[0]["id"] if subsource_result.data else None
                    
                    if not subsource_id:
                        self.logger.error(f"Failed to store subsource {subsource.name} in database")
                        continue
                    
                    try:
                        # Get document links
                        self.logger.info(f"Fetching document links from {subsource.get_full_url()}")
                        
                        # Use JavaScript-compatible method if needed
                        if hasattr(subsource, 'use_javascript') and subsource.use_javascript:
                            # Get document links using scraper hook
                            if hasattr(subsource.extractor, 'document_links_xpath'):
                                links = scraper_hook.extract_document_links(
                                    url=subsource.get_full_url(),
                                    extractor_type='xpath',
                                    selector=subsource.extractor.document_links_xpath
                                )
                            else:
                                links = scraper_hook.extract_document_links(
                                    url=subsource.get_full_url(),
                                    extractor_type='css',
                                    selector=subsource.extractor.document_links_css
                                )
                        else:
                            # Use standard method from subsource
                            links = subsource.get_document_links()
                        
                        self.logger.info(f"Found {len(links)} document links")
                        
                        # Process each document link
                        found_existing_document = False
                        new_docs_count = 0
                        
                        for link_idx, link in enumerate(links, 1):
                            if limit and new_docs_count >= limit:  # Use local variable
                                self.logger.info(f"Reached document limit of {self.limit}, stopping")
                                break
                                
                            url = link['url']
                            title = link['title']
                            
                            self.logger.info(f"Processing document {link_idx}/{len(links)}: {title}")
                            
                            try:
                                # Check if already in database
                                doc_result = supabase.table("govt_documents").select("*").eq("url", url).execute()
                                existing_doc = None
                                
                                if doc_result.data and len(doc_result.data) > 0:
                                    existing_doc = Document.from_dict(doc_result.data[0])
                                    
                                if existing_doc:
                                    # If document exists and is not an error, we've hit older content
                                    if existing_doc.status != "error":
                                        self.logger.info(f"Document already exists: {url}")
                                        
                                        # Add ID to list if it's scraped
                                        if existing_doc.status == "scraped" and existing_doc.doc_id:
                                            scraped_doc_ids.append(existing_doc.doc_id)
                                        
                                        # If this is a successfully processed document,
                                        # stop processing this subsource as we've reached existing content
                                        if existing_doc.status in ["scraped", "processed"]:
                                            self.logger.info(f"Found existing document that's already processed. " +
                                                           f"Skipping remaining {len(links) - link_idx} documents " +
                                                           f"in this subsource (chronological optimization).")
                                            found_existing_document = True
                                            break
                                            
                                        # Otherwise continue with next link (e.g., if document had errors)
                                        continue
                                    else:
                                        # If it was an error, try to process it again
                                        self.logger.info(f"Re-processing previously errored document: {url}")
                                
                                # Create new document
                                doc = Document(
                                    url=url,
                                    title=title,
                                    source_name=source.name,
                                    subsource_name=subsource.name
                                )
                                # Force JavaScript mode for all documents
                                doc.use_javascript = True
                                
                                # Fetch content
                                self.logger.info(f"Fetching document: {url}")
                                if scraper_hook.fetch_document_content(doc, subsource.extractor, use_javascript=True):
                                    # Store in database
                                    doc_dict = doc.to_dict()
                                    now = datetime.now().isoformat()
                                    doc_dict["created_at"] = now
                                    doc_dict["updated_at"] = now
                                    
                                    # Remove ID for insertion
                                    doc_dict.pop("id", None)
                                    
                                    result = supabase.table("govt_documents").insert(doc_dict).execute()
                                    
                                    if result.data and len(result.data) > 0:
                                        doc_id = result.data[0]["id"]
                                        doc.doc_id = doc_id
                                        scraped_doc_ids.append(doc_id)
                                        self.logger.info(f"Document stored with ID: {doc_id}")
                                        new_docs_count += 1
                                    else:
                                        self.logger.error(f"Failed to store document in database: {url}")
                                else:
                                    self.logger.error(f"Failed to fetch content for: {url}, status: {doc.status}")
                                    
                            except Exception as e:
                                self.logger.error(f"Error processing document {url}: {str(e)}", exc_info=True)
                                continue
                        
                        # Log summary for this subsource
                        if found_existing_document:
                            self.logger.info(f"Completed subsource with {new_docs_count} new documents (stopped at existing document)")
                        else:
                            self.logger.info(f"Completed subsource with {new_docs_count} new documents (processed all links)")
                    
                    except Exception as e:
                        self.logger.error(f"Error processing subsource {subsource.name}: {str(e)}", exc_info=True)
                        continue
            
            self.logger.info(f"Successfully scraped {len(scraped_doc_ids)} documents")
            return scraped_doc_ids
            
        except Exception as e:
            self.logger.error(f"Error in scrape_documents: {str(e)}", exc_info=True)
            raise
        finally:
            # Ensure scraper is closed
            scraper_hook.close()