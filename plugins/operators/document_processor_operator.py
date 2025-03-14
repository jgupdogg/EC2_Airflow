from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

class DocumentProcessorOperator(BaseOperator):
    """
    Operator that processes documents with AI:
    1. Generate summaries
    2. Create embeddings
    3. Store in vector database
    4. Extract entities and add to knowledge graph
    
    :param doc_ids: List of document IDs to process (optional)
    :type doc_ids: List[str]
    :param supabase_conn_id: Connection ID for Supabase
    :type supabase_conn_id: str
    :param langchain_conn_id: Connection ID for LangChain
    :type langchain_conn_id: str
    :param pinecone_conn_id: Connection ID for Pinecone
    :type pinecone_conn_id: str
    :param neo4j_conn_id: Connection ID for Neo4j
    :type neo4j_conn_id: str
    :param limit: Maximum number of documents to process
    :type limit: int
    :param enable_kg: Enable knowledge graph processing
    :type enable_kg: bool
    """
    
    @apply_defaults
    def __init__(
        self,
        doc_ids: Optional[List[str]] = None,
        supabase_conn_id: str = 'supabase_default',
        langchain_conn_id: str = 'anthropic_default',
        pinecone_conn_id: str = 'pinecone_default',
        neo4j_conn_id: str = 'neo4j_default',
        limit: int = 20,
        enable_kg: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.doc_ids = doc_ids
        self.supabase_conn_id = supabase_conn_id
        self.langchain_conn_id = langchain_conn_id
        self.pinecone_conn_id = pinecone_conn_id
        self.neo4j_conn_id = neo4j_conn_id
        self.limit = limit
        self.enable_kg = enable_kg
        self.logger = logging.getLogger(__name__)
    
    def execute(self, context):
        """
        Process documents with AI.
        
        Returns:
            List of processed document IDs
        """
        from hooks.supabase_hook import SupabaseHook
        from hooks.langchain_hook import LangChainHook
        from hooks.pinecone_hook import PineconeHook
        from hooks.neo4j_hook import Neo4jHook
        from utils.document import Document
        from utils.entity_extractor import EntityExtractor
        
        # Initialize hooks
        supabase_hook = SupabaseHook(self.supabase_conn_id)
        langchain_hook = LangChainHook(self.langchain_conn_id)
        pinecone_hook = PineconeHook(
            pinecone_conn_id=self.pinecone_conn_id,
            langchain_conn_id=self.langchain_conn_id
        )
        
        # Get documents to process
        supabase = supabase_hook.get_conn()
        
        if self.doc_ids:
            # Process specific documents by ID
            self.logger.info(f"Processing {len(self.doc_ids)} specific documents")
            documents = []
            
            for doc_id in self.doc_ids[:self.limit]:
                result = supabase.table("govt_documents").select("*").eq("id", doc_id).execute()
                if result.data and len(result.data) > 0:
                    documents.append(Document.from_dict(result.data[0]))
        else:
            # Get unprocessed documents
            self.logger.info(f"Fetching up to {self.limit} unprocessed documents")
            result = supabase.table("govt_documents").select("*").eq(
                "status", "scraped"
            ).order("scrape_time").limit(self.limit).execute()
            
            documents = [Document.from_dict(doc) for doc in result.data] if result.data else []
        
        if not documents:
            self.logger.info("No documents to process")
            return []
        
        self.logger.info(f"Processing {len(documents)} documents")
        processed_doc_ids = []
        
        # Initialize Neo4j hook if knowledge graph is enabled
        neo4j_hook = None
        try:
            neo4j_hook = Neo4jHook(self.neo4j_conn_id)
            # Setup constraints
            neo4j_hook.setup_constraints()
            self.logger.info("Successfully initialized Neo4j knowledge graph")
        except Exception as e:
            self.logger.warning(f"Failed to initialize Neo4j: {e}. Knowledge graph features will be disabled.")
            neo4j_hook = None
        
        # Create entity extractor if knowledge graph is enabled
        entity_extractor = None
        if neo4j_hook:
            entity_extractor = EntityExtractor(langchain_hook.get_llm())
        
        # Process each document
        for idx, doc in enumerate(documents, 1):
            self.logger.info(f"Processing document {idx}/{len(documents)}: {doc.url}")
            
            try:
                # Skip if already processed
                if doc.embedding_id and doc.status == "processed":
                    self.logger.info(f"Document already processed with embedding_id: {doc.embedding_id}")
                    if doc.doc_id:
                        processed_doc_ids.append(doc.doc_id)
                    continue
                
                # Generate summary if needed
                if not doc.summary:
                    doc.summary = langchain_hook.generate_summary(doc)
                    self.logger.info(f"Generated summary for document: {doc.url}")
                
                # Create and store embedding
                doc.embedding_id = pinecone_hook.store_embedding(doc)
                
                # Extract entities and add to knowledge graph if enabled
                if neo4j_hook and entity_extractor:
                    self.logger.info(f"Extracting entities from document: {doc.url}")
                    extraction_result = entity_extractor.extract_entities(doc)
                    
                    if extraction_result.get("entities") or extraction_result.get("relationships"):
                        # Add document to Neo4j
                        neo4j_hook.run_query("""
                        MERGE (d:Document {url: $url})
                        ON CREATE SET 
                            d.title = $title,
                            d.source_name = $source_name,
                            d.subsource_name = $subsource_name,
                            d.created_at = $created_at,
                            d.doc_id = $doc_id
                        ON MATCH SET
                            d.title = $title,
                            d.updated_at = $updated_at
                        """, {
                            "url": doc.url,
                            "title": doc.title,
                            "source_name": doc.source_name,
                            "subsource_name": doc.subsource_name,
                            "created_at": datetime.now().isoformat(),
                            "updated_at": datetime.now().isoformat(),
                            "doc_id": str(doc.doc_id)
                        })
                        
                        # Add entities
                        for entity in extraction_result.get("entities", []):
                            if not entity.get("canonical_name") or not entity.get("entity_type"):
                                continue
                                
                            # Create canonical entity node
                            neo4j_hook.run_query("""
                            MERGE (e:Entity {canonical_name: $canonical_name})
                            ON CREATE SET 
                                e.type = $entity_type,
                                e.created_at = $created_at
                            ON MATCH SET
                                e.type = $entity_type,
                                e.updated_at = $updated_at
                            """, {
                                "canonical_name": entity["canonical_name"],
                                "entity_type": entity["entity_type"],
                                "created_at": datetime.now().isoformat(),
                                "updated_at": datetime.now().isoformat()
                            })
                            
                            # Create mention and relationships
                            mention_text = entity.get("mention", entity["canonical_name"])
                            neo4j_hook.run_query("""
                            MATCH (e:Entity {canonical_name: $canonical_name})
                            MATCH (d:Document {url: $doc_url})
                            MERGE (m:Mention {
                                text: $mention,
                                document_url: $doc_url
                            })
                            MERGE (m)-[:REFERS_TO]->(e)
                            MERGE (m)-[:APPEARS_IN]->(d)
                            """, {
                                "canonical_name": entity["canonical_name"],
                                "mention": mention_text,
                                "doc_url": doc.url
                            })
                        
                        # Add relationships
                        for relationship in extraction_result.get("relationships", []):
                            if not relationship.get("source_canonical") or not relationship.get("target_canonical"):
                                continue
                                
                            relation = relationship.get("relation", "RELATED_TO")
                            # Create relationship
                            neo4j_hook.run_query(
                                f"""
                                MATCH (source:Entity {{canonical_name: $source_canonical}})
                                MATCH (target:Entity {{canonical_name: $target_canonical}})
                                MERGE (source)-[r:{relation}]->(target)
                                SET r.updated_at = $updated_at,
                                    r.document_url = $doc_url,
                                    r.document_id = $doc_id
                                """, {
                                    "source_canonical": relationship["source_canonical"],
                                    "target_canonical": relationship["target_canonical"],
                                    "doc_url": doc.url,
                                    "doc_id": str(doc.doc_id) if doc.doc_id else None,
                                    "updated_at": datetime.now().isoformat()
                                }
                            )
                        
                        self.logger.info(f"Added document and entities to knowledge graph: {doc.url}")
                
                # Update document status
                doc.status = "processed"
                doc.process_time = datetime.now()
                
                # Store updated document in Supabase
                doc_dict = doc.to_dict()
                doc_dict["updated_at"] = datetime.now().isoformat()
                
                supabase.table("govt_documents").update(doc_dict).eq("id", doc.doc_id).execute()
                
                processed_doc_ids.append(doc.doc_id)
                self.logger.info(f"Successfully processed document: {doc.url}")
                
            except Exception as e:
                self.logger.error(f"Error processing document {doc.url}: {e}", exc_info=True)
                # Update document status to error
                doc.status = "error_processing"
                supabase.table("govt_documents").update({
                    "status": "error_processing",
                    "updated_at": datetime.now().isoformat()
                }).eq("id", doc.doc_id).execute()
        
        # Close Neo4j connection if used
        if neo4j_hook:
            neo4j_hook.close()
            
        self.logger.info(f"Successfully processed {len(processed_doc_ids)}/{len(documents)} documents")
        return processed_doc_ids