from airflow.hooks.base import BaseHook
from langchain.schema import Document as LCDocument
import logging
import hashlib
from datetime import datetime

class PineconeHook(BaseHook):
    """
    Hook for interacting with Pinecone vector database.
    
    :param pinecone_conn_id: Connection ID for Pinecone
    :type pinecone_conn_id: str
    """
    
    def __init__(self, pinecone_conn_id='pinecone_default', langchain_conn_id='langchain_default'):
        super().__init__()
        self.pinecone_conn_id = pinecone_conn_id
        self.langchain_conn_id = langchain_conn_id
        self._vector_store = None
        self.logger = logging.getLogger(__name__)
    
    def get_conn(self, index_name="govt-scrape-index", namespace="govt-content"):
        """Returns a Pinecone vector store."""
        
        from langchain_hook import LangChainHook
        if self._vector_store is not None:
            return self._vector_store
            
        # Get Pinecone credentials
        conn = self.get_connection(self.pinecone_conn_id)
        pinecone_api_key = conn.password
        
        if not pinecone_api_key:
            raise ValueError("Pinecone API key is required")
        
        # Get embedding model from LangChain hook
        langchain_hook = LangChainHook(self.langchain_conn_id)
        embedding_model = langchain_hook.get_embedding_model()
        
        try:
            # Initialize Pinecone client
            import pinecone
            pc = pinecone.Pinecone(api_key=pinecone_api_key)
            
            # Try multiple import paths based on what's available
            vector_store_class = None
            
            # Option 1: Try to import from langchain-pinecone package (newest)
            try:
                from langchain_pinecone import PineconeVectorStore
                vector_store_class = PineconeVectorStore
                self.logger.info("Using PineconeVectorStore from langchain_pinecone")
            except ImportError:
                self.logger.warning("Could not import PineconeVectorStore from langchain_pinecone")
            
            # Option 2: Try newer Pinecone class from langchain_pinecone
            if not vector_store_class:
                try:
                    from langchain_pinecone import Pinecone as PineconeStore
                    vector_store_class = PineconeStore
                    self.logger.info("Using Pinecone from langchain_pinecone")
                except ImportError:
                    self.logger.warning("Could not import Pinecone from langchain_pinecone")
            
            # Option 3: Try community package
            if not vector_store_class:
                try:
                    from langchain_community.vectorstores import Pinecone as PineconeStore
                    vector_store_class = PineconeStore
                    self.logger.info("Using Pinecone from langchain_community.vectorstores")
                except ImportError:
                    self.logger.warning("Could not import Pinecone from langchain_community.vectorstores")
            
            # Option 4: Try old langchain package (legacy)
            if not vector_store_class:
                try:
                    from langchain.vectorstores import Pinecone as PineconeStore
                    vector_store_class = PineconeStore
                    self.logger.info("Using Pinecone from langchain.vectorstores (legacy)")
                except ImportError:
                    self.logger.warning("Could not import Pinecone from langchain.vectorstores")
            
            if not vector_store_class:
                raise ImportError("Could not find a compatible Pinecone vector store class in any package")
            
            # Check if index exists
            index_exists = False
            try:
                indexes = pc.list_indexes()
                index_exists = index_name in [idx.name for idx in indexes]
            except Exception as e:
                self.logger.warning(f"Error checking indexes using new API: {e}")
                # Handle different versions of Pinecone SDK
                try:
                    existing_indexes = pc.list_indexes().names()
                    index_exists = index_name in existing_indexes
                except Exception as e2:
                    self.logger.warning(f"Error checking indexes using fallback method: {e2}")
                    # Just try to create it anyway and handle any errors there
            
            # Create index if it doesn't exist
            if not index_exists:
                self.logger.warning(f"Index {index_name} not found, creating...")
                # Create the index with appropriate dimensions for the embedding model
                try:
                    from pinecone import ServerlessSpec
                    pc.create_index(
                        name=index_name,
                        dimension=1536,  # Dimension for text-embedding-3-small
                        metric="cosine",
                        spec=ServerlessSpec(
                            cloud="aws",
                            region="us-west-2"
                        )
                    )
                except Exception as e:
                    self.logger.warning(f"Error creating index with ServerlessSpec: {e}")
                    # Fallback for older versions
                    try:
                        pc.create_index(
                            name=index_name,
                            dimension=1536,  # Dimension for text-embedding-3-small
                            metric="cosine"
                        )
                    except Exception as e2:
                        self.logger.error(f"Error creating index with fallback method: {e2}")
                        raise
            
            # Initialize vector store
            # Handle different constructor signatures for different versions
            try:
                # Try to get the Pinecone index object first
                index = pc.Index(index_name)
                
                # Try init with index object (newer versions)
                self._vector_store = vector_store_class(
                    index=index,
                    embedding=embedding_model,
                    text_key="content",
                    namespace=namespace
                )
            except Exception as e:
                self.logger.warning(f"Error initializing with index object: {e}")
                
                # Try init with index_name (older versions)
                try:
                    self._vector_store = vector_store_class(
                        index_name=index_name,
                        embedding=embedding_model,
                        text_key="content",
                        namespace=namespace
                    )
                except Exception as e2:
                    self.logger.warning(f"Error initializing with index_name: {e2}")
                    
                    # Last resort, try the simplest constructor
                    self._vector_store = vector_store_class(
                        embedding=embedding_model,
                        index_name=index_name
                    )
            
            self.logger.info(f"Successfully initialized Pinecone vector store with index: {index_name}")
            return self._vector_store
            
        except Exception as e:
            self.logger.error(f"Error connecting to Pinecone: {e}")
            raise
        
    def store_embedding(self, document):
        """Store document embedding in Pinecone."""
        if not document.summary:
            raise ValueError("Document has no summary to embed")
        
        vector_store = self.get_conn()
        
        # Create a unique ID
        embedding_id = f"gov-{hashlib.md5(document.url.encode()).hexdigest()[:12]}"
        
        try:
            # Create LangChain Document
            lc_doc = LCDocument(
                page_content=document.summary,
                metadata={
                    "url": document.url,
                    "title": document.title,
                    "source": document.source_name, 
                    "subsource": document.subsource_name,
                    "doc_id": document.doc_id,
                    "processed_at": datetime.now().isoformat()
                }
            )
            
            # Store in vector store
            ids = vector_store.add_documents([lc_doc], ids=[embedding_id])
            
            self.logger.info(f"Successfully stored document in vector store with ID: {ids[0]}")
            return ids[0]
        
        except Exception as e:
            self.logger.error(f"Error storing embedding: {e}")
            raise
    
    def search_similar_documents(self, query, k=5):
        """Search for documents similar to the query."""
        vector_store = self.get_conn()
        
        try:
            results = vector_store.similarity_search_with_score(query, k=k)
            
            # Format results
            formatted_results = []
            for doc, score in results:
                formatted_results.append({
                    "title": doc.metadata.get("title", "Untitled"),
                    "url": doc.metadata.get("url", ""),
                    "source": doc.metadata.get("source", ""),
                    "subsource": doc.metadata.get("subsource", ""),
                    "summary": doc.page_content,
                    "similarity_score": score
                })
            
            return formatted_results
            
        except Exception as e:
            self.logger.error(f"Error searching similar documents: {e}")
            return []
    
    def test_connection(self):
        """Test the Pinecone connection."""
        try:
            vector_store = self.get_conn()
            # Try a simple query
            results = vector_store.similarity_search_with_score("test query", k=1)
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)