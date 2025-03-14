from airflow.hooks.base import BaseHook
from supabase import create_client, Client
import logging

class SupabaseHook(BaseHook):
    """
    Hook for interacting with Supabase.
    
    :param supabase_conn_id: Connection ID for Supabase
    :type supabase_conn_id: str
    """
    
    def __init__(self, supabase_conn_id='supabase_default'):
        super().__init__()
        self.supabase_conn_id = supabase_conn_id
        self.client = None
        self.logger = logging.getLogger(__name__)
    
    def get_conn(self):
        """Returns a Supabase client."""
        if self.client is not None:
            return self.client
            
        conn = self.get_connection(self.supabase_conn_id)
        supabase_url = conn.host
        supabase_key = conn.password
        
        if not supabase_url or not supabase_key:
            raise ValueError("Supabase URL and API key are required")
        
        try:
            self.client = create_client(supabase_url, supabase_key)
            self.logger.info("Successfully initialized Supabase client")
            return self.client
        except Exception as e:
            self.logger.error(f"Error connecting to Supabase: {e}")
            raise
    
    def test_connection(self):
        """Test the Supabase connection by making a simple query."""
        client = self.get_conn()
        try:
            # Try to get a single row from a table
            result = client.table("govt_documents").select("id").limit(1).execute()
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
    
    def setup_tables(self):
        """Verify that necessary tables exist in Supabase."""
        client = self.get_conn()
        try:
            # Check each required table
            client.table("govt_documents").select("id").limit(1).execute()
            client.table("govt_sources").select("id").limit(1).execute()
            client.table("govt_subsources").select("id").limit(1).execute()
            return True
        except Exception as e:
            self.logger.error(f"Error verifying tables: {e}")
            return False