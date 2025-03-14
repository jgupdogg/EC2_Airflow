from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class TableVerificationOperator(BaseOperator):
    """
    Operator that verifies the existence of required tables in Supabase.
    
    :param supabase_conn_id: Connection ID for Supabase
    :type supabase_conn_id: str
    """
    
    @apply_defaults
    def __init__(self, supabase_conn_id='supabase_default', **kwargs):
        super().__init__(**kwargs)
        self.supabase_conn_id = supabase_conn_id
        self.logger = logging.getLogger(__name__)
    
    def execute(self, context):
        """
        Verify that necessary tables exist in Supabase.
        Raises AirflowException if tables don't exist.
        """
        from airflow.exceptions import AirflowException
        from plugins.hooks.supabase_hook import SupabaseHook
        
        hook = SupabaseHook(self.supabase_conn_id)
        
        self.logger.info("Verifying Supabase tables exist")
        if hook.setup_tables():
            self.logger.info("All required Supabase tables exist")
            return True
        else:
            error_msg = "Required tables don't exist in Supabase. Please run setup_supabase.py to generate SQL."
            self.logger.error(error_msg)
            raise AirflowException(error_msg)