# dags/snowflake_debug_dag.py
"""
Minimal DAG to debug Snowflake connection and table access issues.
"""
from datetime import datetime, timedelta
import logging
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Configure logging
logger = logging.getLogger(__name__)

# DAG default arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": True,
    "email_on_retry": False,
}

@dag(
    dag_id='snowflake_debug_dag',
    default_args=default_args,
    description='Debug Snowflake connection and table access issues',
    schedule_interval='@hourly',
    start_date=datetime(2025, 3, 4),
    catchup=False,
    tags=['snowflake', 'debug'],
)
def snowflake_debug_dag():
    """
    DAG to debug Snowflake connection and table access issues.
    """
    
    @task(task_id="test_connection")
    def test_connection():
        """
        Test the Snowflake connection and print detailed information.
        """
        logger.info("Testing Snowflake connection...")
        
        try:
            # Get the connection
            snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
            connection = snowflake_hook.get_connection(conn_id="snowflake_default")
            
            # Log full connection details
            conn_details = {
                "conn_id": connection.conn_id,
                "conn_type": connection.conn_type,
                "host": connection.host,
                "schema": connection.schema,
                "login": connection.login,
                "port": connection.port,
                "account": connection.extra_dejson.get("account"),
                "warehouse": connection.extra_dejson.get("warehouse"),
                "database": connection.extra_dejson.get("database"),
                "role": connection.extra_dejson.get("role"),
                "region": connection.extra_dejson.get("region"),
                "insecure_mode": connection.extra_dejson.get("insecure_mode"),
            }
            logger.info(f"Connection details: {conn_details}")
            
            # Test with a simple query
            user_info = snowflake_hook.get_first("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
            logger.info(f"Connected as User: {user_info[0]}, Role: {user_info[1]}, DB: {user_info[2]}, Schema: {user_info[3]}, Warehouse: {user_info[4]}")
            
            return "Connection successful"
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return f"Connection failed: {e}"
    
    @task(task_id="list_tables")
    def list_tables():
        """
        List all tables in the schema.
        """
        logger.info("Listing tables in schema...")
        
        try:
            snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
            
            # Get tables
            tables_query = "SHOW TABLES IN SCHEMA DEV.BRONZE"
            tables_df = snowflake_hook.get_pandas_df(tables_query)
            
            if not tables_df.empty:
                table_list = tables_df['name'].tolist()
                logger.info(f"Tables in schema: {table_list}")
                return table_list
            else:
                logger.warning("No tables found in schema")
                return []
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return f"Error: {e}"
    
    @task(task_id="check_helius_swaps")
    def check_helius_swaps():
        """
        Try different ways to access the HELIUS_SWAPS table.
        """
        logger.info("Checking HELIUS_SWAPS table...")
        
        results = {}
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        
        # Try different case variations and quoting
        variations = [
            "SELECT COUNT(*) FROM DEV.BRONZE.HELIUS_SWAPS",
            "SELECT COUNT(*) FROM DEV.BRONZE.helius_swaps",
            "SELECT COUNT(*) FROM DEV.BRONZE.\"HELIUS_SWAPS\"",
            "SELECT COUNT(*) FROM DEV.BRONZE.\"helius_swaps\"",
            "SELECT COUNT(*) FROM DEV.\"BRONZE\".HELIUS_SWAPS",
            "SELECT COUNT(*) FROM \"DEV\".\"BRONZE\".\"HELIUS_SWAPS\"",
        ]
        
        for query in variations:
            try:
                logger.info(f"Trying query: {query}")
                result = snowflake_hook.get_first(query)
                results[query] = f"Success: {result[0]} rows"
                logger.info(f"Query succeeded: {result[0]} rows")
            except Exception as e:
                results[query] = f"Failed: {str(e)}"
                logger.error(f"Query failed: {str(e)}")
        
        # Try to describe the table
        try:
            desc_query = "DESC TABLE DEV.BRONZE.HELIUS_SWAPS"
            desc_df = snowflake_hook.get_pandas_df(desc_query)
            results["describe"] = f"Success: {desc_df['name'].tolist()}"
            logger.info(f"Describe succeeded: {desc_df['name'].tolist()}")
        except Exception as e:
            results["describe"] = f"Failed: {str(e)}"
            logger.error(f"Describe failed: {str(e)}")
        
        return results
        
    @task(task_id="format_results")
    def format_results(conn_result, tables, helius_check):
        """
        Format the results for Slack notification.
        """
        message_parts = ["*Snowflake Debug Results*\n"]
        
        # Connection result
        message_parts.append(f"*Connection Test*: {conn_result}")
        
        # Tables list
        message_parts.append("\n*Tables in Schema*:")
        if isinstance(tables, list):
            if tables:
                message_parts.append(", ".join(tables))
            else:
                message_parts.append("No tables found")
        else:
            message_parts.append(f"Error: {tables}")
        
        # HELIUS_SWAPS check
        message_parts.append("\n*HELIUS_SWAPS Access Tests*:")
        for query, result in helius_check.items():
            message_parts.append(f"• {query}: {result}")
        
        return "\n".join(message_parts)
    
    # Define task flow
    conn_result = test_connection()
    tables = list_tables()
    helius_check = check_helius_swaps()
    
    message = format_results(conn_result, tables, helius_check)
    
    send_results = SlackWebhookOperator(
        task_id='send_results',
        slack_webhook_conn_id='slack_webhook',
        message="{{ ti.xcom_pull(task_ids='format_results') }}",
        channel="#data-monitoring"
    )
    
    # Set task dependencies
    conn_result >> tables >> helius_check >> message >> send_results

# Instantiate the DAG
debug_dag = snowflake_debug_dag()