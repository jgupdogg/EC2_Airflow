# dags/bitcoin_dag.py
"""
DAG to fetch Bitcoin data from FMP API and store in Snowflake.
"""
from datetime import datetime, timedelta
import json
import logging
from typing import Dict, Any, List

from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models.baseoperator import chain

# DAG default arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

@dag(
    dag_id="bitcoin_price_to_snowflake",
    description="Fetch Bitcoin price data from FMP API and store in Snowflake",
    schedule_interval="5 * * * *",  # Every hour at 5 min past the hour
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bitcoin", "crypto", "snowflake", "etl"],
    default_args=default_args,
)
def bitcoin_price_etl():
    """
    ETL pipeline to fetch Bitcoin price data and store it in Snowflake.
    
    This DAG uses the TaskFlow API pattern for cleaner code and implicit
    data passing between tasks.
    """
    
    @task(task_id="fetch_bitcoin_data")
    def fetch_bitcoin_data() -> List[Dict[str, Any]]:
        """
        Fetch Bitcoin price data from the Financial Modeling Prep API.
        """
        # Create the hook
        hook = HttpHook(method="GET", http_conn_id="fmp_api")
        
        # Get connection to extract API key
        conn = hook.get_connection("fmp_api")
        extra = conn.extra_dejson
        api_key = extra.get("apikey", "")
        
        # Use just the endpoint path
        endpoint = f"/api/v3/historical-chart/1hour/BTCUSD"
        
        # Pass parameters as a dictionary
        params = {"apikey": api_key}
        
        # Run the request
        response = hook.run(
            endpoint=endpoint,
            data=params,
            headers={"Content-Type": "application/json"},
        )
        
        # Parse JSON response
        data = response.json()
        
        logging.info(f"Fetched {len(data)} records from API")
        return data
        
    @task(task_id="prepare_data_for_snowflake")
    def prepare_data_for_snowflake(data: List[Dict[str, Any]]) -> str:
        """
        Process the API response and return a JSON string safe for Snowflake insertion.
        """
        # If response is not a list, wrap it in a list
        if not isinstance(data, list):
            data = [data]
            
        # Convert API response to a JSON string
        raw_json = json.dumps(data)
        
        # Replace any single quotes with double quotes to avoid SQL injection
        safe_json = raw_json.replace("'", "''")
        
        logging.info(f"Prepared {len(data)} records for insertion")
        return safe_json
    
    @task(task_id="load_to_snowflake")
    def load_to_snowflake(safe_json: str) -> str:
        """
        Store the prepared JSON data in Snowflake raw table.
        """
        # Connect to Snowflake using the connection we set up
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        
        # Insert the raw JSON string into Snowflake
        insert_query = f"""
        INSERT INTO DEV.BRONZE.BTC_HISTORIC 
        (RAW_DATA)
        SELECT PARSE_JSON('{safe_json}');
        """
        
        try:
            snowflake_hook.run(insert_query)
            record_count = len(json.loads(safe_json.replace("''", "'")))
            logging.info(f"Successfully stored {record_count} records in Snowflake")
            return f"Successfully stored {record_count} records in Snowflake"
        except Exception as e:
            logging.error(f"Error storing data in Snowflake: {str(e)}")
            raise
    
    # Define the task dependencies
    raw_data = fetch_bitcoin_data()
    prepared_data = prepare_data_for_snowflake(raw_data)
    load_result = load_to_snowflake(prepared_data)

# Create the DAG instance
bitcoin_etl_dag = bitcoin_price_etl()