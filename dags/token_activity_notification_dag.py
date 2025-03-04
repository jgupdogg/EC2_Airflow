# dags/token_activity_notification_dag.py
"""
DAG to check for unusual token trading activity and send notifications via Slack.
This DAG queries Snowflake for recent swap data, identifies tokens with high activity,
and sends alerts when specific thresholds are exceeded.
"""
from datetime import datetime, timedelta
import logging
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# Configure logging
logger = logging.getLogger(__name__)

# DAG default arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
}

def branch_on_notification(**kwargs):
    """
    Branch based on the stored notifications message.
    If no notifications were triggered, skip sending a Slack alert.
    """
    ti = kwargs['ti']
    message = ti.xcom_pull(task_ids="store_notifications")
    logger.info("Branching based on notifications message: %s", message)
    if message.strip() == "No new whale notifications triggered.":
        return "skip_notification"
    else:
        return "send_notification"

@dag(
    dag_id='token_activity_notification_dag',
    default_args=default_args,
    description='Check for unusual token trading activity and send notifications',
    schedule_interval='*/10 * * * *',  # Run every 10 minutes
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=['token', 'notification', 'snowflake', 'slack'],
)
def token_activity_notification_dag():
    """
    DAG to check for unusual token trading activity and send notifications.
    """
    
    @task(task_id="check_whale_notifications")
    def check_whale_notifications(dag_interval: int = 10) -> list:
        """
        For each time interval, identify tokens with high activity.
        Includes robust error handling and proper type conversion.
        """
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # First check if the required table exists
        try:
            # Check if HELIUS_SWAPS exists and we can access it
            logger.info("Checking if HELIUS_SWAPS table exists and is accessible...")
            
            table_check_query = "SHOW TABLES LIKE 'HELIUS_SWAPS' IN SCHEMA DEV.BRONZE"
            table_result = snowflake_hook.get_pandas_df(table_check_query)
            
            if table_result.empty:
                logger.error("Table DEV.BRONZE.HELIUS_SWAPS doesn't exist or is not accessible.")
                # Try to show tables that do exist
                try:
                    all_tables_query = "SHOW TABLES IN SCHEMA DEV.BRONZE"
                    all_tables_result = snowflake_hook.get_pandas_df(all_tables_query)
                    logger.info(f"Available tables in DEV.BRONZE: {all_tables_result['name'].tolist() if not all_tables_result.empty else 'None'}")
                except Exception as table_e:
                    logger.error(f"Error listing tables: {table_e}")
                    
                return []  # Return empty list if table doesn't exist
            else:
                logger.info(f"Table HELIUS_SWAPS exists. Checking if we can query it...")
                # Try a simple count to see if we can access the data
                try:
                    count_query = "SELECT COUNT(*) FROM DEV.BRONZE.HELIUS_SWAPS"
                    count_result = snowflake_hook.get_first(count_query)
                    logger.info(f"HELIUS_SWAPS has {count_result[0]} rows")
                except Exception as count_e:
                    logger.error(f"Cannot query HELIUS_SWAPS: {count_e}")
                    return []
                
        except Exception as e:
            logger.error(f"Error checking for table existence: {e}")
            return []  # Return empty list on error
            
        # Define time intervals and thresholds
        intervals = {
            "30min": {"hours": 0.5, "threshold": 2},
            "1hour": {"hours": 1, "threshold": 3},
            "3hours": {"hours": 3, "threshold": 5}
        }

        candidates = []
        
        # Query template for token activity analysis - using correct case for tables
        swap_data_query_template = """
        WITH recent_tokens AS (
        SELECT DISTINCT SWAPFROMTOKEN AS token
        FROM DEV.BRONZE.HELIUS_SWAPS 
        WHERE TIMESTAMP > DATEADD(minute, -{dag_interval}, CURRENT_TIMESTAMP())
        UNION
        SELECT DISTINCT SWAPTOTOKEN AS token
        FROM DEV.BRONZE.HELIUS_SWAPS 
        WHERE TIMESTAMP > DATEADD(minute, -{dag_interval}, CURRENT_TIMESTAMP())
        ),
        sales AS (
        SELECT
            SWAPFROMTOKEN AS token,
            COUNT(DISTINCT USER_ADDRESS) AS num_users_sold
        FROM DEV.BRONZE.HELIUS_SWAPS  
        WHERE TIMESTAMP > DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
            AND SOURCE != 'PUMP_FUN'
            AND SWAPFROMTOKEN IN (SELECT token FROM recent_tokens)
        GROUP BY SWAPFROMTOKEN
        ),
        buys AS (
        SELECT
            SWAPTOTOKEN AS token,
            COUNT(DISTINCT USER_ADDRESS) AS num_users_bought
        FROM DEV.BRONZE.HELIUS_SWAPS 
        WHERE TIMESTAMP > DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
            AND SOURCE != 'PUMP_FUN'
            AND SWAPTOTOKEN IN (SELECT token FROM recent_tokens)
        GROUP BY SWAPTOTOKEN
        )
        SELECT
        COALESCE(s.token, b.token) AS token,
        COALESCE(s.num_users_sold, 0) AS num_users_sold,
        COALESCE(b.num_users_bought, 0) AS num_users_bought
        FROM sales s
        FULL OUTER JOIN buys b
        ON s.token = b.token
        """

        # Process each time interval
        for interval_label, params in intervals.items():
            hours = params["hours"]
            threshold = params["threshold"]
            
            # Format the query with specific interval parameters
            query = swap_data_query_template.format(hours=hours, dag_interval=dag_interval)
            logger.info(f"Running swap query for interval {interval_label} (last {hours} hours)")
            
            try:
                # Execute query and get results with error handling
                df_swap = snowflake_hook.get_pandas_df(query)
                
                if df_swap.empty:
                    logger.info(f"No swap data found for interval {interval_label}.")
                    continue

                # Ensure columns exist before conversion
                if 'num_users_bought' not in df_swap.columns:
                    logger.warning("Column 'num_users_bought' not found in results, adding with zeros")
                    df_swap['num_users_bought'] = 0
                if 'num_users_sold' not in df_swap.columns:
                    logger.warning("Column 'num_users_sold' not found in results, adding with zeros")
                    df_swap['num_users_sold'] = 0

                # Safer conversion to numeric with error handling
                try:
                    # First make sure we have a DataFrame column, not a scalar
                    if isinstance(df_swap['num_users_bought'], pd.Series):
                        df_swap['num_users_bought'] = pd.to_numeric(df_swap['num_users_bought'], errors='coerce').fillna(0).astype(int)
                    else:
                        # If it's not a Series (might be a scalar), convert directly
                        df_swap['num_users_bought'] = int(df_swap['num_users_bought']) if df_swap['num_users_bought'] else 0
                        
                    if isinstance(df_swap['num_users_sold'], pd.Series):
                        df_swap['num_users_sold'] = pd.to_numeric(df_swap['num_users_sold'], errors='coerce').fillna(0).astype(int)
                    else:
                        # If it's not a Series (might be a scalar), convert directly
                        df_swap['num_users_sold'] = int(df_swap['num_users_sold']) if df_swap['num_users_sold'] else 0
                except Exception as type_e:
                    logger.error(f"Error converting numeric types: {type_e}")
                    logger.info(f"DataFrame info: {df_swap.info()}")
                    logger.info(f"DataFrame head: {df_swap.head().to_string()}")
                    # Try alternative approach if conversion fails
                    for col in ['num_users_bought', 'num_users_sold']:
                        try:
                            df_swap[col] = [int(x) if x and pd.notnull(x) else 0 for x in df_swap[col]]
                        except Exception as alt_e:
                            logger.error(f"Alternative conversion for {col} failed: {alt_e}")
                            df_swap[col] = 0

                # Filter tokens where either buy count or sell count exceeds threshold
                df_filtered = df_swap[
                    (df_swap['num_users_bought'] > threshold) | (df_swap['num_users_sold'] > threshold)
                ]
                
                if df_filtered.empty:
                    logger.info(f"No tokens exceeded the threshold for interval {interval_label}.")
                    continue

                # Add qualifying tokens to candidates list
                for _, row in df_filtered.iterrows():
                    candidate = {
                        "timestamp": now_str,
                        "token": row['token'],
                        "time_interval": interval_label,
                        "num_users_bought": int(row['num_users_bought']),
                        "num_users_sold": int(row['num_users_sold'])
                    }
                    candidates.append(candidate)
                    
            except Exception as e:
                logger.error(f"Error running swap query for interval {interval_label}: {e}")
                # Continue with next interval instead of failing the entire task
                
        logger.info(f"Found {len(candidates)} candidate notifications")
        return candidates

    @task(task_id="store_notifications")
    def store_notifications(candidates: list) -> str:
        """
        Process notification candidates:
        - Fetch token metadata (symbol and name) for the candidate tokens
        - Filter out major tokens (SOL, USDC, etc.)
        - Check for and skip recent duplicate notifications
        - Store new notifications in Snowflake
        - Return a formatted message for Slack
        """
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        
        if not candidates:
            logger.info("No candidate notifications to store.")
            return "No new whale notifications triggered."

        # Gather candidate token addresses
        tokens = list({cand["token"] for cand in candidates})
        token_list_str = ", ".join(f"'{token}'" for token in tokens)
        
        # Query to get token metadata - using correct case
        token_details_query = f"""
        WITH latest_metadata AS (
        SELECT ADDRESS, SYMBOL, NAME,
                ROW_NUMBER() OVER (PARTITION BY ADDRESS ORDER BY TIMESTAMP DESC) as rn
        FROM DEV.BRONZE.TOKEN_METADATA_RAW
        WHERE ADDRESS IN ({token_list_str})
        )
        SELECT 
        ADDRESS,
        SYMBOL,
        NAME
        FROM latest_metadata 
        WHERE rn = 1
        """
        
        logger.info(f"Fetching token details for {len(tokens)} tokens.")
        
        try:
            df_token_details = snowflake_hook.get_pandas_df(token_details_query)
        except Exception as e:
            logger.error(f"Error fetching token details: {e}")
            df_token_details = pd.DataFrame(columns=['ADDRESS', 'SYMBOL', 'NAME'])
        
        if df_token_details.empty:
            logger.info("No token details found; cannot complete notifications insertion.")
            return "No new whale notifications triggered."

        # Safely convert candidates to DataFrame
        try:
            df_candidates = pd.DataFrame(candidates)
        except Exception as e:
            logger.error(f"Error converting candidates to DataFrame: {e}")
            return "Error processing notifications."

        # Safely merge with error handling
        try:
            df_merged = pd.merge(df_candidates, df_token_details, 
                            left_on="token", right_on="ADDRESS", how="left")
        except Exception as e:
            logger.error(f"Error merging DataFrames: {e}")
            logger.info(f"df_candidates columns: {df_candidates.columns.tolist()}")
            logger.info(f"df_token_details columns: {df_token_details.columns.tolist()}")
            return "Error processing notifications."

        # Handle missing columns
        if 'SYMBOL' not in df_merged.columns:
            logger.warning("SYMBOL column missing, adding empty column")
            df_merged['SYMBOL'] = None
        if 'NAME' not in df_merged.columns:
            logger.warning("NAME column missing, adding empty column")
            df_merged['NAME'] = None

        # Filter out banned tokens with safer handling
        banned_symbols = ["SOL", "USDT", "USDC", "WSOL", "WBTC"]
        try:
            # Convert symbols to uppercase for consistent comparison
            if df_merged['SYMBOL'].dtype == 'object':  # Check if it's string/object type
                df_merged = df_merged[~df_merged['SYMBOL'].str.upper().isin([s.upper() for s in banned_symbols])]
            else:
                # If not string type, use a different approach
                df_merged = df_merged[~df_merged['SYMBOL'].isin(banned_symbols)]
        except Exception as e:
            logger.error(f"Error filtering banned tokens: {e}")
            # Continue with unfiltered data
        
        if df_merged.empty:
            logger.info("All candidate notifications were filtered out due to banned symbols.")
            return "No new whale notifications triggered."

        # Rename 'token' to 'address' for consistency and select desired columns
        df_merged = df_merged.rename(columns={"token": "address"})
        
        # Safely select columns with error handling
        try:
            df_notifications = df_merged[['timestamp', 'address', 'SYMBOL', 'NAME', 'time_interval',
                                        'num_users_bought', 'num_users_sold']]
        except KeyError as e:
            logger.error(f"Missing columns when selecting: {e}")
            logger.info(f"Available columns: {df_merged.columns.tolist()}")
            # Create a minimal DataFrame with required columns
            df_notifications = pd.DataFrame({
                'timestamp': df_merged['timestamp'] if 'timestamp' in df_merged else [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * len(df_merged),
                'address': df_merged['address'] if 'address' in df_merged else ["unknown"] * len(df_merged),
                'SYMBOL': df_merged['SYMBOL'] if 'SYMBOL' in df_merged else ["unknown"] * len(df_merged),
                'NAME': df_merged['NAME'] if 'NAME' in df_merged else ["unknown"] * len(df_merged),
                'time_interval': df_merged['time_interval'] if 'time_interval' in df_merged else ["1hour"] * len(df_merged),
                'num_users_bought': df_merged['num_users_bought'] if 'num_users_bought' in df_merged else [0] * len(df_merged),
                'num_users_sold': df_merged['num_users_sold'] if 'num_users_sold' in df_merged else [0] * len(df_merged)
            })

        # Mappings for SQL intervals and human-readable text
        interval_sql_mapping = {
            "30min": "30 minutes",
            "1hour": "1 hour",
            "3hours": "3 hours"
        }
        interval_readable_mapping = {
            "30min": "30 min",
            "1hour": "1 hour",
            "3hours": "3 hours"
        }

        new_notifications = []
        
        # Check for recent duplicate notifications - using correct case
        for _, row in df_notifications.iterrows():
            try:
                symbol = row['SYMBOL'] if pd.notnull(row['SYMBOL']) else "unknown"
                time_interval = row['time_interval'] if pd.notnull(row['time_interval']) else "1hour"
                sql_interval = interval_sql_mapping.get(time_interval, time_interval)
                
                # Query to check for recent identical notification
                check_query = f"""
                SELECT NUM_USERS_BOUGHT, NUM_USERS_SOLD 
                FROM DEV.BRONZE.WHALE_NOTIFICATIONS 
                WHERE SYMBOL = '{symbol}'
                AND TIME_INTERVAL = '{time_interval}'
                AND TIMESTAMP > DATEADD(interval, '{sql_interval}', CURRENT_TIMESTAMP())
                ORDER BY TIMESTAMP DESC
                LIMIT 1
                """
                
                existing = snowflake_hook.get_pandas_df(check_query)
                
                if not existing.empty:
                    # Try to safely compare values
                    try:
                        existing_row = existing.iloc[0]
                        if (int(existing_row['NUM_USERS_BOUGHT']) == int(row['num_users_bought']) and
                            int(existing_row['NUM_USERS_SOLD']) == int(row['num_users_sold'])):
                            logger.info(f"Notification for {symbol} in interval {time_interval} already sent with same counts.")
                            continue
                    except Exception as comp_e:
                        logger.error(f"Error comparing counts: {comp_e}")
                        # Continue and potentially create duplicate notification
                        
                new_notifications.append(row.to_dict())
            except Exception as row_e:
                logger.error(f"Error processing notification row: {row_e}")
                # Skip this row and continue with others

        if not new_notifications:
            logger.info("No new notifications after checking against recent records.")
            return "No new whale notifications triggered."

        # Insert new notifications into Snowflake - using correct case
        success_count = 0
        for notification in new_notifications:
            try:
                # Ensure all values are present or provide defaults
                timestamp = notification.get('timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                address = notification.get('address', 'unknown')
                symbol = notification.get('SYMBOL', 'unknown')
                name = notification.get('NAME', 'unknown')
                time_interval = notification.get('time_interval', '1hour')
                num_users_bought = int(notification.get('num_users_bought', 0))
                num_users_sold = int(notification.get('num_users_sold', 0))
                
                insert_query = f"""
                INSERT INTO DEV.BRONZE.WHALE_NOTIFICATIONS ( 
                    TIMESTAMP, 
                    ADDRESS, 
                    SYMBOL, 
                    NAME, 
                    TIME_INTERVAL, 
                    NUM_USERS_BOUGHT, 
                    NUM_USERS_SOLD
                ) VALUES (
                    '{timestamp}',
                    '{address}',
                    '{symbol}',
                    '{name}',
                    '{time_interval}',
                    {num_users_bought},
                    {num_users_sold}
                )
                """
                snowflake_hook.run(insert_query)
                success_count += 1
            except Exception as insert_e:
                logger.error(f"Error inserting notification: {insert_e}")
                # Continue with next notification
        
        logger.info(f"Successfully inserted {success_count} of {len(new_notifications)} notifications")

        # Build a Slack message summarizing the notifications
        messages = []
        for notification in new_notifications:
            try:
                interval_msg = interval_readable_mapping.get(notification.get('time_interval', '1hour'), '1 hour')
                symbol = notification.get('SYMBOL', 'unknown')
                name = notification.get('NAME', 'unknown')
                token_info = f"{name} ({symbol})" if pd.notnull(name) and pd.notnull(symbol) else symbol
                num_bought = notification.get('num_users_bought', 0)
                num_sold = notification.get('num_users_sold', 0)
                
                messages.append(
                    f"Whale Alert: {token_info} saw {num_bought} buys and {num_sold} sells in the last {interval_msg}."
                )
            except Exception as msg_e:
                logger.error(f"Error formatting message: {msg_e}")
                # Add a simplified message instead
                messages.append("Whale Alert detected (error formatting details)")

        if not messages:
            return "Whale notifications processed but no message could be formatted."
            
        final_message = "\n".join(messages)
        logger.info(final_message)
        return final_message


    # Define task flow
    candidates = check_whale_notifications()
    notifications_message = store_notifications(candidates)
    
    branch = BranchPythonOperator(
        task_id='branch_notification',
        python_callable=branch_on_notification,
        provide_context=True
    )
    
    send_notification = SlackWebhookOperator(
        task_id='send_notification',
        slack_webhook_conn_id='slack_webhook',  # Must match your Airflow connection name
        message="{{ ti.xcom_pull(task_ids='store_notifications') }}",
        channel="#alpha-notifications"
    )
    
    skip_notification = EmptyOperator(task_id='skip_notification')
    
    # Set task dependencies
    notifications_message >> branch
    branch >> [send_notification, skip_notification]

# Instantiate the DAG
token_notification_dag = token_activity_notification_dag()