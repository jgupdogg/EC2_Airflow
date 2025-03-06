# dags/token_activity_notification_dag.py
from datetime import datetime, timedelta, timezone
import logging
import pandas as pd
from functools import lru_cache
from airflow.decorators import dag, task
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# Import utility functions - adjust import as needed based on your MWAA setup
try:
    # Try importing from utils package (if using plugins directory)
    from utils.token_utils import fetch_token_metadata_from_api
except ImportError:
    # Fallback to direct import (if module is in the same directory)
    from token_utils import fetch_token_metadata_from_api

logger = logging.getLogger(__name__)

# DAG default arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
}

# Global variable to store connection
_SNOWFLAKE_CONN = None

def get_snowflake_connection():
    """Get a single Snowflake connection to reuse across the task."""
    global _SNOWFLAKE_CONN
    
    if _SNOWFLAKE_CONN is None:
        try:
            hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
            _SNOWFLAKE_CONN = hook.get_conn()
            logger.info("Created new Snowflake connection")
        except Exception as e:
            logger.warning(f"Failed to use snowflake_default connection: {e}")
            try:
                hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
                _SNOWFLAKE_CONN = hook.get_conn()
                logger.info("Created new Snowflake connection using fallback")
            except Exception as e2:
                logger.error(f"Failed to connect to Snowflake: {e2}")
                raise ValueError("No valid Snowflake connection found.")
    
    return _SNOWFLAKE_CONN

def execute_snowflake_query(query, fetch=True):
    """Execute a query using a single connection."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(query)
        if fetch:
            columns = [col[0] for col in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            return df
        return None
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        logger.error(f"Failed query: {query}")
        if fetch:
            return pd.DataFrame()
        return None
    finally:
        cursor.close()
        # Note: We don't close the connection here, to reuse it

def branch_on_notification(**kwargs):
    """Branch based on whether notifications were triggered."""
    ti = kwargs['ti']
    message = ti.xcom_pull(task_ids="process_notifications")
    return "skip_notification" if message.strip() == "No new whale notifications triggered." else "send_notification"

@dag(
    dag_id='token_activity_notification_dag',
    default_args=default_args,
    description='Check for unusual token trading activity and send notifications',
    # Using a more frequent schedule since this is triggered by Lambda
    # This can be set to None if using external triggers only
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=['token', 'notification', 'snowflake', 'slack'],
)
def token_activity_notification_dag():
    
    @task
    def detect_whale_activity():
        """
        Identify tokens with unusual trading activity within defined time intervals
        focusing on the most recently traded tokens.
        """
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        lookback_minutes = 10  # Minutes to look back for recent tokens
        
        # Define time intervals and thresholds
        intervals = {
            "30min": {"hours": 0.5, "threshold": 2},
            "1hour": {"hours": 1, "threshold": 3},
            "3hours": {"hours": 3, "threshold": 5}
        }
        
        # First identify recently traded tokens
        recent_tokens_query = f"""
        SELECT DISTINCT token_address FROM (
            SELECT SWAPFROMTOKEN AS token_address FROM DEV.BRONZE.HELIUS_SWAPS 
            WHERE TIMESTAMP > DATEADD(minute, -{lookback_minutes}, CURRENT_TIMESTAMP())
            UNION ALL
            SELECT SWAPTOTOKEN AS token_address FROM DEV.BRONZE.HELIUS_SWAPS 
            WHERE TIMESTAMP > DATEADD(minute, -{lookback_minutes}, CURRENT_TIMESTAMP())
        )
        """
        
        try:
            recent_tokens_df = execute_snowflake_query(recent_tokens_query)
            # If no recent tokens, exit early
            if recent_tokens_df.empty:
                logger.info("No recent token activity detected.")
                return []
                
            # Create token list for IN clause
            token_list = list(recent_tokens_df['TOKEN_ADDRESS'])
            token_list_str = ", ".join(f"'{token}'" for token in token_list)
            
        except Exception as e:
            logger.error(f"Error fetching recent tokens: {e}")
            return []
        
        candidates = []
        
        # Check each interval for the recent tokens
        for interval_label, params in intervals.items():
            query = f"""
            WITH activity AS (
                SELECT
                    token_address,
                    SUM(CASE WHEN transaction_type = 'buy' THEN user_count ELSE 0 END) AS num_users_bought,
                    SUM(CASE WHEN transaction_type = 'sell' THEN user_count ELSE 0 END) AS num_users_sold,
                    MAX(last_notification) AS last_notification
                FROM (
                    -- Buy transactions
                    SELECT
                        SWAPTOTOKEN AS token_address,
                        'buy' AS transaction_type,
                        COUNT(DISTINCT USER_ADDRESS) AS user_count,
                        NULL AS last_notification
                    FROM DEV.BRONZE.HELIUS_SWAPS
                    WHERE TIMESTAMP > DATEADD(hour, -{params['hours']}, CURRENT_TIMESTAMP())
                        AND SOURCE != 'PUMP_FUN'
                        AND SWAPTOTOKEN IN ({token_list_str})
                    GROUP BY SWAPTOTOKEN
                    
                    UNION ALL
                    
                    -- Sell transactions
                    SELECT
                        SWAPFROMTOKEN AS token_address,
                        'sell' AS transaction_type,
                        COUNT(DISTINCT USER_ADDRESS) AS user_count,
                        NULL AS last_notification
                    FROM DEV.BRONZE.HELIUS_SWAPS
                    WHERE TIMESTAMP > DATEADD(hour, -{params['hours']}, CURRENT_TIMESTAMP())
                        AND SOURCE != 'PUMP_FUN'
                        AND SWAPFROMTOKEN IN ({token_list_str})
                    GROUP BY SWAPFROMTOKEN
                    
                    UNION ALL
                    
                    -- Most recent notification for each token in this interval
                    SELECT
                        ADDRESS AS token_address,
                        NULL AS transaction_type,
                        NULL AS user_count,
                        MAX(INSERTED_AT) AS last_notification
                    FROM DEV.BRONZE.WHALE_NOTIFICATIONS
                    WHERE ADDRESS IN ({token_list_str})
                      AND TIME_INTERVAL = '{interval_label}'
                    GROUP BY ADDRESS
                ) subquery
                GROUP BY token_address
            )
            SELECT
                token_address,
                COALESCE(num_users_bought, 0) AS num_users_bought,
                COALESCE(num_users_sold, 0) AS num_users_sold,
                last_notification
            FROM activity
            WHERE num_users_bought > {params['threshold']} OR num_users_sold > {params['threshold']}
            """
            
            try:
                df = execute_snowflake_query(query)
                
                if not df.empty:
                    for _, row in df.iterrows():
                        # Convert pandas Timestamp or NaT to a serializable format
                        last_notification = None
                        if pd.notna(row['LAST_NOTIFICATION']):
                            last_notification = row['LAST_NOTIFICATION'].isoformat()
                            
                        candidates.append({
                            "timestamp": now_str,
                            "token": row['TOKEN_ADDRESS'],
                            "time_interval": interval_label,
                            "num_users_bought": int(row['NUM_USERS_BOUGHT']),
                            "num_users_sold": int(row['NUM_USERS_SOLD']),
                            "last_notification": last_notification
                        })
            except Exception as e:
                logger.error(f"Error executing query for interval {interval_label}: {e}")
        
        return candidates
    
    @task
    def process_notifications(candidates):
        """
        Process candidate tokens:
        1. Get metadata from Snowflake
        2. Fetch missing metadata from API
        3. Update Snowflake with new metadata
        4. Filter out duplicate notifications based on cooldown period
        5. Generate notification messages
        """
        if not candidates:
            return "No new whale notifications triggered."
                
        # Extract token addresses
        tokens = list({cand["token"] for cand in candidates})
        token_list_str = ", ".join(f"'{token}'" for token in tokens)
        
        # 1. Get existing metadata from Snowflake
        metadata_query = f"""
        SELECT ADDRESS, SYMBOL, NAME, DECIMALS
        FROM DEV.BRONZE.TOKEN_METADATA
        WHERE ADDRESS IN ({token_list_str})
        """
        
        try:
            df_metadata = execute_snowflake_query(metadata_query)
        except Exception as e:
            logger.error(f"Error fetching token metadata: {e}")
            df_metadata = pd.DataFrame(columns=['ADDRESS', 'SYMBOL', 'NAME'])
        
        # 2. Identify tokens needing metadata
        found_addresses = set(df_metadata['ADDRESS'].tolist()) if not df_metadata.empty else set()
        missing_tokens = [token for token in tokens if token not in found_addresses]
        
        incomplete_tokens = []
        if not df_metadata.empty:
            # Find tokens with missing/empty symbol or name
            incomplete_mask = (
                df_metadata['SYMBOL'].isna() | 
                (df_metadata['SYMBOL'] == '') |
                df_metadata['NAME'].isna() | 
                (df_metadata['NAME'] == '')
            )
            incomplete_tokens = df_metadata.loc[incomplete_mask, 'ADDRESS'].tolist()
        
        # 3. Fetch metadata for missing/incomplete tokens
        tokens_to_fetch = list(set(missing_tokens + incomplete_tokens))
        
        if tokens_to_fetch:
            logger.info(f"Fetching metadata for {len(tokens_to_fetch)} tokens")
            token_data = fetch_token_metadata_from_api(tokens_to_fetch)
            
            # Update Snowflake with fetched metadata
            for address, metadata in token_data.items():
                try:
                    # Escape single quotes in string values
                    symbol = metadata.get("SYMBOL", "").replace("'", "''") if metadata.get("SYMBOL") else ""
                    name = metadata.get("NAME", "").replace("'", "''") if metadata.get("NAME") else ""
                    
                    upsert_query = f"""
                    MERGE INTO DEV.BRONZE.TOKEN_METADATA AS target
                    USING (SELECT 
                        '{address}' AS ADDRESS, 
                        '{symbol}' AS SYMBOL,
                        '{name}' AS NAME,
                        {metadata.get("DECIMALS", "NULL")} AS DECIMALS,
                        'solana' AS BLOCKCHAIN,
                        CURRENT_TIMESTAMP() AS INSERTED_AT
                    ) AS source
                    ON target.ADDRESS = source.ADDRESS
                    WHEN MATCHED THEN UPDATE SET
                        target.SYMBOL = source.SYMBOL,
                        target.NAME = source.NAME,
                        target.DECIMALS = source.DECIMALS,
                        target.INSERTED_AT = source.INSERTED_AT
                    WHEN NOT MATCHED THEN INSERT (
                        ADDRESS, SYMBOL, NAME, DECIMALS, BLOCKCHAIN, INSERTED_AT
                    ) VALUES (
                        source.ADDRESS, source.SYMBOL, source.NAME, 
                        source.DECIMALS, source.BLOCKCHAIN, source.INSERTED_AT
                    )
                    """
                    execute_snowflake_query(upsert_query, fetch=False)
                except Exception as e:
                    logger.error(f"Error updating metadata for {address}: {e}")
            
            # Refresh metadata from Snowflake after updates
            try:
                df_metadata = execute_snowflake_query(metadata_query)
            except Exception as e:
                logger.error(f"Error refreshing token metadata: {e}")
        
        # 4. Process candidates with metadata
        df_candidates = pd.DataFrame(candidates)
        
        if df_metadata.empty:
            # Use addresses as symbols if no metadata available
            df_candidates['SYMBOL'] = df_candidates['token']
            df_candidates['NAME'] = df_candidates['token']
            df_merged = df_candidates
        else:
            # Merge candidates with metadata
            df_merged = pd.merge(
                df_candidates, 
                df_metadata, 
                left_on="token", 
                right_on="ADDRESS", 
                how="left"
            )
            
            # Fill missing values
            for idx, row in df_merged.iterrows():
                if pd.isna(row['SYMBOL']) or row['SYMBOL'] == '':
                    df_merged.at[idx, 'SYMBOL'] = row['token'][:10]  # Truncate long addresses
                if pd.isna(row['NAME']) or row['NAME'] == '':
                    df_merged.at[idx, 'NAME'] = row['token'][:15]  # Truncate long addresses
        
        # 5. Filter out major tokens
        banned_symbols = ["SOL", "USDT", "USDC", "WSOL", "WBTC", "ETH"]
        try:
            df_merged = df_merged[~df_merged['SYMBOL'].str.upper().isin([s.upper() for s in banned_symbols])]
        except Exception as e:
            logger.error(f"Error filtering banned symbols: {e}")
            # Continue if filtering fails
            pass
        
        if df_merged.empty:
            return "No new whale notifications triggered."
        
        # 6. Check for recent duplicate notifications based on cooldown periods
        new_notifications = []
        interval_mapping = {"30min": "30 min", "1hour": "1 hour", "3hours": "3 hours"}
        
        # Define cooldown periods for each interval (in minutes)
        cooldown_periods = {
            "30min": 15,  # 15 minutes cooldown for 30min interval
            "1hour": 30,   # 30 minutes cooldown for 1hour interval
            "3hours": 60   # 60 minutes cooldown for 3hours interval
        }
        
        now = datetime.utcnow()
        
        for _, notification in df_merged.iterrows():
            interval = notification['time_interval']
            cooldown_minutes = cooldown_periods.get(interval, 30)
            
            # Check if we've sent a notification recently
            should_notify = True
            if notification['last_notification'] is not None:
                try:
                    # Parse the ISO format timestamp back to datetime with timezone awareness
                    last_notif_time = datetime.fromisoformat(notification['last_notification'].replace('Z', '+00:00'))
                    # Make sure now is also timezone aware by adding UTC timezone
                    now_with_tz = now.replace(tzinfo=timezone.utc)
                    time_since_last = now_with_tz - last_notif_time
                    
                    # Skip if within cooldown period
                    if time_since_last < timedelta(minutes=cooldown_minutes):
                        logger.info(f"Skipping notification for {notification['token']} in {interval} - " 
                                   f"last sent {time_since_last.total_seconds()/60:.1f} minutes ago")
                        should_notify = False
                except Exception as e:
                    logger.error(f"Error comparing timestamps: {e}")
                    # Continue with notification in case of timestamp parsing error
            
            if should_notify:
                # Add notification to Snowflake
                try:
                    insert_query = f"""
                    INSERT INTO DEV.BRONZE.WHALE_NOTIFICATIONS (
                        TIMESTAMP, ADDRESS, SYMBOL, NAME, TIME_INTERVAL, NUM_USERS_BOUGHT, NUM_USERS_SOLD
                    ) VALUES (
                        '{notification['timestamp']}',
                        '{notification['token']}',
                        '{notification['SYMBOL']}',
                        '{notification['NAME'].replace("'", "''")}',
                        '{notification['time_interval']}',
                        {int(notification['num_users_bought'])},
                        {int(notification['num_users_sold'])}
                    )
                    """
                    execute_snowflake_query(insert_query, fetch=False)
                    
                    # Format message for notification
                    interval_text = interval_mapping.get(notification['time_interval'], '1 hour')
                    
                    # Create token info
                    token_info = f"{notification['NAME']} ({notification['SYMBOL']})"
                    token_link = f"https://solscan.io/token/{notification['token']}"
                    
                    message = (f"🐳 *Whale Alert*: <{token_link}|{token_info}> saw "
                              f"{notification['num_users_bought']} buys and "
                              f"{notification['num_users_sold']} sells in the last {interval_text}.")
                    
                    new_notifications.append(message)
                except Exception as e:
                    logger.error(f"Error processing notification: {e}")
        
        # Clean up by closing the connection at the very end
        try:
            if _SNOWFLAKE_CONN is not None:
                _SNOWFLAKE_CONN.close()
                logger.info("Closed Snowflake connection at end of task")
        except Exception as e:
            logger.error(f"Error closing Snowflake connection: {e}")
            
        if not new_notifications:
            return "No new whale notifications triggered."
            
        return "\n".join(new_notifications)
    
    # Task flow
    whale_candidates = detect_whale_activity()
    notifications = process_notifications(whale_candidates)
    
    # Branching logic
    branch = BranchPythonOperator(
        task_id='branch_notification',
        python_callable=branch_on_notification,
        provide_context=True
    )
    
    # Notification tasks
    send_notification = SlackWebhookOperator(
        task_id='send_notification',
        slack_webhook_conn_id='slack_webhook',
        message="{{ ti.xcom_pull(task_ids='process_notifications') }}",
        channel="#alpha-notifications"
    )
    
    skip_notification = EmptyOperator(task_id='skip_notification')
    
    # Set dependencies
    notifications >> branch
    branch >> [send_notification, skip_notification]

# Instantiate the DAG
token_notification_dag = token_activity_notification_dag()