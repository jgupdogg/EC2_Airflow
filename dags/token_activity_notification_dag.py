# dags/token_activity_notification_dag.py
from datetime import datetime, timedelta
import logging
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# Import utility functions
from utils.token_utils import fetch_token_metadata_from_api

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
    """Branch based on whether notifications were triggered."""
    ti = kwargs['ti']
    message = ti.xcom_pull(task_ids="process_notifications")
    return "skip_notification" if message.strip() == "No new whale notifications triggered." else "send_notification"

@dag(
    dag_id='token_activity_notification_dag',
    default_args=default_args,
    description='Check for unusual token trading activity and send notifications',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=['token', 'notification', 'snowflake', 'slack'],
)
def token_activity_notification_dag():
    
    @task
    def detect_whale_activity():
        """
        Identify tokens with unusual trading activity within defined time intervals.
        """
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        dag_interval = 10  # Minutes to look back for recent tokens
        
        # Define time intervals and thresholds
        intervals = {
            "30min": {"hours": 0.5, "threshold": 2},
            "1hour": {"hours": 1, "threshold": 3},
            "3hours": {"hours": 3, "threshold": 5}
        }
        
        candidates = []
        
        # Simplified query structure for each interval
        for interval_label, params in intervals.items():
            query = f"""
            WITH recent_tokens AS (
                SELECT DISTINCT SWAPFROMTOKEN AS token FROM DEV.BRONZE.HELIUS_SWAPS 
                WHERE TIMESTAMP > DATEADD(minute, -{dag_interval}, CURRENT_TIMESTAMP())
                UNION
                SELECT DISTINCT SWAPTOTOKEN AS token FROM DEV.BRONZE.HELIUS_SWAPS 
                WHERE TIMESTAMP > DATEADD(minute, -{dag_interval}, CURRENT_TIMESTAMP())
            ),
            activity AS (
                SELECT
                    token,
                    SUM(CASE WHEN transaction_type = 'buy' THEN user_count ELSE 0 END) AS num_users_bought,
                    SUM(CASE WHEN transaction_type = 'sell' THEN user_count ELSE 0 END) AS num_users_sold
                FROM (
                    SELECT
                        SWAPTOTOKEN AS token,
                        'buy' AS transaction_type,
                        COUNT(DISTINCT USER_ADDRESS) AS user_count
                    FROM DEV.BRONZE.HELIUS_SWAPS
                    WHERE TIMESTAMP > DATEADD(hour, -{params['hours']}, CURRENT_TIMESTAMP())
                        AND SOURCE != 'PUMP_FUN'
                        AND SWAPTOTOKEN IN (SELECT token FROM recent_tokens)
                    GROUP BY SWAPTOTOKEN
                    
                    UNION ALL
                    
                    SELECT
                        SWAPFROMTOKEN AS token,
                        'sell' AS transaction_type,
                        COUNT(DISTINCT USER_ADDRESS) AS user_count
                    FROM DEV.BRONZE.HELIUS_SWAPS
                    WHERE TIMESTAMP > DATEADD(hour, -{params['hours']}, CURRENT_TIMESTAMP())
                        AND SOURCE != 'PUMP_FUN'
                        AND SWAPFROMTOKEN IN (SELECT token FROM recent_tokens)
                    GROUP BY SWAPFROMTOKEN
                )
                GROUP BY token
            )
            SELECT
                token,
                COALESCE(num_users_bought, 0) AS num_users_bought,
                COALESCE(num_users_sold, 0) AS num_users_sold
            FROM activity
            WHERE num_users_bought > {params['threshold']} OR num_users_sold > {params['threshold']}
            """
            
            try:
                df = snowflake_hook.get_pandas_df(query)
                
                if not df.empty:
                    for _, row in df.iterrows():
                        candidates.append({
                            "timestamp": now_str,
                            "token": row['token'],
                            "time_interval": interval_label,
                            "num_users_bought": int(row['num_users_bought']),
                            "num_users_sold": int(row['num_users_sold'])
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
        4. Generate notification messages
        """
        if not candidates:
            return "No new whale notifications triggered."
        
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        
        # Extract token addresses
        tokens = list({cand["token"] for cand in candidates})
        token_list_str = ", ".join(f"'{token}'" for token in tokens)
        
        # 1. Get existing metadata from Snowflake
        metadata_query = f"""
        SELECT ADDRESS, SYMBOL, NAME
        FROM DEV.BRONZE.TOKEN_METADATA
        WHERE ADDRESS IN ({token_list_str})
        """
        
        try:
            df_metadata = snowflake_hook.get_pandas_df(metadata_query)
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
                    upsert_query = f"""
                    MERGE INTO DEV.BRONZE.TOKEN_METADATA AS target
                    USING (SELECT 
                        '{metadata["ADDRESS"]}' AS ADDRESS, 
                        '{metadata["SYMBOL"]}' AS SYMBOL,
                        '{metadata["NAME"].replace("'", "''")}' AS NAME,
                        {metadata["DECIMALS"] if metadata["DECIMALS"] is not None else 'NULL'} AS DECIMALS,
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
                    snowflake_hook.run(upsert_query)
                except Exception as e:
                    logger.error(f"Error updating metadata for {address}: {e}")
            
            # Refresh metadata from Snowflake after updates
            try:
                df_metadata = snowflake_hook.get_pandas_df(metadata_query)
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
                    df_merged.at[idx, 'SYMBOL'] = row['token']
                if pd.isna(row['NAME']) or row['NAME'] == '':
                    df_merged.at[idx, 'NAME'] = row['token']
        
        # 5. Filter out major tokens
        banned_symbols = ["SOL", "USDT", "USDC", "WSOL", "WBTC"]
        try:
            df_merged = df_merged[~df_merged['SYMBOL'].str.upper().isin([s.upper() for s in banned_symbols])]
        except:
            # Continue if filtering fails
            pass
        
        if df_merged.empty:
            return "No new whale notifications triggered."
        
        # 6. Check for recent duplicate notifications and store new ones
        new_notifications = []
        interval_mapping = {"30min": "30 min", "1hour": "1 hour", "3hours": "3 hours"}
        
        for _, notification in df_merged.iterrows():
            # Skip duplicates by checking Snowflake (simplified for brevity)
            # ... duplicate check logic ...
            
            # Add notification to Snowflake
            try:
                insert_query = f"""
                INSERT INTO DEV.BRONZE.WHALE_NOTIFICATIONS (
                    TIMESTAMP, ADDRESS, SYMBOL, NAME, TIME_INTERVAL, NUM_USERS_BOUGHT, NUM_USERS_SOLD
                ) VALUES (
                    '{notification['timestamp']}',
                    '{notification['token']}',
                    '{notification['SYMBOL']}',
                    '{notification['NAME']}',
                    '{notification['time_interval']}',
                    {int(notification['num_users_bought'])},
                    {int(notification['num_users_sold'])}
                )
                """
                snowflake_hook.run(insert_query)
                
                # Format message for notification
                interval = interval_mapping.get(notification['time_interval'], '1 hour')
                token_info = f"{notification['NAME']} ({notification['SYMBOL']})"
                message = f"Whale Alert: {token_info} saw {notification['num_users_bought']} buys and {notification['num_users_sold']} sells in the last {interval}."
                new_notifications.append(message)
            except Exception as e:
                logger.error(f"Error processing notification: {e}")
        
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