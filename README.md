# 🐳 Solana Whale Alert Pipeline

A real-time monitoring system for detecting and notifying about significant trading activity on Solana tokens.

## Overview

This pipeline monitors trading activity on the Solana blockchain, detecting "whale" movements (significant trading volume by multiple unique wallets) for tokens, and sending alerts to Slack channels. The system is designed to provide early signals for tokens experiencing unusual trading activity.

## Architecture

![Architecture Diagram](https://mermaid.ink/img/pako:eNqFkk1vgzAMhv9KlBOVKIVC-dhpp0k77bDDLlMPIXFBahqlqdqq_PeFlrVat80XO8_rxHYceCcZcMJ7xXYSBcm-iXhyEERiLyBWmjyCQCRxIEiVBfGaQ75UO4VcYAJRxNOJzyeSM70DrbBQrbTQQNzm8NmkHlQhpY72bPt6Qb_CaYE71oF2qM16yqZxm9tGGwZZc6UbNM1NG3KpRIs5tLjq5lfHq0QudlCo2pKyHzuWMh_VJTUGmZQ40jZlOHxXqhnHpsFVrKRuxw5cVX_qRMtO82_9LI6Tx0U6-Uifddf83VGz4btXhjkPb2xbW_D95OrMbzVb3FpOdw7lbhSM5aCsjYPQaEo4WGtHVNDBa-gZD2rM4TTTX0P8GZzWfnyfJuV-Q8aZwAksOXQ_L9_qBUdhp-MLVNP5GQ?type=png)

### Components:

1. **AWS Lambda Function**
   - Ingests real-time Solana blockchain data 
   - Processes swap transactions
   - Inserts raw data into Snowflake
   - Triggers the Airflow DAG

2. **Snowflake Database**
   - Stores raw swap data in `DEV.BRONZE.HELIUS_SWAPS`
   - Maintains token metadata in `DEV.BRONZE.TOKEN_METADATA`
   - Records notification history in `DEV.BRONZE.WHALE_NOTIFICATIONS`

3. **Amazon MWAA (Airflow)**
   - Hosts the token activity notification DAG
   - Manages scheduled and triggered executions
   - Handles database connections and Slack notifications

4. **Birdeye API**
   - External service used to fetch token metadata
   - Provides token names, symbols, and other data

5. **Slack**
   - Destination for whale alerts
   - Displays formatted notifications with DEXScreener links

## Data Flow

1. Swap transactions are detected on the Solana blockchain
2. Lambda function processes and inserts swaps into Snowflake
3. Airflow DAG is triggered automatically
4. DAG identifies tokens with unusual activity in multiple time frames
5. Missing token metadata is fetched from Birdeye API
6. Activity thresholds and cooldown periods are applied
7. Alerts are formatted and sent to Slack

## Main DAG: token_activity_notification_dag

This DAG performs the following steps:

1. **detect_whale_activity**
   - Focuses on recently traded tokens (last 10 minutes)
   - Analyzes activity across three time intervals:
     - 30 minutes (threshold: 2 unique wallets)
     - 1 hour (threshold: 3 unique wallets)
     - 3 hours (threshold: 5 unique wallets)
   - Returns candidate tokens for notification

2. **process_notifications**
   - Fetches token metadata from Snowflake
   - Retrieves missing metadata from Birdeye API
   - Filters out major tokens (SOL, USDT, etc.)
   - Applies cooldown periods to prevent notification spam
   - Formats color-coded notifications based on buy/sell ratio
   - Stores notification records in Snowflake

3. **branch_notification**
   - Determines if notifications should be sent
   - Routes to skip_notification if no alerts were triggered

4. **send_notification**
   - Sends formatted alerts to Slack via webhook

## Database Schema

### DEV.BRONZE.HELIUS_SWAPS
```sql
CREATE OR REPLACE TABLE DEV.BRONZE.HELIUS_SWAPS (
    USER_ADDRESS VARCHAR(255),
    SWAPFROMTOKEN VARCHAR(255),
    SWAPFROMAMOUNT FLOAT,
    SWAPTOTOKEN VARCHAR(255),
    SWAPTOAMOUNT FLOAT,
    SIGNATURE VARCHAR(255) NOT NULL,
    SOURCE VARCHAR(255),
    TIMESTAMP TIMESTAMP_NTZ(9),
    INSERTED_AT TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SIGNATURE)
);
```

### DEV.BRONZE.TOKEN_METADATA
```sql
CREATE OR REPLACE TABLE DEV.BRONZE.TOKEN_METADATA (
    ADDRESS VARCHAR(255) NOT NULL,
    SYMBOL VARCHAR(50),
    NAME VARCHAR(255),
    DECIMALS NUMBER(38,0),
    BLOCKCHAIN VARCHAR(50) DEFAULT 'solana',
    INSERTED_AT TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ADDRESS)
);
```

### DEV.BRONZE.WHALE_NOTIFICATIONS
```sql
CREATE OR REPLACE TABLE DEV.BRONZE.WHALE_NOTIFICATIONS (
    NOTIFICATION_ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
    TIMESTAMP TIMESTAMP_NTZ(9),
    ADDRESS VARCHAR(255),
    SYMBOL VARCHAR(50),
    NAME VARCHAR(255),
    TIME_INTERVAL VARCHAR(10),
    NUM_USERS_BOUGHT NUMBER(38,0),
    NUM_USERS_SOLD NUMBER(38,0),
    INSERTED_AT TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (NOTIFICATION_ID)
);
```

## Notification Format

The alerts sent to Slack are color-coded based on buy/sell ratio:

**Bullish Activity** (more buys than sells)
```
🟢 *WHALE ALERT* 🟢
↗️ *BULLISH ACTIVITY*: <DEXScreener Link|Token Name (Symbol)>
• *Time Frame*: Last X min/hour
• *Unique Buyers*: Y users
• *Unique Sellers*: Z users
• *Activity Ratio*: Y/Z buy/sell ratio
• *Token Address*: `address`
```

**Bearish Activity** (more sells than buys)
```
🔴 *WHALE ALERT* 🔴
↘️ *BEARISH ACTIVITY*: <DEXScreener Link|Token Name (Symbol)>
• *Time Frame*: Last X min/hour
• *Unique Buyers*: Y users
• *Unique Sellers*: Z users
• *Activity Ratio*: Z/Y sell/buy ratio
• *Token Address*: `address`
```

## Setup and Configuration

### Airflow Connections

1. **snowflake_default**
   - Connection Type: Snowflake
   - Host: `<your-snowflake-account>.snowflakecomputing.com`
   - Schema: DEV
   - Login: `<username>`
   - Password: `<password>`
   - Extra: `{"warehouse": "<warehouse>", "role": "<role>"}`

2. **birdeye**
   - Connection Type: HTTP
   - Host: `https://public-api.birdeye.so`
   - Extra: `{"apikey": "<your-api-key>"}`

3. **slack_webhook**
   - Connection Type: HTTP
   - Host: `https://hooks.slack.com/services`
   - Password: `/<your-webhook-path>`

### DAG Configuration

The DAG is scheduled to run every 5 minutes, but it's primarily designed to be triggered by the Lambda function whenever new swap data is detected.

## Performance Optimizations

1. **Connection Pooling**
   - Uses a global Snowflake connection to minimize connection overhead
   - Creates cursors for each query while maintaining a single connection
   - Properly handles connection lifecycle and cleanup

2. **Query Efficiency**
   - Focuses analysis on recently traded tokens only
   - Uses WITH clauses and optimized SQL patterns
   - Leverages proper indexing for fast lookups

3. **Notification Control**
   - Applies time-based cooldown periods to prevent alert fatigue
   - Customized cooldown duration based on time interval
   - 15 min cooldown for 30min interval
   - 30 min cooldown for 1hour interval
   - 60 min cooldown for 3hour interval

## Maintenance and Troubleshooting

### Common Issues

1. **API Rate Limiting**
   - If you see 429 errors from Birdeye API, consider reducing the request frequency or implementing backoff
   - Use the connection framework to easily update API keys if needed

2. **Missing Metadata**
   - The DAG will handle missing metadata gracefully, using token addresses as fallbacks
   - Check API errors if many tokens are missing metadata

3. **Connection Errors**
   - Look for connection logs showing "Failed to connect to Snowflake"
   - Verify connection details in Airflow admin

### Monitoring

- Check DAG logs in Airflow UI for detailed execution information
- Monitor the WHALE_NOTIFICATIONS table for alert history
- Set up additional Airflow alerts for DAG failures

## Future Enhancements

1. **Machine Learning Integration**
   - Add anomaly detection to identify unusual patterns beyond simple thresholds
   - Implement token classification to prioritize notifications

2. **Additional Data Sources**
   - Integrate with price data APIs for correlation analysis
   - Add social media sentiment analysis

3. **Enhanced Notifications**
   - Add token price change percentage
   - Include historical price chart images
   - Support for additional notification channels

## License

This project is proprietary and confidential.

## Contributors

- Your Organization's Data Engineering Team