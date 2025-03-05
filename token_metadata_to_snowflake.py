#!/usr/bin/env python3
"""
Script to extract token metadata from PostgreSQL and load it to Snowflake.
This script uses dotenv to load environment variables from a .env file.
"""
import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine
from snowflake.connector import connect as sf_connect
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# PostgreSQL connection
POSTGRES_CONN_STRING = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:St0ck!adePG@localhost:5432/pipeline_db"
)

# Snowflake connection parameters
def get_snowflake_credentials():
    """Get Snowflake credentials from .env file or prompt user if missing"""
    sf_user = os.getenv("SNOWFLAKE_USER")
    sf_password = os.getenv("SNOWFLAKE_PASSWORD")
    sf_account = os.getenv("SNOWFLAKE_ACCOUNT")
    
    # Prompt for missing credentials
    if not sf_user:
        sf_user = input("Enter Snowflake username: ")
    
    if not sf_password:
        import getpass
        sf_password = getpass.getpass("Enter Snowflake password: ")
    
    if not sf_account:
        sf_account = input("Enter Snowflake account identifier: ")
    
    # Other parameters with defaults
    sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    sf_database = os.getenv("SNOWFLAKE_DATABASE", "DEV")
    sf_schema = os.getenv("SNOWFLAKE_SCHEMA", "BRONZE")
    sf_role = os.getenv("SNOWFLAKE_ROLE", "AIRFLOW_ROLE")
    
    # Verify we have the minimum required credentials
    if not all([sf_user, sf_password, sf_account]):
        logger.error("Missing required Snowflake credentials")
        sys.exit(1)
    
    return {
        "user": sf_user,
        "password": sf_password,
        "account": sf_account,
        "warehouse": sf_warehouse,
        "database": sf_database,
        "schema": sf_schema,
        "role": sf_role
    }

def extract_from_postgres():
    """Extract selected columns from PostgreSQL token_metadata_raw table"""
    try:
        # Create engine connection
        logger.info("Connecting to PostgreSQL database")
        engine = create_engine(POSTGRES_CONN_STRING)
        
        # SQL query to select only the columns we need and exclude NULL addresses
        query = """
        SELECT 
            address, 
            name, 
            symbol, 
            blockchain, 
            decimals
        FROM 
            token_metadata_raw
        WHERE 
            address IS NOT NULL
        """
        
        # Read data into pandas DataFrame
        df = pd.read_sql(query, engine)
        
        logger.info(f"Extracted {len(df)} rows from PostgreSQL (all with non-NULL addresses)")
        return df
        
    except Exception as e:
        logger.error(f"Error extracting data from PostgreSQL: {str(e)}")
        raise

def transform_data(df):
    """Transform data to match Snowflake schema"""
    try:
        logger.info("Transforming data")
        
        # Remove rows with NULL addresses since ADDRESS is NOT NULL in Snowflake
        original_count = len(df)
        df = df.dropna(subset=['address'])
        logger.info(f"Removed {original_count - len(df)} rows with NULL addresses")
        
        # Remove duplicates by address, keeping the first occurrence
        df = df.drop_duplicates(subset=['address'], keep='first')
        logger.info(f"After removing duplicates: {len(df)} unique tokens")
        
        # Select and reorder columns to match Snowflake schema
        df_transformed = df[[
            'address', 'symbol', 'name', 'decimals', 'blockchain'
        ]]
        
        # Fill any missing values for non-address columns with None to become NULL in Snowflake
        # (but address should never be NULL due to the dropna above)
        df_transformed = df_transformed.where(pd.notnull(df_transformed), None)
        
        # Convert column names to uppercase to match Snowflake schema
        df_transformed.columns = [col.upper() for col in df_transformed.columns]
        
        logger.info(f"Data transformation complete. Final shape: {df_transformed.shape}")
        logger.info(f"Column names: {df_transformed.columns.tolist()}")
        return df_transformed
        
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise

def get_snowflake_table_schema(conn, database, schema, table_name):
    """Get the schema of a Snowflake table"""
    cursor = conn.cursor()
    cursor.execute(f"DESC TABLE {database}.{schema}.{table_name}")
    columns = cursor.fetchall()
    cursor.close()
    
    schema_info = {}
    for col in columns:
        col_name = col[0]
        col_type = col[1]
        is_nullable = col[3] == 'Y'
        schema_info[col_name] = {
            'type': col_type,
            'nullable': is_nullable
        }
    
    return schema_info

def check_table_exists(conn, database, schema, table_name):
    """Check if a table exists in Snowflake, accounting for case sensitivity"""
    cursor = conn.cursor()
    
    # Use SHOW TABLES command which is case-insensitive
    cursor.execute(f"SHOW TABLES LIKE '{table_name}' IN {database}.{schema}")
    tables = cursor.fetchall()
    cursor.close()
    
    # If tables is empty, table doesn't exist
    if not tables:
        logger.info(f"Table {database}.{schema}.{table_name} does not exist")
        return False, None
    
    # Get the actual table name with correct case
    actual_table_name = tables[0][1]  # Index 1 contains the actual table name
    logger.info(f"Found table: {actual_table_name}")
    return True, actual_table_name

def create_token_metadata_table(conn, database, schema):
    """Create the TOKEN_METADATA table if it doesn't exist"""
    try:
        cursor = conn.cursor()
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.TOKEN_METADATA (
            ADDRESS VARCHAR(255) PRIMARY KEY,
            SYMBOL VARCHAR(50),
            NAME VARCHAR(255),
            DECIMALS NUMBER,
            BLOCKCHAIN VARCHAR(50) DEFAULT 'solana',
            INSERTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        
        cursor.execute(create_table_sql)
        logger.info(f"Successfully created or confirmed table {database}.{schema}.TOKEN_METADATA")
        cursor.close()
        return True
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        cursor.close() if cursor else None
        return False

def insert_data_with_merge(conn, df, database, schema, table_name):
    """Insert data using MERGE statement to handle updates for existing records"""
    try:
        cursor = conn.cursor()
        
        # Verify that there are no NULL addresses
        if df['ADDRESS'].isnull().any():
            logger.error("DataFrame contains NULL values in ADDRESS column which is not allowed")
            return False
        
        # Create a temporary table for staging the data
        temp_table = f"{table_name}_TEMP_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Create temp table matching the structure of destination table
        cursor.execute(f"CREATE TEMPORARY TABLE {temp_table} LIKE {database}.{schema}.{table_name}")
        
        # Reset index to avoid pandas warning
        df = df.reset_index(drop=True)
        
        # Load data into temp table - note: removed the problematic on_error parameter
        logger.info(f"Writing {len(df)} rows to temporary table {temp_table}")
        success, num_chunks, num_rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=temp_table,
            quote_identifiers=False,
            auto_create_table=False
            # Remove the on_error parameter completely
        )
        
        if not success:
            logger.error("Failed to load data to temporary table")
            return False
        
        logger.info(f"Successfully loaded {num_rows} rows to temporary table")
        
        # Perform MERGE operation
        merge_sql = f"""
        MERGE INTO {database}.{schema}.{table_name} target
        USING {temp_table} source
        ON target.ADDRESS = source.ADDRESS
        WHEN MATCHED THEN
            UPDATE SET 
                target.SYMBOL = source.SYMBOL,
                target.NAME = source.NAME,
                target.DECIMALS = source.DECIMALS,
                target.BLOCKCHAIN = source.BLOCKCHAIN,
                target.INSERTED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (ADDRESS, SYMBOL, NAME, DECIMALS, BLOCKCHAIN)
            VALUES (
                source.ADDRESS, 
                source.SYMBOL, 
                source.NAME, 
                source.DECIMALS, 
                source.BLOCKCHAIN
            )
        """
        
        try:
            cursor.execute(merge_sql)
            logger.info("MERGE statement executed successfully")
        except Exception as e:
            logger.error(f"Error executing MERGE statement: {str(e)}")
            return False
        
        # Get count of records
        cursor.execute(f"SELECT COUNT(*) FROM {database}.{schema}.{table_name}")
        final_count = cursor.fetchone()[0]
        
        # Clean up temp table
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
        
        cursor.close()
        logger.info(f"Successfully merged data. Total records in {table_name}: {final_count}")
        return True
        
    except Exception as e:
        logger.error(f"Error merging data: {str(e)}")
        if 'cursor' in locals() and cursor:
            cursor.close()
        return False
    
    
def load_to_snowflake(df, credentials):
    """Load transformed data to Snowflake"""
    try:
        logger.info(f"Connecting to Snowflake account: {credentials['account']}")
        
        # Create Snowflake connection
        conn = sf_connect(
            user=credentials['user'],
            password=credentials['password'],
            account=credentials['account'],
            warehouse=credentials['warehouse'],
            database=credentials['database'],
            schema=credentials['schema'],
            role=credentials['role']
        )
        
        # Check if table exists, create if it doesn't
        table_exists, actual_table_name = check_table_exists(
            conn, 
            credentials['database'], 
            credentials['schema'], 
            'TOKEN_METADATA'
        )
        
        if not table_exists:
            logger.info("TOKEN_METADATA table does not exist, creating it now")
            if not create_token_metadata_table(conn, credentials['database'], credentials['schema']):
                conn.close()
                return False
            actual_table_name = 'TOKEN_METADATA'
        
        # Get table schema
        schema_info = get_snowflake_table_schema(
            conn, 
            credentials['database'], 
            credentials['schema'], 
            actual_table_name
        )
        
        logger.info(f"Snowflake table schema columns: {list(schema_info.keys())}")
        
        # Use MERGE statement to handle updates properly
        load_success = insert_data_with_merge(
            conn=conn,
            df=df,
            database=credentials['database'],
            schema=credentials['schema'],
            table_name=actual_table_name
        )
        
        conn.close()
        return load_success
        
    except Exception as e:
        logger.error(f"Error loading data to Snowflake: {str(e)}")
        raise

def list_all_tables(credentials):
    """List all tables in the specified database and schema"""
    try:
        # Create Snowflake connection
        conn = sf_connect(
            user=credentials['user'],
            password=credentials['password'],
            account=credentials['account'],
            warehouse=credentials['warehouse'],
            database=credentials['database'],
            schema=credentials['schema'],
            role=credentials['role']
        )
        
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES IN {credentials['database']}.{credentials['schema']}")
        tables = cursor.fetchall()
        
        logger.info(f"Found {len(tables)} tables in {credentials['database']}.{credentials['schema']}:")
        for table in tables:
            logger.info(f"  - {table[1]} (created: {table[5]})")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error listing tables: {str(e)}")

def test_snowflake_connection(credentials):
    """Test connection to Snowflake before attempting to load data"""
    try:
        logger.info(f"Testing connection to Snowflake account: {credentials['account']}")
        
        conn = sf_connect(
            user=credentials['user'],
            password=credentials['password'],
            account=credentials['account'],
            warehouse=credentials['warehouse'],
            database=credentials['database'],
            schema=credentials['schema'],
            role=credentials['role']
        )
        
        # Execute a simple query to verify connection
        cursor = conn.cursor()
        cursor.execute("SELECT current_version()")
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully connected to Snowflake. Version: {version}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {str(e)}")
        return False

def main():
    """Main ETL process"""
    try:
        logger.info("Starting Token Metadata ETL process")
        
        # Get Snowflake credentials
        sf_credentials = get_snowflake_credentials()
        
        # Test Snowflake connection
        if not test_snowflake_connection(sf_credentials):
            logger.error("Failed to connect to Snowflake. Please check your credentials.")
            sys.exit(1)
        
        # List all tables in the schema to help diagnose issues
        list_all_tables(sf_credentials)
            
        # Extract
        raw_data = extract_from_postgres()
        
        # Transform
        transformed_data = transform_data(raw_data)
        
        # Show sample of data being loaded
        logger.info("Sample of data being loaded:")
        logger.info(transformed_data.head(2).to_string())
        
        # Load
        load_success = load_to_snowflake(transformed_data, sf_credentials)
        
        if load_success:
            logger.info("ETL process completed successfully")
            logger.info(f"Successfully loaded {len(transformed_data)} unique tokens to Snowflake")
        else:
            logger.error("ETL process failed during Snowflake load")
            
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()