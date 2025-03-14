# dags/govt_data_pipeline_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param

# Import our custom operators
from operators.document_scraper_operator import DocumentScraperOperator
from operators.document_processor_operator import DocumentProcessorOperator

# Default arguments for our DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=6),
}

# Complete DAG with all three tasks
with DAG(
    'govt_data_pipeline',
    default_args=default_args,
    description='Government data scraping and processing pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['government', 'scraping', 'nlp'],
    params={
        'config_path': Param('/opt/airflow/dags/config/scrape_config.json', type='string', description='Path to scraping configuration file'),
        'limit': Param(200, type='integer', description='Maximum number of documents to process per run'),
        'headless': Param(True, type='boolean', description='Run scraper in headless mode'),
        'enable_kg': Param(True, type='boolean', description='Enable knowledge graph processing')
    },
) as dag:
    
    # Task 1: Scrape documents based on configuration
    scrape_documents = DocumentScraperOperator(
        task_id='scrape_documents',
        config_path="{{ params.config_path }}",
        supabase_conn_id='supabase_default',
        headless="{{ params.headless }}",
        use_virtual_display=False,  # Set to False to avoid Xvfb issues
        limit=2,
    )
    
    # Task 2: Process newly scraped documents
    process_documents = DocumentProcessorOperator(
        task_id='process_documents',
        supabase_conn_id='supabase_default',
        langchain_conn_id='langchain_default',
        pinecone_conn_id='pinecone_default',
        neo4j_conn_id='neo4j_default',
        limit="{{ params.limit }}",
        enable_kg="{{ params.enable_kg }}",
    )
    

    
    # Define task dependencies
    scrape_documents >> process_documents