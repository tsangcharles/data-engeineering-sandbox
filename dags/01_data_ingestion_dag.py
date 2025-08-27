"""
Data Ingestion DAG for Data Warehouse Course Project
This DAG demonstrates how to load CSV data into PostgreSQL tables
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os

default_args = {
    'owner': 'data_science_student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_csv_to_postgres(table_name, csv_file, schema='dimensions'):
    """
    Load CSV data into PostgreSQL table
    """
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Read CSV file
    csv_path = f"/opt/airflow/data/{csv_file}"
    df = pd.read_csv(csv_path)
    
    # Load data into PostgreSQL
    pg_hook.insert_rows(
        table=f"{schema}.{table_name}",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist()
    )
    
    print(f"Successfully loaded {len(df)} rows into {schema}.{table_name}")

def load_fact_table_data():
    """
    Load fact table data (transactions, logs, clickstream)
    """
    # Load user activity logs
    load_csv_to_postgres('user_activity_logs', 'user_activity_logs.csv', 'raw_data')
    
    # Load transactions
    load_csv_to_postgres('transactions', 'transactions.csv', 'raw_data')
    
    # Load clickstream
    load_csv_to_postgres('clickstream', 'clickstream.csv', 'raw_data')

# Create the DAG
dag = DAG(
    'data_ingestion_pipeline',
    default_args=default_args,
    description='Load CSV data into data warehouse tables',
    schedule_interval='@daily',
    catchup=False,
    tags=['data-warehouse', 'ingestion', 'csv']
)

# Task 1: Load dimension tables
load_users = PythonOperator(
    task_id='load_users',
    python_callable=load_csv_to_postgres,
    op_kwargs={'table_name': 'users', 'csv_file': 'users.csv', 'schema': 'dimensions'},
    dag=dag
)

load_products = PythonOperator(
    task_id='load_products',
    python_callable=load_csv_to_postgres,
    op_kwargs={'table_name': 'products', 'csv_file': 'products.csv', 'schema': 'dimensions'},
    dag=dag
)

load_dates = PythonOperator(
    task_id='load_dates',
    python_callable=load_csv_to_postgres,
    op_kwargs={'table_name': 'dates', 'csv_file': 'dates.csv', 'schema': 'dimensions'},
    dag=dag
)

# Task 2: Load fact tables
load_fact_data = PythonOperator(
    task_id='load_fact_data',
    python_callable=load_fact_table_data,
    dag=dag
)

# Task 3: Verify data loaded
verify_data = PostgresOperator(
    task_id='verify_data_loaded',
    postgres_conn_id='postgres_default',
    sql="""
    SELECT 
        'users' as table_name, COUNT(*) as row_count FROM dimensions.users
    UNION ALL
    SELECT 'products', COUNT(*) FROM dimensions.products
    UNION ALL
    SELECT 'dates', COUNT(*) FROM dimensions.dates
    UNION ALL
    SELECT 'user_activity_logs', COUNT(*) FROM raw_data.user_activity_logs
    UNION ALL
    SELECT 'transactions', COUNT(*) FROM raw_data.transactions
    UNION ALL
    SELECT 'clickstream', COUNT(*) FROM raw_data.clickstream;
    """,
    dag=dag
)

# Define task dependencies
[load_users, load_products, load_dates] >> load_fact_data >> verify_data
