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
import time

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
    Load CSV data into PostgreSQL table with duplicate handling
    """
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Read CSV file
    csv_path = f"/opt/airflow/data/{csv_file}"
    df = pd.read_csv(csv_path)
    
    # Get raw connection for reliable operations
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Step 1: Clear existing data completely
        print(f"Clearing existing data from {schema}.{table_name}")
        cursor.execute(f"DELETE FROM {schema}.{table_name};")
        conn.commit()
        print(f"Successfully cleared {schema}.{table_name}")
        
        # Step 2: Disable foreign key checks temporarily if needed
        if schema == 'dimensions':
            cursor.execute("SET session_replication_role = replica;")
        
        # Step 3: Insert data row by row to handle any remaining conflicts
        columns = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(columns))
        insert_sql = f"INSERT INTO {schema}.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        successful_inserts = 0
        for index, row in df.iterrows():
            try:
                cursor.execute(insert_sql, row.tolist())
                successful_inserts += 1
            except Exception as row_error:
                print(f"Warning: Skipped row {index} due to conflict: {row_error}")
                continue
        
        # Step 4: Re-enable foreign key checks
        if schema == 'dimensions':
            cursor.execute("SET session_replication_role = DEFAULT;")
        
        conn.commit()
        print(f"Successfully loaded {successful_inserts}/{len(df)} rows into {schema}.{table_name}")
        
        if successful_inserts < len(df):
            print(f"Note: {len(df) - successful_inserts} rows were skipped due to conflicts")
            
    except Exception as e:
        conn.rollback()
        print(f"Error during data loading: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

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
