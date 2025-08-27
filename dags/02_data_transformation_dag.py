"""
Data Transformation DAG for Data Warehouse Course Project
This DAG demonstrates how to transform raw data into processed and aggregated tables
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

default_args = {
    'owner': 'data_science_student',
    'depends_on_past': True,  # This ensures data is loaded first
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_daily_user_metrics():
    """
    Create daily user metrics by aggregating user activity data
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # SQL to create daily user metrics
    sql = """
    INSERT INTO processed_data.daily_user_metrics 
    (user_id, date_id, total_sessions, total_page_views, total_time_spent_seconds, 
     total_transactions, total_revenue, avg_session_duration_seconds)
    SELECT 
        ual.user_id,
        d.date_id,
        COUNT(DISTINCT ual.session_id) as total_sessions,
        COUNT(*) as total_page_views,
        COALESCE(SUM(ual.response_time_ms), 0) / 1000 as total_time_spent_seconds,
        COALESCE(t.total_transactions, 0) as total_transactions,
        COALESCE(t.total_revenue, 0) as total_revenue,
        CASE 
            WHEN COUNT(DISTINCT ual.session_id) > 0 
            THEN COALESCE(SUM(ual.response_time_ms), 0) / 1000.0 / COUNT(DISTINCT ual.session_id)
            ELSE 0 
        END as avg_session_duration_seconds
    FROM raw_data.user_activity_logs ual
    JOIN dimensions.dates d ON DATE(ual.timestamp) = d.full_date
    LEFT JOIN (
        SELECT 
            user_id, 
            DATE(transaction_date) as trans_date,
            COUNT(*) as total_transactions,
            SUM(total_amount) as total_revenue
        FROM raw_data.transactions 
        WHERE transaction_status = 'Completed'
        GROUP BY user_id, DATE(transaction_date)
    ) t ON ual.user_id = t.user_id AND DATE(ual.timestamp) = t.trans_date
    WHERE DATE(ual.timestamp) = CURRENT_DATE - INTERVAL '1 day'
    GROUP BY ual.user_id, d.date_id, t.total_transactions, t.total_revenue
    ON CONFLICT (user_id, date_id) 
    DO UPDATE SET
        total_sessions = EXCLUDED.total_sessions,
        total_page_views = EXCLUDED.total_page_views,
        total_time_spent_seconds = EXCLUDED.total_time_spent_seconds,
        total_transactions = EXCLUDED.total_transactions,
        total_revenue = EXCLUDED.total_revenue,
        avg_session_duration_seconds = EXCLUDED.avg_session_duration_seconds,
        updated_at = CURRENT_TIMESTAMP;
    """
    
    pg_hook.run(sql)
    print("Daily user metrics created successfully")

def create_daily_product_performance():
    """
    Create daily product performance metrics
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql = """
    INSERT INTO aggregated_data.daily_product_performance 
    (product_id, date_id, total_views, total_clicks, total_sales, 
     total_revenue, conversion_rate, avg_price)
    SELECT 
        p.product_id,
        d.date_id,
        COALESCE(views.total_views, 0) as total_views,
        COALESCE(clicks.total_clicks, 0) as total_clicks,
        COALESCE(sales.total_sales, 0) as total_sales,
        COALESCE(sales.total_revenue, 0) as total_revenue,
        CASE 
            WHEN COALESCE(views.total_views, 0) > 0 
            THEN COALESCE(sales.total_sales, 0)::DECIMAL / views.total_views
            ELSE 0 
        END as conversion_rate,
        COALESCE(sales.avg_price, p.price) as avg_price
    FROM dimensions.products p
    CROSS JOIN dimensions.dates d
    LEFT JOIN (
        SELECT 
            product_id,
            DATE(click_timestamp) as click_date,
            COUNT(*) as total_clicks
        FROM raw_data.clickstream
        WHERE DATE(click_timestamp) = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY product_id, DATE(click_timestamp)
    ) clicks ON p.product_id = clicks.product_id AND d.full_date = clicks.click_date
    LEFT JOIN (
        SELECT 
            product_id,
            DATE(transaction_date) as trans_date,
            COUNT(*) as total_sales,
            SUM(total_amount) as total_revenue,
            AVG(unit_price) as avg_price
        FROM raw_data.transactions
        WHERE transaction_status = 'Completed' 
        AND DATE(transaction_date) = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY product_id, DATE(transaction_date)
    ) sales ON p.product_id = sales.product_id AND d.full_date = sales.trans_date
    LEFT JOIN (
        SELECT 
            product_id,
            DATE(timestamp) as view_date,
            COUNT(*) as total_views
        FROM raw_data.user_activity_logs
        WHERE page_url LIKE '/product/%' 
        AND DATE(timestamp) = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY product_id, DATE(timestamp)
    ) views ON p.product_id = views.product_id AND d.full_date = views.view_date
    WHERE d.full_date = CURRENT_DATE - INTERVAL '1 day'
    ON CONFLICT (product_id, date_id) 
    DO UPDATE SET
        total_views = EXCLUDED.total_views,
        total_clicks = EXCLUDED.total_clicks,
        total_sales = EXCLUDED.total_sales,
        total_revenue = EXCLUDED.total_revenue,
        conversion_rate = EXCLUDED.conversion_rate,
        avg_price = EXCLUDED.avg_price,
        updated_at = CURRENT_TIMESTAMP;
    """
    
    pg_hook.run(sql)
    print("Daily product performance metrics created successfully")

def create_daily_overall_metrics():
    """
    Create daily overall metrics for the entire system
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql = """
    INSERT INTO aggregated_data.daily_overall_metrics 
    (date_id, total_users, total_sessions, total_page_views, total_transactions, 
     total_revenue, avg_session_duration_seconds, bounce_rate, conversion_rate)
    SELECT 
        d.date_id,
        COALESCE(users.total_users, 0) as total_users,
        COALESCE(sessions.total_sessions, 0) as total_sessions,
        COALESCE(views.total_page_views, 0) as total_page_views,
        COALESCE(trans.total_transactions, 0) as total_transactions,
        COALESCE(trans.total_revenue, 0) as total_revenue,
        COALESCE(sessions.avg_session_duration, 0) as avg_session_duration_seconds,
        CASE 
            WHEN COALESCE(sessions.total_sessions, 0) > 0 
            THEN COALESCE(bounce.bounce_sessions, 0)::DECIMAL / sessions.total_sessions
            ELSE 0 
        END as bounce_rate,
        CASE 
            WHEN COALESCE(views.total_page_views, 0) > 0 
            THEN COALESCE(trans.total_transactions, 0)::DECIMAL / views.total_page_views
            ELSE 0 
        END as conversion_rate
    FROM dimensions.dates d
    LEFT JOIN (
        SELECT 
            DATE(timestamp) as activity_date,
            COUNT(DISTINCT user_id) as total_users
        FROM raw_data.user_activity_logs
        WHERE DATE(timestamp) = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY DATE(timestamp)
    ) users ON d.full_date = users.activity_date
    LEFT JOIN (
        SELECT 
            DATE(timestamp) as session_date,
            COUNT(DISTINCT session_id) as total_sessions,
            AVG(COALESCE(response_time_ms, 0)) / 1000 as avg_session_duration
        FROM raw_data.user_activity_logs
        WHERE DATE(timestamp) = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY DATE(timestamp)
    ) sessions ON d.full_date = sessions.session_date
    LEFT JOIN (
        SELECT 
            DATE(timestamp) as view_date,
            COUNT(*) as total_page_views
        FROM raw_data.user_activity_logs
        WHERE DATE(timestamp) = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY DATE(timestamp)
    ) views ON d.full_date = views.view_date
    LEFT JOIN (
        SELECT 
            DATE(transaction_date) as trans_date,
            COUNT(*) as total_transactions,
            SUM(total_amount) as total_revenue
        FROM raw_data.transactions
        WHERE transaction_status = 'Completed' 
        AND DATE(transaction_date) = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY DATE(transaction_date)
    ) trans ON d.full_date = trans.trans_date
    LEFT JOIN (
        SELECT 
            DATE(timestamp) as bounce_date,
            COUNT(DISTINCT session_id) as bounce_sessions
        FROM (
            SELECT 
                session_id,
                DATE(timestamp) as timestamp,
                COUNT(*) as page_count
            FROM raw_data.user_activity_logs
            WHERE DATE(timestamp) = CURRENT_DATE - INTERVAL '1 day'
            GROUP BY session_id, DATE(timestamp)
            HAVING COUNT(*) = 1
        ) single_page_sessions
        GROUP BY DATE(timestamp)
    ) bounce ON d.full_date = bounce.bounce_date
    WHERE d.full_date = CURRENT_DATE - INTERVAL '1 day'
    ON CONFLICT (date_id) 
    DO UPDATE SET
        total_users = EXCLUDED.total_users,
        total_sessions = EXCLUDED.total_sessions,
        total_page_views = EXCLUDED.total_page_views,
        total_transactions = EXCLUDED.total_transactions,
        total_revenue = EXCLUDED.total_revenue,
        avg_session_duration_seconds = EXCLUDED.avg_session_duration_seconds,
        bounce_rate = EXCLUDED.bounce_rate,
        conversion_rate = EXCLUDED.conversion_rate,
        updated_at = CURRENT_TIMESTAMP;
    """
    
    pg_hook.run(sql)
    print("Daily overall metrics created successfully")

# Create the DAG
dag = DAG(
    'data_transformation_pipeline',
    default_args=default_args,
    description='Transform raw data into processed and aggregated tables',
    schedule_interval='@daily',
    catchup=False,
    tags=['data-warehouse', 'transformation', 'aggregation']
)

# Task 1: Create daily user metrics
create_user_metrics = PythonOperator(
    task_id='create_daily_user_metrics',
    python_callable=create_daily_user_metrics,
    dag=dag
)

# Task 2: Create daily product performance metrics
create_product_metrics = PythonOperator(
    task_id='create_daily_product_performance',
    python_callable=create_daily_product_performance,
    dag=dag
)

# Task 3: Create daily overall metrics
create_overall_metrics = PythonOperator(
    task_id='create_daily_overall_metrics',
    python_callable=create_daily_overall_metrics,
    dag=dag
)

# Task 4: Verify transformations
verify_transformations = PostgresOperator(
    task_id='verify_transformations',
    postgres_conn_id='postgres_default',
    sql="""
    SELECT 
        'daily_user_metrics' as table_name, COUNT(*) as row_count FROM processed_data.daily_user_metrics
    UNION ALL
    SELECT 'daily_product_performance', COUNT(*) FROM aggregated_data.daily_product_performance
    UNION ALL
    SELECT 'daily_overall_metrics', COUNT(*) FROM aggregated_data.daily_overall_metrics;
    """,
    dag=dag
)

# Define task dependencies - these can run in parallel
[create_user_metrics, create_product_metrics, create_overall_metrics] >> verify_transformations
