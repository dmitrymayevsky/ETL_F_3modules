from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

def create_user_activity_mart():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_activity_mart AS
    SELECT 
        u.user_id,
        DATE(u.start_time) as activity_date,
        COUNT(DISTINCT u.session_id) as session_count,
        AVG(EXTRACT(EPOCH FROM (u.end_time - u.start_time))/60) as avg_session_minutes,
        ARRAY_LENGTH(u.pages_visited, 1) as pages_per_session,
        COUNT(DISTINCT e.event_id) as total_events,
        SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_events
    FROM user_sessions u
    LEFT JOIN event_logs e ON u.user_id = SUBSTRING(e.details, 'user_([0-9]+)')
        AND DATE(e.timestamp) = DATE(u.start_time)
    GROUP BY u.user_id, DATE(u.start_time), u.pages_visited
    """)
    
    conn.commit()
    return "User activity mart created"

def create_top_pages_view():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS top_pages AS
    SELECT 
        UNNEST(pages_visited) as page_url,
        COUNT(DISTINCT session_id) as visit_count,
        COUNT(DISTINCT user_id) as unique_users
    FROM user_sessions
    WHERE start_time >= NOW() - INTERVAL '30 days'
    GROUP BY page_url
    ORDER BY visit_count DESC
    """)
    
    conn.commit()
    return "Top pages view created"

with DAG(
    'user_activity_analytics',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:
    
    user_mart = PythonOperator(
        task_id='create_user_activity_mart',
        python_callable=create_user_activity_mart
    )
    
    top_pages = PythonOperator(
        task_id='create_top_pages',
        python_callable=create_top_pages_view
    )
    
    user_mart >> top_pages
