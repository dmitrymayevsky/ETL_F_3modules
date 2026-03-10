from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def create_support_performance_mart():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS support_performance_mart AS
    SELECT 
        DATE(created_at) as date,
        status,
        issue_type,
        COUNT(*) as ticket_count,
        AVG(EXTRACT(EPOCH FROM (updated_at - created_at))/3600) as avg_resolution_hours,
        SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) as open_tickets,
        SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) as resolved_tickets
    FROM support_tickets
    GROUP BY DATE(created_at), status, issue_type
    """)
    
    conn.commit()
    return "Support performance mart created"

def create_current_backlog():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS current_backlog AS
    SELECT 
        issue_type,
        COUNT(*) as open_tickets,
        COUNT(DISTINCT user_id) as affected_users,
        MAX(created_at) as oldest_ticket,
        AVG(EXTRACT(EPOCH FROM (NOW() - created_at))/3600) as avg_wait_hours
    FROM support_tickets
    WHERE status IN ('open', 'in_progress')
    GROUP BY issue_type
    ORDER BY open_tickets DESC
    """)
    
    conn.commit()
    return "Current backlog created"

with DAG(
    'support_analytics',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:
    
    support_mart = PythonOperator(
        task_id='create_support_performance_mart',
        python_callable=create_support_performance_mart
    )
    
    backlog = PythonOperator(
        task_id='create_current_backlog',
        python_callable=create_current_backlog
    )
    
    support_mart >> backlog
