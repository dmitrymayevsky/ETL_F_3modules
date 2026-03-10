from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pymongo import MongoClient

def create_postgres_tables():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_sessions (
        session_id VARCHAR(50) PRIMARY KEY,
        user_id VARCHAR(50),
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        pages_visited TEXT[],
        device VARCHAR(20),
        actions TEXT[]
    );
    
    CREATE TABLE IF NOT EXISTS event_logs (
        event_id VARCHAR(50) PRIMARY KEY,
        timestamp TIMESTAMP,
        event_type VARCHAR(50),
        details TEXT
    );
    
    CREATE TABLE IF NOT EXISTS support_tickets (
        ticket_id VARCHAR(50) PRIMARY KEY,
        user_id VARCHAR(50),
        status VARCHAR(20),
        issue_type VARCHAR(50),
        messages TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    """)
    conn.commit()
    return "PostgreSQL tables created"

def replicate_user_sessions():
    try:
        print("Starting replicate_user_sessions")
        mongo = MongoClient('mongodb://airflow-mongodb:27017/')
        db = mongo.analytics
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        data = list(db.user_sessions.find())
        print(f"Found {len(data)} records in MongoDB")
        
        if len(data) == 0:
            return "No data to replicate"
        
        count = 0
        for doc in data:
            try:
                if '_id' in doc:
                    del doc['_id']
                
                cursor.execute("""
                    INSERT INTO user_sessions 
                    (session_id, user_id, start_time, end_time, pages_visited, device, actions)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (session_id) DO NOTHING
                """, (
                    doc.get('session_id'),
                    doc.get('user_id'),
                    doc.get('start_time'),
                    doc.get('end_time'),
                    doc.get('pages_visited'),
                    doc.get('device'),
                    doc.get('actions')
                ))
                count += 1
            except Exception as e:
                print(f"Error inserting doc: {e}")
                continue
        
        conn.commit()
        return f"Inserted {count} sessions"
    except Exception as e:
        print(f"ERROR: {e}")
        raise e

def replicate_event_logs():
    try:
        print("Starting replicate_event_logs")
        mongo = MongoClient('mongodb://airflow-mongodb:27017/')
        db = mongo.analytics
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        data = list(db.event_logs.find())
        print(f"Found {len(data)} records in MongoDB")
        
        if len(data) == 0:
            return "No data to replicate"
        
        count = 0
        for doc in data:
            try:
                if '_id' in doc:
                    del doc['_id']
                
                cursor.execute("""
                    INSERT INTO event_logs 
                    (event_id, timestamp, event_type, details)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                """, (
                    doc.get('event_id'),
                    doc.get('timestamp'),
                    doc.get('event_type'),
                    str(doc.get('details', ''))
                ))
                count += 1
            except Exception as e:
                print(f"Error inserting doc: {e}")
                continue
        
        conn.commit()
        return f"Inserted {count} events"
    except Exception as e:
        print(f"ERROR: {e}")
        raise e

def replicate_support_tickets():
    try:
        print("Starting replicate_support_tickets")
        mongo = MongoClient('mongodb://airflow-mongodb:27017/')
        db = mongo.analytics
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        data = list(db.support_tickets.find())
        print(f"Found {len(data)} records in MongoDB")
        
        if len(data) == 0:
            return "No data to replicate"
        
        count = 0
        for doc in data:
            try:
                if '_id' in doc:
                    del doc['_id']
                
                cursor.execute("""
                    INSERT INTO support_tickets 
                    (ticket_id, user_id, status, issue_type, messages, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticket_id) DO NOTHING
                """, (
                    doc.get('ticket_id'),
                    doc.get('user_id'),
                    doc.get('status'),
                    doc.get('issue_type'),
                    str(doc.get('messages', '')),
                    doc.get('created_at'),
                    doc.get('updated_at')
                ))
                count += 1
            except Exception as e:
                print(f"Error inserting doc: {e}")
                continue
        
        conn.commit()
        return f"Inserted {count} tickets"
    except Exception as e:
        print(f"ERROR: {e}")
        raise e

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    'replicate_mongo_to_postgres',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:
    
    create_tables = PythonOperator(
        task_id='create_postgres_tables',
        python_callable=create_postgres_tables
    )
    
    replicate_sessions = PythonOperator(
        task_id='replicate_user_sessions',
        python_callable=replicate_user_sessions
    )
    
    replicate_events = PythonOperator(
        task_id='replicate_event_logs',
        python_callable=replicate_event_logs
    )
    
    replicate_tickets = PythonOperator(
        task_id='replicate_support_tickets',
        python_callable=replicate_support_tickets
    )
    
    create_tables >> [replicate_sessions, replicate_events, replicate_tickets]
