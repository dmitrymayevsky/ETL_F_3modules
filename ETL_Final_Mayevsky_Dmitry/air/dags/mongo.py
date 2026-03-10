from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import json
from pymongo import MongoClient
import uuid

def generate_user_sessions():
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['analytics']
    
    sessions = []
    devices = ['mobile', 'desktop', 'tablet']
    pages = ['/home', '/products', '/cart', '/profile', '/support']
    actions = ['login', 'view', 'click', 'add_to_cart', 'logout']
    
    for i in range(100):
        session = {
            'session_id': f'sess_{uuid.uuid4().hex[:8]}',
            'user_id': f'user_{random.randint(1, 50)}',
            'start_time': datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23)),
            'end_time': datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23)),
            'pages_visited': random.sample(pages, random.randint(1, 4)),
            'device': random.choice(devices),
            'actions': random.sample(actions, random.randint(2, 4))
        }
        sessions.append(session)
    
    db.user_sessions.insert_many(sessions)
    return f"Inserted {len(sessions)} sessions"

def generate_event_logs():
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['analytics']
    
    events = []
    event_types = ['click', 'page_view', 'purchase', 'login', 'logout']
    
    for i in range(200):
        event = {
            'event_id': f'evt_{uuid.uuid4().hex[:8]}',
            'timestamp': datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23)),
            'event_type': random.choice(event_types),
            'details': {
                'page': random.choice(['/home', '/products', '/cart']),
                'user_id': f'user_{random.randint(1, 50)}'
            }
        }
        events.append(event)
    
    db.event_logs.insert_many(events)
    return f"Inserted {len(events)} events"

def generate_support_tickets():
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['analytics']
    
    tickets = []
    statuses = ['open', 'in_progress', 'resolved', 'closed']
    issue_types = ['payment', 'technical', 'account', 'product', 'delivery']
    
    for i in range(50):
        created = datetime.now() - timedelta(days=random.randint(0, 20))
        updated = created + timedelta(hours=random.randint(1, 48))
        
        ticket = {
            'ticket_id': f'ticket_{uuid.uuid4().hex[:8]}',
            'user_id': f'user_{random.randint(1, 50)}',
            'status': random.choice(statuses),
            'issue_type': random.choice(issue_types),
            'messages': [
                {
                    'sender': 'user',
                    'message': f'Problem with {random.choice(issue_types)}',
                    'timestamp': created
                },
                {
                    'sender': 'support',
                    'message': 'We are working on it',
                    'timestamp': updated
                }
            ],
            'created_at': created,
            'updated_at': updated
        }
        tickets.append(ticket)
    
    db.support_tickets.insert_many(tickets)
    return f"Inserted {len(tickets)} tickets"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    'generate_mongo_data',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:
    
    task_sessions = PythonOperator(
        task_id='generate_user_sessions',
        python_callable=generate_user_sessions
    )
    
    task_events = PythonOperator(
        task_id='generate_event_logs',
        python_callable=generate_event_logs
    )
    
    task_tickets = PythonOperator(
        task_id='generate_support_tickets',
        python_callable=generate_support_tickets
    )
    
    task_sessions >> task_events >> task_tickets
