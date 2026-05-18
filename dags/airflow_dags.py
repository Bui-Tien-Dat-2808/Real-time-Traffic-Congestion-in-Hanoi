import os
import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def fetch_and_produce_traffic_data():
    api_key = os.getenv("TOMTOM_API_KEY")
    if not api_key:
        raise ValueError("TOMTOM_API_KEY is not set.")

    kafka_broker = os.getenv("KAFKA_INTERNAL_BROKER", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "hanoi-incidents")

    url = (
        f"https://api.tomtom.com/traffic/services/4/incidentDetails/s3/"
        f"20.9,105.7,21.1,105.9/-1/-1/json?key={api_key}"
    )
    
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    if data and "incidents" in data:
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        incidents = data["incidents"]
        for incident in incidents:
            producer.send(kafka_topic, incident["properties"])
        
        producer.flush()
        print(f"Successfully pushed {len(incidents)} records to Kafka.")
    else:
        print("No incidents found or empty response received.")

with DAG(
    'collect_traffic_data',
    default_args=default_args,
    description='Collect traffic data from TomTom API and push to Kafka every minute',
    schedule_interval='* * * * *',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['ingestion'],
) as dag:

    collect_data_task = PythonOperator(
        task_id='fetch_and_produce',
        python_callable=fetch_and_produce_traffic_data,
    )