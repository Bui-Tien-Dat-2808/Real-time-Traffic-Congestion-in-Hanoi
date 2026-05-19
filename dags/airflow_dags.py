import os
import json
import requests
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def normalize_incident(properties):
    events = properties.get("events") or []
    description = ""
    if events and isinstance(events, list):
        description = events[0].get("description", "")

    return {
        "id": properties.get("id"),
        "iconCategory": properties.get("iconCategory"),
        "magnitudeOfDelay": properties.get("magnitudeOfDelay"),
        "delay": properties.get("delay"),
        "length": properties.get("length"),
        "from": properties.get("from"),
        "to": properties.get("to"),
        "events": description,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_and_produce_traffic_data():
    api_key = os.getenv("TOMTOM_API_KEY")
    if not api_key:
        raise ValueError("TOMTOM_API_KEY is not set.")

    kafka_broker = os.getenv("KAFKA_INTERNAL_BROKER", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "hanoi-incidents")
    url = "https://api.tomtom.com/traffic/services/5/incidentDetails"
    params = {
        "key": api_key,
        "bbox": "105.7,20.9,105.9,21.1",
        "fields": "{incidents{type,geometry{type,coordinates},properties{id,iconCategory,magnitudeOfDelay,delay,length,from,to,events{description}}}}",
        "language": "en-GB",
        "timeValidityFilter": "present",
    }

    response = requests.get(url, params=params, timeout=10)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise requests.HTTPError(
            f"TomTom API request failed with status {response.status_code}: {response.text}"
        ) from exc

    data = response.json()

    if data and "incidents" in data:
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        incidents = data["incidents"]
        for incident in incidents:
            producer.send(kafka_topic, normalize_incident(incident["properties"]))
        
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
