import sys
import time
import json
from datetime import datetime, timezone
from pathlib import Path
import requests
from kafka import KafkaProducer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config import Config
from src.core.logger import get_logger

logger = get_logger("TrafficProducer")

def create_producer():
    return KafkaProducer(
        bootstrap_servers=Config.KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def fetch_traffic_data():
    url = (
        "https://api.tomtom.com/traffic/services/5/incidentDetails"
        f"?key={Config.TOMTOM_API_KEY}"
        "&bbox=105.7,20.9,105.9,21.1"
        "&fields=%7Bincidents%7Btype,geometry%7Btype,coordinates%7D,properties%7Bid,iconCategory,magnitudeOfDelay,delay,length,from,to,events%7Bdescription%7D%7D%7D%7D"
        "&language=en-GB"
        "&timeValidityFilter=present"
    )
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch data from TomTom API: {str(e)}")
        return None

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

def main():
    if not Config.TOMTOM_API_KEY:
        logger.error("TOMTOM_API_KEY is not set. Exiting.")
        return

    producer = create_producer()
    logger.info(f"Producer started. Target Kafka topic: {Config.KAFKA_TOPIC}")

    while True:
        data = fetch_traffic_data()
        
        if data and "incidents" in data:
            incidents = data["incidents"]
            for incident in incidents:
                producer.send(
                    Config.KAFKA_TOPIC,
                    normalize_incident(incident["properties"])
                )
            
            producer.flush()
            logger.info(f"Successfully pushed {len(incidents)} records to Kafka.")
        else:
            logger.warning("No incidents found or empty response received.")
            
        time.sleep(60)

if __name__ == "__main__":
    main()
