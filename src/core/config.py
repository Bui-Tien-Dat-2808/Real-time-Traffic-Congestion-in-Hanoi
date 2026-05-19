import os

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

class Config:
    PROJECT_NAME = os.getenv("PROJECT_NAME", "traffic_analytics")

    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
    KAFKA_INTERNAL_BROKER = os.getenv("KAFKA_INTERNAL_BROKER", "kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "hanoi-incidents")
    KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
    KAFKA_FAIL_ON_DATA_LOSS = os.getenv("KAFKA_FAIL_ON_DATA_LOSS", "false")

    TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")

    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASS = os.getenv("DB_PASS", "postgres")
    DB_NAME = os.getenv("DB_NAME", "traffic_congestion")
    POSTGRES_URL = f"jdbc:postgresql://postgres:5432/{DB_NAME}"

    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "ROOT_USER")
    MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "CHANGEME123")
    MINIO_BUCKET = os.getenv("MINIO_STORAGE_BUCKET", "traffic-data-lake")
