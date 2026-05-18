import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()


@dataclass(frozen=True)
class Settings:
    tomtom_api_key: str
    tomtom_bbox: str
    tomtom_language: str
    kafka_broker: str
    kafka_topic: str
    postgres_host: str
    postgres_port: int
    db_user: str
    db_pass: str
    db_name: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    minio_secure: bool
    producer_poll_seconds: int
    spark_checkpoint_dir: str

    @property
    def postgres_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.db_name}"


def _as_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def get_settings() -> Settings:
    return Settings(
        tomtom_api_key=os.getenv("TOMTOM_API_KEY", ""),
        tomtom_bbox=os.getenv("TOMTOM_BBOX", "105.7000,20.9500,105.9500,21.1000"),
        tomtom_language=os.getenv("TOMTOM_LANGUAGE", "en-GB"),
        kafka_broker=os.getenv("KAFKA_BROKER", "localhost:29092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "hanoi-incidents"),
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        db_user=os.getenv("DB_USER", "postgres"),
        db_pass=os.getenv("DB_PASS", "postgres"),
        db_name=os.getenv("DB_NAME", "traffic_congestion"),
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        minio_bucket=os.getenv("MINIO_BUCKET", "traffic-raw"),
        minio_secure=_as_bool(os.getenv("MINIO_SECURE", "false")),
        producer_poll_seconds=int(os.getenv("PRODUCER_POLL_SECONDS", "180")),
        spark_checkpoint_dir=os.getenv(
            "SPARK_CHECKPOINT_DIR", "/tmp/traffic-congestion-checkpoints"
        ),
    )
