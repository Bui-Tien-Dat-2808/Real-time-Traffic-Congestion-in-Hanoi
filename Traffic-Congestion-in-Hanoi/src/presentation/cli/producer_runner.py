from datetime import datetime

from application.use_cases.fetch_and_publish_incidents import FetchAndPublishIncidents
from config.settings import get_settings
from infrastructure.clients.tomtom_client import TomTomIncidentClient
from infrastructure.logging.setup import configure_logging
from infrastructure.messaging.kafka_producer import KafkaIncidentProducer
from infrastructure.storage.minio_datalake import MinioDataLakeWriter


def main() -> None:
    configure_logging()
    settings = get_settings()
    use_case = FetchAndPublishIncidents(
        incident_client=TomTomIncidentClient(
            api_key=settings.tomtom_api_key,
            bbox=settings.tomtom_bbox,
            language=settings.tomtom_language,
        ),
        event_producer=KafkaIncidentProducer(
            broker=settings.kafka_broker,
            topic=settings.kafka_topic,
        ),
        data_lake_writer=MinioDataLakeWriter(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            bucket_name=settings.minio_bucket,
            secure=settings.minio_secure,
        ),
        poll_seconds=settings.producer_poll_seconds,
        clock=lambda: datetime.now().strftime("%H:%M:%S"),
    )
    use_case.run_forever()
