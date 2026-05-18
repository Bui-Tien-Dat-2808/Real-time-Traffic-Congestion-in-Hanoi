import json
import logging
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from domain.entities.traffic_incident import TrafficIncident

LOGGER = logging.getLogger(__name__)


class MinioDataLakeWriter:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        secure: bool = False,
    ) -> None:
        self.bucket_name = bucket_name
        self.client = boto3.client(
            "s3",
            endpoint_url=f"{'https' if secure else 'http'}://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
        except ClientError:
            LOGGER.info("Creating MinIO bucket %s", self.bucket_name)
            self.client.create_bucket(Bucket=self.bucket_name)

    def write_incident_batch(self, incidents: list[TrafficIncident]) -> None:
        if not incidents:
            return

        now = datetime.now(timezone.utc)
        key = (
            f"raw/year={now:%Y}/month={now:%m}/day={now:%d}/"
            f"incidents_{now:%H%M%S}.json"
        )
        body = json.dumps([incident.to_message() for incident in incidents], ensure_ascii=False)
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )
        LOGGER.info("Stored %s incidents in MinIO key %s", len(incidents), key)
