from minio import Minio
from src.core.config import Config
from src.core.logger import get_logger

logger = get_logger("MinioClient")

class MinioStorage:
    def __init__(self):
        endpoint_no_scheme = Config.MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
        self.client = Minio(
            endpoint_no_scheme,
            access_key=Config.MINIO_ACCESS_KEY,
            secret_key=Config.MINIO_SECRET_KEY,
            secure=False
        )

    def ensure_bucket_exists(self):
        try:
            if not self.client.bucket_exists(Config.MINIO_BUCKET):
                self.client.make_bucket(Config.MINIO_BUCKET)
                logger.info(f"Bucket '{Config.MINIO_BUCKET}' created successfully in Data Lake.")
            else:
                logger.info(f"Bucket '{Config.MINIO_BUCKET}' already exists.")
        except Exception as e:
            logger.error(f"Failed to verify or create bucket in MinIO: {str(e)}")
            raise