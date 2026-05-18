import psycopg2
from src.core.config import Config
from src.core.logger import get_logger

logger = get_logger("PostgresClient")

class PostgresDB:
    def __init__(self):
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=Config.DB_NAME,
                user=Config.DB_USER,
                password=Config.DB_PASS,
                host="postgres",
                port=5432
            )
            self.conn.autocommit = True
            logger.info("Successfully established connection to PostgreSQL.")
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise

    def execute_query(self, query: str):
        if not self.conn:
            self.connect()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                logger.info("Query executed successfully.")
                return cursor.rowcount
        except Exception as e:
            logger.error(f"Failed to execute query: {str(e)}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed.")