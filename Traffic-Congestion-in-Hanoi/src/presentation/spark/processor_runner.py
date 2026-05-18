import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, coalesce, col, count, from_json, lit, max, round, udf, window
from pyspark.sql.types import ArrayType, DoubleType, FloatType, IntegerType, StringType, StructField, StructType

from config.settings import get_settings
from infrastructure.logging.setup import configure_logging

LOGGER = logging.getLogger(__name__)


INCIDENT_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", IntegerType(), True),
        StructField("magnitude", IntegerType(), True),
        StructField("delay_seconds", IntegerType(), True),
        StructField("length_meters", DoubleType(), True),
        StructField("description", StringType(), True),
        StructField("road_from", StringType(), True),
        StructField("road_to", StringType(), True),
        StructField("coordinates", ArrayType(FloatType()), True),
        StructField("timestamp", StringType(), True),
    ]
)


def extract_coord_udf(coords):
    if coords and len(coords) == 2:
        return f"{coords[1]},{coords[0]}"
    return None


def write_to_postgres(df, epoch_id, table_name: str, jdbc_url: str, db_user: str, db_pass: str):
    row_count = df.count()
    LOGGER.info("Batch %s for %s received %s rows", epoch_id, table_name, row_count)
    if row_count == 0:
        return

    df.write.format("jdbc").option("url", jdbc_url).option("dbtable", table_name).option(
        "user", db_user
    ).option("password", db_pass).option("driver", "org.postgresql.Driver").mode("append").save()


def main() -> None:
    configure_logging()
    settings = get_settings()
    spark = SparkSession.builder \
        .appName("HanoiTrafficJamAnalyzer") \
        .config("spark.jars.ivy", "/tmp/.ivy2") \
        .getOrCreate()
#    spark = SparkSession.builder \
#        .appName("HanoiTrafficJamAnalyzer") \
#        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
#        .config("spark.jars.ivy", "/tmp/.ivy2") \
#        .getOrCreate()    
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_broker)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    extract_coord = udf(extract_coord_udf, StringType())

    transformed_stream = (
        raw_stream.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), INCIDENT_SCHEMA).alias("data"))
        .select(
            col("data.id").alias("incident_id"),
            col("data.type").alias("incident_type"),
            col("data.magnitude"),
            coalesce(col("data.delay_seconds").cast("integer"), lit(0)).alias("delay_seconds"),
            round(coalesce(col("data.length_meters").cast("double"), lit(0.0)) / 1000, 2).alias(
                "length_km"
            ),
            col("data.road_from"),
            col("data.road_to"),
            col("data.description"),
            extract_coord(col("data.coordinates")).alias("start_coordinates"),
            col("data.timestamp").cast("timestamp").alias("event_time"),
        )
    )

    filtered_stream = transformed_stream.filter(
        (col("incident_type").isin(1, 6, 8, 9))
        & (
            ((col("incident_type").isin(1, 6)) & (col("delay_seconds") > 0))
            | (col("incident_type").isin(8, 9))
        )
    )

    query_raw = (
        filtered_stream.writeStream.foreachBatch(
            lambda df, epoch_id: write_to_postgres(
                df,
                epoch_id,
                "traffic_incidents",
                settings.postgres_jdbc_url,
                settings.db_user,
                settings.db_pass,
            )
        )
        .option("checkpointLocation", f"{settings.spark_checkpoint_dir}/traffic_incidents")
        .start()
    )

    summary_stream = (
        filtered_stream.withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), "1 minute"))
        .agg(
            count("*").alias("total_incidents"),
            avg("delay_seconds").alias("avg_delay_seconds"),
            max("delay_seconds").alias("max_delay_seconds"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "total_incidents",
            "avg_delay_seconds",
            "max_delay_seconds",
        )
    )

    query_summary = (
        summary_stream.writeStream.foreachBatch(
            lambda df, epoch_id: write_to_postgres(
                df,
                epoch_id,
                "incident_summary_1min",
                settings.postgres_jdbc_url,
                settings.db_user,
                settings.db_pass,
            )
        )
        .option("checkpointLocation", f"{settings.spark_checkpoint_dir}/incident_summary_1min")
        .start()
    )

    LOGGER.info("Spark streaming is running")
    spark.streams.awaitAnyTermination()
    query_raw.awaitTermination()
    query_summary.awaitTermination()
