from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    "owner": "traffic-congestion",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="cleanup_old_traffic_data",
    default_args=default_args,
    description="Delete traffic data older than 7 days",
    start_date=datetime(2026, 4, 10),
    schedule="0 2 * * *",
    catchup=False,
    tags=["maintenance", "postgres"],
) as dag:
    cleanup_incidents = PostgresOperator(
        task_id="cleanup_traffic_incidents",
        postgres_conn_id="traffic_postgres",
        sql="""
            DELETE FROM traffic_incidents
            WHERE event_time < NOW() - INTERVAL '7 days';
        """,
    )

    cleanup_summary = PostgresOperator(
        task_id="cleanup_incident_summary",
        postgres_conn_id="traffic_postgres",
        sql="""
            DELETE FROM incident_summary_1min
            WHERE window_end < NOW() - INTERVAL '7 days';
        """,
    )

    cleanup_incidents >> cleanup_summary
