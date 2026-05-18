FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && apt-get clean && rm -rf /var/lib/apt/lists/*
COPY docker/airflow/entrypoint.sh /opt/airflow/entrypoint.sh
RUN sed -i 's/\r$//' /opt/airflow/entrypoint.sh && chmod +x /opt/airflow/entrypoint.sh
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-postgres psycopg2-binary python-dotenv
