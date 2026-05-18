#!/bin/bash
set -e

airflow db migrate
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true

airflow connections add traffic_postgres \
  --conn-type postgres \
  --conn-host "${POSTGRES_HOST}" \
  --conn-login "${DB_USER}" \
  --conn-password "${DB_PASS}" \
  --conn-port "${POSTGRES_PORT}" \
  --conn-schema "${DB_NAME}" || true

airflow scheduler &
airflow webserver --port 8081
