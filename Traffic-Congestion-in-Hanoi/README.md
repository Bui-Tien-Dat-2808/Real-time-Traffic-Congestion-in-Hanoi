# Realtime Traffic Congestion Analytics For Hanoi

This project rebuilds the original streaming pipeline with a cleaner separation of concerns, raw-data storage in MinIO, structured logging, and Airflow-based retention cleanup.

## Clean Architecture Layout

```text
├── 📁 dags
│   └── 🐍 cleanup_old_traffic_data.py
├── 📁 docker
│   ├── 📁 airflow
│   │   ├── 🐳 Dockerfile
│   │   ├── 📄 entrypoint.ps1
│   │   └── 📄 entrypoint.sh
│   ├── 📁 minio
│   │   └── 📝 README.md
│   ├── 📁 spark
│   │   └── 🐳 Dockerfile
│   ├── ⚙️ docker-compose.yaml
│   └── 📄 init.sql
├── 📁 src
│   ├── 📁 application
│   │   ├── 📁 use_cases
│   │   │   ├── 🐍 __init__.py
│   │   │   └── 🐍 fetch_and_publish_incidents.py
│   │   └── 🐍 __init__.py
│   ├── 📁 config
│   │   ├── 🐍 __init__.py
│   │   └── 🐍 settings.py
│   ├── 📁 domain
│   │   ├── 📁 entities
│   │   │   ├── 🐍 __init__.py
│   │   │   └── 🐍 traffic_incident.py
│   │   ├── 📁 services
│   │   │   ├── 🐍 __init__.py
│   │   │   └── 🐍 traffic_rules.py
│   │   └── 🐍 __init__.py
│   ├── 📁 infrastructure
│   │   ├── 📁 clients
│   │   │   ├── 🐍 __init__.py
│   │   │   └── 🐍 tomtom_client.py
│   │   ├── 📁 logging
│   │   │   ├── 🐍 __init__.py
│   │   │   └── 🐍 setup.py
│   │   ├── 📁 messaging
│   │   │   ├── 🐍 __init__.py
│   │   │   └── 🐍 kafka_producer.py
│   │   ├── 📁 storage
│   │   │   ├── 🐍 __init__.py
│   │   │   └── 🐍 minio_datalake.py
│   │   └── 🐍 __init__.py
│   ├── 📁 presentation
│   │   ├── 📁 cli
│   │   │   ├── 🐍 __init__.py
│   │   │   └── 🐍 producer_runner.py
│   │   ├── 📁 spark
│   │   │   ├── 🐍 __init__.py
│   │   │   └── 🐍 processor_runner.py
│   │   └── 🐍 __init__.py
│   └── 📁 services
│       ├── 🐍 __init__.py
│       ├── 🐍 processor.py
│       └── 🐍 producer.py
├── ⚙️ .env.example
├── ⚙️ .gitignore
├── 📝 README.md
└── 📄 requirement.txt
```

## End-To-End Flow

```text
TomTom API
  -> Producer use case
  -> MinIO data lake (raw JSON snapshots)
  -> Kafka topic hanoi-incidents
  -> Spark Structured Streaming
  -> PostgreSQL tables
     - traffic_incidents
     - incident_summary_1min
  -> Grafana dashboards

Airflow
  -> Daily cleanup DAG
  -> Deletes rows older than 7 days from PostgreSQL
```

## What Changed

- Refactored the project into top-level layers directly under `src/`.
- Removed the `traffic_congestion` package wrapper.
- Moved the producer and processor entrypoints into `src/services/`.
- Restored `.env` and added `.env.example`.
- Replaced `print` statements with Python `logging`.
- Added MinIO as a data lake for raw incident batches.
- Added Airflow with a scheduled retention DAG.
- Added database indexes to support cleanup queries.

## Infrastructure Services

Run from the `docker` directory:

```bash
docker compose up -d --build
```

Available endpoints:

- Kafka broker: `localhost:29092`
- PostgreSQL: `localhost:5432`
- Grafana: `http://localhost:3000` with `admin/admin`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001` with credentials from `.env`
- Airflow: `http://localhost:8081` with `admin/admin`

## Install Python Dependencies

```bash
pip install -r requirement.txt
```

## Start The Producer

Run from the project root:

```bash
python src/services/producer.py
```

## Submit The Spark Job

```bash
docker exec -it -e PYTHONPATH=/app/src -e POSTGRES_HOST=host.docker.internal -u root docker-spark-master-1 /opt/spark/bin/spark-submit /app/src/services/processor.py
```

If your Compose-generated container name differs, inspect it with `docker ps -a` and adjust the command.
