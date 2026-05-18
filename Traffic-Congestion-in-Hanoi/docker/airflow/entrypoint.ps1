$ErrorActionPreference = "Stop"

airflow db migrate

try {
    airflow users create `
      --username admin `
      --password admin `
      --firstname Admin `
      --lastname User `
      --role Admin `
      --email admin@example.com
} catch {
    Write-Host "Airflow admin user already exists or could not be created."
}

airflow connections add traffic_postgres `
  --conn-type postgres `
  --conn-host $env:POSTGRES_HOST `
  --conn-login $env:DB_USER `
  --conn-password $env:DB_PASS `
  --conn-port $env:POSTGRES_PORT `
  --conn-schema $env:DB_NAME

Start-Job { airflow scheduler } | Out-Null
airflow webserver --port 8081
