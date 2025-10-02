# MCP Environment Setup Guide

This document describes how to spin up the complete data-ingestion sandbox (PostgreSQL + MinIO + Redis + Airflow + MCP server) and how to trigger the `pipeline_import_pg` flow that loads CSV files into Postgres through Airflow.

## 1. Prerequisites

- Docker Engine 24+ and `docker-compose` v1.29 (or the `docker compose` plugin).
- Git.
- Python 3.11+ (only if you want to run the MCP server locally outside the containers).
- Network access to the services if you deploy on a remote host.

Clone the repository and switch to the project root:

```bash
git clone <repo-url>
cd mcpmsk
```

All paths below are relative to the repository root unless stated otherwise.

## 2. Configuration Files

The stack relies on environment variables defined through shell export or `.env`. For convenience set them in your terminal session before starting the stack (the defaults work for local sandbox):

```bash
export PG_USER=postgres
export PG_PASSWORD=postgres
export PG_DB=hakaton
export MINIO_ROOT_USER=minioadmin
export MINIO_ROOT_PASSWORD=minioadmin
export ONTO_API_BASE=https://api.example.com
export ONTO_API_TOKEN=dummy-token
export ONTO_REALM_ID=realm-123
# Optional overrides
export MINIO_REGION=us-east-1
```

For production deployments use secure secrets and store them in your secret manager; do not commit them to Git.

## 3. Build the containers

The Airflow services need additional Python packages (`boto3`, `psycopg2-binary`). Build the custom image once:

```bash
cd doc/scripts
docker build -t onto-airflow:2.9.3 -f ../../docker/airflow/Dockerfile ../../
```

Update `docker-compose.yml` if you want to use a different tag.

## 4. Start the stack

Launch the services from `doc/scripts/docker-compose.yml`:

```bash
cd doc/scripts
docker-compose up -d
```

This brings up:

- `pg`: PostgreSQL 16
- `minio`: MinIO object storage
- `redis`: Redis for Airflow Celery backend
- `airflow-init`, `airflow-webserver`, `airflow-scheduler`, `airflow-worker`
- `mcp`: the Onto MCP server with the `pipeline_import_pg` tool

Check the containers:

```bash
docker ps
```

Wait until `airflow-webserver` and `airflow-scheduler` show `Up` status.

## 5. Install the DAG

The DAG file lives in `doc/DAG/csv_ingest_pg.py`. It must be copied into the shared Airflow volume so that the webserver and scheduler can import it:

```bash
docker cp doc/DAG/csv_ingest_pg.py onto-data-stack_airflow-webserver_1:/opt/airflow/dags/
docker cp doc/DAG/csv_ingest_pg.py onto-data-stack_airflow-scheduler_1:/opt/airflow/dags/
docker cp doc/DAG/csv_ingest_pg.py onto-data-stack_airflow-worker_1:/opt/airflow/dags/
```

(If you use a named volume mounted on the host, copy the file to `/var/lib/docker/volumes/airflow_dags/_data/`.)

Restart the Airflow services once:

```bash
docker-compose restart airflow-webserver airflow-scheduler airflow-worker
```

Then verify in the UI (`http://localhost:8080`) that DAG `csv_ingest_pg` appears and is unpaused.

## 6. Access credentials

Default credentials created by `airflow-init`:

- Airflow: `admin` / `admin`
- MinIO: `minioadmin` / `minioadmin`
- PostgreSQL: `postgres` / `postgres`

Change them for any non-local deployment.

## 7. Triggering the pipeline

### Via MCP tool

Run the MCP server locally or connect through your MCP client (the playground or the IDE plugin). Call the tool with arguments similar to:

```json
{
  "signatureId": "sig-123",
  "target": {"schema": "public", "table": "museum_ticket_sales"},
  "options": {"sep": ";", "encoding": "utf-8", "loadMode": "append", "createTable": true},
  "source": {"s3Key": "raw/museum/2025/10/source-abc.csv", "ensureUploaded": false}
}
```

When the file already exists in MinIO the DAG is triggered immediately; if `ensureUploaded=true` the tool will return a presigned upload URL first.

### Manual trigger (for debugging)

```bash
curl -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{
        "dag_run_id": "manual__test",
        "conf": {
          "s3_endpoint": "http://minio:9000",
          "bucket": "raw",
          "key": "raw/museum/2025/10/source.csv",
          "sep": ";",
          "encoding": "utf-8",
          "target": {"schema": "public", "table": "museum_ticket_sales"},
          "loadMode": "append",
          "createTable": true
        }
      }' \
  http://localhost:8080/api/v1/dags/csv_ingest_pg/dagRuns
```

Monitor the progress in the Airflow UI (`Grid` tab) and view task logs to see connection info, row counts, etc.

## 8. Verifying results

Use any SQL client (psql, DBeaver, HeidiSQL) to check the target table:

```bash
docker exec -it onto-data-stack_pg_1 psql -U "$PG_USER" "$PG_DB" -c 'SELECT COUNT(*) FROM public.museum_ticket_sales;'
```

The screenshot above shows the result in HeidiSQL after a successful import run.

## 9. Shutdown

To stop the stack:

```bash
cd doc/scripts
docker-compose down
```

Add `-v` if you also want to remove persistent volumes.

## 10. Troubleshooting

- **DAG not found** – ensure `csv_ingest_pg.py` is present in `/opt/airflow/dags` and Airflow services are restarted.
- **403 from Airflow API** – confirm `AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.basic_auth` is set and curl with basic auth returns 200.
- **Missing S3 credentials** – configure `PIPELINE_IMPORT_S3_ACCESS_KEY`, `PIPELINE_IMPORT_S3_SECRET_KEY`, and `PIPELINE_IMPORT_S3_ENDPOINT`.
- **Import errors** – run `docker exec <webserver> airflow dags list-import-errors` to see Python stack traces.

With this setup the `pipeline_import_pg` tool will automatically create pipeline templates, ensure storage assignments, and launch `csv_ingest_pg` in Airflow to load CSV datasets into Postgres.
