# Onto MCP Server

This repository (https://github.com/OntoNet/mcpmsk) contains the Onto MCP server together with the supporting Airflow pipeline used to ingest CSV datasets into PostgreSQL. The project exposes a FastMCP-compatible tool `pipeline_import_pg` that orchestrates storage discovery, presigned upload generation, and Airflow DAG execution.


## Components

- **FastMCP server (`onto_mcp`)** – validates requests, talks to Onto APIs, resolves storage configs, and triggers Airflow.
- **Airflow DAG `csv_ingest_pg`** – pulls a CSV object from MinIO/S3 and loads it into Postgres using `COPY`.
- **Local stack (doc/scripts/docker-compose.yml)** – PostgreSQL, MinIO, Redis, Airflow, and the MCP server bundled together for end-to-end testing.

## Running the full stack

Detailed instructions live in `doc/README.md`. In short:

```bash
cd doc/scripts
# build custom airflow image with boto3/psycopg2
docker build -t onto-airflow:2.9.3 -f ../../docker/airflow/Dockerfile ../../
# start services
docker-compose up -d
# copy DAG into the shared volume
docker cp ../../doc/DAG/csv_ingest_pg.py onto-data-stack_airflow-webserver_1:/opt/airflow/dags/
```

After the containers are up, open the Airflow UI (`http://<host>:8080`) and ensure `csv_ingest_pg` is visible and unpaused. Triggering the MCP tool will now launch Airflow runs.

## Using the MCP tool

The tool is available through FastMCP clients (Cursor, CLI, playground). Example payload:

```json
{
  "signatureId": "sig-123",
  "target": {"schema": "public", "table": "museum_ticket_sales"},
  "options": {"sep": ";", "encoding": "utf-8", "loadMode": "append", "createTable": true},
  "source": {"s3Key": "raw/museum/2025/10/source.csv", "ensureUploaded": false}
}
```

If `ensureUploaded` is `true`, the server returns a presigned PUT URL instead of triggering Airflow immediately.

## Local development

```bash
python -m pip install -r requirements.txt
python -m onto_mcp.server          # stdio transport
MCP_TRANSPORT=http python -m onto_mcp.server  # HTTP transport
```

Run tests with:

```bash
python -m pytest
```

## Repository layout

```
onto_mcp/                        # MCP server code and utilities
onto_mcp/pipeline_import_pg.py   # main executor used by the MCP tool
doc/DAG/csv_ingest_pg.py         # Airflow DAG definition (copy into Airflow volume)
doc/scripts/docker-compose.yml   # docker stack for PG + MinIO + Airflow + MCP
doc/README.md                    # Detailed setup guide
```

## Troubleshooting

- `403 Forbidden` from Airflow REST → ensure `AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.basic_auth` is set in the docker compose env.
- `404 dag_not_found` → DAG file not present in `/opt/airflow/dags`, recopy and restart Airflow services.
- Import failures → run `airflow dags list-import-errors` inside the webserver container to inspect stack traces.

## License

The project is distributed under the terms specified in the repository (see `LICENSE` if present) or the accompanying hackathon guidelines.

## MCP client configuration

Add the following block to your `mcp.json` (Cursor, Claude Desktop, etc.):

```json
{
  "mcpServers": {
    "hakaton": {
      "url": "http://***.***.***.***:8899/mcp"
    }
  }
}
```

## Test stand access

```bash
ssh -i id_trash2 yc-user@***.***.***.***
```

(The private key `id_trash2` is distributed together with the presentation.)

## Onto credentials

- URL: https://app.ontonet.ru/
- Login: ``
- Password: ``

