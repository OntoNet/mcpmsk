from __future__ import annotations

import csv
import io
import logging
import os
import tempfile
from datetime import datetime

import boto3
import psycopg2
from psycopg2 import sql
import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator


DAG_ID = "csv_ingest_pg"
DEFAULT_ARGS = {"owner": "onto-mcp", "depends_on_past": False}


def _require(mapping: dict, key: str, *, where: str = "conf") -> str:
    value = mapping.get(key)
    if value in (None, ""):
        raise AirflowFailException(f"Missing required key '{key}' in {where}")
    return value


def _resolve_pg_dsn() -> str:
    for env_key in ("PIPELINE_IMPORT_PG_DSN", "PG_URL", "POSTGRES_DSN"):
        value = os.getenv(env_key)
        if value:
            return value
    raise AirflowFailException("Postgres DSN missing: set PIPELINE_IMPORT_PG_DSN or PG_URL env var")


def _resolve_s3_client(endpoint: str):
    access_key = (
        os.getenv("PIPELINE_IMPORT_S3_ACCESS_KEY")
        or os.getenv("STAGING_S3_ACCESS_KEY")
        or os.getenv("MINIO_ROOT_USER")
    )
    secret_key = (
        os.getenv("PIPELINE_IMPORT_S3_SECRET_KEY")
        or os.getenv("STAGING_S3_SECRET_KEY")
        or os.getenv("MINIO_ROOT_PASSWORD")
    )
    if not access_key or not secret_key:
        raise AirflowFailException("MinIO/S3 credentials missing. Set PIPELINE_IMPORT_S3_ACCESS_KEY/SECRET_KEY")

    region = os.getenv("PIPELINE_IMPORT_S3_REGION") or os.getenv("MINIO_REGION") or "us-east-1"

    session = boto3.session.Session()
    return session.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


def _download_csv(conf: dict) -> tuple[str, str, str]:
    bucket = _require(conf, "bucket")
    key = _require(conf, "key")
    encoding = conf.get("encoding", "utf-8")
    presigned_url = conf.get("presigned_get_url")
    endpoint = _require(conf, "s3_endpoint")

    logging.info("Fetching object s3://%s/%s", bucket, key)

    if presigned_url:
        response = requests.get(presigned_url, timeout=60)
        if response.status_code >= 400:
            raise AirflowFailException(
                f"Failed to download via presigned URL ({response.status_code}): {response.text[:200]}"
            )
        payload = response.content
    else:
        client = _resolve_s3_client(endpoint)
        obj = client.get_object(Bucket=bucket, Key=key)
        payload = obj["Body"].read()

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    tmp.write(payload)
    tmp.flush()
    tmp.close()

    logging.info("Object cached at %s (%d bytes)", tmp.name, len(payload))
    return tmp.name, encoding, conf.get("sep", ";")


def _ensure_table(cursor, schema: str, table: str, columns: list[str], create_table: bool):
    if create_table:
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        column_defs = [
            sql.SQL("{} TEXT").format(sql.Identifier(col)) for col in columns
        ]
        cursor.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(", ").join(column_defs),
            )
        )


def _truncate_if_needed(cursor, schema: str, table: str, load_mode: str):
    if load_mode == "replace":
        cursor.execute(
            sql.SQL("TRUNCATE TABLE {}.{}").format(sql.Identifier(schema), sql.Identifier(table))
        )


def _load_to_postgres(conf: dict) -> dict:
    target = conf.get("target") or {}
    schema = _require(target, "schema", where="conf.target")
    table = _require(target, "table", where="conf.target")

    tmp_path, encoding, sep = _download_csv(conf)

    load_mode = (conf.get("loadMode") or "append").lower()
    create_table = bool(conf.get("createTable", True))

    with open(tmp_path, "r", encoding=encoding, newline="") as csv_file:
        reader = csv.reader(csv_file, delimiter=sep)
        try:
            header = next(reader)
        except StopIteration:
            raise AirflowFailException("CSV file is empty, header not found")
        header = [col.strip() for col in header]
        row_count = sum(1 for _ in reader)

    logging.info("CSV header: %s", header)
    logging.info("CSV rows (excluding header): %d", row_count)

    conn = psycopg2.connect(_resolve_pg_dsn())
    try:
        conn.autocommit = False
        with conn.cursor() as cursor:
            _ensure_table(cursor, schema, table, header, create_table)
            _truncate_if_needed(cursor, schema, table, load_mode)

            copy_statement = sql.SQL(
                "COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER {})"
            ).format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(", ").join(sql.Identifier(col) for col in header),
                sql.Literal(sep),
            )

            with open(tmp_path, "r", encoding=encoding, newline="") as data_stream:
                cursor.copy_expert(copy_statement.as_string(conn), data_stream)

        conn.commit()
        logging.info("Rows loaded into %s.%s: %d", schema, table, row_count)
    finally:
        conn.close()
        os.unlink(tmp_path)

    return {"rows_loaded": row_count, "schema": schema, "table": table}


def run_pipeline(**context):
    conf = context.get("dag_run").conf or {}
    required_keys = ["bucket", "key", "target"]
    for key in required_keys:
        if key not in conf:
            raise AirflowFailException(f"'{key}' missing in dag_run.conf")

    result = _load_to_postgres(conf)
    logging.info("Ingestion complete: %s", result)
    return result


with DAG(
    dag_id=DAG_ID,
    description="Load CSV from S3/MinIO into Postgres for MCP pipeline_import_pg",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["onto", "pipeline"],
) as dag:
    ingest_csv = PythonOperator(
        task_id="ingest_csv_to_postgres",
        python_callable=run_pipeline,
    )
