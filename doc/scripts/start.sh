#!/usr/bin/env bash
set -Eeuo pipefail

# 0) Создаём .env, если его ещё нет
if [ ! -f .env ]; then
  build_version=$(cd mcpmsk && git describe --tags --always 2>/dev/null || echo dev)
  cat > .env <<EOF
PG_USER=pguser
PG_PASSWORD=pgpass
PG_DB=metastore

MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123

# Fill in your own secrets:
ONTO_API_BASE=https://app.ontonet.ru/api/v2/core
ONTO_API_TOKEN=api-key-acdc7d83-bafc-456f-b103-9058e76dcac3
SESSION_STATE_API_KEY=api-key-acdc7d83-bafc-456f-b103-9058e76dcac3
ONTO_REALM_ID=85b39b8e-929f-4ef4-af32-09da985b9974
ONTO_META_COLUMNSIGN_NAME=ColumnSignature
ONTO_META_PIPELINE_NAME=PipelineTemplate
ONTO_DEBUG_HTTP=true
ONTO_BUILD_VERSION=$build_version
EOF
fi

# 1) Папки для Airflow
mkdir -p airflow_dags airflow_logs
