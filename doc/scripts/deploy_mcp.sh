#!/usr/bin/env bash
set -Eeuo pipefail

# запускать из каталога /home/yc-user/onto-data-stack
cd "$(dirname "$0")" || exit 1

echo "==> git pull (mcpmsk)"
git -C mcpmsk pull --ff-only

build_version="$(git -C mcpmsk describe --tags --always)"
echo "==> текущий build_version: $build_version"

# обновляем/добавляем строку ONTO_BUILD_VERSION в .env
if grep -q '^ONTO_BUILD_VERSION=' .env 2>/dev/null; then
  sed -i "s/^ONTO_BUILD_VERSION=.*/ONTO_BUILD_VERSION=$build_version/" .env
else
  printf 'ONTO_BUILD_VERSION=%s\n' "$build_version" >> .env
fi

echo "==> пересобираем и перезапускаем только mcp"
docker-compose rm -sf mcp
docker-compose up -d --build --no-deps mcp

echo "==> проверяем версию и состояние"
docker exec onto-data-stack_mcp_1 env | grep ONTO_BUILD_VERSION
docker logs --tail=5 onto-data-stack_mcp_1
docker-compose ps | grep mcp
