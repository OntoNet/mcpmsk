# ТЗ: MCP-инструмент `pipeline_import_pg`

## Назначение

По сигнатуре файла (или по ключу в MinIO) **создать/выбрать пайплайн** и **импортировать данные в Postgres**. Инструмент сам:

* валидирует источник;
* (опц.) выдаёт ссылку для загрузки файла в MinIO, если файла ещё нет;
* создаёт объект `[DATA] PipelineTemplate` (если отсутствует) и фиксирует выбор `StorageConfig`;
* запускает Airflow DAG `csv_ingest_pg` c нужными параметрами;
* возвращает `runId`, ссылки на объекты в Онто и итоговый статус (если `wait=true`).

---

## Вызов (JSON-RPC → `tools/call`)

**name:** `pipeline_import_pg`

**arguments:**

```json
{
  "signatureId": "UUID-объекта [DATA] DatasetSignature",
  "target": { "schema": "public", "table": "tickets_fact" },

  "options": {
    "sep": ";",
    "encoding": "utf-8",
    "loadMode": "append",         // "append" | "replace"
    "createTable": true
  },

  "storage": {
    "configId": null              // если null → берем isDefault=true
  },

  "source": {
    "s3Key": null,                // если null → берём из signature.s3Key
    "ensureUploaded": true,       // если файла нет → вернуть presigned PUT для загрузки
    "fileName": "tickets.csv",    // нужно только для ensureUploaded
    "fileSize": 348243289,
    "contentType": "text/csv"
  },

  "execution": {
    "wait": false,                // true → ждём завершения DAG
    "waitTimeoutSec": 1800
  }
}
```

> Минимальный случай: передай `signatureId` и `target`. Остальное — по умолчанию.

---

## Результат (`result.content[0].json`)

```json
{
  "created": {
    "pipelineTemplateId": "TPL_UUID_or_existing"
  },
  "storage": {
    "configId": "SC_UUID",
    "bucket": "raw",
    "s3Key": "raw/<dataset>/<yyyy>/<mm>/source-<uuid>.csv",
    "upload": {
      "mode": "single",
      "putUrl": "https://minio/...&X-Amz-Signature=...",
      "expiresInSec": 3600
    }
  },
  "airflow": {
    "dagId": "csv_ingest_pg",
    "runId": "manual__2025-10-02T10:22:03Z",
    "state": "queued|running|success|failed",
    "webUrl": "http://<airflow-host>:8080"
  },
  "onto": {
    "signatureUrl": "https://app.ontonet.ru/ru/context/{realm}/entity/<SIG>",
    "pipelineTemplateUrl": "https://app.ontonet.ru/ru/context/{realm}/entity/<TPL>"
  },
  "notes": [
    "Если присутствует блок storage.upload — сначала загрузите файл, затем вызовите инструмент повторно с ensureUploaded=false"
  ]
}
```

---

## Пошаговое поведение (внутри тула)

1. **Валидация входа**

   * `signatureId`, `target.schema`, `target.table` — обязательны.
   * Если `source.s3Key` не задан, берём `signature.s3Key`; если и его нет — сформируем по StorageConfig.
   * Если `options.sep/encoding` не заданы, берём из `signature` (если есть) или дефолты `;` и `utf-8`.

2. **Разрешение Storage**

   * Если передан `storage.configId` → берём его.
   * Иначе: ищем `StorageConfig.isDefault=true`.
   * Если не найдено → ошибка `424 storage_config_not_found`.

3. **s3Key**

   * Если у сигнатуры нет `s3Key` → вычислить по `StorageConfig.pathPatternRaw` (см. ранее) и сохранить в `DatasetSignature`.
   * Проверить существование объекта в MinIO:

     * если нет и `source.ensureUploaded=true` → сгенерировать **presigned PUT** (`upload.putUrl`) и вернуть **без запуска DAG** (клиент загружает и вызывает инструмент снова с `ensureUploaded=false`);
     * если нет и `ensureUploaded=false` → ошибка `409 object_not_uploaded`.

4. **Пайплайн**

   * Найти существующий объект `[DATA] PipelineTemplate` для данной пары (`signature.datasetClass`, `target.table`), иначе создать **draft** со значениями:

     * `defaults`: `{"sep": "...", "encoding": "...", "loadMode":"append","createTable": true}`
     * `target`: `{"storage":"postgres","schema":"...","table":"..."}`
   * (Связи можешь добавить позже — ТЗ не требует, объект достаточно вернуть.)

5. **Запуск DAG**

   * Сконструировать `conf`:

     ```json
     {
       "presigned_get_url": null,             // не нужен, если воркеры умеют presign
       "s3_endpoint": "<endpoint из StorageConfig>",
       "bucket": "<bucket>",
       "key": "<s3Key>",
       "sep": "<...>",
       "encoding": "<...>",
       "target": { "schema":"...","table":"..." },
       "loadMode": "append|replace",
       "createTable": true,
       "signature_id":"SIG_UUID",
       "template_id":"TPL_UUID"
     }
     ```
   * Вызвать Airflow: `POST /api/v1/dags/csv_ingest_pg/dagRuns` с `{"conf": conf, "dag_run_id": "mcp__<ts>__<short>"}`.
   * Вернуть `runId`, `state`, `webUrl`.

6. **Ожидание (если `execution.wait=true`)**

   * Поллить `GET /api/v1/dags/{dagId}/dagRuns/{runId}` до `success|failed` или таймаута.
   * Вернуть итоговый `state` и, если есть, артефакты (путь отчёта/DDL).

---

## Ошибки (JSON-RPC `error`)

* `400 invalid_arguments` — отсутствует `signatureId` или цель.
* `424 storage_config_not_found` — нет активного `StorageConfig`.
* `409 object_not_uploaded` — файла в MinIO нет, а загрузку не запросили.
* `404 signature_not_found` — нет такой сигнатуры в Онто.
* `404 dag_not_found` — нет DAG `csv_ingest_pg`.
* `502 airflow_unreachable` — недоступен Airflow API.
* `500 internal_error` — прочие ошибки; лог с `requestId`.

---

## Идемпотентность

* Повторный вызов с тем же `signatureId` и `target`:

  * не создаёт новый PipelineTemplate, если уже есть совместимый (с тем же `target` и `defaults`);
  * не меняет `signature.s3Key`, если он уже сохранён;
  * может запускать **новый** импорт (новый `runId`), это ожидаемо.

---

## Конфигурация (ENV MCP)

* `ONTO_API_BASE`, `ONTO_API_TOKEN`, `ONTO_REALM_ID`
* `AIRFLOW_API_URL`, `AIRFLOW_API_USER`, `AIRFLOW_API_PASS`
* `MINIO_REGION` (если нужен для presign внутри DAG; для варианта с trio-параметрами не обязателен)
* Тайминги: `AIRFLOW_TIMEOUT_SEC=30`, `AIRFLOW_RETRY=3`

---

## Примеры вызова

### A) Файл уже в MinIO (самый частый)

```json
{
  "name": "pipeline_import_pg",
  "arguments": {
    "signatureId": "SIG_UUID",
    "target": { "schema": "public", "table": "tickets_fact" },
    "options": { "sep": ";", "encoding": "utf-8", "loadMode": "append", "createTable": true },
    "storage": { "configId": null },
    "source": { "s3Key": "raw/tickets/2025/09/source-...csv", "ensureUploaded": false },
    "execution": { "wait": false }
  }
}
```

### B) Файл ещё не загружен — получить ссылку и потом запустить

1. Вызов №1:

```json
{
  "name": "pipeline_import_pg",
  "arguments": {
    "signatureId": "SIG_UUID",
    "target": { "schema": "public", "table": "tickets_fact" },
    "source": { "s3Key": null, "ensureUploaded": true, "fileName": "tickets.csv", "fileSize": 348243289, "contentType": "text/csv" }
  }
}
```

**Ответ:** вернётся блок `storage.upload.putUrl` и подсказка загрузить файл.

2. Загрузил файл → вызов №2 (та же команда с `ensureUploaded=false`). Тул запустит DAG и вернёт `runId`.

---

## Короткий чеклист реализации

* [ ] Чтение `DatasetSignature` и дописывание `s3Key`, если пуст.
* [ ] Выбор `StorageConfig` (по `configId`/default).
* [ ] `HEAD` в MinIO для проверки наличия объекта (через MinIO SDK или presign).
* [ ] Генерация presigned **PUT** при `ensureUploaded=true`.
* [ ] Поиск/создание `[DATA] PipelineTemplate` (draft) под `target` + `sep/encoding`.
* [ ] Вызов Airflow REST с корректным `conf`.
* [ ] Опциональное ожидание `success|failed`.
* [ ] Возврат ссылок на объекты Онто: `…/context/{realmId}/entity/{id}`.
