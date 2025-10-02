# ТЗ для Codex: Preflight-процесс и сверка с Онто

## 0. Цель

Реализовать в MCP-сервере инструменты и логику, позволяющие:

1. получить от клиента «сигнатуру» CSV-файла без передачи больших данных;
2. сопоставить сигнатуру с каталогом **DatasetClass** в Онто;
3. при отсутствии подходящего класса — создать черновую запись (draft) и связанный шаблон обработки;
4. вернуть клиенту план дальнейших действий (загрузка файла, запуск пайплайна).

---

## 1. Артефакты MCP (инструменты)

### 1.1 `preflight_plan`

Возвращает **план действий** для клиента (без S3, только локальные файлы).

**Вход (arguments):**

```json
{
  "source": "/absolute/path/to/file.csv",
  "forceSep": null,        // "," | ";" | null
  "forceEncoding": null    // "utf-8" | "cp1251" | null
}
```

**Выход (result.content[0].json):**

```json
{
  "actions": [
    {
      "type": "shell",
      "name": "build-signature",
      "cmd": "python3 - <<'PY'\n...python-болванка...\nPY",
      "env": { "SRC": "/absolute/path/to/file.csv", "FORCE_SEP": "", "FORCE_ENCODING": "" }
    },
    {
      "type": "mcp_call",
      "name": "send-signature",
      "tool": "preflight_submit",
      "args_from_file": "payload.json"
    }
  ],
  "notes": ["Команда создаст payload.json в текущей директории клиента"]
}
```

**Требования к python-болванке:**

* Нормализация заголовка: `lower → trim → пробелы/дефисы в '_' → удалить [^a-z0-9_а-яё] → схлопнуть '_'`.
* Дедукция разделителя (`,`/`;`) с возможностью принудительной установки `FORCE_SEP`.
* Вычислить: `headers[]`, `numCols`, `headerHash`, `headerSortedHash`, `encoding`, `sep`.
* Записать `payload.json` формата:

```json
{
  "fileName": "tickets.csv",
  "fileSize": 348243289,
  "signature": {
    "encoding": "utf-8",
    "sep": ";",
    "hasHeader": true,
    "numCols": 27,
    "headers": ["created","order_status", "..."],
    "headerHash": "sha256:...",
    "headerSortedHash": "sha256:...",
    "stats": { "rowsScanned": 0 }
  }
}
```

**Ошибки:**

* `400` — отсутствует/невалидный `source`.
* `501` — клиент не поддерживает `shell`.

---

### 1.2 `preflight_submit`

Принимает `payload.json`, ищет соответствие в Онто и формирует рекомендации.

**Вход (arguments):**

```json
{ "payload": { /* содержимое payload.json */ } }
```

**Действия:**

1. Извлечь `signature` (headers, numCols, headerHash, headerSortedHash).
2. **Поиск DatasetClass в Онто** (см. §3).
3. Если класс найден:

   * Создать `DatasetSignature` и `RecognitionResult` в Онто, проставить связи.
   * Вернуть `match`, `recommendation` и `upload.s3Key`.
4. Если не найден:

   * Создать `DatasetClass(draft=true)` с полями из сигнатуры.
   * Создать дочерние `ColumnSignature` (минимум: name, dtypeGuess?, examples? — допускается пусто на первом шаге).
   * Создать `PipelineTemplate(draft=true)` с дефолтными правилами.
   * Создать `DatasetSignature` + `RecognitionResult` и связи.
   * Вернуть `match(draft=true)`, `next="review_required"`, `upload.s3Key`.

**Выход (result.content[0].json):**

```json
{
  "match": { "classId": "uuid", "templateId": "uuid", "confidence": 0.92, "draft": false },
  "recommendation": {
    "storage": "postgres",
    "schema": "public",
    "table": "tickets_fact",
    "partitionBy": "date_trunc('day', start_datetime)"
  },
  "upload": { "s3Key": "raw/tickets/2021/09/source-<uuid>.csv" },
  "onto": {
    "classUrl": "https://app.ontonet.ru/ru/context/{realmId}/entity/{classId}",
    "signatureUrl": "https://app.ontonet.ru/ru/context/{realmId}/entity/{signatureId}"
  }
}
```

**Ошибки:**

* `422` — payload не содержит нужных полей.
* `502` — недоступен API Онто.
* `500` — прочие.

---

## 2. Модель Онто (минимум полей)

### 2.1 `DatasetClass` (шаблон: META_UUID_DATASETCLASS)

Поля:

* `headerHash: string`
* `headerSortedHash: string`
* `headersSorted: text` (строка `;`-разделённых имён, отсортированных)
* `numCols: int`
* `keywords: text` (через запятую; нормализованные токены)
* `piiPhone: bool`, `piiFio: bool`, `piiInn: bool`, `piiBirthday: bool` (минимум)
* `priority: int` (по умолчанию 0)
* `draft: bool`

### 2.2 `ColumnSignature` (опц., привязка к DatasetClass)

* `name: string`
* `dtypeGuess: string` (one of: int/float/datetime/bool/text)
* `examples: text` (короткая строка с 1-3 примерами)

### 2.3 `PipelineTemplate`

* `name: string`
* `defaults: json/text` (sep, encoding, mapping типов, PII-маскирование)
* `target: json/text` (storage/schema/table, partition/indexing)
* `draft: bool`

### 2.4 `DatasetSignature` (факт полученной сигнатуры)

* `fileName: string`
* `fileSize: long`
* `headerHash: string`
* `headerSortedHash: string`
* `numCols: int`
* `headersSorted: text`
* `sep: string`, `encoding: string`
* связь: `recognized_as -> DatasetClass`
* связь: `based_on -> PipelineTemplate` (если есть)

### 2.5 `RecognitionResult`

* `score: float` (0..1)
* `matchedBy: enum` (`headerHash|headerSortedHash|numCols|keywords|manual`)
* `timestamp: datetime`
* связи на Signature и Class.

> Ссылки на объекты в интерфейсе формируем как
> `https://app.ontonet.ru/ru/context/{realmId}/entity/{entityId}`.

---

## 3. Поиск и скоринг (внутри `preflight_submit`)

### 3.1 Запросы к Онто (`/entity/find/v2`)

Всегда с пагинацией:

```json
"pagination": { "first": 0, "offset": 20 }
```

**A. По `headerHash`:**

```json
{ "metaEntityRequest": { "uuid": "META_UUID_DATASETCLASS" },
  "metaFieldFilters": [{ "fieldUuid": "FIELD_headerHash", "value": "<sha256:...>" }],
  "pagination": { "first": 0, "offset": 20 } }
```

**B. Если пусто — по `headerSortedHash`:**

```json
{ "metaEntityRequest": { "uuid": "META_UUID_DATASETCLASS" },
  "metaFieldFilters": [{ "fieldUuid": "FIELD_headerSortedHash", "value": "<sha256:...>" }],
  "pagination": { "first": 0, "offset": 20 } }
```

**C. Если пусто — кандидаты по `numCols`:**

```json
{ "metaEntityRequest": { "uuid": "META_UUID_DATASETCLASS" },
  "metaFieldFilters": [{ "fieldUuid": "FIELD_numCols", "value": "27" }],
  "pagination": { "first": 0, "offset": 100 } }
```

### 3.2 Локальный скоринг кандидатов

```
score =
  1.0 * [headerHash match] +
  0.8 * [headerSortedHash match] +
  0.4 * jaccard(headers_incoming, headers_class) +
  0.2 * overlap(keywords)/|keywords| +
  0.2 * piiMatchRatio +
  0.05* normalizedPriority
```

Порог «нашли»: **score ≥ 0.7**.

---

## 4. Действия при «нашли / не нашли»

### 4.1 Нашли

* Создать `DatasetSignature` + `RecognitionResult(score, matchedBy)`.
* Подтянуть `PipelineTemplate` из класса или по умолчанию.
* Вернуть `match/recommendation/upload`.

### 4.2 Не нашли

* Создать `DatasetClass(draft=true)` с полями из сигнатуры.
* (Опц.) Создать `ColumnSignature` по каждому полю.
* Создать `PipelineTemplate(draft=true)` с дефолтами.
* Создать `DatasetSignature` + `RecognitionResult(score=<0.7>, matchedBy='numCols')`.
* Вернуть `match(draft=true)/next='review_required'/upload`.

---

## 5. Нефункциональные требования

* **Без ML**: только детерминированные эвристики и хэши.
* Время ответа `preflight_submit`: P95 < 300 мс (без создания сущностей) и < 1.5 с (с созданием).
* Логирование: `correlationId` из `X-Request-Id` или генерить UUID.
* Пагинация соблюдается всегда (см. правила пользователя).
* Секьюрность: токен Онто — из переменных окружения MCP; не логировать секреты.
* Все новые сущности Онто создавать в привязанном `realmId`.

---

## 6. Примеры ответов инструмента

### 6.1 Совпадение найдено

```json
{
  "match": { "classId": "f1e2...", "templateId": "a0b1...", "confidence": 0.91, "draft": false },
  "recommendation": { "storage": "postgres", "schema": "public", "table": "tickets_fact", "partitionBy": "date_trunc('day', start_datetime)" },
  "upload": { "s3Key": "raw/tickets/2021/09/source-0b23.csv" },
  "onto": {
    "classUrl": "https://app.ontonet.ru/ru/context/REALM/entity/f1e2...",
    "signatureUrl": "https://app.ontonet.ru/ru/context/REALM/entity/9c7d..."
  }
}
```

### 6.2 Новый класс (draft)

```json
{
  "match": { "classId": "new-123", "templateId": "tpl-456", "confidence": 0.48, "draft": true },
  "next": "review_required",
  "upload": { "s3Key": "raw/unknown/2021/source-77aa.csv" },
  "onto": {
    "classUrl": "https://app.ontonet.ru/ru/context/REALM/entity/new-123",
    "signatureUrl": "https://app.ontonet.ru/ru/context/REALM/entity/sig-789"
  }
}
```

---

## 7. Тест-кейсы

1. **Точный заголовок**: `headerHash` совпадает → один кандидат, score=1.0.
2. **Переставленные колонки**: `headerSortedHash` совпадает, `headerHash` — нет → score≈0.8.
3. **Тот же домен, другой набор**: `numCols` совпало, Jaccard ≥ 0.8 → score≥0.7.
4. **Новый файл**: ничего не найдено → создаётся `DatasetClass(draft)` и возвращается `review_required`.
5. **Дубликаты имён колонок**: нормализация даёт совпадения → предупреждение, но обработка продолжается (фиксируем в `notes`).
6. **Неверный путь/кодировка**: план строится, но Python падает → клиент сигналит, инструмент возвращает `error`.

---

## 8. Конфигурация среды

Переменные окружения MCP:

* `ONTO_API_BASE` — `https://app.ontonet.ru/api/v2/core`
* `ONTO_API_TOKEN` — токен
* `ONTO_REALM_ID` — целевой `realmId`

---

## 9. Дальше

После реализации `preflight_plan` и `preflight_submit` подключить инструмент **`upload_url`** (presigned PUT) и DAG `tickets_ingest_csv` (Airflow) — по тем же идентификаторам класса/шаблона, чтобы завершить «нюх → импорт».
