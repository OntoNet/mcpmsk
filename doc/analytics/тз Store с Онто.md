# ТЗ: OntoStore — реальный стор для сохранения результатов preflight в Онто

## 0. Цель

Заменить `_MemoryStore` в `PreflightService` на **реальный стор** (OntoStore), который создаёт и обновляет сущности в выбранном пространстве Онто (realm) через API `v2/core`. Результат preflight (сигнатура датасета, найденный/созданный класс, шаблон пайплайна, связи и метаданные) должен появляться в Онто и быть доступным по прямым ссылкам вида:

```
https://app.ontonet.ru/ru/context/{realmId}/entity/{entityId}
```

## 1. Область работ

* Реализовать интерфейс стора `PreflightStore` с реализацией `OntoStore`.
* Внедрить `OntoStore` в `PreflightService` через DI/фабрику.
* Поддержать «идемпотентное» создание сущностей (upsert по хэшу/уникальным ключам).
* Добавить конфиг через env-переменные.
* Написать интеграционные тесты с моками Онто API.

# ТЗ: `preflight_submit` — Поиск и создание объектов в Онто (без связей)

## 0. Цель

По результату локального preflight (JSON с сигнатурой CSV) найти **объект** шаблона `[DATA] DatasetClass` в Онто.
Если не найден — **создать** новый объект (draft).
Всегда создать **DatasetSignature** и **RecognitionResult**. **Связи пока не создаём.**

---

## 1. Вход инструмента

JSON (тот же, что вернул `preflight_plan`):

```json
{
  "fileName": "tickets.csv",
  "fileSize": 314572800,
  "signature": {
    "encoding": "utf-8",
    "sep": ";",
    "hasHeader": true,
    "numCols": 27,
    "headers": ["created","order_status","ticket_status","..."],
    "headerHash": "sha256:…",
    "headerSortedHash": "sha256:…",
    "stats": { "rowsScanned": 1000 }
  }
}
```

---

## 2. Конфигурация (ENV)

* `ONTO_API_BASE` — `https://app.ontonet.ru/api/v2/core`
* `ONTO_API_TOKEN` — `api-key-…`
* `ONTO_REALM_ID` — UUID realm
* `ENABLE_CREATE` — `true|false` (если `false`, делаем только поиск)
* **Динамическая мета** (рекомендуется, без FIELD_MAP_PATH):

  * `ONTO_META_DATASETCLASS_NAME=DatasetClass`
  * `ONTO_META_SIGNATURE_NAME=DatasetSignature`
  * `ONTO_META_RECOG_NAME=RecognitionResult`
  * `ONTO_META_COLUMNSIGN_NAME=ColumnSignature`
  * `ONTO_META_PIPELINE_NAME=PipelineTemplate`
    *При старте сервис делает discovery: получает metaEntityUuid и fieldUuid по именам.*

---

## 3. Используемые API Онто

* `POST /realm/{realmId}/entity/find/v2` — поиск (обязательно `pagination`)
* `POST /realm/{realmId}/entity` — создание сущности

Пример поиска (всегда с пагинацией):

```json
{
  "metaEntityRequest": { "uuid": "<META_UUID_DATASETCLASS>" },
  "metaFieldFilters": [{ "fieldUuid": "<FIELD_headerHash>", "value": "sha256:..." }],
  "pagination": { "first": 0, "offset": 20 }
}
```

Пример создания:

```json
{
  "metaEntityUuid": "<META_UUID_DATASETCLASS>",
  "fields": { "<FIELD_headerHash>": "sha256:...", "...": "..." }
}
```

---

## 4. Алгоритм (поиск → создание)

1. Извлечь из входа: `H=headerHash`, `Hs=headerSortedHash`, `N=numCols`, `Hdr=headers[]`.

2. **Поиск А — точный `headerHash`**
   `find/v2` по полю `headerHash`. Если найдено ≥1 → берём первый, `matchedBy="headerHash"`, `confidence=1.0`.

3. **Поиск B — `headerSortedHash`**
   Если А пуст: `find/v2` по полю `headerSortedHash`. Если найдено → `matchedBy="headerSortedHash"`, `confidence=0.8`.

4. **Поиск C — кандидаты по `numCols`**
   Если B пуст: `find/v2` по `numCols` (догружаем все страницы).
   Локально считаем **Jaccard** между `Hdr` и `headersSorted` кандидата.
   `score = 0.4 * jaccard`. Порог матча `score ≥ 0.7`.
   Если есть — `matchedBy="numCols+jaccard"`, `confidence=score`.

5. **Если не найдено и `ENABLE_CREATE=true`** — создаём **новый объект** `[DATA] DatasetClass (draft=true)` (см. п.5.1).
   Если `ENABLE_CREATE=false` — возвращаем `match=null`.

6. Всегда **создаём** `[DATA] DatasetSignature` (см. п.5.3) и `[DATA] RecognitionResult` (см. п.5.4).

> Связи намеренно **не** создаём на этом этапе.



## 2. Конфигурация (ENV)

| Переменная                 | Обязательна | Пример                               | Описание                              |
| -------------------------- | ----------- | ------------------------------------ | ------------------------------------- |
| `ONTO_API_BASE`            | да          | `https://app.ontonet.ru/api/v2/core` | базовый URL API Онто                  |
| `ONTO_API_TOKEN`           | да          | `api-key-xxxxx`                      | Bearer токен                          |
| `ONTO_REALM_ID`            | да          | `000ba00a-...`                       | целевой `realmId`                     |
| `ONTO_TIMEOUT_SEC`         | нет         | `15`                                 | таймаут HTTP                          |
| `ONTO_RETRY`               | нет         | `3`                                  | число retries (5xx/timeout)           |
| `ONTO_PREFLIGHT_NAMESPACE` | нет         | `hakaton`                            | префикс для служебных имён            |
| `ONTO_DRY_RUN`             | нет         | `false`                              | если `true` — только логируем запросы |


## 3. Контракты Онто API (используемые)

> Ниже названия эндпоинтов даны по смыслу; точные пути/форматы берём из вашей спецификации v2. Мы уже используем `POST /realm/{realmId}/entity/find/v2`. Аналогично ожидаются:

* `POST /realm/{realmId}/entity` — создание сущности.
* `PATCH /realm/{realmId}/entity/{entityId}` — обновление полей.
* `POST /realm/{realmId}/entity/find/v2` — поиск (с пагинацией, обязательна `pagination`).

### Поиск (обязательная пагинация)

```json
{
  "metaEntityRequest": { "uuid": "META_UUID" },
  "metaFieldFilters": [{ "fieldUuid": "FIELD_UUID", "value": "..." }],
  "pagination": { "first": 0, "offset": 20 }
}
```

### Создание сущности (пример)

```json
{
  "metaEntityUuid": "META_UUID_DATASETCLASS",
  "fields": {
    "FIELD_UUID_headerHash": "sha256:...",
    "FIELD_UUID_numCols": 27,
    "FIELD_UUID_draft": true
  }
}
```

**Ответ:** `{ "entityId": "uuid", ... }`


## 4. Интерфейс стора

```python
class PreflightStore(Protocol):
    def find_dataset_class_by_header_hash(self, hh: str) -> list[Entity]: ...
    def find_dataset_class_by_header_sorted_hash(self, hhs: str) -> list[Entity]: ...
    def find_dataset_class_by_num_cols(self, n: int, limit=100) -> list[Entity]: ...
    def create_dataset_class(self, payload: DatasetClassPayload) -> str: ...
    def create_column_signatures(self, class_id: str, columns: list[ColumnPayload]) -> list[str]: ...
    def create_pipeline_template(self, class_id: str, tpl: TemplatePayload) -> str: ...
    def create_dataset_signature(self, sig: SignaturePayload) -> str: ...
    def create_recognition_result(self, rec: RecognitionPayload) -> str: ...
    def link(self, link_uuid: str, from_id: str, to_id: str) -> None: ...
```

## 5. Алгоритм онтологического сохранения

### 5.1 «Класс найден»

1. `create_dataset_signature(sig)` → `signatureId`.
2. `create_recognition_result(rec)` → `recId`.
3. `link(LINK_UUID_REC_OF_SIG, recId, signatureId)`.
4. `link(LINK_UUID_REC_TO_CLASS, recId, classId)`.
5. `link(LINK_UUID_SIG_AS_CLASS, signatureId, classId)`.
6. Если у класса есть активный `PipelineTemplate` → `link(LINK_UUID_SIG_TPL, signatureId, tplId)`.

### 5.2 «Класса нет — создаём draft»

1. `create_dataset_class(payload)` → `classId`.
2. `create_column_signatures(classId, columns)` (+ `link class_has_column`).
3. `create_pipeline_template(classId, defaults)` (+ `link class_has_template`).
4. Дальше — шаги 5.1 (signature + recognition + links).

### 5.3 Идемпотентность

* Для `DatasetClass` используем **уникальные ключи**: `headerHash` (строго), если пусто — `headerSortedHash + numCols + headersSorted`.
* Для `ColumnSignature` уникальность по `(classId, position)`; при повторе — `PATCH`.
* Для `PipelineTemplate` уникальность по `(classId, name)`.
* Для `DatasetSignature` уникальность по `headerHash + fileName + fileSize` (если надо вести историю — не включать `fileName`).
* Для `RecognitionResult` можно хранить историю; связать с текущей `signature`.

## 6. Сопоставление полей (mapping)

Вся адресация полей выполняется через `field-map.json` (см. §2). В коде **нет** захардкоженных UUID — только ключи словаря.

## 7. Поиск кандидатов (логика скоринга)

* Запуск трёх поисков: `headerHash`, затем `headerSortedHash`, затем `numCols`.
* MCP ранжирует результаты локально: Jaccard(headers), PII-флаги, приоритет.
* Порог «найдено»: `score ≥ 0.7`.

## 8. Обработка ошибок и ретраи

* HTTP-коды `5xx` и timeout — с экспоненциальной паузой, `ONTO_RETRY` попыток.
* `4xx` — логировать и отдавать ошибку инструменту.
* Все запросы сопровождаем заголовком `X-Request-Id` (UUID).
* Логировать компактно: метод, статус, длительность, часть тела (без секретов).

## 9. Производительность и лимиты

* /find — всегда с пагинацией (`offset`=20..100).
* Создание колонок батчами: по 50 штук на запрос (если API позволяет), иначе циклом.
* Таймауты по умолчанию: 10–15 сек.

## 10. Тесты

* Юнит-тесты маппинга полей и сборки тел.
* Интеграционные тесты с мок-сервером Онто:

  * найденный класс по `headerHash`;
  * «не найден» → создаём draft;
  * идемпотентный повтор — без дублей;
  * ошибки сети/ретраи.
* e2e: прогнать `preflight_submit` с реальным токеном в отдельном тестовом realm (фичефлаг).

## 11. Выходные артефакты (что должно появляться в Онто)

* `DatasetSignature` (entity + ссылка),
* `DatasetClass` (новый, если не найден) + `ColumnSignature` + `PipelineTemplate`,
* `RecognitionResult`,
* связи:

  * `signature_recognized_as_class`,
  * `recognition_of_signature`,
  * `recognition_to_class`,
  * `class_has_column` (много),
  * `class_has_template`,
  * `signature_based_on_template`.

## 12. Примеры (curl) — find и create

### Поиск по `headerHash`

```bash
curl -s -X POST "$ONTO_API_BASE/realm/$ONTO_REALM_ID/entity/find/v2" \
 -H "Authorization: Bearer $ONTO_API_TOKEN" -H "Content-Type: application/json" \
 -d '{
  "metaEntityRequest":{"uuid":"META_UUID_DATASETCLASS"},
  "metaFieldFilters":[{"fieldUuid":"FIELD_UUID_headerHash","value":"sha256:..."}],
  "pagination":{"first":0,"offset":20}
 }'
```

### Создание `DatasetClass`

```bash
curl -s -X POST "$ONTO_API_BASE/realm/$ONTO_REALM_ID/entity" \
 -H "Authorization: Bearer $ONTO_API_TOKEN" -H "Content-Type: application/json" \
 -d '{
  "metaEntityUuid":"META_UUID_DATASETCLASS",
  "fields":{
    "FIELD_UUID_headerHash":"sha256:...",
    "FIELD_UUID_headerSortedHash":"sha256:...",
    "FIELD_UUID_headersSorted":"created;order_status;...",
    "FIELD_UUID_numCols":27,
    "FIELD_UUID_keywords":"ticket,event,museum,visitor",
    "FIELD_UUID_piiPhone":true,
    "FIELD_UUID_piiFio":true,
    "FIELD_UUID_piiInn":true,
    "FIELD_UUID_piiBirthday":true,
    "FIELD_UUID_priority":0,
    "FIELD_UUID_draft":true
  }
 }'
```

### Создание связи

```bash
curl -s -X POST "$ONTO_API_BASE/realm/$ONTO_REALM_ID/link" \
 -H "Authorization: Bearer $ONTO_API_TOKEN" -H "Content-Type: application/json" \
 -d '{
  "linkUuid":"LINK_UUID_SIG_AS_CLASS",
  "fromEntityId":"<signatureId>",
  "toEntityId":"<classId>"
 }'
```

## 13. Интеграция в код (швы)

* `onto_mcp/preflight_service.py`:

  * добавить фабрику `store = OntoStore.from_env()` вместо `_MemoryStore`.
  * переписать места `create_*`/`link` на вызовы стора.
  * строку референсной ссылки в ответе формировать как
    `f"https://app.ontonet.ru/ru/context/{realmId}/entity/{entityId}"` (требование пользователя).
* Логи оставить как в текущем сервисе, добавить `realmId` и `entityId` в сообщения.

## 14. Критерии приемки

* При вызове `preflight_submit`:

  1. если класс найден — в Онто создаётся **только** `DatasetSignature` + `RecognitionResult` и связи; дублей нет;
  2. если не найден — создаётся `DatasetClass(draft)` + `ColumnSignature` + `PipelineTemplate` + `DatasetSignature` + `RecognitionResult` + связи;
  3. на выходе инструмента возвращаются рабочие ссылки на объекты Онто;
  4. повторный вызов с тем же `payload` **не создаёт** дублей (идемпотентность).
* Логи содержат `X-Request-Id`, метод, статус, длительность.
* Покрытие тестами ключевых веток ≥ 80%.
