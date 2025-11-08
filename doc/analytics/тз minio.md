# ТЗ: Шаг 3 — Загрузка исходного файла в MinIO (S3-совместимо)

Цель: передать **крупный CSV** напрямую из клиентской машины в MinIO без проксирования через MCP. MCP выдаёт presigned URL(ы), клиент выполняет загрузку и (при необходимости) подтверждает завершение.

---

## Инструменты MCP

### 1) `upload_url`

Выдаёт presigned URL для **single PUT** или **multipart**.

**Вход (arguments):**

```json
{
  "s3Key": "raw/<dataset>/<yyyy>/<mm>/source-<uuid>.csv",
  "fileName": "tickets.csv",
  "fileSize": 348243289,
  "contentType": "text/csv",
  "strategy": "auto"        // "auto" | "single" | "multipart"
}
```

**Выход (результат):**

**а) Single PUT**

```json
{
  "mode": "single",
  "s3Key": "raw/.../source-....csv",
  "putUrl": "https://<minio>/raw/...?...X-Amz-Signature=...",
  "expiresInSec": 3600,
  "headers": {
    "Content-Type": "text/csv"
  }
}
```

**b) Multipart**

```json
{
  "mode": "multipart",
  "s3Key": "raw/.../source-....csv",
  "uploadId": "2~aoJ8...",
  "partSize": 67108864,         // 64 MiB (рекомендация)
  "expiresInSec": 86400,
  "parts": [
    {"partNumber": 1, "putUrl": "https://<minio>/...&partNumber=1&uploadId=..."},
    {"partNumber": 2, "putUrl": "https://<minio>/...&partNumber=2&uploadId=..."}
    // генерация пачками допустима
  ],
  "completeUrl": "https://<minio>/?uploadId=...&X-Amz-Signature=..."
}
```

> `s3Key` MCP получает/генерирует на шаге `preflight_submit` и **не меняет** на этом шаге.

---

### 2) `upload_complete` (опционально)

Подтверждение для single/multipart (если нужно вести явный журнал загрузок в Онто).

**Вход:**

```json
{
  "s3Key": "raw/.../source-....csv",
  "eTag": "…",                 // для single PUT
  "parts": [                   // для multipart: номер + ETag каждой части
    {"partNumber":1, "eTag":"…"},
    {"partNumber":2, "eTag":"…"}
  ]
}
```

**Выход:**

```json
{ "ok": true, "size": 348243289 }
```

---

## Бизнес-правила и поведение

1. **Стратегия выбора:**

   * `auto`: если `fileSize` ≤ 5 GiB → single; иначе multipart.
   * `single|multipart`: принудительный режим (полезно для тестов).

2. **Идемпотентность:**

   * Повторный запрос `upload_url` на тот же `s3Key` разрешён — новый presigned URL заменяет старый (старая ссылка просто истечёт).
   * `upload_complete` можно дергать повторно с одинаковыми параметрами (должно быть безвредно).

3. **Ограничения/тайминги:**

   * `expiresInSec`: single — 3600, multipart — 86400.
   * `partSize`: по умолчанию 64–128 MiB; число частей ≤ 10 000.

4. **Валидация входа:**

   * `s3Key` обязателен и должен начинаться с префикса `raw/`.
   * `fileSize > 0`.
   * `contentType` заполнен.

5. **Безопасность:**

   * MCP никогда не хранит сам файл; только выдаёт подписи.
   * Presigned URL ограничен временем и путём (`s3Key`).

---

## Действия клиента

### A. Single PUT

1. Получить ответ `upload_url` (`mode="single"`).
2. Выполнить загрузку:

```bash
curl -T ./tickets.csv \
  -H "Content-Type: text/csv" \
  "https://<minio>/raw/.../source-....csv?...X-Amz-Signature=..."
```

3. (Опц.) Вызвать `upload_complete` с `s3Key` и `ETag` из ответа `curl` (если требуется журнал).

### B. Multipart

1. Получить `uploadId`, `partSize`, список `putUrl` для частей (или запрашивать порциями).
2. Порезать файл и грузить по каждой ссылке:

```bash
# иллюстрация: отправка части №1
curl -X PUT --data-binary @part1.bin "https://<minio>/...&partNumber=1&uploadId=..."
# сохранить ETag из заголовка ответа
```

3. После всех частей выполнить **CompleteMultipartUpload** по `completeUrl` (или через `upload_complete` в MCP, который сам ударит `completeUrl`) с XML-списком `{PartNumber, ETag}`.
4. Проверить статус (200) и, при необходимости, вызвать `upload_complete` для фиксации в Онто.

---

## Ошибки и обработка

* `400`: некорректные аргументы (пустой `s3Key`, отрицательный `fileSize`).
* `409`: объект уже существует и политика хранилища «no overwrite». Решение: сгенерировать новый `s3Key` на шаге анализа.
* `413/EntityTooLarge`: при `single` файл > 5 GiB — повторить с `strategy:"multipart"`.
* `SignatureDoesNotMatch`/`AccessDenied`: истёк presigned или неверно отправлены заголовки — запросить новую ссылку.
* Сетевые сбои при multipart — допускается переотправка **конкретной** части (идемпотентно для S3).

---

## Логирование (на стороне MCP)

* В `upload_url`: `realmId`, `s3Key`, `mode`, `partSize`, `expiresInSec`, `requestId`.
* В `upload_complete`: `s3Key`, `totalParts`, `size`, `requestId`.

---

## Стандартизованные префиксы путей

* `raw/<dataset>/<yyyy>/<mm>/source-<uuid>.csv` — исходники (только чтение для ETL).
* `reports/<yyyy>/<mm>/<signatureId>.json` — отчёты профилирования.
* `ddl/<yyyy>/<mm>/<table>.sql` — предложенные DDL.
* `curated/...` — очищенные/нормализованные экспортируемые файлы (по решению DAG).

---

## SLA/проверки готовности к шагу 4 (DAG)

* После **успешной** загрузки:

  * объект доступен по `s3Key` (HEAD возвращает 200);
  * есть `size` и, для multipart, `ETag` финального объекта;
  * MCP возвращает клиенту `ok: true` (через `upload_complete` или непосредственный успех `single`).

Далее запускаем `dag_trigger` с параметрами `{ s3_key, target, templateId, signatureId, classId }`.
