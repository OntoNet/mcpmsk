"""Implementation of the `pipeline_import_pg` MCP tool."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
import requests
from fastmcp.exceptions import ValidationError

from .preflight_service import (
    MetaEntity,
    PreflightProcessingError,
    PreflightService,
    SignaturePayload,
    StorageAssignment,
    StorageConfigData,
)
from .settings import (
    AIRFLOW_API_PASS,
    AIRFLOW_API_URL,
    AIRFLOW_API_USER,
    AIRFLOW_RETRY,
    AIRFLOW_TIMEOUT_SEC,
    MINIO_REGION,
    ONTO_REALM_ID,
)
from .utils import safe_print


@dataclass
class SignatureDetails:
    """Subset of fields extracted from a DatasetSignature entity."""

    signature_id: str
    dataset_class_id: Optional[str]
    file_name: Optional[str]
    file_size: Optional[int]
    encoding: Optional[str]
    separator: Optional[str]
    header_hash: Optional[str]
    header_sorted_hash: Optional[str]
    headers_sorted: Optional[str]
    num_cols: Optional[int]
    storage_s3_key: Optional[str]
    storage_config_id: Optional[str]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class PipelineImportPGExecutor:
    """Service object orchestrating the `pipeline_import_pg` flow."""

    DAG_ID = "csv_ingest_pg"

    def __init__(
        self,
        *,
        preflight_service: Optional[PreflightService] = None,
        airflow_session: Optional[requests.Session] = None,
        time_provider: Callable[[], datetime] = _now_utc,
        sleep: Callable[[float], None] = time.sleep,
        s3_client_factory: Optional[Callable[[StorageConfigData], Any]] = None,
    ) -> None:
        self.preflight = preflight_service or PreflightService()
        self.airflow_session = airflow_session or requests.Session()
        self._time_provider = time_provider
        self._sleep = sleep
        self._s3_client_factory = s3_client_factory or self._create_s3_client

    # ------------------------------------------------------------------
    # Entry-point
    # ------------------------------------------------------------------
    def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        signature_id = self._require_string(arguments.get("signatureId"), "signatureId")

        target_raw = arguments.get("target") or {}
        target_schema = self._require_string(target_raw.get("schema"), "target.schema")
        target_table = self._require_string(target_raw.get("table"), "target.table")

        options_in = arguments.get("options") or {}
        load_mode = (options_in.get("loadMode") or "append").lower()
        if load_mode not in {"append", "replace"}:
            raise ValidationError("400: invalid_arguments: loadMode must be 'append' or 'replace'")
        create_table = bool(options_in.get("createTable", True))

        source_in = arguments.get("source") or {}
        ensure_uploaded = bool(source_in.get("ensureUploaded", False))
        provided_s3_key = self._normalize_s3_key(source_in.get("s3Key"))
        file_name_for_upload = source_in.get("fileName")
        file_size_for_upload = source_in.get("fileSize")
        content_type_for_upload = source_in.get("contentType" or "text/csv")

        execution_in = arguments.get("execution") or {}
        wait_for_completion = bool(execution_in.get("wait", False))
        wait_timeout_sec = self._normalize_positive_int(
            execution_in.get("waitTimeoutSec"), default=1800, field="execution.waitTimeoutSec"
        )

        signature = self._load_signature(signature_id)

        provided_dataset_class_id = arguments.get("datasetClassId")
        if provided_dataset_class_id is not None:
            if not isinstance(provided_dataset_class_id, str) or not provided_dataset_class_id.strip():
                raise ValidationError("400: invalid_arguments: datasetClassId must be a non-empty string when provided")
            dataset_class_id = provided_dataset_class_id.strip()
        else:
            dataset_class_id = signature.dataset_class_id

        if not dataset_class_id:
            raise ValidationError("422: signature_missing_dataset_class")

        separator = self._pick_option(
            user_value=options_in.get("sep"),
            signature_value=signature.separator,
            default=";",
        )
        encoding = self._pick_option(
            user_value=options_in.get("encoding"),
            signature_value=signature.encoding,
            default="utf-8",
        )

        defaults_payload = {
            "sep": separator,
            "encoding": encoding,
            "loadMode": load_mode,
            "createTable": create_table,
        }

        storage_config_id = self._resolve_storage_config_id(
            signature_id=signature.signature_id,
            explicit_id=(arguments.get("storage") or {}).get("configId"),
            signature=signature,
        )

        config = self._resolve_storage_config(storage_config_id)

        existing_assignment_entry = self.preflight._load_signature_storage_fields(signature.signature_id)
        existing_assignment = self.preflight._storage_entry_to_assignment(existing_assignment_entry)

        assignment: Optional[StorageAssignment] = None
        s3_key: Optional[str] = None

        if provided_s3_key:
            s3_key = provided_s3_key
            assignment = StorageAssignment(config=config, s3_key=s3_key)
            self.preflight._persist_storage_assignment(signature.signature_id, assignment, dataset_class_id)
        elif existing_assignment is not None:
            assignment = existing_assignment
            s3_key = assignment.s3_key
        elif signature.storage_s3_key:
            s3_key = signature.storage_s3_key
            assignment = StorageAssignment(config=config, s3_key=s3_key)
            self.preflight._persist_storage_assignment(signature.signature_id, assignment, dataset_class_id)

        if assignment is None or not s3_key:
            raise ValidationError("409: storage_key_missing: run upload_url first or provide source.s3Key")

        template_id = self._ensure_pipeline_template(
            dataset_class_id=dataset_class_id,
            defaults=defaults_payload,
            target={"schema": target_schema, "table": target_table},
            storage_config_id=config.config_id,
        )

        upload_block: Optional[Dict[str, Any]] = None

        object_exists = self._check_object_exists(assignment)
        if not object_exists:
            if ensure_uploaded:
                upload_block = self._generate_presigned_upload(
                    assignment=assignment,
                    file_name=file_name_for_upload or signature.file_name or "dataset.csv",
                    file_size=file_size_for_upload,
                    content_type=content_type_for_upload,
                )
            else:
                raise RuntimeError(
                f"409: object_not_uploaded: upload file or set ensureUploaded=true (expected s3://{assignment.config.bucket}/{assignment.s3_key})"
            )

        response: Dict[str, Any] = {
            "created": {"pipelineTemplateId": template_id},
            "storage": {
                "configId": config.config_id,
                "bucket": config.bucket,
                "s3Key": assignment.s3_key,
            },
            "onto": self._build_onto_links(signature.signature_id, template_id),
            "notes": [],
        }

        if upload_block is not None:
            response["storage"]["upload"] = upload_block
            response["notes"].append(
                "Если присутствует блок storage.upload — сначала загрузите файл, затем вызовите инструмент повторно с ensureUploaded=false"
            )
            return response

        airflow_result = self._trigger_airflow_run(
            assignment=assignment,
            defaults=defaults_payload,
            target={"schema": target_schema, "table": target_table},
            signature_id=signature.signature_id,
            template_id=template_id,
            wait=wait_for_completion,
            wait_timeout_sec=wait_timeout_sec,
        )

        response["airflow"] = airflow_result
        return response

    # ------------------------------------------------------------------
    # Helpers - validation / normalisation
    # ------------------------------------------------------------------
    @staticmethod
    def _require_string(value: Any, field: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValidationError(f"400: invalid_arguments: {field} is required")
        return value.strip()

    @staticmethod
    def _normalize_positive_int(value: Any, *, default: int, field: str) -> int:
        if value in (None, ""):
            return default
        try:
            numeric = int(value)
        except (TypeError, ValueError):
            raise ValidationError(f"400: invalid_arguments: {field} must be a positive integer")
        if numeric <= 0:
            raise ValidationError(f"400: invalid_arguments: {field} must be a positive integer")
        return numeric

    @staticmethod
    def _normalize_s3_key(value: Any) -> Optional[str]:
        if not value:
            return None
        if not isinstance(value, str):
            raise ValidationError("400: invalid_arguments: source.s3Key must be a string")
        return value.strip() or None

    @staticmethod
    def _pick_option(*, user_value: Any, signature_value: Optional[str], default: str) -> str:
        if isinstance(user_value, str) and user_value.strip():
            return user_value.strip()
        if isinstance(signature_value, str) and signature_value.strip():
            return signature_value.strip()
        return default

    # ------------------------------------------------------------------
    # Helpers - signature / storage
    # ------------------------------------------------------------------
    def _load_signature(self, signature_id: str) -> SignatureDetails:
        entity = self.preflight._get_entity(signature_id)
        if not entity:
            raise RuntimeError("404: signature_not_found")

        meta_signature = self.preflight.meta.dataset_signature

        def read(names: Iterable[str]) -> Optional[str]:
            for name in names:
                field_uuid = meta_signature.get(name)
                if not field_uuid:
                    continue
                value = self.preflight._extract_field_value(entity, field_uuid)
                if value not in (None, ""):
                    return value
            for name in names:
                if isinstance(entity.get(name), str) and entity[name].strip():
                    return entity[name].strip()
            return None

        dataset_class_id = read(["datasetClass", "datasetClassId", "class", "classId"])
        file_name = read(["fileName"])
        file_size_raw = read(["fileSize"])
        encoding = read(["encoding"])
        separator = read(["sep", "separator"])
        header_hash = read(["headerHash"])
        header_sorted_hash = read(["headerSortedHash"])
        headers_sorted = read(["headersSorted"])
        num_cols_raw = read(["numCols"])
        storage_s3_key = read(["storageS3Key", "s3Key"])
        storage_config_id = read(["storageConfigId"])

        file_size = None
        if file_size_raw is not None:
            try:
                file_size = int(str(file_size_raw))
            except (TypeError, ValueError):
                file_size = None

        num_cols = None
        if num_cols_raw is not None:
            try:
                num_cols = int(str(num_cols_raw))
            except (TypeError, ValueError):
                num_cols = None

        return SignatureDetails(
            signature_id=signature_id,
            dataset_class_id=dataset_class_id,
            file_name=file_name,
            file_size=file_size,
            encoding=encoding,
            separator=separator,
            header_hash=header_hash,
            header_sorted_hash=header_sorted_hash,
            headers_sorted=headers_sorted,
            num_cols=num_cols,
            storage_s3_key=storage_s3_key,
            storage_config_id=storage_config_id,
        )

    def _resolve_storage_config_id(
        self,
        *,
        signature_id: str,
        explicit_id: Any,
        signature: SignatureDetails,
    ) -> str:
        if explicit_id:
            if not isinstance(explicit_id, str) or not explicit_id.strip():
                raise ValidationError("400: invalid_arguments: storage.configId must be a string")
            return explicit_id.strip()

        if signature.storage_config_id:
            return signature.storage_config_id

        entry = self.preflight._load_signature_storage_fields(signature_id)
        config_id = entry.get("configId") if isinstance(entry, dict) else None
        if isinstance(config_id, str) and config_id.strip():
            return config_id.strip()

        default_id = self.preflight._find_default_storage_config_id()
        if not default_id:
            raise RuntimeError("424: storage_config_not_found")
        return default_id

    def _resolve_storage_config(self, config_id: str) -> StorageConfigData:
        try:
            return self.preflight._resolve_storage_config(config_id)
        except PreflightProcessingError as exc:
            if getattr(exc, "status_code", 500) == 424:
                raise RuntimeError("424: storage_config_not_found") from exc
            raise RuntimeError(f"500: internal_error: {exc}") from exc

    @staticmethod
    def _assignments_equal(left: StorageAssignment, right: StorageAssignment) -> bool:
        return (
            left.config.config_id == right.config.config_id
            and left.s3_key == right.s3_key
        )

    def _build_signature_payload(self, signature: SignatureDetails) -> SignaturePayload:
        headers: list[str] = []
        if signature.headers_sorted:
            headers = [item for item in signature.headers_sorted.split(";") if item]
        num_cols = signature.num_cols if signature.num_cols is not None else len(headers)
        header_hash = signature.header_hash or ""
        header_sorted_hash = signature.header_sorted_hash or ""
        file_name = signature.file_name or "dataset.csv"
        file_size = signature.file_size if signature.file_size is not None else 0
        encoding = signature.encoding or "utf-8"
        separator = signature.separator or ";"

        return SignaturePayload(
            file_name=file_name,
            file_size=file_size,
            headers=headers or [f"col{i}" for i in range(num_cols or 0)],
            header_hash=header_hash,
            header_sorted_hash=header_sorted_hash,
            num_cols=num_cols or len(headers) or 0,
            encoding=encoding,
            separator=separator,
        )

    def _create_s3_client(self, config: StorageConfigData) -> Any:
        endpoint = config.endpoint or config.external_endpoint
        if not endpoint:
            raise RuntimeError("422: storage configuration missing endpoint")
        session = boto3.session.Session(
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            region_name=config.region or MINIO_REGION or "us-east-1",
        )
        boto_cfg = BotoConfig(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        )
        return session.client("s3", endpoint_url=endpoint, config=boto_cfg)

    def _check_object_exists(self, assignment: StorageAssignment) -> bool:
        client = self._s3_client_factory(assignment.config)
        safe_print(f"[pipeline_import_pg] checking object s3://{assignment.config.bucket}/{assignment.s3_key}")
        try:
            client.head_object(Bucket=assignment.config.bucket, Key=assignment.s3_key)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code") if hasattr(exc, "response") else None
            if code in {"NoSuchKey", "404", "NotFound"}:
                return False
            raise RuntimeError(f"500: internal_error: failed to check object: {exc}") from exc

    def _generate_presigned_upload(
        self,
        *,
        assignment: StorageAssignment,
        file_name: str,
        file_size: Any,
        content_type: Optional[str],
    ) -> Dict[str, Any]:
        client = self._s3_client_factory(assignment.config)
        params = {
            "Bucket": assignment.config.bucket,
            "Key": assignment.s3_key,
        }
        if content_type:
            params["ContentType"] = content_type
        try:
            url = client.generate_presigned_url(
                "put_object",
                Params=params,
                ExpiresIn=assignment.config.presign_expiry_sec or 3600,
                HttpMethod="PUT",
            )
        except Exception as exc:  # pragma: no cover - boto failure should be rare
            raise RuntimeError(f"500: internal_error: failed to generate presigned URL: {exc}") from exc

        result = {
            "mode": "single",
            "putUrl": url,
            "expiresInSec": assignment.config.presign_expiry_sec or 3600,
        }
        if file_size is not None:
            try:
                result["expectedSize"] = int(file_size)
            except (TypeError, ValueError):
                pass
        if content_type:
            result["contentType"] = content_type
        result["fileName"] = file_name
        return result

    # ------------------------------------------------------------------
    # Pipeline template helpers
    # ------------------------------------------------------------------
    def _ensure_pipeline_template(
        self,
        *,
        dataset_class_id: Optional[str],
        defaults: Dict[str, Any],
        target: Dict[str, str],
        storage_config_id: str,
    ) -> str:
        if not dataset_class_id:
            raise RuntimeError("422: signature_missing_dataset_class")

        meta_template = self.preflight.pipeline_template_meta
        if meta_template is None:
            raise RuntimeError("500: internal_error: pipeline template metadata unavailable")

        existing = self._find_matching_template(
            meta_template=meta_template,
            dataset_class_id=dataset_class_id,
            defaults=defaults,
            target=target,
            storage_config_id=storage_config_id,
        )
        if existing:
            return existing

        return self._create_pipeline_template(
            meta_template=meta_template,
            dataset_class_id=dataset_class_id,
            defaults=defaults,
            target=target,
            storage_config_id=storage_config_id,
        )

    def _find_matching_template(
        self,
        *,
        meta_template: MetaEntity,
        dataset_class_id: str,
        defaults: Dict[str, Any],
        target: Dict[str, str],
        storage_config_id: str,
    ) -> Optional[str]:
        field_dataset = meta_template.get("datasetClass") or meta_template.get("dataset")
        filters = []
        if field_dataset:
            filters.append((field_dataset, dataset_class_id))

        candidates = self.preflight._find_entities(
            meta_template.meta_uuid,
            filters,
            page_size=50,
        )

        defaults_field = meta_template.get("defaults")
        target_field = meta_template.get("target")
        storage_field = meta_template.get("storageConfig") or meta_template.get("storage")

        for entity in candidates:
            template_id = self.preflight._extract_entity_id(entity)
            if not template_id:
                continue

            if storage_field:
                storage_value = self.preflight._extract_field_value(entity, storage_field)
                if storage_value and storage_value != storage_config_id:
                    continue

            if defaults_field and not self._compare_json_field(
                entity, defaults_field, defaults
            ):
                continue

            if target_field and not self._compare_json_field(entity, target_field, target):
                continue

            return template_id

        return None

    def _compare_json_field(self, entity: Dict[str, Any], field_uuid: str, expected: Dict[str, Any]) -> bool:
        raw_value = self.preflight._extract_field_value(entity, field_uuid)
        if not raw_value:
            return False
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            return False
        for key, value in expected.items():
            if parsed.get(key) != value:
                return False
        return True

    def _create_pipeline_template(
        self,
        *,
        meta_template: MetaEntity,
        dataset_class_id: str,
        defaults: Dict[str, Any],
        target: Dict[str, str],
        storage_config_id: str,
    ) -> str:
        fields: Dict[str, Any] = {}

        def maybe_set(name: str, value: Any) -> None:
            field_uuid = meta_template.get(name)
            if field_uuid:
                fields[field_uuid] = value

        maybe_set("datasetClass", dataset_class_id)
        maybe_set("defaults", json.dumps(defaults, ensure_ascii=False))
        maybe_set("target", json.dumps({"storage": "postgres", **target}, ensure_ascii=False))
        maybe_set("storageConfig", storage_config_id)

        name = f"CSV import to {target['schema']}.{target['table']}"
        return self.preflight._create_entity(
            meta_template,
            fields,
            name=name,
            comment="Created by pipeline_import_pg",
        )

    # ------------------------------------------------------------------
    # Airflow integration
    # ------------------------------------------------------------------
    def _trigger_airflow_run(
        self,
        *,
        assignment: StorageAssignment,
        defaults: Dict[str, Any],
        target: Dict[str, str],
        signature_id: str,
        template_id: str,
        wait: bool,
        wait_timeout_sec: int,
    ) -> Dict[str, Any]:
        if not AIRFLOW_API_URL or not AIRFLOW_API_USER or not AIRFLOW_API_PASS:
            raise RuntimeError("500: internal_error: Airflow configuration missing")

        run_id = self._build_run_id()
        conf = {
            "presigned_get_url": None,
            "s3_endpoint": assignment.config.endpoint,
            "bucket": assignment.config.bucket,
            "key": assignment.s3_key,
            "sep": defaults["sep"],
            "encoding": defaults["encoding"],
            "target": {"schema": target["schema"], "table": target["table"]},
            "loadMode": defaults["loadMode"],
            "createTable": defaults["createTable"],
            "signature_id": signature_id,
            "template_id": template_id,
        }

        payload = {"conf": conf, "dag_run_id": run_id}

        url = f"{AIRFLOW_API_URL.rstrip('/')}/api/v1/dags/{self.DAG_ID}/dagRuns"

        response = self._airflow_request("POST", url, json=payload)

        data = self._decode_json(response)
        dag_run_id = data.get("dag_run_id") or data.get("dagRunId") or run_id
        state = data.get("state") or "queued"

        result = {
            "dagId": self.DAG_ID,
            "runId": dag_run_id,
            "state": state,
            "webUrl": self._build_airflow_web_url(dag_run_id),
        }

        if wait:
            final_state = self._wait_for_run_state(dag_run_id, wait_timeout_sec)
            result["state"] = final_state

        return result

    def _build_run_id(self) -> str:
        timestamp = self._time_provider().strftime("%Y%m%dT%H%M%SZ")
        suffix = uuid.uuid4().hex[:8]
        return f"mcp__{timestamp}__{suffix}"

    def _airflow_request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        last_error: Optional[str] = None
        for attempt in range(1, AIRFLOW_RETRY + 1):
            try:
                response = self.airflow_session.request(
                    method,
                    url,
                    timeout=AIRFLOW_TIMEOUT_SEC,
                    auth=(AIRFLOW_API_USER, AIRFLOW_API_PASS),
                    **kwargs,
                )
            except requests.RequestException as exc:
                last_error = str(exc)
                safe_print(
                    f"[pipeline_import_pg] airflow request error {method} {url} (attempt {attempt}/{AIRFLOW_RETRY}): {exc}"
                )
                if attempt == AIRFLOW_RETRY:
                    raise RuntimeError("502: airflow_unreachable") from exc
                continue

            if response.status_code == 404:
                raise RuntimeError("404: dag_not_found")
            if response.status_code >= 500:
                last_error = f"{response.status_code}: {response.text.strip()}"
                if attempt == AIRFLOW_RETRY:
                    raise RuntimeError("502: airflow_unreachable")
                continue
            if response.status_code >= 400:
                raise RuntimeError(f"{response.status_code}: airflow_error {response.text.strip()}")

            return response

        raise RuntimeError(f"502: airflow_unreachable: {last_error}")

    def _wait_for_run_state(self, run_id: str, timeout_sec: int) -> str:
        deadline = self._time_provider().timestamp() + timeout_sec
        status_url = f"{AIRFLOW_API_URL.rstrip('/')}/api/v1/dags/{self.DAG_ID}/dagRuns/{run_id}"
        while True:
            response = self._airflow_request("GET", status_url)
            data = self._decode_json(response)
            state = data.get("state")
            if state in {"success", "failed"}:
                return state
            if self._time_provider().timestamp() >= deadline:
                raise RuntimeError("504: airflow_wait_timeout")
            self._sleep(5)

    @staticmethod
    def _decode_json(response: requests.Response) -> Dict[str, Any]:
        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError(f"500: internal_error: invalid JSON from Airflow: {exc}") from exc
        if not isinstance(data, dict):
            raise RuntimeError("500: internal_error: Airflow returned unexpected payload")
        return data

    def _build_airflow_web_url(self, run_id: str) -> str:
        base = AIRFLOW_API_URL.rstrip("/")
        return f"{base}/dags/{self.DAG_ID}/grid?dag_run_id={run_id}"

    # ------------------------------------------------------------------
    # Misc helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _build_onto_links(signature_id: str, template_id: str) -> Dict[str, Any]:
        if not ONTO_REALM_ID:
            return {}
        base = f"https://app.ontonet.ru/ru/context/{ONTO_REALM_ID}/entity"
        return {
            "signatureUrl": f"{base}/{signature_id}",
            "pipelineTemplateUrl": f"{base}/{template_id}",
        }


__all__ = ["PipelineImportPGExecutor"]

