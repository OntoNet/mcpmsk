import json
import os
import pathlib
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable

import pytest
from botocore.exceptions import ClientError

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("KEYCLOAK_BASE_URL", "https://example.com")
os.environ.setdefault("KEYCLOAK_REALM", "example")
os.environ.setdefault("KEYCLOAK_CLIENT_ID", "client")
os.environ.setdefault("ONTO_API_BASE", "https://api.example.com")
os.environ.setdefault("ONTO_API_TOKEN", "token")
os.environ.setdefault("ONTO_REALM_ID", "realm-123")
os.environ.setdefault("AIRFLOW_API_URL", "https://airflow.example.com")
os.environ.setdefault("AIRFLOW_API_USER", "airflow")
os.environ.setdefault("AIRFLOW_API_PASS", "secret")

from onto_mcp.pipeline_import_pg import PipelineImportPGExecutor
from onto_mcp.preflight_service import (
    MetaEntity,
    MetaField,
    StorageAssignment,
    StorageConfigData,
)


class DummyResponse:
    def __init__(self, status_code: int, payload: Dict[str, Any]):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self) -> Dict[str, Any]:
        return self._payload


class DummyAirflowSession:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str, Dict[str, Any]]] = []
        self._responses: list[DummyResponse] = []

    def queue_response(self, status_code: int, payload: Dict[str, Any]) -> None:
        self._responses.append(DummyResponse(status_code, payload))

    def request(self, method: str, url: str, timeout: float | None = None, auth=None, **kwargs: Any):
        payload = dict(kwargs)
        if auth is not None:
            payload["auth"] = auth
        if timeout is not None:
            payload["timeout"] = timeout
        self.requests.append((method, url, payload))
        if not self._responses:
            raise AssertionError("No response queued for Airflow request")
        return self._responses.pop(0)


class DummyS3Client:
    def __init__(self, *, missing: bool) -> None:
        self.missing = missing
        self.head_calls: list[tuple[str, str]] = []
        self.presigned: list[Dict[str, Any]] = []

    def head_object(self, Bucket: str, Key: str) -> None:
        self.head_calls.append((Bucket, Key))
        if self.missing:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

    def generate_presigned_url(self, operation_name: str, Params: Dict[str, Any], ExpiresIn: int, HttpMethod: str):
        record = {
            "operation": operation_name,
            "params": Params,
            "expires": ExpiresIn,
            "method": HttpMethod,
        }
        self.presigned.append(record)
        return "https://minio/upload"


class DummyPreflight:
    def __init__(self) -> None:
        self.meta = type(
            "Meta", (), {"dataset_signature": self._build_signature_meta()}
        )()
        self.pipeline_template_meta = self._build_pipeline_meta()
        self.storage_configs = {
            "sc-default": StorageConfigData(
                config_id="sc-default",
                endpoint="http://minio:9000",
                external_endpoint=None,
                bucket="raw",
                base_prefix="raw",
                path_pattern_raw="raw/{dataset}/{yyyy}/{mm}/source-{uuid}.csv",
                presign_expiry_sec=3600,
                multipart_threshold_mib=None,
                multipart_part_size_mib=None,
                overwrite_policy="allow",
                access_key_ref=None,
                secret_key_ref=None,
                access_key="access",
                secret_key="secret",
                region="us-east-1",
            )
        }
        self.default_storage_config_id = "sc-default"
        self._signature_entities: Dict[str, Dict[str, Any]] = {
            "sig-1": {
                "id": "sig-1",
                "fields": {
                    "sig-dataset": "class-1",
                    "sig-filename": "tickets.csv",
                    "sig-filesize": "1024",
                    "sig-encoding": "utf-8",
                    "sig-sep": ";",
                    "sig-hash": "sha256:abc",
                    "sig-hash-sorted": "sha256:def",
                    "sig-headers": "col1;col2",
                    "sig-numcols": "2",
                    "sig-configid": "sc-default",
                    "sig-s3key": "raw/tickets/sig-1.csv",
                },
            }
        }
        self._storage_assignments: Dict[str, Dict[str, Any]] = {}
        self._templates: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _build_signature_meta() -> MetaEntity:
        fields = {
            "datasetClass": MetaField(uuid="sig-dataset"),
            "datasetClassId": MetaField(uuid="sig-dataset"),
            "fileName": MetaField(uuid="sig-filename"),
            "fileSize": MetaField(uuid="sig-filesize"),
            "encoding": MetaField(uuid="sig-encoding"),
            "sep": MetaField(uuid="sig-sep"),
            "headerHash": MetaField(uuid="sig-hash"),
            "headerSortedHash": MetaField(uuid="sig-hash-sorted"),
            "headersSorted": MetaField(uuid="sig-headers"),
            "numCols": MetaField(uuid="sig-numcols"),
            "storageS3Key": MetaField(uuid="sig-s3key"),
            "storageConfigId": MetaField(uuid="sig-configid"),
        }
        return MetaEntity(name="DatasetSignature", meta_uuid="meta-sig", fields=fields)

    @staticmethod
    def _build_pipeline_meta() -> MetaEntity:
        fields = {
            "defaults": MetaField(uuid="tpl-defaults"),
            "target": MetaField(uuid="tpl-target"),
            "datasetClass": MetaField(uuid="tpl-dataset"),
            "storageConfig": MetaField(uuid="tpl-storage"),
        }
        return MetaEntity(name="PipelineTemplate", meta_uuid="meta-tpl", fields=fields)

    def _get_entity(self, entity_id: str) -> Dict[str, Any] | None:
        if entity_id in self._signature_entities:
            return self._signature_entities[entity_id]
        return self._templates.get(entity_id)

    def _extract_field_value(self, entity: Dict[str, Any], field_uuid: str) -> Any:
        fields = entity.get("fields", {})
        value = fields.get(field_uuid)
        if isinstance(value, dict):
            return value.get("value")
        return value

    def _load_signature_storage_fields(self, signature_id: str) -> Dict[str, Any]:
        return dict(self._storage_assignments.get(signature_id, {}))

    def _storage_entry_to_assignment(self, entry: Dict[str, Any]) -> StorageAssignment | None:
        config_id = entry.get("configId")
        s3_key = entry.get("s3Key")
        if isinstance(config_id, str) and isinstance(s3_key, str):
            config = self._resolve_storage_config(config_id)
            return StorageAssignment(config=config, s3_key=s3_key)
        return None

    def _persist_storage_assignment(
        self, signature_id: str, assignment: StorageAssignment, dataset_class_id: str | None
    ) -> Dict[str, Any]:
        data = {
            "configId": assignment.config.config_id,
            "bucket": assignment.config.bucket,
            "s3Key": assignment.s3_key,
        }
        if dataset_class_id:
            data["datasetClassId"] = dataset_class_id
        self._storage_assignments[signature_id] = data
        signature = self._signature_entities[signature_id]["fields"]
        signature["sig-configid"] = assignment.config.config_id
        signature["sig-s3key"] = assignment.s3_key
        return data

    def _resolve_storage_config(self, config_id: str) -> StorageConfigData:
        return self.storage_configs[config_id]

    def _find_default_storage_config_id(self) -> str:
        return self.default_storage_config_id

    def _generate_s3_key(
        self,
        config: StorageConfigData,
        dataset_slug: str,
        signature_payload,
        signature_id: str,
    ) -> str:
        return f"raw/{dataset_slug}/{signature_id}.csv"

    def _determine_dataset_slug(self, dataset_class_id: str | None) -> str:
        return "tickets"

    def _find_entities(
        self,
        meta_uuid: str,
        filters: Iterable[tuple[str, Any]],
        *,
        page_size: int,
        first: int = 0,
    ) -> list[Dict[str, Any]]:
        results = []
        for template in self._templates.values():
            match = True
            for field_uuid, expected in filters:
                if self._extract_field_value(template, field_uuid) != expected:
                    match = False
                    break
            if match:
                results.append(template)
        return results

    def _create_entity(
        self,
        meta_entity: MetaEntity,
        fields: Dict[str, Any],
        *,
        name: str | None = None,
        comment: str | None = None,
    ) -> str:
        template_id = f"tpl-{len(self._templates) + 1}"
        self._templates[template_id] = {"id": template_id, "fields": dict(fields)}
        return template_id

    def _extract_entity_id(self, entity: Dict[str, Any]) -> str | None:
        return entity.get("id")

    # Helpers for tests
    def add_template(self, template_id: str, defaults: Dict[str, Any], target: Dict[str, Any], storage_config_id: str) -> None:
        fields = {
            "tpl-defaults": json.dumps(defaults),
            "tpl-target": json.dumps({"storage": "postgres", **target}),
            "tpl-dataset": "class-1",
            "tpl-storage": storage_config_id,
        }
        self._templates[template_id] = {"id": template_id, "fields": fields}


def fixed_now() -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_pipeline_import_requests_upload_when_object_missing() -> None:
    preflight = DummyPreflight()
    s3_client = DummyS3Client(missing=True)
    airflow = DummyAirflowSession()

    executor = PipelineImportPGExecutor(
        preflight_service=preflight,
        airflow_session=airflow,
        time_provider=fixed_now,
        sleep=lambda _: None,
        s3_client_factory=lambda config: s3_client,
    )

    result = executor.execute(
        {
            "signatureId": "sig-1",
            "target": {"schema": "public", "table": "tickets"},
            "source": {
                "ensureUploaded": True,
                "fileName": "tickets.csv",
                "fileSize": 1024,
                "contentType": "text/csv",
            },
        }
    )

    assert "airflow" not in result
    assert result["storage"]["upload"]["putUrl"] == "https://minio/upload"
    assert result["storage"]["configId"] == "sc-default"
    assert result["notes"], "Expected helpful notes about upload"
    assert not airflow.requests


def test_pipeline_import_triggers_airflow_when_object_exists() -> None:
    preflight = DummyPreflight()
    defaults = {"sep": ";", "encoding": "utf-8", "loadMode": "append", "createTable": True}
    target = {"schema": "public", "table": "tickets"}
    preflight.add_template("tpl-existing", defaults, target, "sc-default")

    s3_client = DummyS3Client(missing=False)
    airflow = DummyAirflowSession()
    airflow.queue_response(200, {"dag_run_id": "manual__123", "state": "queued"})

    executor = PipelineImportPGExecutor(
        preflight_service=preflight,
        airflow_session=airflow,
        time_provider=fixed_now,
        sleep=lambda _: None,
        s3_client_factory=lambda config: s3_client,
    )

    result = executor.execute(
        {
            "signatureId": "sig-1",
            "target": target,
            "source": {"s3Key": "raw/tickets/custom.csv", "ensureUploaded": False},
        }
    )

    assert result["created"]["pipelineTemplateId"] == "tpl-existing"
    assert result["storage"]["s3Key"] == "raw/tickets/custom.csv"
    airflow_requests = airflow.requests
    assert len(airflow_requests) == 1
    method, url, payload = airflow_requests[0]
    assert method == "POST"
    assert "csv_ingest_pg" in url
    assert result["airflow"]["runId"] == "manual__123"
    assert result["airflow"]["state"] == "queued"

def test_pipeline_import_retries_airflow_login_on_forbidden() -> None:
    preflight = DummyPreflight()
    defaults = {"sep": ";", "encoding": "utf-8", "loadMode": "append", "createTable": True}
    target = {"schema": "public", "table": "tickets"}
    preflight.add_template("tpl-existing", defaults, target, "sc-default")

    s3_client = DummyS3Client(missing=False)
    airflow = DummyAirflowSession()
    airflow.queue_response(403, {"title": "Forbidden"})
    airflow.queue_response(200, {"access_token": "jwt-token"})
    airflow.queue_response(200, {"dag_run_id": "manual__456", "state": "queued"})

    executor = PipelineImportPGExecutor(
        preflight_service=preflight,
        airflow_session=airflow,
        time_provider=fixed_now,
        sleep=lambda _: None,
        s3_client_factory=lambda config: s3_client,
    )

    result = executor.execute(
        {
            "signatureId": "sig-1",
            "target": target,
            "source": {"s3Key": "raw/tickets/custom.csv", "ensureUploaded": False},
        }
    )

    assert result["airflow"]["runId"] == "manual__456"

    requests = airflow.requests
    assert len(requests) == 3

    first_method, first_url, first_payload = requests[0]
    assert first_method == "POST"
    assert first_payload.get("auth") == ("airflow", "secret")

    second_method, second_url, second_payload = requests[1]
    assert second_method == "POST"
    assert "/security/login" in second_url
    assert second_payload.get("auth") is None or second_payload.get("auth") == ()
    assert second_payload.get("json") == {"username": "airflow", "password": "secret"}

    third_method, third_url, third_payload = requests[2]
    assert third_method == "POST"
    assert third_payload["headers"]["Authorization"] == "Bearer jwt-token"
    assert "csv_ingest_pg" in third_url


def test_pipeline_import_raises_permission_denied_when_airflow_login_fails() -> None:
    preflight = DummyPreflight()
    defaults = {"sep": ";", "encoding": "utf-8", "loadMode": "append", "createTable": True}
    target = {"schema": "public", "table": "tickets"}
    preflight.add_template("tpl-existing", defaults, target, "sc-default")

    s3_client = DummyS3Client(missing=False)
    airflow = DummyAirflowSession()
    airflow.queue_response(403, {"title": "Forbidden"})
    airflow.queue_response(403, {"title": "Forbidden"})

    executor = PipelineImportPGExecutor(
        preflight_service=preflight,
        airflow_session=airflow,
        time_provider=fixed_now,
        sleep=lambda _: None,
        s3_client_factory=lambda config: s3_client,
    )

    with pytest.raises(RuntimeError) as excinfo:
        executor.execute(
            {
                "signatureId": "sig-1",
                "target": target,
                "source": {"s3Key": "raw/tickets/custom.csv", "ensureUploaded": False},
            }
        )

    assert str(excinfo.value) == "403: airflow_permission_denied Forbidden"

    requests = airflow.requests
    assert len(requests) == 2
    assert "/security/login" in requests[1][1]
