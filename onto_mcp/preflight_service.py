"""Preflight service implementing Onto search and entity creation workflow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Protocol
import uuid

import requests

from . import storage_cache
from .settings import (
    ALLOW_S3KEY_REGENERATE,
    ENABLE_CREATE,
    ENABLE_STORAGE_LINKS,
    FIELD_MAP_PATH,
    ONTO_API_BASE,
    ONTO_API_TOKEN,
    ONTO_META_DATASETCLASS_NAME,
    ONTO_META_RECOG_NAME,
    ONTO_META_SIGNATURE_NAME,
    ONTO_META_COLUMNSIGN_NAME,
    ONTO_META_PIPELINE_NAME,
    ONTO_META_STORAGECONFIG_NAME,
    ONTO_DEBUG_HTTP,
    ONTO_REALM_ID,
)
from .utils import safe_print


@dataclass(frozen=True)
class MetaField:
    uuid: str
    field_type: str | None = None


class PreflightPayloadError(ValueError):
    """Raised when the incoming payload is missing required fields."""


class PreflightProcessingError(RuntimeError):
    """Raised when the service cannot reach Onto or receives an invalid response."""

    def __init__(self, message: str, status_code: int = 500) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class SignaturePayload:
    """Normalized signature payload extracted from a preflight request."""

    file_name: str
    file_size: int
    headers: List[str]
    header_hash: str
    header_sorted_hash: str
    num_cols: int
    encoding: str
    separator: str

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "SignaturePayload":
        if not isinstance(payload, dict):
            raise PreflightPayloadError("payload must be an object")

        file_name = payload.get("fileName")
        file_size = payload.get("fileSize")
        signature = payload.get("signature")

        if not isinstance(file_name, str) or not file_name:
            raise PreflightPayloadError("'fileName' must be a non-empty string")
        if not isinstance(file_size, (int, float)):
            raise PreflightPayloadError("'fileSize' must be a number")
        if not isinstance(signature, dict):
            raise PreflightPayloadError("'signature' must be an object")

        required_fields = [
            "headers",
            "numCols",
            "headerHash",
            "headerSortedHash",
        ]

        for field in required_fields:
            if field not in signature:
                raise PreflightPayloadError(f"signature missing required field '{field}'")

        headers = signature["headers"]
        num_cols = signature["numCols"]
        header_hash = signature["headerHash"]
        header_sorted_hash = signature["headerSortedHash"]
        encoding = signature.get("encoding", "utf-8")
        separator = signature.get("sep", ",")

        if not isinstance(headers, list) or not headers:
            raise PreflightPayloadError("signature.headers must be a non-empty list")
        if not isinstance(num_cols, int) or num_cols <= 0:
            raise PreflightPayloadError("signature.numCols must be a positive integer")
        if len(headers) != num_cols:
            raise PreflightPayloadError("signature.headers length must equal signature.numCols")
        if not isinstance(header_hash, str) or not header_hash.startswith("sha256:"):
            raise PreflightPayloadError("signature.headerHash must start with 'sha256:'")
        if not isinstance(header_sorted_hash, str) or not header_sorted_hash.startswith("sha256:"):
            raise PreflightPayloadError("signature.headerSortedHash must start with 'sha256:'")

        normalized_headers: List[str] = []
        for header in headers:
            if not isinstance(header, str) or not header:
                raise PreflightPayloadError("signature.headers must contain non-empty strings")
            normalized_headers.append(header)

        if not isinstance(encoding, str) or not encoding:
            encoding = "utf-8"
        if not isinstance(separator, str) or not separator:
            separator = ","

        return cls(
            file_name=file_name,
            file_size=int(file_size),
            headers=normalized_headers,
            header_hash=header_hash,
            header_sorted_hash=header_sorted_hash,
            num_cols=num_cols,
            encoding=encoding,
            separator=separator,
        )

    def header_set(self) -> Set[str]:
        return set(self.headers)

    def headers_sorted(self) -> List[str]:
        return sorted(self.headers)

    def headers_sorted_string(self) -> str:
        return ";".join(self.headers_sorted())


@dataclass(frozen=True)
class MetaEntity:
    """Metadata description of an Onto entity template."""

    name: str
    meta_uuid: str
    fields: Dict[str, MetaField]

    def _get_field(self, field_name: str) -> MetaField:
        try:
            field = self.fields[field_name]
        except KeyError as exc:  # pragma: no cover - configuration issue
            raise PreflightProcessingError(
                f"meta entity '{self.name}' missing field '{field_name}'",
                status_code=500,
            ) from exc
        if not isinstance(field, MetaField) or not field.uuid:
            raise PreflightProcessingError(
                f"field mapping for '{self.name}.{field_name}' must define a uuid",
                status_code=500,
            )
        return field

    def field(self, field_name: str) -> str:
        return self._get_field(field_name).uuid

    def get(self, field_name: str) -> Optional[str]:
        field = self.fields.get(field_name)
        if isinstance(field, MetaField) and field.uuid:
            return field.uuid
        return None

    def field_info(self, field_name: str) -> Optional[MetaField]:
        field = self.fields.get(field_name)
        if isinstance(field, MetaField) and field.uuid:
            return field
        return None

    def field_name_for_uuid(self, field_uuid: str) -> Optional[str]:
        for name, field in self.fields.items():
            if field.uuid == field_uuid:
                return name
        return None

    def require(self, required: Sequence[str]) -> None:
        missing = [field for field in required if field not in self.fields]
        if missing:
            missing_str = ", ".join(missing)
            raise PreflightProcessingError(
                f"meta entity '{self.name}' missing required fields: {missing_str}",
                status_code=500,
            )


@dataclass(frozen=True)
class MetaConfig:
    """Resolved metadata required by the preflight workflow."""

    dataset_class: MetaEntity
    dataset_signature: MetaEntity
    recognition_result: MetaEntity

    @classmethod
    def from_file(cls, path: Path) -> "MetaConfig":
        try:
            data = json.loads(path.read_text("utf-8"))
        except FileNotFoundError as exc:  # pragma: no cover - configuration issue
            raise PreflightProcessingError(
                f"field map file not found at {path}", status_code=500
            ) from exc
        except json.JSONDecodeError as exc:
            raise PreflightProcessingError(
                f"failed to parse field map JSON at {path}: {exc}", status_code=500
            ) from exc

        meta_section = data.get("meta")
        fields_section = data.get("fields")

        if not isinstance(meta_section, dict) or not isinstance(fields_section, dict):
            raise PreflightProcessingError(
                "field map must contain 'meta' and 'fields' sections", status_code=500
            )

        def _load_entity(name: str, required: Sequence[str]) -> MetaEntity:
            meta_uuid = meta_section.get(name)
            if not isinstance(meta_uuid, str) or not meta_uuid:
                raise PreflightProcessingError(
                    f"field map missing meta uuid for {name}", status_code=500
                )
            entity_fields = fields_section.get(name)
            if not isinstance(entity_fields, dict):
                raise PreflightProcessingError(
                    f"field map missing field mapping for {name}", status_code=500
                )
            fields = {
                field_name: MetaField(uuid=str(field_uuid))
                for field_name, field_uuid in entity_fields.items()
                if isinstance(field_uuid, str) and field_uuid
            }
            entity = MetaEntity(name=name, meta_uuid=meta_uuid, fields=fields)
            entity.require(required)
            return entity

        dataset_class = _load_entity(
            "DatasetClass",
            ["headerHash", "headerSortedHash", "headersSorted", "numCols", "draft"],
        )
        dataset_signature = _load_entity(
            "DatasetSignature",
            [
                "fileName",
                "fileSize",
                "encoding",
                "sep",
                "headerHash",
                "headerSortedHash",
                "numCols",
                "headersSorted",
            ],
        )
        recognition_result = _load_entity(
            "RecognitionResult", ["score", "matchedBy", "timestamp"]
        )

        return cls(
            dataset_class=dataset_class,
            dataset_signature=dataset_signature,
            recognition_result=recognition_result,
        )

    @classmethod
    def from_discovery(
        cls,
        resolver: "MetaResolver",
        *,
        dataset_class_name: str,
        dataset_signature_name: str,
        recognition_result_name: str,
    ) -> "MetaConfig":
        dataset_class = resolver.resolve(
            dataset_class_name,
            ["headerHash", "headerSortedHash", "headersSorted", "numCols", "draft"],
        )
        dataset_signature = resolver.resolve(
            dataset_signature_name,
            [
                "fileName",
                "fileSize",
                "encoding",
                "sep",
                "headerHash",
                "headerSortedHash",
                "numCols",
                "headersSorted",
            ],
        )
        recognition_result = resolver.resolve(
            recognition_result_name, ["score", "matchedBy", "timestamp"]
        )

        return cls(
            dataset_class=dataset_class,
            dataset_signature=dataset_signature,
            recognition_result=recognition_result,
        )


class MetaResolver:
    """Resolve metadata by entity and field names using Onto discovery API."""

    def __init__(self, request_fn: Callable[..., Any], realm_id: str) -> None:
        self._request = request_fn
        self.realm_id = realm_id
        self._cache: Dict[str, MetaEntity] = {}

    def resolve(self, expected_name: str, required: Sequence[str]) -> MetaEntity:
        if expected_name in self._cache:
            entity = self._cache[expected_name]
        else:
            entity = self._fetch_entity(expected_name)
            self._cache[expected_name] = entity
        entity.require(required)
        return entity

    def _fetch_entity(self, expected_name: str) -> MetaEntity:
        if not expected_name:
            raise PreflightProcessingError(
                "meta entity name must be provided for discovery", status_code=500
            )

        endpoints: List[Tuple[str, str, Optional[Dict[str, Any]]]] = [
            ("POST", f"/realm/{self.realm_id}/entity/find/v2", {
                "metaEntityRequest": {"name": expected_name},
                "returnMeta": True,
                "pagination": {"first": 0, "offset": 5},
            }),
            ("POST", f"/realm/{self.realm_id}/meta/find", {
                "namePart": expected_name,
                "children": False,
                "parents": False,
                "returnMeta": True,
            }),
            ("GET", f"/realm/{self.realm_id}/meta/entity/list", None),
        ]

        last_error: Optional[PreflightProcessingError] = None
        for method, path, payload in endpoints:
            try:
                response = self._request(method, path, payload, params=None)
            except PreflightProcessingError as exc:
                last_error = exc
                continue

            candidates: Optional[List[Dict[str, Any]]] = None
            if isinstance(response, dict):
                result = response.get("result")
                if isinstance(result, Sequence):
                    candidates = [item for item in result if isinstance(item, dict)]
            if not candidates:
                candidates = [response] if isinstance(response, dict) else None

            entity = None
            if candidates:
                entity = self._select_from_candidates(candidates, expected_name)
                if entity is None:
                    # Some endpoints return nested structures; fall back to deep extraction
                    for candidate in candidates:
                        entity = self._extract_entity(candidate, expected_name)
                        if entity:
                            break
            else:
                entity = self._extract_entity(response, expected_name)

            if entity:
                safe_print(
                    "[preflight_submit] resolved meta entity "
                    f"'{expected_name}' -> {entity.meta_uuid}"
                )
                return entity

        if last_error is not None:
            raise last_error

        raise PreflightProcessingError(
            f"meta entity '{expected_name}' not found in Onto metadata", status_code=500
        )

    def _extract_entity(self, payload: Any, expected_name: str) -> Optional[MetaEntity]:
        if isinstance(payload, dict):
            candidates = [payload]
            meta_entity = payload.get("metaEntity")
            if isinstance(meta_entity, dict):
                candidates.append(meta_entity)
            entity = self._select_from_candidates(candidates, expected_name)
            if entity:
                return entity
            for value in payload.values():
                nested = self._extract_entity(value, expected_name)
                if nested:
                    return nested
        elif isinstance(payload, list):
            for item in payload:
                nested = self._extract_entity(item, expected_name)
                if nested:
                    return nested
        return None

    def _select_from_candidates(
        self, candidates: Sequence[Dict[str, Any]], expected_name: str
    ) -> Optional[MetaEntity]:
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            actual_name = (
                candidate.get("name")
                or candidate.get("label")
                or candidate.get("code")
                or candidate.get("entityName")
            )
            if actual_name and not self._name_matches(actual_name, expected_name):
                continue
            meta_uuid = self._extract_id(candidate)
            if not meta_uuid:
                continue
            fields = self._extract_fields(candidate)
            if not fields:
                continue
            name = actual_name or expected_name
            return MetaEntity(name=name, meta_uuid=meta_uuid, fields=fields)
        return None

    @staticmethod
    def _name_matches(actual: str, expected: str) -> bool:
        if not expected:
            return True
        if actual == expected:
            return True
        actual_lower = actual.lower()
        expected_lower = expected.lower()
        if actual_lower == expected_lower:
            return True
        if actual_lower.endswith(expected_lower):
            return True
        if expected_lower.endswith(actual_lower):
            return True
        if expected_lower in actual_lower:
            return True
        return False

    @staticmethod
    def _extract_id(obj: Dict[str, Any]) -> Optional[str]:
        for key in ("uuid", "id", "metaEntityUuid", "metaEntityId"):
            value = obj.get(key)
            if isinstance(value, str) and value:
                return value
        return None

    def _extract_fields(self, obj: Dict[str, Any]) -> Dict[str, MetaField]:
        field_map: Dict[str, MetaField] = {}
        containers: List[Any] = []

        for key in ("fields", "metaFields", "attributes"):
            value = obj.get(key)
            if isinstance(value, dict):
                containers.extend(value.values())
            elif isinstance(value, list):
                containers.extend(value)

        for item in containers:
            if not isinstance(item, dict):
                continue
            field_obj = item.get("field") if isinstance(item.get("field"), dict) else item
            name = (
                field_obj.get("name")
                or field_obj.get("code")
                or field_obj.get("label")
                or field_obj.get("fieldName")
            )
            uuid_value = (
                field_obj.get("uuid")
                or field_obj.get("id")
                or item.get("fieldUuid")
                or item.get("uuid")
                or item.get("id")
            )
            field_type = field_obj.get("fieldTypeName") or item.get("fieldTypeName")
            if isinstance(name, str) and name and isinstance(uuid_value, str) and uuid_value:
                field_map[name] = MetaField(uuid=uuid_value, field_type=field_type)
        return field_map


@dataclass
class SearchOutcome:
    matched: bool
    class_id: Optional[str]
    confidence: float
    matched_by: Optional[str]
    candidates: List[Dict[str, Any]]


@dataclass(frozen=True)
class StorageConfigData:
    config_id: str
    endpoint: str
    external_endpoint: Optional[str]
    bucket: str
    base_prefix: str
    path_pattern_raw: str
    presign_expiry_sec: int
    multipart_threshold_mib: Optional[int]
    multipart_part_size_mib: Optional[int]
    overwrite_policy: str

    def to_dict(self) -> Dict[str, Any]:
        multipart: Dict[str, Any] = {}
        if self.multipart_threshold_mib is not None:
            multipart["thresholdMiB"] = self.multipart_threshold_mib
        if self.multipart_part_size_mib is not None:
            multipart["partSizeMiB"] = self.multipart_part_size_mib
        return {
            "configId": self.config_id,
            "endpoint": self.endpoint,
            "externalEndpoint": self.external_endpoint,
            "bucket": self.bucket,
            "presignExpirySec": self.presign_expiry_sec,
            "multipart": multipart or None,
            "basePrefix": self.base_prefix,
            "pathPatternRaw": self.path_pattern_raw,
            "overwritePolicy": self.overwrite_policy,
        }


@dataclass(frozen=True)
class StorageAssignment:
    config: StorageConfigData
    s3_key: str

    def to_response(self) -> Dict[str, Any]:
        data = self.config.to_dict()
        multipart = data.pop("multipart")
        external_endpoint = data.pop("externalEndpoint")
        response = {
            "configId": data.pop("configId"),
            "endpoint": data.pop("endpoint"),
            "bucket": data.pop("bucket"),
            "s3Key": self.s3_key,
            "presignExpirySec": data.pop("presignExpirySec"),
        }
        if external_endpoint:
            response["externalEndpoint"] = external_endpoint
        if multipart:
            response["multipart"] = multipart
        if data.get("overwritePolicy"):
            response["overwritePolicy"] = data["overwritePolicy"]
        return response


class PreflightStore(Protocol):
    def set_request_id(self, request_id: str) -> None: ...

    def clear_request_id(self) -> None: ...

    def find_dataset_class_by_header_hash(
        self, header_hash: str, page_size: int
    ) -> List[Dict[str, Any]]: ...

    def find_dataset_class_by_header_sorted_hash(
        self, header_sorted_hash: str, page_size: int
    ) -> List[Dict[str, Any]]: ...

    def find_dataset_class_by_num_cols(
        self, num_cols: int, page_size: int
    ) -> List[Dict[str, Any]]: ...

    def create_dataset_class(self, signature: SignaturePayload) -> str: ...

    def ensure_dataset_signature(self, signature: SignaturePayload) -> str: ...

    def create_recognition_result(self, outcome: SearchOutcome) -> str: ...

    def build_entity_url(self, entity_id: Optional[str]) -> Optional[str]: ...


class OntoStore(PreflightStore):
    """Default store that reuses PreflightService internals."""

    def __init__(self, service: "PreflightService") -> None:
        self._service = service

    def set_request_id(self, request_id: str) -> None:
        self._service._request_id = request_id

    def clear_request_id(self) -> None:
        self._service._request_id = None

    def find_dataset_class_by_header_hash(
        self, header_hash: str, page_size: int
    ) -> List[Dict[str, Any]]:
        meta = self._service.meta.dataset_class
        return self._service._find_entities(
            meta.meta_uuid,
            [(meta.field("headerHash"), header_hash)],
            page_size=page_size,
        )

    def find_dataset_class_by_header_sorted_hash(
        self, header_sorted_hash: str, page_size: int
    ) -> List[Dict[str, Any]]:
        meta = self._service.meta.dataset_class
        return self._service._find_entities(
            meta.meta_uuid,
            [(meta.field("headerSortedHash"), header_sorted_hash)],
            page_size=page_size,
        )

    def find_dataset_class_by_num_cols(
        self, num_cols: int, page_size: int
    ) -> List[Dict[str, Any]]:
        meta = self._service.meta.dataset_class
        return self._service._find_all_pages(
            meta.meta_uuid,
            [(meta.field("numCols"), num_cols)],
            page_size=page_size,
        )

    def create_dataset_class(self, signature: SignaturePayload) -> str:
        existing = self.find_dataset_class_by_header_hash(signature.header_hash, page_size=1)
        if existing:
            entity_id = self._service._extract_entity_id(existing[0])
            if entity_id:
                safe_print(
                    f"[preflight_submit] dataset class already exists for {signature.header_hash}: {entity_id}"
                )
                return entity_id
        return self._service._create_dataset_class(signature)

    def ensure_dataset_signature(self, signature: SignaturePayload) -> str:
        meta = self._service.meta.dataset_signature
        filters: List[Tuple[str, Any]] = [(meta.field("headerHash"), signature.header_hash)]
        file_name_field = meta.get("fileName")
        if file_name_field:
            filters.append((file_name_field, signature.file_name))
        file_size_field = meta.get("fileSize")
        if file_size_field:
            filters.append((file_size_field, signature.file_size))

        existing = self._service._find_entities(
            meta.meta_uuid,
            filters,
            page_size=1,
        )
        if existing:
            entity_id = self._service._extract_entity_id(existing[0])
            if entity_id:
                safe_print(
                    f"[preflight_submit] dataset signature already exists: {entity_id}"
                )
                return entity_id
        return self._service._create_dataset_signature(signature)

    def create_recognition_result(self, outcome: SearchOutcome) -> str:
        return self._service._create_recognition_result(outcome)

    def build_entity_url(self, entity_id: Optional[str]) -> Optional[str]:
        return self._service._build_entity_url(entity_id)


class PreflightService:
    """Service executing the matching and creation workflow against Onto."""

    HASH_PAGE_SIZE = 20
    NUMCOLS_PAGE_SIZE = 100
    MAX_RETRIES = 3

    def __init__(
        self,
        *,
        api_base: Optional[str] = None,
        realm_id: Optional[str] = None,
        api_token: Optional[str] = None,
        field_map_path: Optional[str] = None,
        session: Optional[requests.Session] = None,
        timeout: float = 15.0,
        enable_create: Optional[bool] = None,
        meta_config: Optional[MetaConfig] = None,
        store: Optional[PreflightStore] = None,
    ) -> None:
        self.api_base = (api_base or ONTO_API_BASE or "").rstrip("/")
        self.realm_id = realm_id or ONTO_REALM_ID or ""
        self.api_token = api_token or ONTO_API_TOKEN or ""
        raw_field_map_path = field_map_path or FIELD_MAP_PATH or ""
        self.field_map_path = Path(raw_field_map_path) if raw_field_map_path else None
        self.session = session or requests.Session()
        self.timeout = timeout
        self.enable_create = ENABLE_CREATE if enable_create is None else enable_create
        self.meta_names = {
            "dataset_class": ONTO_META_DATASETCLASS_NAME or "DatasetClass",
            "dataset_signature": ONTO_META_SIGNATURE_NAME or "DatasetSignature",
            "recognition_result": ONTO_META_RECOG_NAME or "RecognitionResult",
            "column_signature": ONTO_META_COLUMNSIGN_NAME or "ColumnSignature",
            "pipeline_template": ONTO_META_PIPELINE_NAME or "PipelineTemplate",
            "storage_config": ONTO_META_STORAGECONFIG_NAME or "StorageConfig",
        }
        self.debug_http = ONTO_DEBUG_HTTP
        self._request_id: Optional[str] = None
        self.enable_storage_links = ENABLE_STORAGE_LINKS
        self.allow_s3key_regenerate = ALLOW_S3KEY_REGENERATE

        if not self.api_base or not self.realm_id or not self.api_token:
            raise PreflightProcessingError(
                "ONTO_API_BASE, ONTO_REALM_ID and ONTO_API_TOKEN must be configured",
                status_code=500,
            )

        self._meta: Optional[MetaConfig] = meta_config
        self.store: PreflightStore = store or OntoStore(self)
        self._relation_cache: Set[Tuple[str, str, str]] = set()
        self._last_created_column_signatures: List[str] = []
        self._last_created_pipeline_template_id: Optional[str] = None
        self._storage_meta: Optional[MetaEntity] = None
        self._pipeline_template_meta: Optional[MetaEntity] = None

    @property
    def meta(self) -> MetaConfig:
        if self._meta is None:
            if self.field_map_path:
                safe_print(
                    f"[preflight_submit] loading metadata from field map {self.field_map_path}"
                )
                self._meta = MetaConfig.from_file(self.field_map_path)
            else:
                resolver = MetaResolver(self._request, self.realm_id)
                self._meta = MetaConfig.from_discovery(
                    resolver,
                    dataset_class_name=self.meta_names["dataset_class"],
                    dataset_signature_name=self.meta_names["dataset_signature"],
                    recognition_result_name=self.meta_names["recognition_result"],
                )
        return self._meta

    @property
    def storage_meta(self) -> MetaEntity:
        if self._storage_meta is None:
            resolver = MetaResolver(self._request, self.realm_id)
            self._storage_meta = resolver.resolve(
                self.meta_names["storage_config"],
                [
                    "bucketRaw",
                    "bucket",
                    "basePrefix",
                    "pathPatternRaw",
                ],
            )
        return self._storage_meta

    @property
    def pipeline_template_meta(self) -> Optional[MetaEntity]:
        if self._pipeline_template_meta is None:
            resolver = MetaResolver(self._request, self.realm_id)
            try:
                self._pipeline_template_meta = resolver.resolve(
                    self.meta_names["pipeline_template"],
                    [],
                )
            except PreflightProcessingError:
                self._pipeline_template_meta = None
        return self._pipeline_template_meta

    def process(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        signature = SignaturePayload.from_payload(payload)
        request_id = uuid.uuid4().hex
        self.store.set_request_id(request_id)

        safe_print(
            "[preflight_submit] searching Onto for signature "
            f"{signature.header_hash} ({signature.file_name}), request {request_id}"
        )

        headers_in = signature.header_set()
        self._last_created_column_signatures = []
        self._last_created_pipeline_template_id = None

        try:
            outcome = self._search_for_dataset_class(signature, headers_in)

            notes: List[str] = []
            match_block: Optional[Dict[str, Any]] = None
            dataset_class_id: Optional[str] = outcome.class_id
            created_dataset_class_id: Optional[str] = None

            if outcome.matched and dataset_class_id:
                safe_print(
                    "[preflight_submit] matched dataset class "
                    f"{dataset_class_id} via {outcome.matched_by}"
                )
                match_block = {
                    "classId": dataset_class_id,
                    "confidence": round(outcome.confidence, 4),
                    "matchedBy": outcome.matched_by,
                }
            else:
                if self.enable_create:
                    safe_print(
                        "[preflight_submit] dataset class not found, creating draft in Onto"
                    )
                    dataset_class_id = self.store.create_dataset_class(signature)
                    created_dataset_class_id = dataset_class_id
                    notes.append("Created draft dataset class.")
                else:
                    safe_print(
                        "[preflight_submit] dataset class not found, creation disabled by configuration"
                    )
                    notes.append("Dataset class creation disabled (ENABLE_CREATE=false).")

            dataset_signature_id = self.store.ensure_dataset_signature(signature)
            notes.append("Created dataset signature.")

            recognition_result_id = self.store.create_recognition_result(outcome)
            notes.append("Recorded recognition result.")

            pipeline_template_id = (
                self._last_created_pipeline_template_id
                or self._resolve_existing_pipeline_template_id(dataset_class_id)
            )

            relations_created = self._create_relations(
                dataset_class_id=dataset_class_id,
                dataset_signature_id=dataset_signature_id,
                recognition_result_id=recognition_result_id,
                column_signature_ids=self._last_created_column_signatures,
                pipeline_template_id=pipeline_template_id,
            )

            storage_assignment = self._resolve_storage(
                signature=signature,
                dataset_signature_id=dataset_signature_id,
                dataset_class_id=dataset_class_id,
                pipeline_template_id=pipeline_template_id,
            )

            response: Dict[str, Any] = {
                "match": match_block,
                "created": {
                    "datasetClassId": created_dataset_class_id,
                    "datasetSignatureId": dataset_signature_id,
                    "recognitionResultId": recognition_result_id,
                },
                "links": {
                    "classUrl": self.store.build_entity_url(dataset_class_id),
                    "signatureUrl": self.store.build_entity_url(dataset_signature_id),
                    "recognitionUrl": self.store.build_entity_url(recognition_result_id),
                },
                "notes": notes,
            }

            response["matched"] = outcome.matched
            response["datasetClassEntityId"] = dataset_class_id
            response["datasetSignatureId"] = dataset_signature_id
            response["templateId"] = pipeline_template_id
            response["storage"] = storage_assignment.to_response()

            if relations_created:
                response["relations"] = relations_created

            if outcome.candidates:
                response["candidates"] = outcome.candidates[:5]

            return response
        finally:
            self.store.clear_request_id()

    # ------------------------------------------------------------------
    # Onto API helpers
    # ------------------------------------------------------------------

    def _find_entities(
        self,
        meta_uuid: str,
        filters: Sequence[Tuple[str, Any]],
        *,
        page_size: int,
        first: int = 0,
    ) -> List[Dict[str, Any]]:
        payload = {
            "metaEntityRequest": {"uuid": meta_uuid},
            "metaFieldFilters": [
                {"uuid": field_uuid, "value": self._normalize_filter_value(value)}
                for field_uuid, value in filters
            ],
            "pagination": {"first": first, "offset": page_size},
        }

        response = self._post_find(payload)
        return self._flatten_entities(response)

    def _find_all_pages(
        self,
        meta_uuid: str,
        filters: Sequence[Tuple[str, Any]],
        *,
        page_size: int,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        first = 0

        while True:
            page = self._find_entities(
                meta_uuid,
                filters,
                page_size=page_size,
                first=first,
            )
            if not page:
                break
            results.extend(page)
            if len(page) < page_size:
                break
            first += page_size

        return results

    @staticmethod
    def _flatten_entities(response: Any) -> List[Dict[str, Any]]:
        def _iter_entities(obj: Any) -> Iterable[Dict[str, Any]]:
            if isinstance(obj, dict):
                if "entities" in obj and isinstance(obj["entities"], Sequence):
                    for item in obj["entities"]:
                        yield from _iter_entities(item)
                else:
                    yield obj
            elif isinstance(obj, list):
                for item in obj:
                    yield from _iter_entities(item)

        return [entity for entity in _iter_entities(response) if isinstance(entity, dict)]

    # ------------------------------------------------------------------
    # Candidate processing helpers
    # ------------------------------------------------------------------

    def _summarise_candidates(
        self,
        entities: Sequence[Dict[str, Any]],
        incoming_headers: Set[str],
        *,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        scored: List[Dict[str, Any]] = []

        for entity in entities:
            class_id = self._extract_entity_id(entity)
            if not class_id:
                continue

            headers_sorted = self._extract_field_value(
                entity, self.meta.dataset_class.field("headersSorted")
            )

            headers_overlap = 0.0
            if headers_sorted:
                class_headers = self._parse_headers(headers_sorted)
                headers_overlap = self._jaccard(incoming_headers, class_headers)

            score = round(0.4 * headers_overlap, 4)

            scored.append(
                {
                    "classId": class_id,
                    "score": score,
                    "headersOverlap": round(headers_overlap, 4),
                }
            )

        scored.sort(key=lambda item: item["score"], reverse=True)
        if limit is not None:
            return scored[:limit]
        return scored

    @staticmethod
    def _extract_entity_id(entity: Dict[str, Any]) -> Optional[str]:
        candidates = [
            entity.get("uuid"),
            entity.get("id"),
            entity.get("entityUuid"),
            entity.get("entityId"),
        ]

        for candidate in candidates:
            if isinstance(candidate, str) and candidate:
                return candidate

        inner = entity.get("entity") if isinstance(entity, dict) else None
        if isinstance(inner, dict):
            return PreflightService._extract_entity_id(inner)

        return None

    @staticmethod
    def _extract_field_value(entity: Dict[str, Any], field_uuid: str) -> Optional[str]:
        fields = entity.get("fields")
        if isinstance(fields, dict):
            value = fields.get(field_uuid)
            if isinstance(value, dict):
                if isinstance(value.get("value"), str):
                    return value["value"]
                if isinstance(value.get("values"), list) and value["values"]:
                    first_value = value["values"][0]
                    if isinstance(first_value, str):
                        return first_value
            elif isinstance(value, str):
                return value
        elif isinstance(fields, list):
            for item in fields:
                if not isinstance(item, dict):
                    continue
                if item.get("fieldUuid") != field_uuid:
                    continue
                raw_value = item.get("value")
                if isinstance(raw_value, str):
                    return raw_value
                values = item.get("values")
                if isinstance(values, list) and values and isinstance(values[0], str):
                    return values[0]
        return None

    @staticmethod
    def _parse_headers(headers_sorted: str) -> Set[str]:
        return {part for part in headers_sorted.split(";") if part}

    @staticmethod
    def _jaccard(left: Set[str], right: Set[str]) -> float:
        if not left and not right:
            return 1.0
        union = left | right
        if not union:
            return 0.0
        intersection = left & right
        return len(intersection) / len(union)

    def _search_for_dataset_class(
        self, signature: SignaturePayload, headers_in: Set[str]
    ) -> SearchOutcome:
        exact_matches = self.store.find_dataset_class_by_header_hash(
            signature.header_hash, page_size=self.HASH_PAGE_SIZE
        )
        candidate_summaries = self._summarise_candidates(
            exact_matches, headers_in, limit=5
        )
        if candidate_summaries:
            top_candidate = candidate_summaries[0]
            return SearchOutcome(
                matched=True,
                class_id=top_candidate["classId"],
                confidence=1.0,
                matched_by="headerHash",
                candidates=candidate_summaries,
            )

        sorted_matches = self.store.find_dataset_class_by_header_sorted_hash(
            signature.header_sorted_hash, page_size=self.HASH_PAGE_SIZE
        )
        candidate_summaries = self._summarise_candidates(
            sorted_matches, headers_in, limit=5
        )
        if candidate_summaries:
            top_candidate = candidate_summaries[0]
            return SearchOutcome(
                matched=True,
                class_id=top_candidate["classId"],
                confidence=0.8,
                matched_by="headerSortedHash",
                candidates=candidate_summaries,
            )

        all_candidates = self.store.find_dataset_class_by_num_cols(
            signature.num_cols, page_size=self.NUMCOLS_PAGE_SIZE
        )
        scored_candidates = self._summarise_candidates(
            all_candidates, headers_in, limit=None
        )

        if scored_candidates:
            best = scored_candidates[0]
            normalized_score = best["score"] / 0.4 if 0.4 else 0.0
            if normalized_score >= 0.7:
                return SearchOutcome(
                    matched=True,
                    class_id=best["classId"],
                    confidence=best["score"],
                    matched_by="numCols+jaccard",
                    candidates=scored_candidates[:5],
                )
            return SearchOutcome(
                matched=False,
                class_id=None,
                confidence=best["score"],
                matched_by=None,
                candidates=scored_candidates[:5],
            )

        return SearchOutcome(
            matched=False, class_id=None, confidence=0.0, matched_by=None, candidates=[]
        )


    def _resolve_storage(
        self,
        *,
        signature: SignaturePayload,
        dataset_signature_id: str,
        dataset_class_id: Optional[str],
        pipeline_template_id: Optional[str],
    ) -> StorageAssignment:
        existing_entry = self._load_signature_storage_fields(dataset_signature_id)
        existing_assignment = self._storage_entry_to_assignment(existing_entry)

        if existing_assignment and not self.allow_s3key_regenerate:
            safe_print(
                "[preflight_submit] reusing existing storage assignment %s for signature %s"
                % (existing_assignment.config.config_id, dataset_signature_id)
            )
            return existing_assignment

        chosen_config_id = (
            existing_assignment.config.config_id if existing_assignment else None
        )

        template_config_id = self._resolve_template_storage_config(pipeline_template_id)
        if template_config_id:
            chosen_config_id = template_config_id

        if not chosen_config_id:
            chosen_config_id = self._find_default_storage_config_id()

        if not chosen_config_id:
            raise PreflightProcessingError(
                "storage_config_not_found: create [INFRA] StorageConfig or set template_uses_storage",
                status_code=424,
            )

        config = self._resolve_storage_config(chosen_config_id)

        dataset_slug = self._determine_dataset_slug(dataset_class_id)
        s3_key = self._generate_s3_key(
            config, dataset_slug, signature, dataset_signature_id
        )

        assignment = StorageAssignment(config=config, s3_key=s3_key)
        persisted = self._persist_storage_assignment(dataset_signature_id, assignment)

        if self.enable_storage_links:
            self._create_relation(
                "signature_uses_storage",
                dataset_signature_id,
                assignment.config.config_id,
            )

        storage_cache.set_storage(dataset_signature_id, persisted)

        safe_print(
            "[preflight_submit] resolved storage config %s for signature %s (s3Key=%s)"
            % (assignment.config.config_id, dataset_signature_id, assignment.s3_key)
        )

        return assignment

    def _load_signature_storage_fields(self, signature_id: str) -> Dict[str, Any]:
        cached = storage_cache.get_storage(signature_id)
        if cached:
            return cached

        entity = self._get_entity(signature_id)
        if not entity:
            return {}

        meta_signature = self.meta.dataset_signature
        mapping = {
            "storageConfigId": "configId",
            "storageEndpoint": "endpoint",
            "storageExternalEndpoint": "externalEndpoint",
            "storageBucket": "bucket",
            "storageS3Key": "s3Key",
            "storagePresignExpirySec": "presignExpirySec",
            "storageMultipartThresholdMiB": "multipartThresholdMiB",
            "storageMultipartPartSizeMiB": "multipartPartSizeMiB",
            "storageOverwritePolicy": "overwritePolicy",
            "storageBasePrefix": "basePrefix",
            "storagePathPatternRaw": "pathPatternRaw",
        }

        result: Dict[str, Any] = {}
        for field_name, output_name in mapping.items():
            field_uuid = meta_signature.get(field_name)
            if not field_uuid:
                continue
            value = self._extract_field_value(entity, field_uuid)
            if value is None or value == "":
                continue
            result[output_name] = value

        if result:
            storage_cache.set_storage(signature_id, result)
        return result

    def _storage_entry_to_assignment(
        self, entry: Dict[str, Any]
    ) -> Optional[StorageAssignment]:
        config_id = entry.get("configId")
        s3_key = entry.get("s3Key")
        if not isinstance(config_id, str) or not config_id:
            return None
        if not isinstance(s3_key, str) or not s3_key:
            return None

        config = StorageConfigData(
            config_id=config_id,
            endpoint=str(entry.get("endpoint") or ""),
            external_endpoint=(
                str(entry["externalEndpoint"])
                if entry.get("externalEndpoint") not in (None, "")
                else None
            ),
            bucket=str(entry.get("bucket") or "raw"),
            base_prefix=str(entry.get("basePrefix") or ""),
            path_pattern_raw=str(entry.get("pathPatternRaw") or ""),
            presign_expiry_sec=self._to_int(entry.get("presignExpirySec"), 3600),
            multipart_threshold_mib=self._to_optional_int(
                entry.get("multipartThresholdMiB")
            ),
            multipart_part_size_mib=self._to_optional_int(
                entry.get("multipartPartSizeMiB")
            ),
            overwrite_policy=str(entry.get("overwritePolicy") or ""),
        )
        return StorageAssignment(config=config, s3_key=str(s3_key))

    def _resolve_template_storage_config(
        self, template_id: Optional[str]
    ) -> Optional[str]:
        if not template_id:
            return None
        meta_template = self.pipeline_template_meta
        if meta_template is None:
            return None
        entity = self._get_entity(template_id)
        if not entity:
            return None

        candidate_fields = [
            "storageConfig",
            "storageConfigId",
            "uses_storage",
            "storage",
        ]

        for field_name in candidate_fields:
            field_uuid = meta_template.get(field_name)
            if not field_uuid:
                continue
            value = self._extract_field_value(entity, field_uuid)
            if isinstance(value, str) and value:
                return value

        fields = entity.get("fields")
        if isinstance(fields, dict):
            for raw_value in fields.values():
                if isinstance(raw_value, dict):
                    candidate = (
                        raw_value.get("id")
                        or raw_value.get("uuid")
                        or raw_value.get("value")
                    )
                    if isinstance(candidate, str) and candidate:
                        return candidate
                elif isinstance(raw_value, str) and raw_value:
                    return raw_value

        return None

    def _find_default_storage_config_id(self) -> Optional[str]:
        meta_storage = self.storage_meta
        field_uuid = meta_storage.get("isDefault")
        if not field_uuid:
            return None

        candidates = self._find_entities(
            meta_storage.meta_uuid,
            [(field_uuid, True)],
            page_size=1,
        )
        if not candidates:
            return None
        return self._extract_entity_id(candidates[0])

    def _resolve_storage_config(self, config_id: str) -> StorageConfigData:
        entity = self._get_entity(config_id)
        if not entity:
            raise PreflightProcessingError(
                f"storage config {config_id} not found",
                status_code=424,
            )

        meta_storage = self.storage_meta

        def read(field_name: str) -> Optional[str]:
            field_uuid = meta_storage.get(field_name)
            if not field_uuid:
                return None
            value = self._extract_field_value(entity, field_uuid)
            if isinstance(value, str):
                return value
            if isinstance(value, (int, float)):
                return str(value)
            return None

        endpoint = read("endpoint") or ""
        external_endpoint = read("externalEndpoint")
        bucket = read("bucketRaw") or read("bucket") or "raw"
        base_prefix = read("basePrefix") or ""
        path_pattern = read("pathPatternRaw") or ""
        overwrite_policy = (read("overwritePolicy") or "").lower()
        presign_expiry = self._to_int(read("presignExpirySec"), 3600)
        multipart_threshold = self._to_optional_int(read("multipartThresholdMiB"))
        multipart_part_size = self._to_optional_int(read("multipartPartSizeMiB"))

        if not path_pattern.strip():
            raise PreflightProcessingError(
                "invalid_storage_config: pathPatternRaw is required",
                status_code=422,
            )

        bucket_normalized = bucket or "raw"
        if bucket_normalized.lower() != "raw":
            safe_print(
                f"[preflight_submit] warning: storage config {config_id} bucket is '{bucket_normalized}' (expected 'raw')"
            )

        return StorageConfigData(
            config_id=config_id,
            endpoint=endpoint,
            external_endpoint=external_endpoint,
            bucket=bucket_normalized,
            base_prefix=base_prefix,
            path_pattern_raw=path_pattern,
            presign_expiry_sec=presign_expiry,
            multipart_threshold_mib=multipart_threshold,
            multipart_part_size_mib=multipart_part_size,
            overwrite_policy=overwrite_policy,
        )

    def _persist_storage_assignment(
        self, signature_id: str, assignment: StorageAssignment
    ) -> Dict[str, Any]:
        data = {
            "configId": assignment.config.config_id,
            "endpoint": assignment.config.endpoint,
            "externalEndpoint": assignment.config.external_endpoint,
            "bucket": assignment.config.bucket,
            "s3Key": assignment.s3_key,
            "presignExpirySec": assignment.config.presign_expiry_sec,
            "multipartThresholdMiB": assignment.config.multipart_threshold_mib,
            "multipartPartSizeMiB": assignment.config.multipart_part_size_mib,
            "basePrefix": assignment.config.base_prefix,
            "pathPatternRaw": assignment.config.path_pattern_raw,
            "overwritePolicy": assignment.config.overwrite_policy,
        }

        meta_signature = self.meta.dataset_signature

        def maybe_add(field_name: str, value: Any) -> None:
            field_uuid = meta_signature.get(field_name)
            if not field_uuid:
                return
            if value is None:
                return
            fields[field_uuid] = value

        fields: Dict[str, Any] = {}
        maybe_add("storageConfigId", assignment.config.config_id)
        maybe_add("storageEndpoint", assignment.config.endpoint)
        maybe_add("storageExternalEndpoint", assignment.config.external_endpoint or "")
        maybe_add("storageBucket", assignment.config.bucket)
        maybe_add("storageS3Key", assignment.s3_key)
        maybe_add("storagePresignExpirySec", assignment.config.presign_expiry_sec)
        maybe_add(
            "storageMultipartThresholdMiB",
            assignment.config.multipart_threshold_mib,
        )
        maybe_add(
            "storageMultipartPartSizeMiB",
            assignment.config.multipart_part_size_mib,
        )
        maybe_add("storageOverwritePolicy", assignment.config.overwrite_policy)
        maybe_add("storageBasePrefix", assignment.config.base_prefix)
        maybe_add("storagePathPatternRaw", assignment.config.path_pattern_raw)

        if fields:
            self._update_entity_fields(signature_id, fields, meta_signature)

        return data

    def _determine_dataset_slug(self, dataset_class_id: Optional[str]) -> str:
        if not dataset_class_id:
            return "unknown"
        entity = self._get_entity(dataset_class_id)
        if not entity:
            return "unknown"

        name = entity.get("name")
        if isinstance(name, dict):
            name = name.get("value")
        keywords = None
        keywords_field = self.meta.dataset_class.get("keywords")
        if keywords_field:
            keywords = self._extract_field_value(entity, keywords_field)

        for candidate in (name, keywords):
            slug = self._slugify(candidate)
            if slug:
                return slug

        return "unknown"

    @staticmethod
    def _slugify(value: Any) -> str:
        if not isinstance(value, str):
            return ""
        normalized = value.strip().lower()
        if not normalized:
            return ""
        normalized = normalized.replace(" ", "-")
        normalized = re.sub(r"[^0-9a-zа-яё_-]+", "-", normalized)
        normalized = re.sub(r"-+", "-", normalized)
        normalized = normalized.strip("-_")
        return normalized

    def _generate_s3_key(
        self,
        config: StorageConfigData,
        dataset_slug: str,
        signature: SignaturePayload,
        signature_id: str,
    ) -> str:
        today = datetime.now(timezone.utc)
        placeholders = {
            "dataset": dataset_slug or "unknown",
            "yyyy": today.strftime("%Y"),
            "mm": today.strftime("%m"),
            "dd": today.strftime("%d"),
            "uuid": uuid.uuid4().hex,
            "fileName": signature.file_name,
            "fileNameNoExt": self._file_stem(signature.file_name),
            "signatureId": signature_id,
        }

        key_body = self._apply_path_pattern(config.path_pattern_raw, placeholders)
        if not key_body:
            raise PreflightProcessingError(
                "invalid_storage_config: pathPatternRaw produced empty key",
                status_code=422,
            )

        base_prefix = config.base_prefix or ""
        if base_prefix and not base_prefix.endswith("/"):
            base_prefix = f"{base_prefix}/"

        key_body = key_body.lstrip("/")
        key = f"{base_prefix}{key_body}" if base_prefix else key_body

        if config.overwrite_policy == "deny":
            # TODO: optionally check object existence via storage API.
            pass

        return key

    @staticmethod
    def _apply_path_pattern(pattern: str, mapping: Dict[str, str]) -> str:
        def replace(match: "re.Match") -> str:
            token = match.group(1)
            return mapping.get(token, "")

        return re.sub(r"\{([a-zA-Z0-9_]+)\}", replace, pattern)

    @staticmethod
    def _file_stem(file_name: str) -> str:
        stem = Path(file_name).stem
        return stem or file_name

    def _resolve_existing_pipeline_template_id(
        self, dataset_class_id: Optional[str]
    ) -> Optional[str]:
        if not dataset_class_id:
            return None
        entity = self._get_entity(dataset_class_id)
        if not entity:
            return None

        candidates = [
            "pipelineTemplate",
            "template",
            "defaultTemplate",
            "pipelineTemplateId",
        ]

        for field_name in candidates:
            field_uuid = self.meta.dataset_class.get(field_name)
            if not field_uuid:
                continue
            value = self._extract_field_value(entity, field_uuid)
            if isinstance(value, str) and value:
                return value
        return None

    def _get_entity(self, entity_id: str) -> Optional[Dict[str, Any]]:
        if not entity_id:
            return None
        try:
            response = self._request(
                "GET", f"/realm/{self.realm_id}/entity/{entity_id}", None, params=None
            )
        except PreflightProcessingError as exc:
            safe_print(
                f"[preflight_submit] failed to fetch entity {entity_id}: {exc}"
            )
            return None

        if isinstance(response, dict):
            entity = response.get("entity")
            if isinstance(entity, dict):
                return entity
            return response
        return None

    @staticmethod
    def _to_int(value: Any, default: int = 0) -> int:
        if value in (None, ""):
            return default
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _to_optional_int(value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return None

    def _create_dataset_class(self, signature: SignaturePayload) -> str:
        self._last_created_column_signatures = []
        self._last_created_pipeline_template_id = None
        fields = {
            self.meta.dataset_class.field("headerHash"): signature.header_hash,
            self.meta.dataset_class.field(
                "headerSortedHash"
            ): signature.header_sorted_hash,
            self.meta.dataset_class.field(
                "headersSorted"
            ): signature.headers_sorted_string(),
            self.meta.dataset_class.field("numCols"): signature.num_cols,
            self.meta.dataset_class.field("draft"): True,
        }

        name_value = f"Draft dataset {signature.header_hash[-8:]}"
        comment_value = ""

        keywords_field = self.meta.dataset_class.get("keywords")
        if keywords_field:
            fields[keywords_field] = ""

        for pii_field in ("piiPhone", "piiFio", "piiInn", "piiBirthday"):
            uuid_field = self.meta.dataset_class.get(pii_field)
            if uuid_field:
                fields[uuid_field] = False

        priority_field = self.meta.dataset_class.get("priority")
        if priority_field:
            fields[priority_field] = 0

        safe_print("[preflight_submit] creating DatasetClass draft in Onto")
        return self._create_entity(
            self.meta.dataset_class,
            fields,
            name=name_value,
            comment=comment_value,
        )

    def _create_dataset_signature(self, signature: SignaturePayload) -> str:
        fields = {
            self.meta.dataset_signature.field("fileName"): signature.file_name,
            self.meta.dataset_signature.field("fileSize"): signature.file_size,
            self.meta.dataset_signature.field("encoding"): signature.encoding,
            self.meta.dataset_signature.field("sep"): signature.separator,
            self.meta.dataset_signature.field("headerHash"): signature.header_hash,
            self.meta.dataset_signature.field(
                "headerSortedHash"
            ): signature.header_sorted_hash,
            self.meta.dataset_signature.field(
                "numCols"
            ): signature.num_cols,
            self.meta.dataset_signature.field(
                "headersSorted"
            ): signature.headers_sorted_string(),
        }

        name_value = signature.file_name
        comment_value = ""

        safe_print("[preflight_submit] creating DatasetSignature in Onto")
        return self._create_entity(
            self.meta.dataset_signature,
            fields,
            name=name_value,
            comment=comment_value,
        )

    def _create_recognition_result(self, outcome: SearchOutcome) -> str:
        timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        if timestamp.endswith("+00:00"):
            timestamp = timestamp.replace("+00:00", "Z")

        matched_by = outcome.matched_by if outcome.matched else "not_matched"
        fields = {
            self.meta.recognition_result.field("score"): round(outcome.confidence, 4),
            self.meta.recognition_result.field("matchedBy"): matched_by,
            self.meta.recognition_result.field("timestamp"): timestamp,
        }

        safe_print("[preflight_submit] creating RecognitionResult entry in Onto")
        name_value = f"Recognition {self._request_id[:8]}" if self._request_id else "Recognition"
        return self._create_entity(
            self.meta.recognition_result,
            fields,
            name=name_value,
            comment="",
        )

    def _create_entity(
        self,
        meta_entity: MetaEntity,
        fields: Dict[str, Any],
        *,
        name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> str:
        meta_uuid = meta_entity.meta_uuid
        entity_id = str(uuid.uuid4())
        payload = {
            "metaEntityId": meta_uuid,
            "id": entity_id,
            "name": name or f"entity-{entity_id[:8]}",
            "comment": comment or "",
        }
        self._request(
            "POST", f"/realm/{self.realm_id}/entity", payload, params=None
        )

        if fields:
            self._update_entity_fields(entity_id, fields, meta_entity)

        return entity_id

    def _update_entity_fields(
        self,
        entity_id: str,
        fields: Dict[str, Any],
        meta_entity: MetaEntity,
    ) -> None:
        patches: List[Dict[str, Any]] = []
        seen: Set[str] = set()

        for field_uuid, raw_value in fields.items():
            field_uuid = str(field_uuid)
            if not field_uuid or field_uuid in seen:
                continue
            seen.add(field_uuid)

            field_name = meta_entity.field_name_for_uuid(field_uuid) or field_uuid
            meta_field = meta_entity.field_info(field_name)

            value_str = self._stringify_field_value(raw_value)
            patch: Dict[str, Any] = {
                "metaFieldUuid": field_uuid,
                "fieldTypeName": "T_STRING",
                "name": field_name,
                "value": value_str,
                "comment": "",
                "id": str(uuid.uuid4()),
            }

            patches.append(patch)

        if not patches:
            return

        try:
            self._request(
                "PATCH",
                f"/realm/{self.realm_id}/entity/{entity_id}/fields",
                patches,
                params=None,
            )
        except PreflightProcessingError as exc:
            field_summary = ", ".join(patch.get("name", patch.get("metaFieldUuid")) for patch in patches)
            raise PreflightProcessingError(
                f"{exc} (while updating fields: {field_summary})",
                status_code=getattr(exc, "status_code", 400),
            ) from exc

    
    def _create_relations(
        self,
        *,
        dataset_class_id: Optional[str],
        dataset_signature_id: Optional[str],
        recognition_result_id: Optional[str],
        column_signature_ids: Optional[Sequence[str]] = None,
        pipeline_template_id: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        relations_spec: List[Tuple[str, Optional[str], Optional[str]]] = []

        if dataset_signature_id and dataset_class_id:
            relations_spec.append(
                ("signature_recognized_as_class", dataset_signature_id, dataset_class_id)
            )
        if recognition_result_id and dataset_signature_id:
            relations_spec.append(
                ("recognition_of_signature", recognition_result_id, dataset_signature_id)
            )
        if recognition_result_id and dataset_class_id:
            relations_spec.append(
                ("recognition_to_class", recognition_result_id, dataset_class_id)
            )

        if pipeline_template_id and dataset_class_id:
            relations_spec.append(
                ("class_has_template", dataset_class_id, pipeline_template_id)
            )
        if pipeline_template_id and dataset_signature_id:
            relations_spec.append(
                ("signature_based_on_template", dataset_signature_id, pipeline_template_id)
            )

        if column_signature_ids and dataset_class_id:
            for column_id in column_signature_ids:
                if not column_id:
                    continue
                relations_spec.append(
                    ("class_has_column", dataset_class_id, str(column_id))
                )

        results: List[Dict[str, str]] = []
        for relation_type, start_id, end_id in relations_spec:
            result = self._create_relation(relation_type, start_id, end_id)
            if result:
                results.append(result)
        return results

    def _create_relation(
        self,
        relation_type: str,
        start_id: Optional[str],
        end_id: Optional[str],
    ) -> Optional[Dict[str, str]]:
        if not start_id or not end_id:
            return None

        start_id = str(start_id).strip()
        end_id = str(end_id).strip()
        if not start_id or not end_id:
            return None

        relation_type = relation_type.strip()
        if relation_type not in {
            "class_has_column",
            "class_has_template",
            "signature_recognized_as_class",
            "signature_based_on_template",
            "recognition_of_signature",
            "recognition_to_class",
            "signature_uses_storage",
        }:
            raise PreflightProcessingError(
                f"Unsupported relation type '{relation_type}'",
                status_code=500,
            )

        cache_key = (relation_type, start_id, end_id)
        if cache_key in self._relation_cache:
            return None

        payload = {
            "startRelatedEntity": {"id": start_id, "role": ""},
            "endRelatedEntity": {"id": end_id, "role": ""},
            "type": relation_type,
        }

        safe_print(
            f"[preflight_submit] relation {relation_type}: {start_id} -> {end_id} (realm {self.realm_id}, request {self._request_id})"
        )

        try:
            self._request(
                "POST",
                f"/realm/{self.realm_id}/entity/relation",
                payload,
                params=None,
            )
        except PreflightProcessingError as exc:
            status = getattr(exc, "status_code", None)
            message = str(exc)
            safe_print(
                f"[preflight_submit] relation error {relation_type}: {start_id} -> {end_id} (status={status}) {message}"
            )
            if status == 409:
                self._relation_cache.add(cache_key)
                return {
                    "type": relation_type,
                    "startId": start_id,
                    "endId": end_id,
                    "status": "duplicate",
                }
            raise

        self._relation_cache.add(cache_key)
        return {
            "type": relation_type,
            "startId": start_id,
            "endId": end_id,
            "status": "created",
        }

    @staticmethod
    def _stringify_field_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
        if isinstance(value, str):
            return value
        if isinstance(value, (list, tuple, set)):
            return ";".join(str(item) for item in value)
        if isinstance(value, dict):
            try:
                return json.dumps(value, ensure_ascii=False)
            except (TypeError, ValueError):
                return str(value)
        return str(value)

    def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Any],
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        url = f"{self.api_base}{path}"
        headers = {
            "X-API-Key": self.api_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Request-Id": self._request_id or uuid.uuid4().hex,
        }

        last_error: Optional[str] = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            if self.debug_http:
                body_repr = ""
                if payload is not None:
                    try:
                        body_repr = json.dumps(payload, ensure_ascii=False)
                    except (TypeError, ValueError):
                        body_repr = str(payload)
                request_log = f"[preflight_submit][http] --> {method.upper()} {path}"
                if body_repr:
                    request_log = f"{request_log}\n{body_repr}"
                safe_print(request_log)
            try:
                response = self.session.request(
                    method,
                    url,
                    json=payload if method.upper() != "GET" else None,
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                last_error = str(exc)
                if self.debug_http:
                    safe_print(
                        f"[preflight_submit][http] !! network error on {method.upper()} {path}: {exc}"
                    )
                if attempt == self.MAX_RETRIES:
                    raise PreflightProcessingError(
                        f"failed to reach Onto API: {exc}", status_code=502
                    ) from exc
                continue

            if self.debug_http:
                preview = response.text.strip()
                if len(preview) > 2000:
                    preview = preview[:2000] + "..."
                response_log = (
                    f"[preflight_submit][http] <-- {response.status_code} {method.upper()} {path}"
                )
                if preview:
                    response_log = f"{response_log}\n{preview}"
                safe_print(response_log)

            if response.status_code >= 500:
                last_error = (
                    f"Onto API error {response.status_code}: {response.text.strip()}"
                )
                if attempt == self.MAX_RETRIES:
                    raise PreflightProcessingError(last_error, status_code=502)
                continue

            if response.status_code >= 400:
                raise PreflightProcessingError(
                    f"Onto API error {response.status_code}: {response.text}",
                    status_code=response.status_code,
                )

            if response.status_code == 204:
                return {}

            try:
                return response.json()
            except ValueError as exc:
                raise PreflightProcessingError(
                    f"failed to decode Onto response: {exc}", status_code=502
                ) from exc

        raise PreflightProcessingError(
            last_error or "failed to reach Onto API", status_code=502
        )

    @staticmethod
    def _normalize_filter_value(value: Any) -> Any:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        return value

    def _build_entity_url(self, entity_id: Optional[str]) -> Optional[str]:
        if not entity_id:
            return None
        return f"https://app.ontonet.ru/ru/context/{self.realm_id}/entity/{entity_id}"

    def _post_find(self, payload: Dict[str, Any]) -> Any:
        return self._request(
            "POST", f"/realm/{self.realm_id}/entity/find/v2", payload, params=None
        )

