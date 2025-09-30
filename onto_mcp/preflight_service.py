"""Preflight service implementing Onto search and entity creation workflow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Protocol
import uuid

import requests

from .settings import (
    ENABLE_CREATE,
    FIELD_MAP_PATH,
    ONTO_API_BASE,
    ONTO_API_TOKEN,
    ONTO_META_DATASETCLASS_NAME,
    ONTO_META_RECOG_NAME,
    ONTO_META_SIGNATURE_NAME,
    ONTO_META_COLUMNSIGN_NAME,
    ONTO_META_PIPELINE_NAME,
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
        }
        self.debug_http = ONTO_DEBUG_HTTP
        self._request_id: Optional[str] = None

        if not self.api_base or not self.realm_id or not self.api_token:
            raise PreflightProcessingError(
                "ONTO_API_BASE, ONTO_REALM_ID and ONTO_API_TOKEN must be configured",
                status_code=500,
            )

        self._meta: Optional[MetaConfig] = meta_config
        self.store: PreflightStore = store or OntoStore(self)

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

    def process(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        signature = SignaturePayload.from_payload(payload)
        request_id = uuid.uuid4().hex
        self.store.set_request_id(request_id)

        safe_print(
            "[preflight_submit] searching Onto for signature "
            f"{signature.header_hash} ({signature.file_name}), request {request_id}"
        )

        headers_in = signature.header_set()

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


    def _create_dataset_class(self, signature: SignaturePayload) -> str:
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

