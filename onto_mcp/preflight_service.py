"""Preflight service implementing Onto search and entity creation workflow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
import uuid

import requests

from .settings import FIELD_MAP_PATH, ONTO_API_BASE, ONTO_API_TOKEN, ONTO_REALM_ID
from .utils import safe_print


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
class EntityFieldMap:
    name: str
    meta_uuid: str
    fields: Dict[str, str]

    def field(self, field_name: str) -> str:
        try:
            value = self.fields[field_name]
        except KeyError as exc:  # pragma: no cover - configuration issue
            raise PreflightProcessingError(
                f"field map missing '{field_name}' for {self.name}", status_code=500
            ) from exc
        if not isinstance(value, str) or not value:
            raise PreflightProcessingError(
                f"field map value for '{self.name}.{field_name}' must be a non-empty string",
                status_code=500,
            )
        return value

    def get(self, field_name: str) -> Optional[str]:
        value = self.fields.get(field_name)
        if isinstance(value, str) and value:
            return value
        return None

    def require(self, required: Sequence[str]) -> None:
        missing = [field for field in required if field not in self.fields]
        if missing:
            missing_str = ", ".join(missing)
            raise PreflightProcessingError(
                f"field map missing required fields for {self.name}: {missing_str}",
                status_code=500,
            )


@dataclass(frozen=True)
class FieldMap:
    dataset_class: EntityFieldMap
    column_signature: EntityFieldMap
    pipeline_template: EntityFieldMap
    dataset_signature: EntityFieldMap
    recognition_result: EntityFieldMap

    @classmethod
    def load(cls, path: Path) -> "FieldMap":
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

        def _load_entity(name: str, required: Sequence[str]) -> EntityFieldMap:
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
            entity_map = EntityFieldMap(name=name, meta_uuid=meta_uuid, fields=entity_fields)
            entity_map.require(required)
            return entity_map

        dataset_class = _load_entity(
            "DatasetClass",
            ["headerHash", "headerSortedHash", "headersSorted", "numCols", "draft"],
        )
        column_signature = _load_entity("ColumnSignature", ["name", "position"])
        pipeline_template = _load_entity("PipelineTemplate", ["name", "defaults", "draft"])
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
            column_signature=column_signature,
            pipeline_template=pipeline_template,
            dataset_signature=dataset_signature,
            recognition_result=recognition_result,
        )


@dataclass
class SearchOutcome:
    matched: bool
    class_id: Optional[str]
    confidence: float
    matched_by: Optional[str]
    candidates: List[Dict[str, Any]]


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
    ) -> None:
        self.api_base = (api_base or ONTO_API_BASE or "").rstrip("/")
        self.realm_id = realm_id or ONTO_REALM_ID or ""
        self.api_token = api_token or ONTO_API_TOKEN or ""
        self.field_map_path = Path(field_map_path or FIELD_MAP_PATH or "")
        self.session = session or requests.Session()
        self.timeout = timeout
        self._request_id: Optional[str] = None

        if not self.api_base or not self.realm_id or not self.api_token:
            raise PreflightProcessingError(
                "ONTO_API_BASE, ONTO_REALM_ID and ONTO_API_TOKEN must be configured",
                status_code=500,
            )
        if not self.field_map_path:
            raise PreflightProcessingError(
                "FIELD_MAP_PATH must be configured", status_code=500
            )

        self._field_map: Optional[FieldMap] = None

    @property
    def field_map(self) -> FieldMap:
        if self._field_map is None:
            self._field_map = FieldMap.load(self.field_map_path)
        return self._field_map

    def process(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        signature = SignaturePayload.from_payload(payload)
        self._request_id = uuid.uuid4().hex

        safe_print(
            "[preflight_submit] searching Onto for signature "
            f"{signature.header_hash} ({signature.file_name}), request {self._request_id}"
        )

        headers_in = signature.header_set()
        outcome = self._search_for_dataset_class(signature, headers_in)

        created_dataset_class_id: Optional[str] = None
        column_signature_ids: List[str] = []
        pipeline_template_id: Optional[str] = None
        notes: List[str] = ["Связи пока не создаём (этап отключён)"]

        dataset_class_id = outcome.class_id

        if outcome.matched:
            safe_print(
                "[preflight_submit] matched dataset class "
                f"{dataset_class_id} via {outcome.matched_by}"
            )
        else:
            safe_print(
                "[preflight_submit] dataset class not found, creating draft in Onto"
            )
            dataset_class_id, dataset_class_created = self._create_dataset_class(
                signature
            )
            if dataset_class_created:
                created_dataset_class_id = dataset_class_id
                notes.append("Создан черновик класса данных.")
                column_signature_ids = self._create_column_signatures(signature)
                if column_signature_ids:
                    notes.append("Созданы сигнатуры колонок.")
                pipeline_template_id = self._create_pipeline_template(signature)
                if pipeline_template_id:
                    notes.append("Создан шаблон пайплайна по умолчанию.")
                outcome = SearchOutcome(
                    matched=False,
                    class_id=dataset_class_id,
                    confidence=0.0,
                    matched_by=None,
                    candidates=outcome.candidates,
                )
            else:
                outcome = SearchOutcome(
                    matched=True,
                    class_id=dataset_class_id,
                    confidence=0.8,
                    matched_by="headerSortedHash+numCols+headersSorted",
                    candidates=outcome.candidates,
                )
                notes.append(
                    "Найден существующий класс по композитному ключу, создание пропущено."
                )

        dataset_signature_id, dataset_signature_created = self._ensure_dataset_signature(
            signature
        )
        if dataset_signature_created:
            notes.append("Зарегистрирована сигнатура датасета.")
        else:
            notes.append("Сигнатура датасета уже существовала в Онто.")

        recognition_result_id = self._create_recognition_result(outcome)

        response: Dict[str, Any] = {
            "matched": outcome.matched,
            "classId": dataset_class_id,
            "matchInfo": {
                "confidence": round(outcome.confidence, 4),
                "matchedBy": outcome.matched_by,
            },
            "created": {
                "datasetClassId": created_dataset_class_id,
                "columnSignatureIds": column_signature_ids,
                "pipelineTemplateId": pipeline_template_id,
                "datasetSignatureId": dataset_signature_id
                if dataset_signature_created
                else None,
                "recognitionResultId": recognition_result_id,
            },
            "links": {
                "classUrl": self._build_entity_url(dataset_class_id),
                "signatureUrl": self._build_entity_url(dataset_signature_id),
                "templateUrl": self._build_entity_url(pipeline_template_id),
            },
            "notes": notes,
        }

        if outcome.candidates:
            response["candidates"] = outcome.candidates[:5]

        return response

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
                {"fieldUuid": field_uuid, "value": self._normalize_filter_value(value)}
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
                entity, self.field_map.dataset_class.field("headersSorted")
            )

            headers_overlap = 0.0
            if headers_sorted:
                class_headers = self._parse_headers(headers_sorted)
                headers_overlap = self._jaccard(incoming_headers, class_headers)

            score = round(0.6 + 0.4 * headers_overlap, 4)

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

        # Some responses may wrap the actual entity under "entity"
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
        # Step A: exact header hash
        dataset_class_meta = self.field_map.dataset_class.meta_uuid
        hash_field = self.field_map.dataset_class.field("headerHash")
        exact_matches = self._find_entities(
            dataset_class_meta,
            [(hash_field, signature.header_hash)],
            page_size=self.HASH_PAGE_SIZE,
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

        # Step B: sorted header hash
        sorted_field = self.field_map.dataset_class.field("headerSortedHash")
        sorted_matches = self._find_entities(
            dataset_class_meta,
            [(sorted_field, signature.header_sorted_hash)],
            page_size=self.HASH_PAGE_SIZE,
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

        # Step C: candidates by number of columns
        num_cols_field = self.field_map.dataset_class.field("numCols")
        all_candidates = self._find_all_pages(
            dataset_class_meta,
            [(num_cols_field, str(signature.num_cols))],
            page_size=self.NUMCOLS_PAGE_SIZE,
        )
        scored_candidates = self._summarise_candidates(
            all_candidates, headers_in, limit=None
        )

        if scored_candidates:
            best = scored_candidates[0]
            if best["score"] >= 0.7:
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

    def _create_dataset_class(self, signature: SignaturePayload) -> Tuple[str, bool]:
        composite_filters = [
            (
                self.field_map.dataset_class.field("headerSortedHash"),
                signature.header_sorted_hash,
            ),
            (
                self.field_map.dataset_class.field("numCols"),
                str(signature.num_cols),
            ),
            (
                self.field_map.dataset_class.field("headersSorted"),
                signature.headers_sorted_string(),
            ),
        ]

        existing = self._find_entities(
            self.field_map.dataset_class.meta_uuid,
            composite_filters,
            page_size=1,
        )
        if existing:
            entity_id = self._extract_entity_id(existing[0])
            if entity_id:
                safe_print(
                    "[preflight_submit] found existing DatasetClass via composite key"
                )
                return entity_id, False

        fields = {
            self.field_map.dataset_class.field("headerHash"): signature.header_hash,
            self.field_map.dataset_class.field(
                "headerSortedHash"
            ): signature.header_sorted_hash,
            self.field_map.dataset_class.field(
                "headersSorted"
            ): signature.headers_sorted_string(),
            self.field_map.dataset_class.field("numCols"): signature.num_cols,
            self.field_map.dataset_class.field("draft"): True,
        }

        keywords_field = self.field_map.dataset_class.get("keywords")
        if keywords_field:
            fields[keywords_field] = ""

        for pii_field in ("piiPhone", "piiFio", "piiInn", "piiBirthday"):
            uuid_field = self.field_map.dataset_class.get(pii_field)
            if uuid_field:
                fields[uuid_field] = False

        priority_field = self.field_map.dataset_class.get("priority")
        if priority_field:
            fields[priority_field] = 0

        safe_print("[preflight_submit] creating DatasetClass draft in Onto")
        entity_id = self._create_entity(self.field_map.dataset_class.meta_uuid, fields)
        return entity_id, True

    def _create_column_signatures(self, signature: SignaturePayload) -> List[str]:
        ids: List[str] = []
        name_field = self.field_map.column_signature.field("name")
        position_field = self.field_map.column_signature.field("position")
        dtype_field = self.field_map.column_signature.get("dtypeGuess")
        examples_field = self.field_map.column_signature.get("examples")

        for index, header in enumerate(signature.headers):
            fields: Dict[str, Any] = {
                name_field: header,
                position_field: index,
            }
            if dtype_field:
                fields[dtype_field] = "text"
            if examples_field:
                fields[examples_field] = []

            ids.append(
                self._create_entity(self.field_map.column_signature.meta_uuid, fields)
            )

        return ids

    def _create_pipeline_template(self, signature: SignaturePayload) -> Optional[str]:
        defaults_field = self.field_map.pipeline_template.field("defaults")
        fields = {
            self.field_map.pipeline_template.field("name"): "default",
            defaults_field: json.dumps(
                {"sep": signature.separator, "encoding": signature.encoding},
                ensure_ascii=False,
            ),
            self.field_map.pipeline_template.field("draft"): True,
        }

        target_field = self.field_map.pipeline_template.get("target")
        if target_field:
            fields[target_field] = "{}"

        safe_print("[preflight_submit] creating PipelineTemplate draft in Onto")
        return self._create_entity(self.field_map.pipeline_template.meta_uuid, fields)

    def _ensure_dataset_signature(
        self, signature: SignaturePayload
    ) -> Tuple[str, bool]:
        filters = [
            (
                self.field_map.dataset_signature.field("headerHash"),
                signature.header_hash,
            ),
            (
                self.field_map.dataset_signature.field("fileName"),
                signature.file_name,
            ),
            (
                self.field_map.dataset_signature.field("fileSize"),
                str(signature.file_size),
            ),
        ]
        existing = self._find_entities(
            self.field_map.dataset_signature.meta_uuid,
            filters,
            page_size=1,
        )
        if existing:
            entity_id = self._extract_entity_id(existing[0])
            if entity_id:
                return entity_id, False

        fields = {
            self.field_map.dataset_signature.field("fileName"): signature.file_name,
            self.field_map.dataset_signature.field("fileSize"): signature.file_size,
            self.field_map.dataset_signature.field("encoding"): signature.encoding,
            self.field_map.dataset_signature.field("sep"): signature.separator,
            self.field_map.dataset_signature.field("headerHash"): signature.header_hash,
            self.field_map.dataset_signature.field(
                "headerSortedHash"
            ): signature.header_sorted_hash,
            self.field_map.dataset_signature.field(
                "numCols"
            ): signature.num_cols,
            self.field_map.dataset_signature.field(
                "headersSorted"
            ): signature.headers_sorted_string(),
        }

        safe_print("[preflight_submit] creating DatasetSignature in Onto")
        entity_id = self._create_entity(self.field_map.dataset_signature.meta_uuid, fields)
        return entity_id, True

    def _create_recognition_result(self, outcome: SearchOutcome) -> str:
        timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        if timestamp.endswith("+00:00"):
            timestamp = timestamp.replace("+00:00", "Z")

        fields = {
            self.field_map.recognition_result.field("score"): round(
                outcome.confidence, 4
            ),
            self.field_map.recognition_result.field("matchedBy"): outcome.matched_by
            or "not_matched",
            self.field_map.recognition_result.field("timestamp"): timestamp,
        }

        safe_print("[preflight_submit] creating RecognitionResult entry in Onto")
        return self._create_entity(self.field_map.recognition_result.meta_uuid, fields)

    def _create_entity(self, meta_uuid: str, fields: Dict[str, Any]) -> str:
        payload = {"metaEntityUuid": meta_uuid, "fields": fields}
        response = self._request(
            "POST", f"/realm/{self.realm_id}/entity", payload
        )
        entity_id = self._extract_entity_id(response)
        if not entity_id and isinstance(response, dict):
            entity = response.get("entity")
            if isinstance(entity, dict):
                entity_id = self._extract_entity_id(entity)
        if not entity_id:
            raise PreflightProcessingError(
                "Onto API response did not include entity identifier", status_code=502
            )
        return entity_id

    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]]) -> Any:
        url = f"{self.api_base}{path}"
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Request-Id": self._request_id or uuid.uuid4().hex,
        }

        last_error: Optional[str] = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = self.session.request(
                    method,
                    url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                last_error = str(exc)
                if attempt == self.MAX_RETRIES:
                    raise PreflightProcessingError(
                        f"failed to reach Onto API: {exc}", status_code=502
                    ) from exc
                continue

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
            "POST", f"/realm/{self.realm_id}/entity/find/v2", payload
        )

