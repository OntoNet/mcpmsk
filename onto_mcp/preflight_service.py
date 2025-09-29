"""Preflight service implementing lightweight Onto catalogue search."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

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

        return cls(
            file_name=file_name,
            file_size=int(file_size),
            headers=normalized_headers,
            header_hash=header_hash,
            header_sorted_hash=header_sorted_hash,
            num_cols=num_cols,
        )

    def header_set(self) -> Set[str]:
        return set(self.headers)


@dataclass(frozen=True)
class FieldMap:
    meta_dataset_class: str
    header_hash_field: str
    header_sorted_hash_field: str
    num_cols_field: str
    headers_sorted_field: str

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

        meta_uuid = data.get("metaDatasetClass")
        fields = data.get("fields", {})

        required_fields = {
            "headerHash": "header_hash_field",
            "headerSortedHash": "header_sorted_hash_field",
            "numCols": "num_cols_field",
            "headersSorted": "headers_sorted_field",
        }

        missing = [name for name in required_fields if name not in fields]
        if not isinstance(meta_uuid, str) or not meta_uuid:
            raise PreflightProcessingError("field map missing 'metaDatasetClass'", status_code=500)
        if missing:
            missing_str = ", ".join(missing)
            raise PreflightProcessingError(
                f"field map missing required fields: {missing_str}", status_code=500
            )

        return cls(
            meta_dataset_class=meta_uuid,
            header_hash_field=fields["headerHash"],
            header_sorted_hash_field=fields["headerSortedHash"],
            num_cols_field=fields["numCols"],
            headers_sorted_field=fields["headersSorted"],
        )


class PreflightService:
    """Service executing the read-only matching workflow against Onto."""

    HASH_PAGE_SIZE = 20
    NUMCOLS_PAGE_SIZE = 100

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

        safe_print(
            "[preflight_submit] searching Onto for signature "
            f"{signature.header_hash} ({signature.file_name})"
        )

        headers_in = signature.header_set()

        # Step A: exact header hash
        exact_matches = self._find_entities(
            self.field_map.header_hash_field,
            signature.header_hash,
            page_size=self.HASH_PAGE_SIZE,
        )
        if exact_matches:
            candidate_summaries = self._summarise_candidates(
                exact_matches, headers_in
            )
            if candidate_summaries:
                top_candidate = candidate_summaries[0]
                return self._build_success_response(
                    class_id=top_candidate["classId"],
                    confidence=1.0,
                    matched_by="headerHash",
                    candidates=candidate_summaries,
                )

        # Step B: sorted header hash
        sorted_matches = self._find_entities(
            self.field_map.header_sorted_hash_field,
            signature.header_sorted_hash,
            page_size=self.HASH_PAGE_SIZE,
        )
        if sorted_matches:
            candidate_summaries = self._summarise_candidates(
                sorted_matches, headers_in
            )
            if candidate_summaries:
                top_candidate = candidate_summaries[0]
                return self._build_success_response(
                    class_id=top_candidate["classId"],
                    confidence=0.8,
                    matched_by="headerSortedHash",
                    candidates=candidate_summaries,
                )

        # Step C: candidates by number of columns
        all_candidates = self._find_all_pages(
            self.field_map.num_cols_field,
            str(signature.num_cols),
            page_size=self.NUMCOLS_PAGE_SIZE,
        )
        candidate_summaries = self._summarise_candidates(all_candidates, headers_in)

        if candidate_summaries:
            best = candidate_summaries[0]
            if best["score"] >= 0.7:
                return self._build_success_response(
                    class_id=best["classId"],
                    confidence=best["score"],
                    matched_by="numCols+jaccard",
                    candidates=candidate_summaries,
                )

        return {"match": None, "candidates": candidate_summaries[:5]}

    # ------------------------------------------------------------------
    # Onto API helpers
    # ------------------------------------------------------------------

    def _find_entities(
        self,
        field_uuid: str,
        value: str,
        *,
        page_size: int,
        first: int = 0,
    ) -> List[Dict[str, Any]]:
        payload = {
            "metaEntityRequest": {"uuid": self.field_map.meta_dataset_class},
            "metaFieldFilters": [{"fieldUuid": field_uuid, "value": value}],
            "pagination": {"first": first, "offset": page_size},
        }

        response = self._post_find(payload)
        return self._flatten_entities(response)

    def _find_all_pages(
        self,
        field_uuid: str,
        value: str,
        *,
        page_size: int,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        first = 0

        while True:
            page = self._find_entities(field_uuid, value, page_size=page_size, first=first)
            if not page:
                break
            results.extend(page)
            if len(page) < page_size:
                break
            first += page_size

        return results

    def _post_find(self, payload: Dict[str, Any]) -> Any:
        url = f"{self.api_base}/realm/{self.realm_id}/entity/find/v2"
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            response = self.session.post(
                url, json=payload, headers=headers, timeout=self.timeout
            )
        except requests.RequestException as exc:
            raise PreflightProcessingError(
                f"failed to reach Onto API: {exc}", status_code=502
            ) from exc

        if response.status_code >= 500:
            raise PreflightProcessingError(
                f"Onto API error {response.status_code}", status_code=502
            )
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
    ) -> List[Dict[str, Any]]:
        scored: List[Dict[str, Any]] = []

        for entity in entities:
            class_id = self._extract_entity_id(entity)
            if not class_id:
                continue

            headers_sorted = self._extract_field_value(
                entity, self.field_map.headers_sorted_field
            )

            headers_overlap = 0.0
            if headers_sorted:
                class_headers = self._parse_headers(headers_sorted)
                headers_overlap = self._jaccard(incoming_headers, class_headers)

            # Treat numCols match as baseline 0.6 so that good overlaps cross 0.7
            score = round(0.6 + 0.4 * headers_overlap, 4)

            scored.append(
                {
                    "classId": class_id,
                    "score": score,
                    "headersOverlap": round(headers_overlap, 4),
                }
            )

        scored.sort(key=lambda item: item["score"], reverse=True)
        return scored[:5]

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

    # ------------------------------------------------------------------
    # Response helpers
    # ------------------------------------------------------------------

    def _build_success_response(
        self,
        *,
        class_id: str,
        confidence: float,
        matched_by: str,
        candidates: Sequence[Dict[str, Any]],
    ) -> Dict[str, Any]:
        return {
            "match": {
                "classId": class_id,
                "confidence": round(confidence, 4),
                "matchedBy": matched_by,
            },
            "onto": {"classUrl": self._build_class_url(class_id)},
            "candidates": list(candidates),
        }

    def _build_class_url(self, class_id: str) -> str:
        return (
            f"https://app.ontonet.ru/ru/context/{self.realm_id}/entity/{class_id}"
        )

