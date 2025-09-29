from __future__ import annotations

"""Preflight service implementing signature matching logic.

This module provides an in-memory implementation of the matching and
draft-creation workflow described in the specification.  It does not talk to
the real Onto API yet, but exposes a clean interface so that the transport can
be swapped later with minimal effort.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import threading
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
import uuid

from .settings import ONTO_PREFLIGHT_STORE_PATH, ONTO_REALM_ID
from .utils import safe_print


class PreflightPayloadError(ValueError):
    """Raised when the incoming payload is missing required fields."""


class PreflightProcessingError(RuntimeError):
    """Raised when an internal processing error occurs."""

    def __init__(self, message: str, status_code: int = 500) -> None:
        super().__init__(message)
        self.status_code = status_code


def _slugify(value: str) -> str:
    normalized = value.lower()
    normalized = re.sub(r"[^a-z0-9_]+", "-", normalized)
    normalized = re.sub(r"-+", "-", normalized).strip("-_")
    return normalized or "dataset"


def _infer_keywords(headers: Iterable[str]) -> Set[str]:
    keywords: Set[str] = set()
    for header in headers:
        for token in re.split(r"[_\s]+", header):
            token = token.strip()
            if len(token) < 3:
                continue
            if token.isdigit():
                continue
            keywords.add(token)
    return keywords


def _detect_pii_flags(headers: Iterable[str]) -> Dict[str, bool]:
    flags = {"piiPhone": False, "piiFio": False, "piiInn": False, "piiBirthday": False}
    for header in headers:
        h = header.lower()
        if any(token in h for token in ("phone", "tel", "mobile")):
            flags["piiPhone"] = True
        if any(token in h for token in ("fio", "fullname", "surname", "lastname", "firstname", "name")):
            flags["piiFio"] = True
        if "inn" in h:
            flags["piiInn"] = True
        if any(token in h for token in ("birth", "dob", "birthday", "birthdate")):
            flags["piiBirthday"] = True
    return flags


def _guess_dtype(header: str) -> str:
    h = header.lower()
    if any(token in h for token in ("date", "time", "dt")):
        return "datetime"
    if any(token in h for token in ("count", "num", "qty", "amount", "total", "sum")):
        if "amount" in h or "sum" in h:
            return "float"
        return "int"
    if h.startswith("is_") or h.endswith("_flag") or h.startswith("has_"):
        return "bool"
    if h.endswith("_id") and "uuid" not in h:
        return "int"
    return "text"


@dataclass
class PipelineTemplateRecord:
    id: str
    name: str
    defaults: Dict[str, Any]
    target: Dict[str, Any]
    draft: bool = True


@dataclass
class DatasetClassRecord:
    id: str
    name: str
    header_hash: str
    header_sorted_hash: str
    headers: List[str]
    headers_sorted: str
    num_cols: int
    keywords: Set[str] = field(default_factory=set)
    pii: Dict[str, bool] = field(default_factory=dict)
    priority: int = 0
    draft: bool = True
    pipeline_template_id: Optional[str] = None

    def header_set(self) -> Set[str]:
        return set(self.headers)


@dataclass
class ColumnSignatureRecord:
    id: str
    class_id: str
    name: str
    dtype_guess: str
    examples: List[str] = field(default_factory=list)


@dataclass
class DatasetSignatureRecord:
    id: str
    file_name: str
    file_size: int
    header_hash: str
    header_sorted_hash: str
    num_cols: int
    headers_sorted: str
    sep: str
    encoding: str
    class_id: str
    template_id: Optional[str]


@dataclass
class RecognitionResultRecord:
    id: str
    class_id: str
    signature_id: str
    score: float
    matched_by: str
    timestamp: datetime


@dataclass
class CandidateScore:
    dataset_class: DatasetClassRecord
    score: float
    matched_by: str
    components: Dict[str, float]


@dataclass
class SignaturePayload:
    file_name: str
    file_size: int
    encoding: str
    sep: str
    has_header: bool
    num_cols: int
    headers: List[str]
    header_hash: str
    header_sorted_hash: str
    stats: Dict[str, Any]

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
            "encoding",
            "sep",
            "hasHeader",
            "numCols",
            "headers",
            "headerHash",
            "headerSortedHash",
        ]

        for field in required_fields:
            if field not in signature:
                raise PreflightPayloadError(f"signature missing required field '{field}'")

        encoding = signature["encoding"]
        sep = signature["sep"]
        has_header = signature["hasHeader"]
        num_cols = signature["numCols"]
        headers = signature["headers"]
        header_hash = signature["headerHash"]
        header_sorted_hash = signature["headerSortedHash"]
        stats = signature.get("stats", {})

        if not isinstance(encoding, str) or not encoding:
            raise PreflightPayloadError("signature.encoding must be a non-empty string")
        if not isinstance(sep, str) or not sep:
            raise PreflightPayloadError("signature.sep must be a non-empty string")
        if not isinstance(has_header, bool):
            raise PreflightPayloadError("signature.hasHeader must be a boolean")
        if not isinstance(num_cols, int) or num_cols <= 0:
            raise PreflightPayloadError("signature.numCols must be a positive integer")
        if not isinstance(headers, list) or not headers:
            raise PreflightPayloadError("signature.headers must be a non-empty list")
        if len(headers) != num_cols:
            raise PreflightPayloadError("signature.headers length must equal signature.numCols")
        if not isinstance(header_hash, str) or not header_hash.startswith("sha256:"):
            raise PreflightPayloadError("signature.headerHash must start with 'sha256:'")
        if not isinstance(header_sorted_hash, str) or not header_sorted_hash.startswith("sha256:"):
            raise PreflightPayloadError("signature.headerSortedHash must start with 'sha256:'")
        if not isinstance(stats, dict):
            raise PreflightPayloadError("signature.stats must be an object")

        normalized_headers: List[str] = []
        for header in headers:
            if not isinstance(header, str):
                raise PreflightPayloadError("signature.headers must contain only strings")
            normalized_headers.append(header)

        return cls(
            file_name=file_name,
            file_size=int(file_size),
            encoding=encoding,
            sep=sep,
            has_header=has_header,
            num_cols=num_cols,
            headers=normalized_headers,
            header_hash=header_hash,
            header_sorted_hash=header_sorted_hash,
            stats=stats,
        )

    def duplicate_headers(self) -> List[str]:
        from collections import Counter

        counter = Counter(self.headers)
        return [header for header, count in counter.items() if count > 1]


class _MemoryStore:
    """In-memory (optionally persisted) store that mimics Onto entities."""

    _lock = threading.Lock()
    _dataset_classes: Dict[str, DatasetClassRecord] = {}
    _pipeline_templates: Dict[str, PipelineTemplateRecord] = {}
    _column_signatures: Dict[str, ColumnSignatureRecord] = {}
    _dataset_signatures: Dict[str, DatasetSignatureRecord] = {}
    _recognition_results: Dict[str, RecognitionResultRecord] = {}
    _initialised = False

    @classmethod
    def _storage_path(cls) -> Optional[Path]:
        if not ONTO_PREFLIGHT_STORE_PATH:
            return None
        return Path(ONTO_PREFLIGHT_STORE_PATH)

    @classmethod
    def _ensure_loaded(cls) -> None:
        if cls._initialised:
            return
        with cls._lock:
            if cls._initialised:
                return
            path = cls._storage_path()
            if path and path.exists():
                try:
                    data = json.loads(path.read_text("utf-8"))
                except Exception:
                    data = {}
                for cls_data in data.get("dataset_classes", []):
                    record = DatasetClassRecord(
                        id=cls_data["id"],
                        name=cls_data.get("name", f"Dataset {cls_data['id'][:8]}") or f"Dataset {cls_data['id'][:8]}",
                        header_hash=cls_data.get("header_hash", ""),
                        header_sorted_hash=cls_data.get("header_sorted_hash", ""),
                        headers=cls_data.get("headers", []),
                        headers_sorted=cls_data.get("headers_sorted", ";".join(sorted(cls_data.get("headers", [])))),
                        num_cols=cls_data.get("num_cols", 0),
                        keywords=set(cls_data.get("keywords", [])),
                        pii=cls_data.get("pii", {}),
                        priority=cls_data.get("priority", 0),
                        draft=cls_data.get("draft", True),
                        pipeline_template_id=cls_data.get("pipeline_template_id"),
                    )
                    cls._dataset_classes[record.id] = record

                for tpl_data in data.get("pipeline_templates", []):
                    record = PipelineTemplateRecord(
                        id=tpl_data["id"],
                        name=tpl_data.get("name", f"Pipeline {tpl_data['id'][:8]}") or f"Pipeline {tpl_data['id'][:8]}",
                        defaults=tpl_data.get("defaults", {}),
                        target=tpl_data.get("target", {}),
                        draft=tpl_data.get("draft", True),
                    )
                    cls._pipeline_templates[record.id] = record

                for sig_data in data.get("dataset_signatures", []):
                    record = DatasetSignatureRecord(
                        id=sig_data["id"],
                        file_name=sig_data.get("file_name", "unknown.csv"),
                        file_size=sig_data.get("file_size", 0),
                        header_hash=sig_data.get("header_hash", ""),
                        header_sorted_hash=sig_data.get("header_sorted_hash", ""),
                        num_cols=sig_data.get("num_cols", 0),
                        headers_sorted=sig_data.get("headers_sorted", ""),
                        sep=sig_data.get("sep", ","),
                        encoding=sig_data.get("encoding", "utf-8"),
                        class_id=sig_data.get("class_id"),
                        template_id=sig_data.get("template_id"),
                    )
                    cls._dataset_signatures[record.id] = record

                for rec_data in data.get("recognition_results", []):
                    timestamp = rec_data.get("timestamp")
                    try:
                        ts = datetime.fromisoformat(timestamp)
                    except Exception:
                        ts = datetime.now(timezone.utc)
                    record = RecognitionResultRecord(
                        id=rec_data["id"],
                        class_id=rec_data.get("class_id", ""),
                        signature_id=rec_data.get("signature_id", ""),
                        score=rec_data.get("score", 0.0),
                        matched_by=rec_data.get("matched_by", "numCols"),
                        timestamp=ts,
                    )
                    cls._recognition_results[record.id] = record

                for col_data in data.get("column_signatures", []):
                    record = ColumnSignatureRecord(
                        id=col_data["id"],
                        class_id=col_data.get("class_id", ""),
                        name=col_data.get("name", ""),
                        dtype_guess=col_data.get("dtype_guess", "text"),
                        examples=col_data.get("examples", []),
                    )
                    cls._column_signatures[record.id] = record

            cls._initialised = True

    @classmethod
    def _persist(cls) -> None:
        path = cls._storage_path()
        if not path:
            return
        data = {
            "dataset_classes": [
                {
                    "id": record.id,
                    "name": record.name,
                    "header_hash": record.header_hash,
                    "header_sorted_hash": record.header_sorted_hash,
                    "headers": record.headers,
                    "headers_sorted": record.headers_sorted,
                    "num_cols": record.num_cols,
                    "keywords": sorted(record.keywords),
                    "pii": record.pii,
                    "priority": record.priority,
                    "draft": record.draft,
                    "pipeline_template_id": record.pipeline_template_id,
                }
                for record in cls._dataset_classes.values()
            ],
            "pipeline_templates": [
                {
                    "id": record.id,
                    "name": record.name,
                    "defaults": record.defaults,
                    "target": record.target,
                    "draft": record.draft,
                }
                for record in cls._pipeline_templates.values()
            ],
            "dataset_signatures": [
                {
                    "id": record.id,
                    "file_name": record.file_name,
                    "file_size": record.file_size,
                    "header_hash": record.header_hash,
                    "header_sorted_hash": record.header_sorted_hash,
                    "num_cols": record.num_cols,
                    "headers_sorted": record.headers_sorted,
                    "sep": record.sep,
                    "encoding": record.encoding,
                    "class_id": record.class_id,
                    "template_id": record.template_id,
                }
                for record in cls._dataset_signatures.values()
            ],
            "recognition_results": [
                {
                    "id": record.id,
                    "class_id": record.class_id,
                    "signature_id": record.signature_id,
                    "score": record.score,
                    "matched_by": record.matched_by,
                    "timestamp": record.timestamp.isoformat(),
                }
                for record in cls._recognition_results.values()
            ],
            "column_signatures": [
                {
                    "id": record.id,
                    "class_id": record.class_id,
                    "name": record.name,
                    "dtype_guess": record.dtype_guess,
                    "examples": record.examples,
                }
                for record in cls._column_signatures.values()
            ],
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        os.replace(tmp_path, path)

    @classmethod
    def list_dataset_classes(cls) -> List[DatasetClassRecord]:
        cls._ensure_loaded()
        with cls._lock:
            return list(cls._dataset_classes.values())

    @classmethod
    def get_pipeline_template(cls, template_id: Optional[str]) -> Optional[PipelineTemplateRecord]:
        if not template_id:
            return None
        cls._ensure_loaded()
        with cls._lock:
            return cls._pipeline_templates.get(template_id)

    @classmethod
    def save_dataset_class(cls, record: DatasetClassRecord) -> None:
        cls._ensure_loaded()
        with cls._lock:
            cls._dataset_classes[record.id] = record
            cls._persist()

    @classmethod
    def save_pipeline_template(cls, record: PipelineTemplateRecord) -> None:
        cls._ensure_loaded()
        with cls._lock:
            cls._pipeline_templates[record.id] = record
            cls._persist()

    @classmethod
    def save_column_signatures(cls, records: List[ColumnSignatureRecord]) -> None:
        cls._ensure_loaded()
        with cls._lock:
            for record in records:
                cls._column_signatures[record.id] = record
            cls._persist()

    @classmethod
    def create_dataset_signature(
        cls,
        payload: SignaturePayload,
        dataset_class_id: str,
        template_id: Optional[str],
    ) -> DatasetSignatureRecord:
        cls._ensure_loaded()
        record = DatasetSignatureRecord(
            id=str(uuid.uuid4()),
            file_name=payload.file_name,
            file_size=payload.file_size,
            header_hash=payload.header_hash,
            header_sorted_hash=payload.header_sorted_hash,
            num_cols=payload.num_cols,
            headers_sorted=";".join(sorted(payload.headers)),
            sep=payload.sep,
            encoding=payload.encoding,
            class_id=dataset_class_id,
            template_id=template_id,
        )
        with cls._lock:
            cls._dataset_signatures[record.id] = record
            cls._persist()
        return record

    @classmethod
    def create_recognition_result(
        cls,
        dataset_class_id: str,
        signature_id: str,
        score: float,
        matched_by: str,
    ) -> RecognitionResultRecord:
        cls._ensure_loaded()
        record = RecognitionResultRecord(
            id=str(uuid.uuid4()),
            class_id=dataset_class_id,
            signature_id=signature_id,
            score=score,
            matched_by=matched_by,
            timestamp=datetime.now(timezone.utc),
        )
        with cls._lock:
            cls._recognition_results[record.id] = record
            cls._persist()
        return record


class PreflightService:
    """Core service that performs matching and draft creation."""

    MATCH_THRESHOLD = 0.7

    def __init__(
        self,
        store: type[_MemoryStore] = _MemoryStore,
        realm_id: Optional[str] = None,
    ) -> None:
        self.store = store
        self.realm_id = realm_id or ONTO_REALM_ID or "local"

    def process(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        signature = SignaturePayload.from_payload(payload)
        duplicates = signature.duplicate_headers()

        safe_print(
            f"[preflight_submit] processing signature for {signature.file_name} "
            f"({signature.header_hash})"
        )

        incoming_keywords = _infer_keywords(signature.headers)
        incoming_pii = _detect_pii_flags(signature.headers)

        candidates = self._collect_candidates(signature, incoming_keywords, incoming_pii)
        best_candidate = max(candidates, key=lambda item: item.score) if candidates else None

        notes: List[str] = []
        if duplicates:
            notes.append(
                "⚠️ Обнаружены дубли имён колонок после нормализации: "
                + ", ".join(sorted(duplicates))
            )

        if best_candidate and best_candidate.score >= self.MATCH_THRESHOLD:
            result = self._handle_match(signature, best_candidate, notes)
        else:
            best_score = best_candidate.score if best_candidate else 0.0
            result = self._handle_no_match(signature, incoming_keywords, incoming_pii, notes, best_score)

        if notes:
            result.setdefault("notes", notes)

        return result

    def _collect_candidates(
        self,
        signature: SignaturePayload,
        incoming_keywords: Set[str],
        incoming_pii: Dict[str, bool],
    ) -> List[CandidateScore]:
        candidates: List[CandidateScore] = []

        for dataset_class in self.store.list_dataset_classes():
            seed_match = None
            if dataset_class.header_hash == signature.header_hash:
                seed_match = "headerHash"
            elif dataset_class.header_sorted_hash == signature.header_sorted_hash:
                seed_match = "headerSortedHash"
            elif dataset_class.num_cols == signature.num_cols:
                seed_match = "numCols"

            if seed_match is None:
                continue

            candidate_score = self._score_candidate(
                dataset_class, signature, incoming_keywords, incoming_pii
            )

            # Ensure matched_by reflects the strongest signal respecting the seed
            if seed_match == "headerHash":
                candidate_score.matched_by = "headerHash"
            elif seed_match == "headerSortedHash" and candidate_score.matched_by != "headerHash":
                candidate_score.matched_by = "headerSortedHash"
            elif candidate_score.matched_by not in {"headerHash", "headerSortedHash", "keywords"}:
                candidate_score.matched_by = seed_match

            candidates.append(candidate_score)

        return candidates

    def _score_candidate(
        self,
        dataset_class: DatasetClassRecord,
        signature: SignaturePayload,
        incoming_keywords: Set[str],
        incoming_pii: Dict[str, bool],
    ) -> CandidateScore:
        score = 0.0
        components: Dict[str, float] = {}
        matched_by = "numCols"

        if dataset_class.header_hash == signature.header_hash:
            components["headerHash"] = 1.0
            score += 1.0
            matched_by = "headerHash"

        if dataset_class.header_sorted_hash == signature.header_sorted_hash:
            components["headerSortedHash"] = 0.8
            score += 0.8
            if matched_by != "headerHash":
                matched_by = "headerSortedHash"

        class_headers = dataset_class.header_set()
        incoming_headers = set(signature.headers)
        union = class_headers | incoming_headers
        if union:
            intersection = class_headers & incoming_headers
            jaccard = len(intersection) / len(union)
            components["headersJaccard"] = jaccard
            score += 0.4 * jaccard
            if matched_by not in {"headerHash", "headerSortedHash"} and jaccard >= 0.5:
                matched_by = "keywords"

        if dataset_class.keywords:
            overlap = dataset_class.keywords & incoming_keywords
            ratio = len(overlap) / len(dataset_class.keywords)
            components["keywordOverlap"] = ratio
            score += 0.2 * ratio
            if matched_by not in {"headerHash", "headerSortedHash"} and ratio >= 0.5:
                matched_by = "keywords"

        pii_fields = ["piiPhone", "piiFio", "piiInn", "piiBirthday"]
        matches = 0
        for field in pii_fields:
            if dataset_class.pii.get(field) == incoming_pii.get(field):
                matches += 1
        pii_ratio = matches / len(pii_fields)
        components["piiMatch"] = pii_ratio
        score += 0.2 * pii_ratio

        priority_component = max(min(dataset_class.priority, 100), 0) / 100 if dataset_class.priority else 0
        components["priority"] = priority_component
        score += 0.05 * priority_component

        score = min(score, 1.0)

        return CandidateScore(
            dataset_class=dataset_class,
            score=score,
            matched_by=matched_by,
            components=components,
        )

    def _handle_match(
        self,
        signature: SignaturePayload,
        candidate: CandidateScore,
        notes: List[str],
    ) -> Dict[str, Any]:
        dataset_class = candidate.dataset_class
        pipeline_template = self.store.get_pipeline_template(dataset_class.pipeline_template_id)
        if not pipeline_template:
            pipeline_template = self._create_pipeline_template(dataset_class, signature)

        signature_record = self.store.create_dataset_signature(
            signature, dataset_class.id, pipeline_template.id if pipeline_template else None
        )
        recognition = self.store.create_recognition_result(
            dataset_class.id, signature_record.id, candidate.score, candidate.matched_by
        )

        response = {
            "match": {
                "classId": dataset_class.id,
                "templateId": pipeline_template.id if pipeline_template else None,
                "confidence": round(candidate.score, 4),
                "draft": dataset_class.draft,
            },
            "recommendation": (pipeline_template.target if pipeline_template else {}),
            "upload": {"s3Key": self._generate_upload_key(signature)},
            "onto": {
                "classUrl": self._build_entity_url(dataset_class.id),
                "signatureUrl": self._build_entity_url(signature_record.id),
            },
        }

        if dataset_class.draft:
            response["next"] = "review_required"

        return response

    def _handle_no_match(
        self,
        signature: SignaturePayload,
        incoming_keywords: Set[str],
        incoming_pii: Dict[str, bool],
        notes: List[str],
        best_score: float,
    ) -> Dict[str, Any]:
        dataset_class = self._create_dataset_class(signature, incoming_keywords, incoming_pii)
        pipeline_template = self.store.get_pipeline_template(dataset_class.pipeline_template_id)

        signature_record = self.store.create_dataset_signature(
            signature, dataset_class.id, pipeline_template.id if pipeline_template else None
        )
        recognition = self.store.create_recognition_result(
            dataset_class.id, signature_record.id, min(best_score, 0.69), "numCols"
        )

        notes.append("Создан черновой класс и шаблон обработки для нового набора данных.")

        response = {
            "match": {
                "classId": dataset_class.id,
                "templateId": pipeline_template.id if pipeline_template else None,
                "confidence": round(min(best_score, 0.69), 4),
                "draft": True,
            },
            "next": "review_required",
            "recommendation": pipeline_template.target if pipeline_template else {},
            "upload": {"s3Key": self._generate_upload_key(signature)},
            "onto": {
                "classUrl": self._build_entity_url(dataset_class.id),
                "signatureUrl": self._build_entity_url(signature_record.id),
            },
        }
        return response

    def _create_dataset_class(
        self,
        signature: SignaturePayload,
        incoming_keywords: Set[str],
        incoming_pii: Dict[str, bool],
    ) -> DatasetClassRecord:
        dataset_class_id = str(uuid.uuid4())
        class_name = f"Draft dataset {dataset_class_id[:8]}"
        headers_sorted = ";".join(sorted(signature.headers))
        dataset_class = DatasetClassRecord(
            id=dataset_class_id,
            name=class_name,
            header_hash=signature.header_hash,
            header_sorted_hash=signature.header_sorted_hash,
            headers=list(signature.headers),
            headers_sorted=headers_sorted,
            num_cols=signature.num_cols,
            keywords=set(incoming_keywords),
            pii=dict(incoming_pii),
            priority=0,
            draft=True,
        )

        pipeline_template = self._create_pipeline_template(dataset_class, signature, draft=True)
        dataset_class.pipeline_template_id = pipeline_template.id if pipeline_template else None

        column_signatures = [
            ColumnSignatureRecord(
                id=str(uuid.uuid4()),
                class_id=dataset_class.id,
                name=header,
                dtype_guess=_guess_dtype(header),
            )
            for header in signature.headers
        ]

        self.store.save_dataset_class(dataset_class)
        self.store.save_column_signatures(column_signatures)

        safe_print(
            f"[preflight_submit] created draft dataset class {dataset_class.id} "
            f"with {len(column_signatures)} column signatures"
        )

        return dataset_class

    def _create_pipeline_template(
        self,
        dataset_class: DatasetClassRecord,
        signature: SignaturePayload,
        draft: Optional[bool] = None,
    ) -> PipelineTemplateRecord:
        template_id = dataset_class.pipeline_template_id or str(uuid.uuid4())
        template_name = f"Pipeline {template_id[:8]}"
        slug = _slugify(Path(signature.file_name).stem)
        defaults = {
            "encoding": signature.encoding,
            "sep": signature.sep,
            "hasHeader": signature.has_header,
            "numCols": signature.num_cols,
        }
        target = {
            "storage": "postgres",
            "schema": "public",
            "table": f"{slug}_fact",
            "partitionBy": "date_trunc('day', ingestion_ts)",
        }

        pipeline_template = PipelineTemplateRecord(
            id=template_id,
            name=template_name,
            defaults=defaults,
            target=target,
            draft=dataset_class.draft if draft is None else draft,
        )

        self.store.save_pipeline_template(pipeline_template)

        if not dataset_class.pipeline_template_id:
            dataset_class.pipeline_template_id = pipeline_template.id
            self.store.save_dataset_class(dataset_class)

        return pipeline_template

    def _generate_upload_key(self, signature: SignaturePayload) -> str:
        now = datetime.now(timezone.utc)
        slug = _slugify(Path(signature.file_name).stem)
        return (
            f"raw/{slug}/{now.year:04d}/{now.month:02d}/source-{uuid.uuid4()}.csv"
        )

    def _build_entity_url(self, entity_id: str) -> str:
        return f"https://app.ontonet.ru/ru/context/{self.realm_id}/entity/{entity_id}"
