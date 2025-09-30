"""Simple persistent cache for dataset signature storage assignments."""
from __future__ import annotations

import json
from pathlib import Path
from threading import RLock
from typing import Any, Dict, Optional

from .settings import ONTO_PREFLIGHT_STORE_PATH
from .utils import safe_print

__all__ = ["get_storage", "set_storage", "clear_storage", "clear_all", "list_storage"]

_LOCK = RLock()
_CACHE: Dict[str, Dict[str, Any]] = {}
_PATH: Optional[Path] = None
_LOADED = False


if ONTO_PREFLIGHT_STORE_PATH:
    try:
        path = Path(ONTO_PREFLIGHT_STORE_PATH)
        if path.is_dir():
            path = path / "storage-cache.json"
        _PATH = path
    except Exception as exc:  # pragma: no cover - configuration issue
        safe_print(f"[storage_cache] failed to initialise path '{ONTO_PREFLIGHT_STORE_PATH}': {exc}")
        _PATH = None


def _load() -> None:
    global _LOADED
    if _LOADED:
        return
    _LOADED = True
    if _PATH is None or not _PATH.exists():
        return
    try:
        with _PATH.open("r", encoding="utf-8") as handler:
            data = json.load(handler)
    except Exception as exc:  # pragma: no cover - corrupted file or IO error
        safe_print(f"[storage_cache] failed to load cache from {_PATH}: {exc}")
        return
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(key, str) and isinstance(value, dict):
                _CACHE[key] = value


def _persist() -> None:
    if _PATH is None:
        return
    try:
        _PATH.parent.mkdir(parents=True, exist_ok=True)
        with _PATH.open("w", encoding="utf-8") as handler:
            json.dump(_CACHE, handler, ensure_ascii=False, indent=2)
    except Exception as exc:  # pragma: no cover - IO error should not crash client
        safe_print(f"[storage_cache] failed to persist cache to {_PATH}: {exc}")


def get_storage(signature_id: str) -> Optional[Dict[str, Any]]:
    """Return cached storage assignment for a dataset signature."""
    _load()
    with _LOCK:
        entry = _CACHE.get(signature_id)
        if entry is None:
            return None
        return dict(entry)


def set_storage(signature_id: str, data: Dict[str, Any]) -> None:
    """Store storage assignment for later reuse."""
    if not isinstance(signature_id, str) or not signature_id:
        raise ValueError("signature_id must be a non-empty string")
    if not isinstance(data, dict):
        raise ValueError("data must be a dictionary")
    _load()
    with _LOCK:
        _CACHE[signature_id] = dict(data)
        _persist()


def clear_storage(signature_id: str) -> None:
    """Remove cached storage assignment for the given signature."""
    _load()
    with _LOCK:
        if signature_id in _CACHE:
            del _CACHE[signature_id]
            _persist()


def clear_all() -> None:
    """Drop all cached storage assignments."""
    _load()
    with _LOCK:
        _CACHE.clear()
        _persist()


def list_storage() -> Dict[str, Dict[str, Any]]:
    """Return a shallow copy of the cache for inspection/testing."""
    _load()
    with _LOCK:
        return {key: dict(value) for key, value in _CACHE.items()}
