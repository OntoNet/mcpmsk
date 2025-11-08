"""Utilities for interacting with the session-state service."""
from __future__ import annotations

from typing import Any, Callable, Dict, Tuple

import requests

from .settings import ONTO_API_BASE, SESSION_STATE_API_BASE, SESSION_STATE_API_KEY
from .utils import safe_print

SESSION_STATE_TIMEOUT_SECONDS = 10


class SessionStateError(RuntimeError):
    """Raised when session-state operations fail."""


def is_session_state_configured() -> bool:
    """Return True if the session-state service credentials are configured."""
    return bool(SESSION_STATE_API_KEY)


def _base_url() -> str:
    base = SESSION_STATE_API_BASE or ONTO_API_BASE
    if not base:
        raise SessionStateError("SESSION_STATE_API_BASE or ONTO_API_BASE must be configured for session-state access.")
    return base.rstrip("/")


def _headers() -> Dict[str, str]:
    if not SESSION_STATE_API_KEY:
        raise SessionStateError("SESSION_STATE_API_KEY is not configured for session-state access.")
    return {
        "X-API-Key": SESSION_STATE_API_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def get_session_state(context_id: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Fetch the current session-state payload for the given context ID."""
    url = f"{_base_url()}/session-state/{context_id}"
    try:
        response = requests.get(url, headers=_headers(), timeout=SESSION_STATE_TIMEOUT_SECONDS)
    except Exception as exc:  # pragma: no cover - network layer
        raise SessionStateError(f"Failed to GET session-state for {context_id}: {exc}") from exc

    if response.status_code == 404:
        return {}, {"contextId": context_id, "exists": False, "createdAt": None}

    if response.status_code >= 400:
        snippet = response.text[:200] if response.text else ""
        raise SessionStateError(
            f"Session-state GET {context_id} failed with HTTP {response.status_code}: {snippet}"
        )

    try:
        data = response.json()
    except ValueError:
        data = {}

    payload = data.get("payload") if isinstance(data, dict) else {}
    if not isinstance(payload, dict):
        payload = {}

    meta = {
        "contextId": data.get("contextId", context_id) if isinstance(data, dict) else context_id,
        "createdAt": data.get("createdAt") if isinstance(data, dict) else None,
        "exists": True,
    }
    return payload, meta


def set_session_state(context_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Persist the provided payload for the context ID."""
    url = f"{_base_url()}/session-state/{context_id}"
    try:
        response = requests.post(
            url,
            json={"payload": payload},
            headers=_headers(),
            timeout=SESSION_STATE_TIMEOUT_SECONDS,
        )
    except Exception as exc:  # pragma: no cover - network layer
        raise SessionStateError(f"Failed to POST session-state for {context_id}: {exc}") from exc

    if response.status_code >= 400:
        snippet = response.text[:200] if response.text else ""
        raise SessionStateError(
            f"Session-state POST {context_id} failed with HTTP {response.status_code}: {snippet}"
        )

    try:
        data = response.json()
    except ValueError:
        data = {}

    saved_payload = data.get("payload") if isinstance(data, dict) else {}
    if not isinstance(saved_payload, dict):
        saved_payload = {}

    return {
        "contextId": data.get("contextId", context_id) if isinstance(data, dict) else context_id,
        "createdAt": data.get("createdAt") if isinstance(data, dict) else None,
        "payload": saved_payload,
    }


def merge_session_state(context_id: str, updater: Callable[[Dict[str, Any]], Dict[str, Any]]) -> Dict[str, Any]:
    """Fetch, mutate, and persist the payload for the given context ID."""
    current_payload, _meta = get_session_state(context_id)
    new_payload = updater(dict(current_payload))
    if new_payload is None:
        new_payload = {}
    if not isinstance(new_payload, dict):
        raise SessionStateError("Session-state updater must return a dictionary payload.")
    return set_session_state(context_id, new_payload)
