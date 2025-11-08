"""
Persistent token storage for Onto MCP Server.
Supports both local file-based storage (stdio mode) and remote session-state storage (HTTP mode).
"""
from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional, Callable

from .settings import IS_HTTP_TRANSPORT
from .session_state_client import (
    SessionStateError,
    get_session_state,
    is_session_state_configured,
    merge_session_state,
)
from .utils import safe_print

try:  # FastMCP sets the current context per request; fallback gracefully if unavailable.
    from fastmcp.server.context import _current_context  # type: ignore
except Exception:  # pragma: no cover - defensive fallback
    _current_context = None  # type: ignore


class FileTokenStorage:
    """Handle persistent storage of authentication tokens on the local filesystem."""

    supports_legacy_token = True

    def __init__(self, storage_dir: Optional[str] = None):
        if storage_dir:
            self.storage_dir = Path(storage_dir)
        else:
            self.storage_dir = Path.home() / ".onto_mcp"

        self.storage_dir.mkdir(exist_ok=True)
        self.token_file = self.storage_dir / "tokens.json"

        self._obfuscation_key = "onto_mcp_2025"
        self._session_data: Dict[str, Any] = {}
        self._load_tokens()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _obfuscate(self, data: str) -> str:
        if not data:
            return ""
        try:
            key_bytes = self._obfuscation_key.encode("utf-8")
            data_bytes = data.encode("utf-8")
            obfuscated = bytearray()
            for i, byte in enumerate(data_bytes):
                obfuscated.append(byte ^ key_bytes[i % len(key_bytes)])
            return base64.b64encode(obfuscated).decode("utf-8")
        except Exception:  # pragma: no cover - fallback
            return data

    def _deobfuscate(self, obfuscated_data: str) -> str:
        if not obfuscated_data:
            return ""
        try:
            key_bytes = self._obfuscation_key.encode("utf-8")
            data_bytes = base64.b64decode(obfuscated_data.encode("utf-8"))
            deobfuscated = bytearray()
            for i, byte in enumerate(data_bytes):
                byte ^= key_bytes[i % len(key_bytes)]
                deobfuscated.append(byte)
            return deobfuscated.decode("utf-8")
        except Exception:  # pragma: no cover - fallback
            return obfuscated_data

    def _load_tokens(self) -> None:
        try:
            if self.token_file.exists():
                with open(self.token_file, "r", encoding="utf-8") as handler:
                    stored_data = json.load(handler)

                if stored_data.get("access_token"):
                    stored_data["access_token"] = self._deobfuscate(stored_data["access_token"])
                if stored_data.get("refresh_token"):
                    stored_data["refresh_token"] = self._deobfuscate(stored_data["refresh_token"])

                self._session_data = stored_data
                safe_print(f"[token-storage] loaded tokens from {self.token_file}")
            else:
                self._session_data = {}
        except Exception as exc:  # pragma: no cover - defensive path
            safe_print(f"[token-storage] failed to load tokens: {exc}")
            self._session_data = {}

    def _save_tokens(self) -> None:
        try:
            storage_data = self._session_data.copy()
            if storage_data.get("access_token"):
                storage_data["access_token"] = self._obfuscate(storage_data["access_token"])
            if storage_data.get("refresh_token"):
                storage_data["refresh_token"] = self._obfuscate(storage_data["refresh_token"])
            with open(self.token_file, "w", encoding="utf-8") as handler:
                json.dump(storage_data, handler, indent=2)
            safe_print(f"[token-storage] saved tokens to {self.token_file}")
        except Exception as exc:  # pragma: no cover - defensive path
            safe_print(f"[token-storage] failed to save tokens: {exc}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def store_tokens(self, token_data: Dict[str, Any]) -> None:
        now = time.time()
        if "access_token" in token_data:
            self._session_data["access_token"] = token_data["access_token"]
        if "refresh_token" in token_data:
            self._session_data["refresh_token"] = token_data["refresh_token"]
        if "expires_in" in token_data:
            self._session_data["access_token_expires_at"] = now + token_data["expires_in"]
        if "refresh_expires_in" in token_data:
            self._session_data["refresh_token_expires_at"] = now + token_data["refresh_expires_in"]
        self._session_data["last_updated"] = now
        self._session_data["token_type"] = token_data.get("token_type", "Bearer")
        self._save_tokens()

    def get_access_token(self) -> Optional[str]:
        return self._session_data.get("access_token")

    def get_refresh_token(self) -> Optional[str]:
        return self._session_data.get("refresh_token")

    def is_access_token_expired(self, buffer_seconds: int = 30) -> bool:
        expires_at = self._session_data.get("access_token_expires_at")
        if not expires_at:
            return True
        return time.time() + buffer_seconds >= expires_at

    def is_refresh_token_expired(self, buffer_seconds: int = 30) -> bool:
        expires_at = self._session_data.get("refresh_token_expires_at")
        if not expires_at:
            return True
        return time.time() + buffer_seconds >= expires_at

    def get_token_info(self) -> Dict[str, Any]:
        info: Dict[str, Any] = {
            "has_access_token": bool(self._session_data.get("access_token")),
            "has_refresh_token": bool(self._session_data.get("refresh_token")),
            "access_token_expired": self.is_access_token_expired()
            if self._session_data.get("access_token")
            else True,
            "refresh_token_expired": self.is_refresh_token_expired()
            if self._session_data.get("refresh_token")
            else True,
            "last_updated": self._session_data.get("last_updated"),
        }
        if self._session_data.get("access_token"):
            info["access_token_length"] = len(self._session_data["access_token"])
        if self._session_data.get("refresh_token"):
            info["refresh_token_length"] = len(self._session_data["refresh_token"])
        return info

    def clear_tokens(self) -> None:
        self._session_data.clear()
        try:
            if self.token_file.exists():
                self.token_file.unlink()
                safe_print(f"[token-storage] removed token file {self.token_file}")
        except Exception as exc:  # pragma: no cover - defensive path
            safe_print(f"[token-storage] failed to remove token file: {exc}")

    def has_valid_session(self) -> bool:
        if not self.get_access_token():
            return False
        if not self.is_access_token_expired():
            return True
        refresh = self.get_refresh_token()
        return bool(refresh and not self.is_refresh_token_expired())

    def get_session_status(self) -> str:
        if not self.get_access_token():
            return "No authentication tokens"
        if not self.is_access_token_expired():
            return "Authenticated (access token valid)"
        if self.get_refresh_token() and not self.is_refresh_token_expired():
            return "Authenticated (access token expired, refresh available)"
        return "Authentication expired (re-authentication required)"


class SessionStateTokenStorage:
    """Token storage backed by the session-state service (per MCP session)."""

    supports_legacy_token = False
    token_file = "session-state://tokens"

    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _current_session_id(self, required: bool = True) -> Optional[str]:
        if _current_context is not None:  # type: ignore[truthy-bool]
            try:
                ctx = _current_context.get(None)
            except LookupError:  # pragma: no cover - defensive
                ctx = None
            if ctx is not None:
                session_id = getattr(ctx, "session_id", None)
                if session_id:
                    return session_id
        if required:
            raise SessionStateError("Session-state token storage requires an active MCP session context.")
        return None

    def _load_tokens(self, session_id: str, force: bool = False) -> Dict[str, Any]:
        if not force and session_id in self._cache:
            return self._cache[session_id]
        payload, _meta = get_session_state(session_id)
        tokens = payload.get("tokens") if isinstance(payload, dict) else {}
        if not isinstance(tokens, dict):
            tokens = {}
        self._cache[session_id] = tokens
        return tokens

    def _update_cache(self, session_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        tokens = payload.get("tokens") if isinstance(payload, dict) else {}
        if not isinstance(tokens, dict):
            tokens = {}
        self._cache[session_id] = tokens
        return tokens

    def _touch_tokens(self, session_id: str, mutator: Callable[[Dict[str, Any]], Dict[str, Any]]) -> Dict[str, Any]:
        def updater(existing: Dict[str, Any]) -> Dict[str, Any]:
            payload = dict(existing)
            tokens = payload.get("tokens")
            if not isinstance(tokens, dict):
                tokens = {}
            tokens = mutator(tokens or {})
            payload["tokens"] = tokens
            return payload

        result = merge_session_state(session_id, updater)
        return self._update_cache(session_id, result.get("payload", {}))

    def _has_tokens(self, tokens: Dict[str, Any]) -> bool:
        return bool(tokens.get("access_token") or tokens.get("refresh_token"))

    # ------------------------------------------------------------------
    # Public API mirroring FileTokenStorage
    # ------------------------------------------------------------------

    def store_tokens(self, token_data: Dict[str, Any]) -> None:
        session_id = self._current_session_id()
        now = time.time()

        def mutate(tokens: Dict[str, Any]) -> Dict[str, Any]:
            updated = dict(tokens)
            if "access_token" in token_data:
                updated["access_token"] = token_data["access_token"]
            if "refresh_token" in token_data:
                updated["refresh_token"] = token_data["refresh_token"]
            if "expires_in" in token_data:
                updated["access_token_expires_at"] = now + token_data["expires_in"]
            if "refresh_expires_in" in token_data:
                updated["refresh_token_expires_at"] = now + token_data["refresh_expires_in"]
            updated["last_updated"] = now
            updated["token_type"] = token_data.get("token_type", "Bearer")
            return updated

        try:
            self._touch_tokens(session_id, mutate)
            safe_print(f"[session-state] stored tokens for session {session_id}")
        except SessionStateError as exc:
            raise RuntimeError(str(exc)) from exc

    def get_access_token(self) -> Optional[str]:
        session_id = self._current_session_id(required=False)
        if not session_id:
            return None
        tokens = self._load_tokens(session_id)
        return tokens.get("access_token")

    def get_refresh_token(self) -> Optional[str]:
        session_id = self._current_session_id(required=False)
        if not session_id:
            return None
        tokens = self._load_tokens(session_id)
        return tokens.get("refresh_token")

    def is_access_token_expired(self, buffer_seconds: int = 30) -> bool:
        session_id = self._current_session_id(required=False)
        if not session_id:
            return True
        tokens = self._load_tokens(session_id)
        expires_at = tokens.get("access_token_expires_at")
        if not expires_at:
            return True
        return time.time() + buffer_seconds >= expires_at

    def is_refresh_token_expired(self, buffer_seconds: int = 30) -> bool:
        session_id = self._current_session_id(required=False)
        if not session_id:
            return True
        tokens = self._load_tokens(session_id)
        expires_at = tokens.get("refresh_token_expires_at")
        if not expires_at:
            return True
        return time.time() + buffer_seconds >= expires_at

    def get_token_info(self) -> Dict[str, Any]:
        session_id = self._current_session_id(required=False)
        tokens: Dict[str, Any] = {}
        if session_id:
            try:
                tokens = self._load_tokens(session_id)
            except SessionStateError as exc:
                safe_print(f"[session-state] failed to load tokens for session {session_id}: {exc}")
                tokens = {}
        info: Dict[str, Any] = {
            "has_access_token": bool(tokens.get("access_token")),
            "has_refresh_token": bool(tokens.get("refresh_token")),
            "access_token_expired": True,
            "refresh_token_expired": True,
            "last_updated": tokens.get("last_updated"),
        }
        if tokens.get("access_token"):
            info["access_token_expired"] = self.is_access_token_expired()
            info["access_token_length"] = len(tokens["access_token"])
        if tokens.get("refresh_token"):
            info["refresh_token_expired"] = self.is_refresh_token_expired()
            info["refresh_token_length"] = len(tokens["refresh_token"])
        return info

    def clear_tokens(self) -> None:
        session_id = self._current_session_id(required=False)
        if not session_id:
            return

        def mutate(_tokens: Dict[str, Any]) -> Dict[str, Any]:
            return {}

        try:
            self._touch_tokens(session_id, mutate)
            safe_print(f"[session-state] cleared tokens for session {session_id}")
        except SessionStateError as exc:
            raise RuntimeError(str(exc)) from exc
        finally:
            self._cache.pop(session_id, None)

    def has_valid_session(self) -> bool:
        session_id = self._current_session_id(required=False)
        if not session_id:
            return False
        tokens = self._load_tokens(session_id)
        if not tokens.get("access_token"):
            return False
        if not self.is_access_token_expired():
            return True
        refresh = tokens.get("refresh_token")
        return bool(refresh and not self.is_refresh_token_expired())

    def get_session_status(self) -> str:
        session_id = self._current_session_id(required=False)
        if not session_id:
            return "No active session context"
        try:
            tokens = self._load_tokens(session_id)
        except SessionStateError as exc:
            safe_print(f"[session-state] status fetch failed for {session_id}: {exc}")
            tokens = {}
        if not tokens.get("access_token"):
            return "No authentication tokens"
        if not self.is_access_token_expired():
            return "Authenticated (access token valid)"
        if tokens.get("refresh_token") and not self.is_refresh_token_expired():
            return "Authenticated (access token expired, refresh available)"
        return "Authentication expired (re-authentication required)"


_token_storage: Optional[Any] = None


def get_token_storage():
    """Return the active token storage implementation."""
    global _token_storage
    if _token_storage is not None:
        return _token_storage

    if IS_HTTP_TRANSPORT:
        if not is_session_state_configured():
            raise EnvironmentError(
                "SESSION_STATE_API_KEY must be configured when running in HTTP transport mode."
            )
        _token_storage = SessionStateTokenStorage()
    else:
        _token_storage = FileTokenStorage()
    return _token_storage
