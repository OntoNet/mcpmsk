"""Utilities for interacting with Onto storage upload API."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests

from .settings import ONTO_API_BASE, ONTO_API_TOKEN, ONTO_REALM_ID
from .utils import safe_print


class UploadServiceError(RuntimeError):
    """Raised when the storage upload API returns an error."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code or 500


@dataclass
class UploadServiceConfig:
    api_base: str
    api_token: str
    realm_id: str


class UploadService:
    """Small helper around the Onto storage upload API."""

    MAX_RETRIES = 3
    TIMEOUT_SECONDS = 30

    def __init__(self, config: UploadServiceConfig | None = None) -> None:
        if config is None:
            api_base = (ONTO_API_BASE or "").rstrip("/")
            api_token = (ONTO_API_TOKEN or "").strip()
            realm_id = (ONTO_REALM_ID or "").strip()
            config = UploadServiceConfig(api_base=api_base, api_token=api_token, realm_id=realm_id)

        if not config.api_base or not config.api_token or not config.realm_id:
            raise UploadServiceError(
                "Storage upload API is not configured. "
                "Please provide ONTO_API_BASE, ONTO_API_TOKEN and ONTO_REALM_ID.",
                status_code=500,
            )

        self.config = config
        self.session = requests.Session()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def request_upload_url(self, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """Request presigned upload URL(s) from the Onto storage API."""

        path = f"/realm/{self.config.realm_id}/storage/upload-url"
        return self._request("POST", path, payload)

    def complete_upload(self, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        """Notify Onto that the upload has been completed."""

        path = f"/realm/{self.config.realm_id}/storage/upload-complete"
        return self._request("POST", path, payload)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]]) -> Tuple[Dict[str, Any], str]:
        url = f"{self.config.api_base}{path}"
        request_id = uuid.uuid4().hex
        headers = {
            "X-API-Key": self.config.api_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Request-Id": request_id,
        }

        last_error: Optional[str] = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = self.session.request(
                    method,
                    url,
                    json=payload if method.upper() != "GET" else None,
                    timeout=self.TIMEOUT_SECONDS,
                    headers=headers,
                )
            except requests.RequestException as exc:
                last_error = str(exc)
                safe_print(
                    f"[upload_service] network error on {method.upper()} {path} (attempt {attempt}/{self.MAX_RETRIES}): {exc}"
                )
                if attempt == self.MAX_RETRIES:
                    raise UploadServiceError(f"failed to reach storage API: {exc}", status_code=502) from exc
                continue

            if response.status_code >= 500:
                last_error = f"storage API error {response.status_code}: {response.text.strip()}"
                safe_print(
                    f"[upload_service] server error {response.status_code} on {method.upper()} {path} (attempt {attempt}/{self.MAX_RETRIES})"
                )
                if attempt == self.MAX_RETRIES:
                    raise UploadServiceError(last_error, status_code=502)
                continue

            if response.status_code >= 400:
                raise UploadServiceError(
                    f"storage API error {response.status_code}: {response.text.strip()}",
                    status_code=response.status_code,
                )

            if response.status_code == 204:
                return {}, request_id

            try:
                data = response.json()
            except ValueError as exc:
                raise UploadServiceError(
                    f"storage API returned invalid JSON: {exc}", status_code=502
                ) from exc

            if not isinstance(data, dict):
                raise UploadServiceError(
                    "storage API returned unexpected payload (expected object)",
                    status_code=502,
                )

            return data, request_id

        raise UploadServiceError(last_error or "failed to reach storage API", status_code=502)


__all__ = ["UploadService", "UploadServiceError", "UploadServiceConfig"]

