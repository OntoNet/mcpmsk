"""Tests for upload_url and upload_complete tools."""

from __future__ import annotations

from typing import Any, Dict

import os
import pathlib
import sys

import pytest
from fastmcp.exceptions import ValidationError

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("KEYCLOAK_BASE_URL", "https://example.com")
os.environ.setdefault("KEYCLOAK_REALM", "example")
os.environ.setdefault("KEYCLOAK_CLIENT_ID", "client")
os.environ.setdefault("ONTO_API_BASE", "https://api.example.com")
os.environ.setdefault("ONTO_API_TOKEN", "token")
os.environ.setdefault("ONTO_REALM_ID", "realm-123")

from onto_mcp import resources

upload_url = resources.upload_url.fn
upload_complete = resources.upload_complete.fn


class DummyUploadService:
    def __init__(self) -> None:
        self.captured_payload: Dict[str, Any] | None = None
        self.config = type("Cfg", (), {"realm_id": "realm-123"})()
        self._response: Dict[str, Any] = {}
        self._request_id = "req-test"

    def set_response(self, response: Dict[str, Any], request_id: str = "req-test") -> None:
        self._response = response
        self._request_id = request_id

    def request_upload_url(self, payload: Dict[str, Any]):
        self.captured_payload = payload
        return self._response, self._request_id

    def complete_upload(self, payload: Dict[str, Any]):
        self.captured_payload = payload
        return self._response, self._request_id


@pytest.fixture(autouse=True)
def ensure_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(resources, "UploadService", lambda: DummyUploadService())


def test_upload_url_requires_raw_prefix() -> None:
    with pytest.raises(ValidationError) as exc:
        upload_url(
            s3Key="dataset/source.csv",
            fileName="file.csv",
            fileSize=10,
            contentType="text/csv",
        )

    assert "s3Key" in str(exc.value)


def test_upload_url_auto_single_invokes_service(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy = DummyUploadService()
    dummy.set_response(
        {
            "mode": "single",
            "s3Key": "raw/ds/2024/01/source-1.csv",
            "putUrl": "https://minio/single",
            "expiresInSec": 3600,
            "headers": {"Content-Type": "text/csv"},
        },
        request_id="req-123",
    )
    monkeypatch.setattr(resources, "UploadService", lambda: dummy)

    result = upload_url(
        s3Key="raw/ds/2024/01/source-1.csv",
        fileName="source-1.csv",
        fileSize=1024,
        contentType="text/csv",
    )

    assert result["mode"] == "single"
    assert dummy.captured_payload is not None
    assert dummy.captured_payload["mode"] == "single"
    assert dummy.captured_payload["strategy"] == "auto"


def test_upload_url_auto_switches_to_multipart(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy = DummyUploadService()
    dummy.set_response(
        {
            "mode": "multipart",
            "s3Key": "raw/ds/2024/01/source-2.csv",
            "uploadId": "u-123",
            "partSize": 64 * 1024 * 1024,
            "parts": [
                {"partNumber": 1, "putUrl": "https://minio/part1"},
                {"partNumber": 2, "putUrl": "https://minio/part2"},
            ],
            "completeUrl": "https://minio/complete",
            "expiresInSec": 86400,
        },
        request_id="req-456",
    )
    monkeypatch.setattr(resources, "UploadService", lambda: dummy)

    result = upload_url(
        s3Key="raw/ds/2024/01/source-2.csv",
        fileName="source-2.csv",
        fileSize=6 * 1024 ** 3,
        contentType="text/csv",
    )

    assert result["mode"] == "multipart"
    assert dummy.captured_payload is not None
    assert dummy.captured_payload["mode"] == "multipart"


def test_upload_url_single_strategy_rejects_large_file() -> None:
    with pytest.raises(ValidationError) as exc:
        upload_url(
            s3Key="raw/ds/2024/01/source-3.csv",
            fileName="source-3.csv",
            fileSize=6 * 1024 ** 3,
            contentType="text/csv",
            strategy="single",
        )

    assert "413" in str(exc.value)


def test_upload_complete_requires_etag_or_parts() -> None:
    with pytest.raises(ValidationError):
        upload_complete(
            s3Key="raw/ds/2024/01/source-1.csv",
        )


def test_upload_complete_with_parts(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy = DummyUploadService()
    dummy.set_response({"ok": True, "size": 1234}, request_id="req-789")
    monkeypatch.setattr(resources, "UploadService", lambda: dummy)

    result = upload_complete(
        s3Key="raw/ds/2024/01/source-1.csv",
        parts=[
            {"partNumber": 2, "eTag": "etag-2"},
            {"partNumber": 1, "eTag": "etag-1"},
        ],
    )

    assert result == {"ok": True, "size": 1234}
    assert dummy.captured_payload is not None
    assert dummy.captured_payload["parts"][0]["partNumber"] == 1
    assert dummy.captured_payload["parts"][1]["partNumber"] == 2

