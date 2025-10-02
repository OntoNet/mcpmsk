from __future__ import annotations

import os
import ntpath
from fastmcp import FastMCP
from fastmcp.exceptions import ValidationError
from fastmcp.server.context import Context
import requests
import uuid
from pathlib import Path
import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from typing import Any, Dict, Optional, Sequence
from . import storage_cache
from .auth import get_token, set_token
from .keycloak_auth import KeycloakAuth
from .session_state_client import (
    SessionStateError,
    get_session_state,
    is_session_state_configured,
    merge_session_state,
)
from .settings import ONTO_API_BASE
from .utils import safe_print
from .preflight_service import (
    PreflightPayloadError,
    PreflightProcessingError,
    PreflightService,
    StorageAssignment,
)

mcp = FastMCP(name="Onto MCP Server")

BUILD_VERSION = os.environ.get("ONTO_BUILD_VERSION") or "0.3.8"

safe_print(f"[startup] Onto MCP build version: {BUILD_VERSION}")

# ONTO_API_BASE now comes from settings (with env/default handling)

# Global Keycloak auth instance
keycloak_auth = KeycloakAuth()
@mcp.tool
def preflight_plan(
    source: str,
    forceSep: str | None = None,
    forceEncoding: str | None = None,
) -> Dict[str, Any]:
    """Return a two-step plan for building and submitting a CSV signature."""

    if not isinstance(source, str):
        raise ValidationError("400: 'source' is required and must be a non-empty string.")

    normalized_source = source.strip()
    if not normalized_source:
        raise ValidationError("400: 'source' is required and must be a non-empty string.")

    if (
        (normalized_source.startswith('"') and normalized_source.endswith('"'))
        or (normalized_source.startswith("'") and normalized_source.endswith("'"))
    ):
        normalized_source = normalized_source[1:-1].strip()

    if not normalized_source:
        raise ValidationError("400: 'source' is required and must be a non-empty string.")

    source_path = normalized_source
    safe_print(f"[preflight_plan] received source: {source_path}")

    normalized_force_sep: str | None = None
    if forceSep is not None:
        if not isinstance(forceSep, str):
            raise ValidationError("400: 'forceSep' must be null, ',' or ';'.")
        normalized_force_sep = forceSep.strip()
        if normalized_force_sep not in (",", ";"):
            raise ValidationError("400: 'forceSep' must be null, ',' or ';'.")

    normalized_force_encoding: str | None = None
    if forceEncoding is not None:
        if not isinstance(forceEncoding, str):
            raise ValidationError("400: 'forceEncoding' must be a string or null.")
        normalized_force_encoding = forceEncoding.strip()
        if not normalized_force_encoding:
            raise ValidationError("400: 'forceEncoding' must be a non-empty string when provided.")

    cmd = """python3 - <<'PY'
import csv, re, json, hashlib, os, sys
SRC = os.environ['SRC']
def norm(s):
  s=s.lower().strip().replace(' ','_').replace('-','_')
  s=re.sub(r'[^a-z0-9_а-яё]','',s); s=re.sub(r'_+','_',s).strip('_'); return s

# авто-детект разделителя (можно переопределить env FORCE_SEP)
with open(SRC,'rb') as f: head=f.read(256*1024)
sep = ';' if head.count(b';')>head.count(b',') else ','
force_sep = os.environ.get('FORCE_SEP')
if force_sep in (',',';'): sep = force_sep

enc = os.environ.get('FORCE_ENCODING') or 'utf-8'

with open(SRC, encoding=enc, newline='') as f:
  rdr = csv.reader(f, delimiter=sep)
  raw = next(rdr)
  headers = [norm(h) for h in raw]

hdr = ';'.join(headers)
payload = {
  "fileName": os.path.basename(SRC),
  "fileSize": os.path.getsize(SRC),
  "signature": {
    "encoding": enc,
    "sep": sep,
    "hasHeader": True,
    "numCols": len(headers),
    "headers": headers,
    "headerHash": "sha256:"+hashlib.sha256(hdr.encode()).hexdigest(),
    "headerSortedHash": "sha256:"+hashlib.sha256(';'.join(sorted(headers)).encode()).hexdigest(),
    "stats": {"rowsScanned": 0}
  }
}
open('payload.json','w',encoding='utf-8').write(json.dumps(payload,ensure_ascii=False,indent=2))
print("OK -> payload.json")
PY"""

    actions = [
        {
            "type": "shell",
            "name": "build-signature",
            "cmd": cmd,
            "env": {
                "SRC": source_path,
                "FORCE_SEP": normalized_force_sep or "",
                "FORCE_ENCODING": normalized_force_encoding or "",
            },
        },
        {
            "type": "mcp_call",
            "name": "send-signature",
            "tool": "preflight_submit",
            "args_from_file": "payload.json",
        },
    ]

    notes = [
        "Команда создаст payload.json в текущей директории клиента",
    ]

    return {
        "buildVersion": BUILD_VERSION,
        "actions": actions,
        "notes": notes,
    }


@mcp.tool
def preflight_submit(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Process CSV signature payload and match it against the Onto catalogue."""

    if not isinstance(payload, dict):
        raise ValidationError("422: 'payload' must be an object with signature data.")

    service = PreflightService()

    try:
        return service.process(payload)
    except PreflightPayloadError as exc:
        raise ValidationError(f"422: {exc}") from exc
    except PreflightProcessingError as exc:
        status = getattr(exc, "status_code", 500)
        raise RuntimeError(f"{status}: {exc}") from exc
    except Exception as exc:  # pragma: no cover - unexpected failures
        raise RuntimeError(f"500: unexpected error during preflight_submit: {exc}") from exc


def _legacy_token_storage_enabled() -> bool:
    return getattr(keycloak_auth.token_storage, "supports_legacy_token", True)


@mcp.tool
def login_with_credentials(username: str, password: str) -> str:
    """
    Authenticate with Keycloak using username and password.
    This method saves tokens persistently for future sessions.
    
    Args:
        username: User's username or email
        password: User's password
    
    Returns:
        Success message or error details
    """
    try:
        if keycloak_auth.authenticate_with_password(username, password):
            # Store token in old system for compatibility
            access_token = keycloak_auth.get_valid_access_token()
            if access_token:
                if _legacy_token_storage_enabled():
                    set_token(access_token)
                user_info = keycloak_auth.get_user_info()
                if user_info:
                    email = user_info.get('email', 'Unknown')
                    return f"✅ Successfully authenticated as {email}. Session saved persistently."
                else:
                    return "✅ Authentication successful. Session saved persistently."
            else:
                return "❌ Authentication succeeded but failed to get access token"
        else:
            return "❌ Authentication failed - invalid credentials"
    except Exception as e:
        return f"❌ Authentication error: {str(e)}"

@mcp.tool
def refresh_token() -> str:
    """
    Refresh the current access token.
    This happens automatically when needed, but you can call it manually.
    
    Returns:
        Success message or error details
    """
    try:
        if keycloak_auth.refresh_access_token():
            # Update stored token
            access_token = keycloak_auth.get_valid_access_token()
            if access_token:
                if _legacy_token_storage_enabled():
                    set_token(access_token)
                return "🔄 Token refreshed successfully"
            else:
                return "❌ Token refresh succeeded but failed to get new access token"
        else:
            return "❌ Failed to refresh token - may need to re-authenticate"
    except Exception as e:
        return f"❌ Token refresh error: {str(e)}"

@mcp.tool
def get_auth_status() -> str:
    """
    Get current authentication status with helpful guidance.
    
    Returns:
        Authentication status information
    """
    try:
        is_authenticated = keycloak_auth.is_authenticated()
        status = keycloak_auth.token_storage.get_session_status()
        
        if is_authenticated:
            user_info = keycloak_auth.get_user_info()
            if user_info:
                username = user_info.get('preferred_username', 'Unknown')
                email = user_info.get('email', 'Unknown')
                result = f"✅ **Authenticated** as: {username} ({email})\n"
                result += f"📊 Status: {status}"
                
                # Add token info
                token_info = keycloak_auth.token_storage.get_token_info()
                if token_info.get('access_token_expired'):
                    result += "\n🔄 Access token expired but refresh available"
                else:
                    result += "\n🟢 Access token valid"
                
                return result
            else:
                return f"✅ Authenticated (token valid)\n📊 Status: {status}"
        else:
            return f"""
❌ **Not authenticated**
📊 Status: {status}

🔧 **To authenticate, use:**
• `login_with_credentials("email", "password")` - Username/password authentication
"""
    except Exception as e:
        return f"❌ Error checking auth status: {str(e)}"

@mcp.tool
def get_session_info() -> str:
    """
    Get detailed session information including token status.
    
    Returns:
        Detailed session information
    """
    try:
        session_info = keycloak_auth.get_session_info()
        
        result = f"""
📊 **Detailed Session Information**

**Status:** {session_info.get('session_status', 'Unknown')}

**Token Information:**
• Has Access Token: {'✅' if session_info.get('has_access_token') else '❌'}
• Has Refresh Token: {'✅' if session_info.get('has_refresh_token') else '❌'}
• Access Token Expired: {'⏰' if session_info.get('access_token_expired') else '🟢'}
• Refresh Token Expired: {'⏰' if session_info.get('refresh_token_expired') else '🟢'}
"""
        
        if 'last_updated' in session_info and session_info['last_updated']:
            import datetime
            last_updated = datetime.datetime.fromtimestamp(session_info['last_updated'])
            result += f"• Last Updated: {last_updated.strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        if 'user' in session_info:
            user = session_info['user']
            result += f"""
**User Information:**
• Email: {user.get('email', 'Unknown')}
• Name: {user.get('name', 'Unknown')}
• Username: {user.get('username', 'Unknown')}
"""
        
        # Add storage location
        storage_path = keycloak_auth.token_storage.token_file
        result += f"\n**Storage:** {storage_path}"
        
        return result
    except Exception as e:
        return f"❌ Error getting session info: {str(e)}"

@mcp.tool
def logout() -> str:
    """
    Logout and clear all authentication tokens.
    
    Returns:
        Logout status message
    """
    try:
        success = keycloak_auth.logout()
        if _legacy_token_storage_enabled():
            try:
                set_token("")
            except Exception:
                pass

        if success:
            return "Logged out successfully. All tokens cleared from persistent storage."
        return "Logged out locally (remote logout may have failed). All local tokens cleared."
    except Exception as e:
        return f"Logout error: {str(e)}"


@mcp.tool
def saveOntoAIThreadID(thread_external_id: str, ctx: Context) -> Dict[str, Any]:
    """Persist the threadExternalId for the active MCP session."""
    context_id = ctx.session_id
    thread_id = (thread_external_id or "").strip()
    if not thread_id:
        return {
            "contextId": context_id,
            "threadExternalId": None,
            "message": "thread_external_id is required.",
        }

    if not is_session_state_configured():
        return {
            "contextId": context_id,
            "threadExternalId": None,
            "message": "Session-state service is not configured for this server.",
        }

    try:
        result = merge_session_state(
            context_id,
            lambda payload: {**payload, "threadExternalId": thread_id},
        )
    except SessionStateError as exc:
        safe_print(f"[session-state] save failed: {exc}")
        return {
            "contextId": context_id,
            "threadExternalId": None,
            "message": str(exc),
        }

    payload = {}
    if isinstance(result, dict):
        payload = result.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}

    return {
        "contextId": result.get("contextId", context_id) if isinstance(result, dict) else context_id,
        "threadExternalId": payload.get("threadExternalId", thread_id),
        "createdAt": result.get("createdAt") if isinstance(result, dict) else None,
    }


@mcp.tool
def getOntoAIThreadID(ctx: Context) -> Dict[str, Any]:
    """Return the stored threadExternalId for the active MCP session."""
    context_id = ctx.session_id

    if not is_session_state_configured():
        return {
            "contextId": context_id,
            "threadExternalId": None,
            "message": "Session-state service is not configured for this server.",
        }

    try:
        payload, meta = get_session_state(context_id)
    except SessionStateError as exc:
        safe_print(f"[session-state] get failed: {exc}")
        return {
            "contextId": context_id,
            "threadExternalId": None,
            "message": str(exc),
        }

    thread_id = payload.get("threadExternalId") if isinstance(payload, dict) else None
    if thread_id is None:
        return {
            "contextId": meta.get("contextId", context_id),
            "threadExternalId": None,
            "message": "No session state stored for this context.",
        }

    return {
        "contextId": meta.get("contextId", context_id),
        "threadExternalId": thread_id,
        "createdAt": meta.get("createdAt"),
    }


def _normalize_s3_key(s3_key: str) -> str:
    if not isinstance(s3_key, str):
        raise ValidationError("400: 's3Key' must be a non-empty string starting with 'raw/'.")
    normalized = s3_key.strip()
    if not normalized or not normalized.startswith("raw/"):
        raise ValidationError("400: 's3Key' must be a non-empty string starting with 'raw/'.")
    return normalized


def _normalize_non_empty_string(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"400: '{field}' must be a non-empty string.")
    normalized = value.strip()
    if not normalized:
        raise ValidationError(f"400: '{field}' must be a non-empty string.")
    return normalized


def _normalize_positive_int(value: Any, field: str) -> int:
    if not isinstance(value, (int, float)):
        raise ValidationError(f"400: '{field}' must be a positive integer.")
    try:
        integer_value = int(value)
    except (TypeError, ValueError):
        raise ValidationError(f"400: '{field}' must be a positive integer.") from None
    if integer_value <= 0:
        raise ValidationError(f"400: '{field}' must be a positive integer.")
    return integer_value


def _normalize_strategy(strategy: Any) -> str:
    if strategy is None:
        return "auto"
    if not isinstance(strategy, str):
        raise ValidationError("400: 'strategy' must be one of 'auto', 'single', 'multipart'.")
    normalized = strategy.strip().lower()
    if normalized not in {"auto", "single", "multipart"}:
        raise ValidationError("400: 'strategy' must be one of 'auto', 'single', 'multipart'.")
    return normalized


def _resolve_upload_mode(
    strategy: str, file_size: int, threshold_mib: int | None
) -> str:
    threshold_bytes = 5 * 1024 ** 3
    if isinstance(threshold_mib, int) and threshold_mib > 0:
        threshold_bytes = threshold_mib * 1024 * 1024

    if strategy == "auto":
        return "single" if file_size <= threshold_bytes else "multipart"

    if strategy == "single" and file_size > threshold_bytes:
        human_threshold = threshold_bytes / (1024 ** 2)
        raise ValidationError(
            "413: 'fileSize' exceeds the %.0f MiB limit for single uploads. "
            "Use strategy='multipart'." % human_threshold
        )

    return "single" if strategy == "single" else "multipart"


def _normalize_parts(parts: Any) -> list[dict[str, Any]]:
    if parts is None:
        return []
    if not isinstance(parts, (list, tuple)):
        raise ValidationError("400: 'parts' must be an array of objects with 'partNumber' and 'eTag'.")

    normalized: list[dict[str, Any]] = []
    seen_numbers: set[int] = set()
    for item in parts:
        if not isinstance(item, dict):
            raise ValidationError("400: each part must be an object with 'partNumber' and 'eTag'.")
        part_number = item.get("partNumber")
        etag = item.get("eTag")
        if not isinstance(part_number, int) or part_number <= 0:
            raise ValidationError("400: 'partNumber' must be a positive integer.")
        if part_number in seen_numbers:
            raise ValidationError("400: duplicate 'partNumber' in 'parts'.")
        seen_numbers.add(part_number)
        if not isinstance(etag, str) or not etag.strip():
            raise ValidationError("400: 'eTag' must be a non-empty string for each part.")
        normalized.append({"partNumber": part_number, "eTag": etag.strip()})

    if len(normalized) > 10_000:
        raise ValidationError("400: number of parts exceeds the S3 limit of 10 000.")

    normalized.sort(key=lambda item: item["partNumber"])
    return normalized


def _safe_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return None


def _normalize_storage_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    if not isinstance(entry, dict):
        return normalized

    config_id = entry.get("configId")
    if config_id:
        normalized["configId"] = str(config_id)

    endpoint = entry.get("endpoint")
    if endpoint:
        normalized["endpoint"] = str(endpoint)

    external_endpoint = entry.get("externalEndpoint")
    if external_endpoint:
        normalized["externalEndpoint"] = str(external_endpoint)

    bucket = entry.get("bucket")
    if bucket:
        normalized["bucket"] = str(bucket)

    key = entry.get("s3Key")
    if key:
        normalized["s3Key"] = str(key)

    presign = _safe_int(entry.get("presignExpirySec"))
    if presign is not None:
        normalized["presignExpirySec"] = presign

    threshold = _safe_int(entry.get("multipartThresholdMiB"))
    if threshold is not None:
        normalized["multipartThresholdMiB"] = threshold

    part_size = _safe_int(entry.get("multipartPartSizeMiB"))
    if part_size is not None:
        normalized["multipartPartSizeMiB"] = part_size

    if "overwritePolicy" in entry and entry.get("overwritePolicy"):
        normalized["overwritePolicy"] = str(entry["overwritePolicy"])

    return normalized


def _get_storage_for_signature(signature_id: str) -> Dict[str, Any]:
    cached = storage_cache.get_storage(signature_id)
    if cached:
        normalized = _normalize_storage_entry(cached)
        if normalized:
            storage_cache.set_storage(signature_id, normalized)
        return normalized

    try:
        service = PreflightService()
        entry = service._load_signature_storage_fields(signature_id)
    except PreflightProcessingError as exc:
        status = getattr(exc, "status_code", 500)
        raise RuntimeError(f"{status}: {exc}") from exc

    normalized = _normalize_storage_entry(entry)
    if normalized:
        storage_cache.set_storage(signature_id, normalized)
    return normalized


@mcp.tool
def upload_url(
    signatureId: str,
    fileName: str,
    fileSize: int,
    contentType: str,
    strategy: str | None = "auto",
    filePath: str | None = None,
) -> Dict[str, Any]:
    """Upload the dataset file directly to MinIO or return upload parameters."""

    normalized_signature_id = _normalize_non_empty_string(signatureId, "signatureId")
    normalized_file_name = _normalize_non_empty_string(fileName, "fileName")
    normalized_size = _normalize_positive_int(fileSize, "fileSize")
    normalized_content_type = _normalize_non_empty_string(contentType, "contentType")
    normalized_strategy = _normalize_strategy(strategy)

    assignment = _load_storage_assignment(normalized_signature_id)
    endpoint = assignment.config.external_endpoint or assignment.config.endpoint
    if not endpoint:
        raise RuntimeError("422: storage configuration missing endpoint")

    actual_path = Path(filePath) if filePath else None
    if actual_path:
        if not actual_path.is_file():
            raise RuntimeError(f"404: local file '{actual_path}' not found")
        actual_size = actual_path.stat().st_size
        if actual_size != normalized_size:
            safe_print(
                f"[upload_url] warning: provided fileSize {normalized_size} differs from actual {actual_size}"
            )
        _upload_file_to_storage(
            assignment=assignment,
            file_path=actual_path,
            content_type=normalized_content_type,
        )
        result_status = "uploaded"
    else:
        result_status = "pending"

    response: Dict[str, Any] = {
        "status": result_status,
        "bucket": assignment.config.bucket,
        "s3Key": assignment.s3_key,
        "endpoint": endpoint,
        "externalEndpoint": assignment.config.external_endpoint,
        "strategy": normalized_strategy,
        "fileName": normalized_file_name,
        "fileSize": normalized_size,
        "contentType": normalized_content_type,
    }

    if assignment.config.base_prefix:
        response["basePrefix"] = assignment.config.base_prefix

    return response


_PREUPLOAD_SERVICE: PreflightService | None = None


def _get_preflight_service() -> PreflightService:
    global _PREUPLOAD_SERVICE
    if _PREUPLOAD_SERVICE is None:
        _PREUPLOAD_SERVICE = PreflightService()
    return _PREUPLOAD_SERVICE


def _load_storage_assignment(signature_id: str) -> StorageAssignment:
    entry = storage_cache.get_storage(signature_id)
    if not entry:
        raise RuntimeError("409: signature_without_storage: run preflight_submit before uploading")
    service = _get_preflight_service()
    assignment = service._storage_entry_to_assignment(entry)
    if not assignment:
        raise RuntimeError("500: failed to reconstruct storage assignment")
    return assignment


def _find_assignment_by_s3key(s3_key: str) -> tuple[str, StorageAssignment] | None:
    cache = storage_cache.list_storage()
    service = _get_preflight_service()
    for signature_id, entry in cache.items():
        if entry.get("s3Key") == s3_key:
            assignment = service._storage_entry_to_assignment(entry)
            if assignment:
                return signature_id, assignment
    return None


def _create_s3_client(config) -> Any:
    endpoint = config.external_endpoint or config.endpoint
    if not endpoint:
        raise RuntimeError("422: storage configuration missing endpoint")
    session = boto3.session.Session(
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name=config.region or "us-east-1",
    )
    boto_cfg = BotoConfig(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"})
    return session.client("s3", endpoint_url=endpoint, config=boto_cfg)


def _upload_file_to_storage(*, assignment: StorageAssignment, file_path: Path, content_type: str) -> None:
    client = _create_s3_client(assignment.config)
    extra_args = {"ContentType": content_type or "application/octet-stream"}
    safe_print(
        f"[upload_url] uploading {file_path} to s3://{assignment.config.bucket}/{assignment.s3_key}"
    )
    client.upload_file(str(file_path), assignment.config.bucket, assignment.s3_key, ExtraArgs=extra_args)
    safe_print(
        f"[upload_url] upload complete: s3://{assignment.config.bucket}/{assignment.s3_key}"
    )




@mcp.tool
def upload_complete(
    s3Key: str,
    eTag: str | None = None,
    parts: list[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """Verify that the uploaded dataset file is present in storage."""

    normalized_key = _normalize_s3_key(s3Key)
    assignment_entry = _find_assignment_by_s3key(normalized_key)
    if assignment_entry is None:
        raise RuntimeError("404: storage assignment not found for provided s3Key")

    signature_id, assignment = assignment_entry
    client = _create_s3_client(assignment.config)

    try:
        head = client.head_object(Bucket=assignment.config.bucket, Key=assignment.s3_key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"NoSuchKey", "404"}:
            raise RuntimeError("404: uploaded object not found in storage") from exc
        raise RuntimeError(f"500: failed to verify upload: {exc}") from exc

    etag = head.get("ETag", "")
    size = head.get("ContentLength")
    last_modified = head.get("LastModified")

    response = {
        "status": "verified",
        "bucket": assignment.config.bucket,
        "s3Key": normalized_key,
        "signatureId": signature_id,
        "size": size,
        "eTag": etag.strip('"') if isinstance(etag, str) else etag,
        "lastModified": last_modified.isoformat() if hasattr(last_modified, "isoformat") else last_modified,
    }

    if assignment.config.external_endpoint:
        response["externalEndpoint"] = assignment.config.external_endpoint

    return response

def _get_valid_token() -> str:
    """Get a valid token, with automatic refresh and helpful error messages."""
    # Try Keycloak auth first (with automatic refresh)
    keycloak_token = keycloak_auth.get_valid_access_token()
    if keycloak_token:
        return keycloak_token
    
    # Fall back to manual token
    try:
        return get_token()
    except RuntimeError:
        # Provide helpful guidance
        if keycloak_auth.token_storage.get_access_token():
            # We have tokens but they're expired and refresh failed
            raise RuntimeError("""
❌ Authentication expired and refresh failed.

🔧 Please re-authenticate using:
• `login_with_credentials("email", "password")` - Username/password authentication
""")
        else:
            # No tokens at all
            raise RuntimeError("""
❌ No authentication tokens found.

🔧 Please authenticate first using:
• `login_with_credentials("email", "password")` - Username/password authentication

ℹ️ Your session will be saved persistently after authentication.
""")

def _get_user_spaces_data() -> list[dict]:
    """Internal function to get user spaces data. Used by both resource and tool."""
    url = f"{ONTO_API_BASE}/user/v2/current"
    
    try:
        token = _get_valid_token()
    except RuntimeError as e:
        # Return helpful error as part of the resource data
        return [{"error": str(e)}]
    
    # Ensure token is clean ASCII
    if isinstance(token, str):
        token = token.encode('ascii', errors='ignore').decode('ascii')
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        roles = data.get("userRealmsRoles", [])
        spaces = [{"id": r["realmId"], "name": r["realmName"]} for r in roles]
        
        # Add session info to the first space entry if available
        if spaces:
            session_status = keycloak_auth.token_storage.get_session_status()
            spaces[0]["_session_info"] = f"✅ Authenticated - {session_status}"
        
        return spaces
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            # Token might be invalid, try to refresh
            try:
                keycloak_auth.refresh_access_token()
                # Retry with refreshed token
                token = keycloak_auth.get_valid_access_token()
                if token:
                    headers["Authorization"] = f"Bearer {token}"
                    resp = requests.get(url, headers=headers, timeout=10)
                    resp.raise_for_status()
                    data = resp.json()
                    roles = data.get("userRealmsRoles", [])
                    return [{"id": r["realmId"], "name": r["realmName"]} for r in roles]
            except:
                pass
            
            return [{"error": "❌ Authentication failed. Please re-authenticate with login_with_credentials()"}]
        else:
            return [{"error": f"❌ API Error: {e.response.status_code} - {e.response.text}"}]
    except Exception as e:
        return [{"error": f"❌ Unexpected error: {str(e)}"}]

@mcp.resource("onto://spaces")
def get_user_spaces() -> list[dict]:
    """Return the list of Onto realms (spaces) visible to the authorised user."""
    return _get_user_spaces_data()

@mcp.resource("onto://user/info")
def get_user_info() -> dict:
    """Get current user information from Keycloak."""
    try:
        user_info = keycloak_auth.get_user_info()
        if user_info:
            # Add session status
            user_info["_session_status"] = keycloak_auth.token_storage.get_session_status()
            return user_info
        else:
            return {
                "error": "❌ Failed to get user info - not authenticated or token invalid",
                "_help": "Use login_with_credentials() to authenticate"
            }
    except Exception as e:
        return {
            "error": f"❌ Error getting user info: {str(e)}",
            "_help": "Use get_auth_status() to check authentication status"
        }

@mcp.tool
def search_templates(name_part: str, realm_id: str = None, include_children: bool = False, include_parents: bool = False) -> str:
    """
    Search for templates (meta entities) in Onto by name.
    
    Args:
        name_part: Partial name to search for (required)
        realm_id: Realm ID to search in (optional - uses first available realm if not specified)
        include_children: Include children in search results
        include_parents: Include parents in search results
        
    Returns:
        JSON string with list of found templates or error message
    """
    try:
        token = _get_valid_token()
    except RuntimeError as e:
        return str(e)
    
    # Get realm_id if not provided
    if not realm_id:
        spaces = _get_user_spaces_data()
        if not spaces or 'error' in spaces[0]:
            return "❌ Failed to get user realms. Please check authentication."
        
        realm_id = spaces[0]['id']
        realm_name = spaces[0]['name']
        safe_print(f"🔍 Using realm: {realm_name} ({realm_id})")
    
    # Prepare API request
    url = f"{ONTO_API_BASE}/realm/{realm_id}/meta/find"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    payload = {
        "namePart": name_part,
        "children": include_children,
        "parents": include_parents
    }
    
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        
        # Debug: check response content
        try:
            response_data = resp.json()
        except Exception as json_err:
            return f"❌ Invalid JSON response: {json_err}\nResponse: {resp.text[:500]}"
        
        # Handle API response format (expecting {result: [...]} structure)
        if isinstance(response_data, dict) and 'result' in response_data:
            templates = response_data['result']
        elif isinstance(response_data, list):
            templates = response_data
        else:
            return f"❌ Unexpected response format: {type(response_data)}\nResponse: {response_data}"
        
        if not isinstance(templates, list):
            return f"❌ Expected list in result field, got: {type(templates)}\nTemplates: {templates}"
        
        if not templates:
            return f"🔍 No templates found matching '{name_part}' in realm {realm_id}"
        
        # Format results nicely
        result_lines = [f"🔍 Found {len(templates)} template(s) matching '{name_part}':\n"]
        
        for i, template in enumerate(templates, 1):
            # Handle both dict and other formats
            if isinstance(template, dict):
                uuid = template.get('uuid', 'N/A')
                name = template.get('name', 'N/A')
                comment = template.get('comment', '')
            else:
                # Fallback for non-dict items
                uuid = str(template)
                name = str(template)
                comment = ''
            
            result_lines.append(f"{i}. **{name}**")
            result_lines.append(f"   UUID: {uuid}")
            if comment:
                result_lines.append(f"   Comment: {comment}")
            result_lines.append("")  # Empty line between templates
        
        return "\n".join(result_lines)
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            # Try to refresh token and retry
            try:
                keycloak_auth.refresh_access_token()
                token = keycloak_auth.get_valid_access_token()
                if token:
                    headers["Authorization"] = f"Bearer {token}"
                    resp = requests.post(url, json=payload, headers=headers, timeout=15)
                    resp.raise_for_status()
                    
                    try:
                        response_data = resp.json()
                    except Exception as json_err:
                        return f"❌ Invalid JSON response after retry: {json_err}\nResponse: {resp.text[:500]}"
                    
                    # Handle API response format
                    if isinstance(response_data, dict) and 'result' in response_data:
                        templates = response_data['result']
                    elif isinstance(response_data, list):
                        templates = response_data
                    else:
                        return f"❌ Unexpected response format after retry: {type(response_data)}\nResponse: {response_data}"
                    
                    if not isinstance(templates, list):
                        return f"❌ Expected list in result field after retry, got: {type(templates)}\nTemplates: {templates}"
                    
                    if not templates:
                        return f"🔍 No templates found matching '{name_part}' in realm {realm_id}"
                    
                    result_lines = [f"🔍 Found {len(templates)} template(s) matching '{name_part}':\n"]
                    for i, template in enumerate(templates, 1):
                        if isinstance(template, dict):
                            uuid = template.get('uuid', 'N/A')
                            name = template.get('name', 'N/A')
                            comment = template.get('comment', '')
                        else:
                            uuid = str(template)
                            name = str(template)
                            comment = ''
                        
                        result_lines.append(f"{i}. **{name}**")
                        result_lines.append(f"   UUID: {uuid}")
                        if comment:
                            result_lines.append(f"   Comment: {comment}")
                        result_lines.append("")
                    
                    return "\n".join(result_lines)
            except Exception:
                pass
            
            return "❌ Authentication failed. Please re-authenticate with login_with_credentials()"
        elif e.response.status_code == 403:
            return f"❌ Access denied to realm {realm_id}. You may not have permission to search templates in this realm."
        elif e.response.status_code == 404:
            return f"❌ Realm {realm_id} not found or meta search endpoint not available."
        else:
            return f"❌ API Error: {e.response.status_code} - {e.response.text}"
    
    except Exception as e:
        return f"❌ Unexpected error: {str(e)}"

@mcp.tool
def list_available_realms() -> str:
    """
    Get list of available realms (spaces) that the user can access.
    Useful for getting realm IDs for template searches.
    
    Returns:
        Formatted list of realms with IDs and names
    """
    spaces = _get_user_spaces_data()
    
    if not spaces:
        return "❌ No realms found. Please check authentication."
    
    if 'error' in spaces[0]:
        return spaces[0]['error']
    
    result_lines = [f"📁 Available realms ({len(spaces)}):\n"]
    
    for i, space in enumerate(spaces, 1):
        realm_id = space.get('id', 'N/A')
        realm_name = space.get('name', 'N/A')
        result_lines.append(f"{i}. **{realm_name}**")
        result_lines.append(f"   ID: {realm_id}")
        result_lines.append("")
    
    result_lines.append("💡 Use the realm ID with search_templates() to search in a specific realm.")
    
    return "\n".join(result_lines)

@mcp.tool
def search_objects(
    realm_id: str = None,
    name_filter: str = "",
    template_uuid: str = "",
    comment_filter: str = "",
    load_all: bool = False,
    page_size: int = 20
) -> str:
    """
    Search for objects in Onto by name, template, or comment with pagination support.
    
    Args:
        realm_id: Realm ID to search in (optional - uses first available realm if not specified)
        name_filter: Partial name to search for
        template_uuid: UUID of template to filter by
        comment_filter: Partial comment to search for
        load_all: If True, loads ALL matching objects using pagination (may be slow for large datasets)
        page_size: Number of items per page (default: 20, will be reduced automatically if payload too large)
        
    Returns:
        JSON-formatted string with list of found objects or error message
    """
    try:
        token = _get_valid_token()
    except RuntimeError as e:
        return str(e)
    
    # Get realm_id if not provided
    if not realm_id:
        spaces = _get_user_spaces_data()
        if not spaces or 'error' in spaces[0]:
            return "❌ Failed to get user realms. Please check authentication."
        
        realm_id = spaces[0]['id']
        realm_name = spaces[0]['name']
        safe_print(f"🔍 Using realm: {realm_name} ({realm_id})")
    
    # Prepare API request
    url = f"{ONTO_API_BASE}/realm/{realm_id}/entity/find/v2"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    def make_request(first: int, offset: int) -> tuple[list, bool]:
        """Make API request and return (results, has_more_data)"""
        payload = {
            "name": name_filter,
            "comment": comment_filter,
            "metaFieldFilters": [],
            "pagination": {
                "first": first,
                "offset": offset
            }
        }
        
        # Add template filter if provided
        if template_uuid:
            payload["metaEntityRequest"] = {"uuid": template_uuid}
        
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=30)
            resp.raise_for_status()
            
            try:
                response_data = resp.json()
            except Exception as json_err:
                raise Exception(f"Invalid JSON response: {json_err}\nResponse: {resp.text[:500]}")
            
            # Response should be a list (may contain group wrappers with "entities")
            if not isinstance(response_data, list):
                raise Exception(f"Expected list response, got: {type(response_data)}\nResponse: {response_data}")

            # Flatten results – each item may be either an entity dict OR a wrapper with "entities" list
            flat_results: list = []
            for item in response_data:
                if isinstance(item, dict) and "entities" in item and isinstance(item["entities"], list):
                    # Wrapper object – extend with its entities
                    flat_results.extend(item["entities"])
                else:
                    flat_results.append(item)

            # Determine if more data likely exists based on original items count OR flattened count
            has_more = len(response_data) == offset or len(flat_results) == offset

            return flat_results, has_more
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 413 or "payload too large" in str(e).lower():
                # Payload too large - reduce page size
                if offset > 5:
                    safe_print(f"⚠️ Payload too large, reducing page size from {offset} to {offset//2}")
                    return make_request(first, offset // 2)
                else:
                    raise Exception(f"❌ Payload too large even with minimum page size (5). Try more specific filters.")
            elif e.response.status_code == 401:
                # Try to refresh token and retry
                try:
                    keycloak_auth.refresh_access_token()
                    new_token = keycloak_auth.get_valid_access_token()
                    if new_token:
                        headers["Authorization"] = f"Bearer {new_token}"
                        resp = requests.post(url, json=payload, headers=headers, timeout=30)
                        resp.raise_for_status()
                        response_data = resp.json()
                        has_more = len(response_data) == offset
                        return response_data, has_more
                except Exception:
                    pass
                raise Exception("❌ Authentication failed. Please re-authenticate with login_with_credentials()")
            elif e.response.status_code == 403:
                raise Exception(f"❌ Access denied to realm {realm_id}. You may not have permission to search objects in this realm.")
            elif e.response.status_code == 404:
                raise Exception(f"❌ Realm {realm_id} not found or entity search endpoint not available.")
            else:
                raise Exception(f"❌ API Error: {e.response.status_code} - {e.response.text}")
    
    # Execute search with pagination
    all_objects = []
    current_first = 0
    current_page_size = page_size
    total_requests = 0
    max_requests = 100  # Safety limit
    
    try:
        while total_requests < max_requests:
            total_requests += 1
            
            # Make request
            objects, has_more = make_request(current_first, current_page_size)
            all_objects.extend(objects)
            
            safe_print(f"📄 Loaded page: first={current_first}, count={len(objects)}, total_so_far={len(all_objects)}")
            
            # If not loading all, or no more data, break
            if not load_all or not has_more:
                break
                
            # Prepare for next page
            current_first += len(objects)
            
        if total_requests >= max_requests:
            safe_print(f"⚠️ Hit safety limit of {max_requests} requests")
            
    except Exception as e:
        return str(e)
    
    # Format results
    if not all_objects:
        filters_desc = []
        if name_filter:
            filters_desc.append(f"name containing '{name_filter}'")
        if template_uuid:
            filters_desc.append(f"template '{template_uuid}'")
        if comment_filter:
            filters_desc.append(f"comment containing '{comment_filter}'")
        
        filters_text = " and ".join(filters_desc) if filters_desc else "any criteria"
        return f"🔍 No objects found matching {filters_text} in realm {realm_id}"
    
    # Build result summary
    result_lines = []
    
    # Header with search info
    search_info = []
    if name_filter:
        search_info.append(f"name: '{name_filter}'")
    if template_uuid:
        search_info.append(f"template: '{template_uuid}'")
    if comment_filter:
        search_info.append(f"comment: '{comment_filter}'")
    
    search_desc = ", ".join(search_info) if search_info else "all objects"
    
    if load_all:
        result_lines.append(f"🔍 **Found {len(all_objects)} objects** (complete dataset) matching {search_desc}:")
    else:
        result_lines.append(f"🔍 **Found {len(all_objects)} objects** (first page) matching {search_desc}:")
    
    result_lines.append("")
    
    # Show objects (limit display to first 50 for readability)
    display_limit = 50
    displayed_count = min(len(all_objects), display_limit)
    
    for i, obj in enumerate(all_objects[:display_limit], 1):
        if isinstance(obj, dict):
            uuid = obj.get('id', 'N/X')
            name = obj.get('name', 'N/X')
            comment = obj.get('comment', '')
            
            # Get template info if available
            meta_entity = obj.get('metaEntity', {})
            template_name = meta_entity.get('name', '') if meta_entity else ''
            template_id = meta_entity.get('id', '') if meta_entity else ''
            
            result_lines.append(f"{i}. **{name}**")
            result_lines.append(f"   UUID: {uuid}")
            if template_name:
                result_lines.append(f"   Template: {template_name} ({template_id})")
            if comment:
                # Truncate long comments
                display_comment = comment[:100] + "..." if len(comment) > 100 else comment
                result_lines.append(f"   Comment: {display_comment}")
            result_lines.append("")
        else:
            result_lines.append(f"{i}. {str(obj)}")
            result_lines.append("")
    
    # Add truncation notice if needed
    if len(all_objects) > display_limit:
        result_lines.append(f"... and {len(all_objects) - display_limit} more objects (truncated for display)")
        result_lines.append("")
    
    # Add usage tips
    if not load_all and len(all_objects) == page_size:
        result_lines.append("💡 **Tip:** There might be more results. Use `load_all=True` to get the complete dataset.")
    
    if load_all and len(all_objects) > 100:
        result_lines.append("📊 **Large Dataset:** Consider using more specific filters for better performance.")
    
    return "\n".join(result_lines)

# ---------------------------------------------------------------------------
# Realm (workspace) management
# ---------------------------------------------------------------------------

@mcp.tool
def create_realm(name: str, comment: str = "") -> str:
    """Create a new workspace (realm).

    Args:
        name: Unique name of the workspace.
        comment: Optional comment (can be empty).

    Returns:
        Formatted string with info about newly created realm or error message.
    """
    if not name or not name.strip():
        return "❌ Parameter 'name' is required and cannot be empty."

    try:
        token = _get_valid_token()
    except RuntimeError as e:
        return str(e)

    url = f"{ONTO_API_BASE}/realm/"  # According to API: POST /api/v2/core/realm/

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    payload = {"name": name.strip(), "comment": comment or ""}

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        realm_id = data.get("id", "N/A")
        realm_name = data.get("name", "N/A")
        realm_comment = data.get("comment", "")

        result = [
            "🎉 **Workspace (realm) created successfully!**",
            f"ID: {realm_id}",
            f"Name: {realm_name}",
        ]
        if realm_comment:
            result.append(f"Comment: {realm_comment}")
        return "\n".join(result)
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code
        if status == 400:
            return "❌ Bad request – please check the input data (maybe the name is missing or invalid)."
        if status == 401:
            return "❌ Authentication failed – please login again."
        if status == 403:
            return "❌ Access denied – you don't have permission to create a workspace."
        if status == 409:
            return f"❌ Workspace with name '{name}' already exists. Choose another name."
        return f"❌ API Error: {status} - {e.response.text[:200]}"
    except Exception as e:
        return f"❌ Unexpected error: {e}"

# ---------------------------------------------------------------------------
# Template (meta entity) management
# ---------------------------------------------------------------------------

@mcp.tool
def create_template(realm_id: str, name: str, comment: str = "") -> str:
    """Create a new template (meta entity) in a specified realm.

    Before creating, searches for an existing template with the same
    name to avoid duplicates.

    Args:
        realm_id: Target realm ID where the template will be created.
        name: Template name (must be unique).
        comment: Optional comment.

    Returns:
        Success message with template info or error details.
    """
    # Validate inputs
    if not realm_id or not realm_id.strip():
        return "❌ Parameter 'realm_id' is required and cannot be empty."
    if not name or not name.strip():
        return "❌ Parameter 'name' is required and cannot be empty."

    try:
        token = _get_valid_token()
    except RuntimeError as e:
        return str(e)

    # Step 1: Search existing templates
    search_url = f"{ONTO_API_BASE}/realm/{realm_id}/meta/find"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    search_payload = {"namePart": name.strip(), "children": False, "parents": False}

    try:
        resp = requests.post(search_url, json=search_payload, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        templates = data.get("result") if isinstance(data, dict) else data
        if isinstance(templates, list):
            for tpl in templates:
                if isinstance(tpl, dict) and tpl.get("name", "").lower() == name.strip().lower():
                    existing_id = tpl.get("uuid") or tpl.get("id", "N/A")
                    return f"❌ Template with name '{name}' already exists (UUID: {existing_id})."
    except requests.exceptions.HTTPError as e:
        if e.response.status_code not in (404, 400):
            return f"❌ Error searching templates: {e.response.text[:200]}"
    except Exception as e:
        return f"❌ Error searching templates: {e}"

    # Step 2: Create new template
    create_url = f"{ONTO_API_BASE}/realm/{realm_id}/meta"
    template_id = str(uuid.uuid4())
    create_payload = {
        "id": template_id,
        "name": name.strip(),
        "comment": comment or "",
    }

    try:
        resp = requests.post(create_url, json=create_payload, headers=headers, timeout=30)
        resp.raise_for_status()
        created = resp.json() if resp.content else {}
        created_id = created.get("id", template_id)
        result = [
            "🎉 **Template created successfully!**",
            f"ID: {created_id}",
            f"Name: {name.strip()}",
        ]
        if comment:
            result.append(f"Comment: {comment}")
        return "\n".join(result)
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code
        if status == 400:
            return "❌ Bad request – please check input data."
        if status == 401:
            return "❌ Authentication failed – please login again."
        if status == 403:
            return "❌ Access denied – you don't have permission to create templates in this realm."
        if status == 409:
            return f"❌ Template with name '{name}' already exists."
        return f"❌ API Error: {status} - {e.response.text[:200]}"
    except Exception as e:
        return f"❌ Unexpected error: {e}"

# ---------------------------------------------------------------------------
# Batch entity creation
# ---------------------------------------------------------------------------

@mcp.tool
def create_entities_batch(realm_id: str, entities: list[dict]) -> str:
    """Create multiple entities in a realm in one batch.

    Args:
        realm_id: Target realm ID.
        entities: List of entity dicts with keys: name (required), id, comment, metaEntityId.

    Returns:
        Formatted success message or detailed error message.
    """
    # Validate inputs
    if not realm_id or not realm_id.strip():
        return "❌ Parameter 'realm_id' is required and cannot be empty."
    if not entities or not isinstance(entities, list):
        return "❌ Parameter 'entities' must be a non-empty list."

    # Ensure all have name
    for i, ent in enumerate(entities, 1):
        if not isinstance(ent, dict):
            return f"❌ Entity #{i} is not a dict."
        if not ent.get("name") or not ent["name"].strip():
            return f"❌ Entity #{i} is missing required 'name'."

    try:
        token = _get_valid_token()
    except RuntimeError as e:
        return str(e)

    # Duplicate check using direct API call (avoid calling mcp tool functions)
    duplicate_names: list[str] = []
    headers_basic = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    for ent in entities:
        name = ent["name"].strip()
        # Build minimal payload to search by name
        search_url = f"{ONTO_API_BASE}/realm/{realm_id}/entity/find/v2"
        search_payload = {
            "name": name,
            "comment": "",
            "metaFieldFilters": [],
            "pagination": {"first": 0, "offset": 5},
        }
        try:
            resp = requests.post(search_url, json=search_payload, headers=headers_basic, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list) and data:
                # Any result means duplicate
                duplicate_names.append(name)
        except Exception:
            # If search fails, skip duplicate detection for this name
            pass

    if duplicate_names:
        dup_list = ", ".join(duplicate_names)
        return f"❌ Duplicate entity names detected in realm {realm_id}: {dup_list}. Aborting creation."

    # Build payload
    payload_entities = []
    for ent in entities:
        item = {
            "id": ent.get("id") or None,
            "name": ent["name"].strip(),
            "comment": ent.get("comment", ""),
            "metaEntityId": ent.get("metaEntityId"),
        }
        payload_entities.append(item)

    payload = {"entities": payload_entities}

    url = f"{ONTO_API_BASE}/realm/{realm_id}/entity/batch"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json() if resp.content else {}

        created = data.get("createdEntities", [])
        result_lines = [
            f"🎉 **Successfully created {len(created)} entities in realm {realm_id}.**",
            "",
        ]
        for i, ent in enumerate(created, 1):
            uuid_ = ent.get("uuid") or ent.get("id", "N/A")
            name = ent.get("name", "N/A")
            comment = ent.get("comment", "")
            result_lines.append(f"{i}. **{name}**")
            result_lines.append(f"   UUID: {uuid_}")
            if comment:
                result_lines.append(f"   Comment: {comment}")
            # Meta entity info
            meta = ent.get("metaEntity") or {}
            if meta:
                meta_name = meta.get("name", "")
                meta_uuid = meta.get("uuid") or meta.get("id", "")
                result_lines.append(f"   Template: {meta_name} ({meta_uuid})")
            result_lines.append("")

        return "\n".join(result_lines)
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code
        if status == 400:
            try:
                err_msg = e.response.json().get("message", "Bad Request")
            except Exception:
                err_msg = e.response.text[:200]
            return f"❌ Bad request – {err_msg}"
        if status == 401:
            return "❌ Authentication failed – please login again."
        if status == 403:
            return "❌ Access denied – you don't have permission to create entities in this realm."
        return f"❌ API Error: {status} - {e.response.text[:200]}"
    except Exception as e:
        return f"❌ Unexpected error: {e}"
