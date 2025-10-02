from __future__ import annotations

"""Central configuration for Onto MCP Server.
Environment variables are provided by MCP client configuration (mcp.json).

Having a single place for configuration simplifies maintenance and eliminates 
scattered hard-coded values.
"""

import os

# ---------------------------------------------------------------------------
# Keycloak configuration
# ---------------------------------------------------------------------------

KEYCLOAK_BASE_URL: str = os.getenv("KEYCLOAK_BASE_URL")
KEYCLOAK_REALM: str = os.getenv("KEYCLOAK_REALM")
KEYCLOAK_CLIENT_ID: str = os.getenv("KEYCLOAK_CLIENT_ID")
KEYCLOAK_CLIENT_SECRET: str = os.getenv("KEYCLOAK_CLIENT_SECRET", "")

# ---------------------------------------------------------------------------
# Onto API configuration
# ---------------------------------------------------------------------------

ONTO_API_BASE: str = os.getenv("ONTO_API_BASE")
ONTO_API_TOKEN: str | None = os.getenv("ONTO_API_TOKEN")
FIELD_MAP_PATH: str | None = os.getenv("FIELD_MAP_PATH")
SESSION_STATE_API_BASE: str = os.getenv("SESSION_STATE_API_BASE", ONTO_API_BASE)
SESSION_STATE_API_KEY: str = os.getenv("SESSION_STATE_API_KEY", "").strip()

# Realm configuration for Onto entities (used by preflight tools)
ONTO_REALM_ID: str | None = os.getenv("ONTO_REALM_ID")


def _env_flag(name: str, default: bool = True) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


ENABLE_CREATE: bool = _env_flag("ENABLE_CREATE", True)
ENABLE_STORAGE_LINKS: bool = _env_flag("ENABLE_LINKS", False)
ALLOW_S3KEY_REGENERATE: bool = _env_flag("ALLOW_S3KEY_REGENERATE", False)

# Names for dynamic metadata discovery (fallback to legacy defaults)
ONTO_META_DATASETCLASS_NAME: str = os.getenv(
    "ONTO_META_DATASETCLASS_NAME", "DatasetClass"
)
ONTO_META_SIGNATURE_NAME: str = os.getenv(
    "ONTO_META_SIGNATURE_NAME", "DatasetSignature"
)
ONTO_META_RECOG_NAME: str = os.getenv(
    "ONTO_META_RECOG_NAME", "RecognitionResult"
)
ONTO_META_COLUMNSIGN_NAME: str = os.getenv(
    "ONTO_META_COLUMNSIGN_NAME", "ColumnSignature"
)
ONTO_META_PIPELINE_NAME: str = os.getenv(
    "ONTO_META_PIPELINE_NAME", "PipelineTemplate"
)
ONTO_META_STORAGECONFIG_NAME: str = os.getenv(
    "ONTO_META_STORAGECONFIG_NAME", "StorageConfig"
)

ONTO_DEBUG_HTTP: bool = _env_flag("ONTO_DEBUG_HTTP", False)

# Optional path for persisting preflight mock storage (defaults to in-memory)
ONTO_PREFLIGHT_STORE_PATH: str | None = os.getenv("ONTO_PREFLIGHT_STORE_PATH")


# ---------------------------------------------------------------------------
# MCP server runtime configuration
# ---------------------------------------------------------------------------

MCP_TRANSPORT: str = os.getenv("MCP_TRANSPORT", "stdio")  # Keep default for transport
PORT: int = int(os.getenv("PORT", "8080"))  # Keep default for port

# Convenience flag
IS_HTTP_TRANSPORT: bool = MCP_TRANSPORT == "http"

# ---------------------------------------------------------------------------
# External integrations
# ---------------------------------------------------------------------------

AIRFLOW_API_URL: str | None = os.getenv("AIRFLOW_API_URL")
AIRFLOW_API_USER: str | None = os.getenv("AIRFLOW_API_USER")
AIRFLOW_API_PASS: str | None = os.getenv("AIRFLOW_API_PASS")
AIRFLOW_TIMEOUT_SEC: float = float(os.getenv("AIRFLOW_TIMEOUT_SEC", "30"))
AIRFLOW_RETRY: int = int(os.getenv("AIRFLOW_RETRY", "3"))
MINIO_REGION: str | None = os.getenv("MINIO_REGION")

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

# Validate required configuration variables
_required_vars = [
    ("KEYCLOAK_BASE_URL", KEYCLOAK_BASE_URL),
    ("KEYCLOAK_REALM", KEYCLOAK_REALM),
    ("KEYCLOAK_CLIENT_ID", KEYCLOAK_CLIENT_ID),
    ("ONTO_API_BASE", ONTO_API_BASE),
    ("ONTO_API_TOKEN", ONTO_API_TOKEN),
]

_missing = [name for name, value in _required_vars if not value]

if _missing:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(_missing)}. "
        f"Please set them in your mcp.json configuration."
    ) 
