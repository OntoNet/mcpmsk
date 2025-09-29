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
SESSION_STATE_API_BASE: str = os.getenv("SESSION_STATE_API_BASE", ONTO_API_BASE)
SESSION_STATE_API_KEY: str = os.getenv("SESSION_STATE_API_KEY", "").strip()

# Realm configuration for Onto entities (used by preflight tools)
ONTO_REALM_ID: str | None = os.getenv("ONTO_REALM_ID")

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
# Validation
# ---------------------------------------------------------------------------

# Validate required configuration variables
_required_vars = [
    ("KEYCLOAK_BASE_URL", KEYCLOAK_BASE_URL),
    ("KEYCLOAK_REALM", KEYCLOAK_REALM),
    ("KEYCLOAK_CLIENT_ID", KEYCLOAK_CLIENT_ID),
    ("ONTO_API_BASE", ONTO_API_BASE),
]

_missing = [name for name, value in _required_vars if not value]

if _missing:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(_missing)}. "
        f"Please set them in your mcp.json configuration."
    ) 
