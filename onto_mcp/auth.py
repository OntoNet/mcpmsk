"""
Simple in-memory token storage.

\u26a0\ufe0f  NOT for production \u2013 replace with DB/Redis later.
"""
from __future__ import annotations
from typing import Final

_TOKEN: str | None = None
TOKEN_HEADER: Final[str] = "Authorization"

def set_token(token: str) -> None:
    global _TOKEN
    _TOKEN = token.strip()


def get_token() -> str:
    if _TOKEN is None:
        raise RuntimeError("Token missing. Use login_with_credentials first.")
    return _TOKEN
