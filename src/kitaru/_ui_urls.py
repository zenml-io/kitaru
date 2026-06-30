"""Resolve base URLs for Kitaru UI deep links."""

from __future__ import annotations

from typing import TYPE_CHECKING

from kitaru._config._env import KITARU_UI_URL_ENV
from kitaru._env import _normalized_kitaru_env

if TYPE_CHECKING:
    from kitaru.client import KitaruClient


def _normalize_http_base_url(value: str | None) -> str | None:
    """Return a trimmed HTTP(S) base URL without a trailing slash."""
    if value is None:
        return None
    candidate = value.strip().rstrip("/")
    if candidate.startswith(("http://", "https://")):
        return candidate
    return None


def resolve_ui_base_url(client: KitaruClient | None = None) -> str | None:
    """Return the dashboard base URL for UI compare and execution links.

    ``KITARU_UI_URL`` overrides the connected server URL when the frontend is
    hosted separately from the API (for example a preview UI pointing at a
    staging workspace server).
    """
    override = _normalize_http_base_url(_normalized_kitaru_env(KITARU_UI_URL_ENV))
    if override is not None:
        return override

    try:
        from kitaru.config import resolve_connection_config

        resolved = resolve_connection_config(validate_for_use=False)
        candidate = _normalize_http_base_url(resolved.server_url)
        if candidate is not None:
            return candidate
    except Exception:
        pass

    if client is None:
        return None

    try:
        zen_store = client._client().zen_store
        url = getattr(zen_store, "url", None)
    except Exception:
        return None
    return _normalize_http_base_url(url if isinstance(url, str) else None)
