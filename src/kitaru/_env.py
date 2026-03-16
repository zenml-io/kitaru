"""Environment variable helpers for Kitaru package initialization."""

from __future__ import annotations

import os
import warnings

KITARU_SERVER_URL_ENV = "KITARU_SERVER_URL"
KITARU_AUTH_TOKEN_ENV = "KITARU_AUTH_TOKEN"
KITARU_PROJECT_ENV = "KITARU_PROJECT"
KITARU_DEBUG_ENV = "KITARU_DEBUG"
KITARU_ANALYTICS_OPT_IN_ENV = "KITARU_ANALYTICS_OPT_IN"

ZENML_STORE_URL_ENV = "ZENML_STORE_URL"
ZENML_STORE_API_KEY_ENV = "ZENML_STORE_API_KEY"
ZENML_ACTIVE_PROJECT_ID_ENV = "ZENML_ACTIVE_PROJECT_ID"
ZENML_DEBUG_ENV = "ZENML_DEBUG"
ZENML_ANALYTICS_OPT_IN_ENV = "ZENML_ANALYTICS_OPT_IN"

_ENV_TRANSLATIONS: tuple[tuple[str, str], ...] = (
    (KITARU_SERVER_URL_ENV, ZENML_STORE_URL_ENV),
    (KITARU_AUTH_TOKEN_ENV, ZENML_STORE_API_KEY_ENV),
    (KITARU_PROJECT_ENV, ZENML_ACTIVE_PROJECT_ID_ENV),
    (KITARU_DEBUG_ENV, ZENML_DEBUG_ENV),
    (KITARU_ANALYTICS_OPT_IN_ENV, ZENML_ANALYTICS_OPT_IN_ENV),
)


def _normalized_kitaru_env(name: str) -> str | None:
    """Return a Kitaru env value, treating blank strings as unset."""
    value = os.environ.get(name)
    if value is None:
        return None
    if not value.strip():
        return None
    return value


def apply_env_translations() -> None:
    """Translate public ``KITARU_*`` env vars into ``ZENML_*`` equivalents."""
    for kitaru_var, zenml_var in _ENV_TRANSLATIONS:
        kitaru_value = _normalized_kitaru_env(kitaru_var)
        if kitaru_value is None:
            continue

        zenml_value = os.environ.get(zenml_var)
        if zenml_value is not None and zenml_value != kitaru_value:
            warnings.warn(
                f"Both {kitaru_var} and {zenml_var} are set with different "
                f"values; using {kitaru_var}.",
                stacklevel=2,
            )

        os.environ[zenml_var] = kitaru_value

    server_url = _normalized_kitaru_env(KITARU_SERVER_URL_ENV)
    auth_token = _normalized_kitaru_env(KITARU_AUTH_TOKEN_ENV) or os.environ.get(
        ZENML_STORE_API_KEY_ENV
    )
    if server_url and not auth_token:
        raise RuntimeError(
            "KITARU_SERVER_URL is set but no auth token is available. "
            "Set KITARU_AUTH_TOKEN (or ZENML_STORE_API_KEY)."
        )

    if _normalized_kitaru_env(KITARU_AUTH_TOKEN_ENV) and not (
        server_url or os.environ.get(ZENML_STORE_URL_ENV)
    ):
        raise RuntimeError(
            "KITARU_AUTH_TOKEN is set but no server URL is available. "
            "Set KITARU_SERVER_URL (or ZENML_STORE_URL)."
        )

    # Disable ZenML Rich traceback formatting — Kitaru handles its own output.
    os.environ.setdefault("ZENML_ENABLE_RICH_TRACEBACK", "0")
