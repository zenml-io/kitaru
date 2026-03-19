"""ZenML init hook for Kitaru env translations.

This is a separate top-level package (not inside ``kitaru``) so that
ZenML can discover and call it via ``zenml.init_hooks`` entry points
without triggering ``import kitaru`` — which would itself import ZenML
and cause a circular import during ZenML's own initialization.

Only ``os`` and ``warnings`` are imported here. No ``kitaru`` or
``zenml`` imports are allowed.
"""

from __future__ import annotations

import os
import warnings

KITARU_REPOSITORY_DIRECTORY_NAME = ".kitaru"

KITARU_SERVER_URL_ENV = "KITARU_SERVER_URL"
KITARU_AUTH_TOKEN_ENV = "KITARU_AUTH_TOKEN"
KITARU_PROJECT_ENV = "KITARU_PROJECT"
KITARU_DEBUG_ENV = "KITARU_DEBUG"
KITARU_ANALYTICS_OPT_IN_ENV = "KITARU_ANALYTICS_OPT_IN"
KITARU_MODEL_REGISTRY_ENV = "KITARU_MODEL_REGISTRY"

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

_applied = False


def _normalized_kitaru_env(name: str) -> str | None:
    """Return a Kitaru env value, treating blank strings as unset."""
    value = os.environ.get(name)
    if value is None:
        return None
    if not value.strip():
        return None
    return value


def _reset_applied() -> None:
    """Reset the re-entry guard so tests can call apply_env_translations again."""
    global _applied
    _applied = False


def apply_env_translations() -> None:
    """Translate public ``KITARU_*`` env vars into ``ZENML_*`` equivalents."""
    global _applied
    if _applied:
        return
    _applied = True

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


def _apply_constants_overrides() -> None:
    """Patch ZenML constants that have no env var backing.

    Called from ``init()`` after env translations are applied. At this
    point we are inside ``zenml.__init__`` (the hook runs at the top),
    but ``zenml.constants`` is a standalone submodule that can be
    safely imported without circular issues.
    """
    import zenml.constants

    setattr(  # noqa: B010
        zenml.constants,
        "REPOSITORY_DIRECTORY_NAME",
        KITARU_REPOSITORY_DIRECTORY_NAME,
    )


def init() -> None:
    """Entry point called by ZenML's init hook system."""
    apply_env_translations()
    _apply_constants_overrides()
