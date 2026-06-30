"""Import-safe helpers for Google live-auth environment shapes."""

import os
from collections.abc import Callable, Mapping, MutableMapping
from typing import Literal

GOOGLE_API_KEY_ENV = "GOOGLE_API_KEY"
GEMINI_API_KEY_ENV = "GEMINI_API_KEY"
VERTEXAI_ENV = "GOOGLE_GENAI_USE_VERTEXAI"
CLOUD_PROJECT_ENV = "GOOGLE_CLOUD_PROJECT"
CLOUD_LOCATION_ENV = "GOOGLE_CLOUD_LOCATION"
TRUTHY_VALUES = frozenset({"1", "true", "yes", "on"})

GoogleApiKeyAliasDirection = Literal[
    "none",
    "gemini_to_google",
    "google_to_gemini",
]


def _read_env(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if environ is None else environ


def _write_env(environ: MutableMapping[str, str] | None) -> MutableMapping[str, str]:
    return os.environ if environ is None else environ


def google_vertex_mode_enabled(environ: Mapping[str, str] | None = None) -> bool:
    """Return whether Google GenAI should use Vertex AI / ADC auth."""
    env = _read_env(environ)
    return env.get(VERTEXAI_ENV, "").strip().lower() in TRUTHY_VALUES


def missing_google_vertex_env_vars(
    environ: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    """Return project/location variables missing from a Vertex auth env shape."""
    env = _read_env(environ)
    return tuple(
        name for name in (CLOUD_PROJECT_ENV, CLOUD_LOCATION_ENV) if not env.get(name)
    )


def has_google_live_auth_env(environ: Mapping[str, str] | None = None) -> bool:
    """Return whether env has API-key auth or complete Vertex / ADC settings."""
    env = _read_env(environ)
    if google_vertex_mode_enabled(env):
        return not missing_google_vertex_env_vars(env)
    return bool(env.get(GEMINI_API_KEY_ENV) or env.get(GOOGLE_API_KEY_ENV))


def apply_google_api_key_alias(
    alias_direction: GoogleApiKeyAliasDirection,
    environ: MutableMapping[str, str] | None = None,
) -> None:
    """Populate the provider-specific API-key alias when only one key is set."""
    env = _write_env(environ)
    if alias_direction == "gemini_to_google":
        gemini_api_key = env.get(GEMINI_API_KEY_ENV)
        if gemini_api_key and not env.get(GOOGLE_API_KEY_ENV):
            env[GOOGLE_API_KEY_ENV] = gemini_api_key
        return
    if alias_direction == "google_to_gemini":
        google_api_key = env.get(GOOGLE_API_KEY_ENV)
        if google_api_key and not env.get(GEMINI_API_KEY_ENV):
            env[GEMINI_API_KEY_ENV] = google_api_key


def require_google_live_auth_env(
    *,
    alias_direction: GoogleApiKeyAliasDirection,
    incomplete_vertex_message: Callable[[tuple[str, ...]], str],
    missing_credentials_message: str,
    environ: MutableMapping[str, str] | None = None,
) -> None:
    """Validate Google live-auth env shape and apply requested key aliasing.

    This checks only the environment shape. It does not create Google clients,
    inspect ADC files, or call gcloud. When Vertex mode is enabled, project and
    location are required even if an API key is also set, because Google GenAI
    will follow the Vertex path.
    """
    env = _write_env(environ)
    if google_vertex_mode_enabled(env):
        missing = missing_google_vertex_env_vars(env)
        if missing:
            raise SystemExit(incomplete_vertex_message(missing))
        return

    if env.get(GEMINI_API_KEY_ENV) or env.get(GOOGLE_API_KEY_ENV):
        apply_google_api_key_alias(alias_direction, env)
        return

    raise SystemExit(missing_credentials_message)
