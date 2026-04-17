"""Public helpers for reading Kitaru-managed secrets."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict
from zenml.client import Client as _ZenMLClient
from zenml.exceptions import ZenKeyError as _ZenMLKeyError

import kitaru.analytics as _analytics
from kitaru.analytics import AnalyticsEvent as _AnalyticsEvent
from kitaru.errors import KitaruBackendError, KitaruRuntimeError, KitaruUsageError


class Secret(BaseModel):
    """Kitaru-native view of a stored secret.

    The model intentionally exposes only stable Kitaru SDK fields and hides the
    underlying backend response object.

    Attributes:
        name: Secret name.
        id: Backend secret ID, normalized to a string.
        values: Readable secret key/value pairs, normalized to strings.
    """

    name: str
    id: str
    values: dict[str, str]

    model_config = ConfigDict(frozen=True)

    def get(self, key: str, default: str | None = None) -> str | None:
        """Return a secret value by key, or a default when the key is absent."""
        return self.values.get(key, default)


def _normalize_name_or_id(name_or_id: str) -> str:
    """Normalize and validate a secret name or ID input."""
    normalized = name_or_id.strip()
    if not normalized:
        raise KitaruUsageError("Secret name or ID cannot be empty.")
    return normalized


def _normalize_secret_values(
    secret_response: Any, *, display_name: str
) -> dict[str, str]:
    """Convert backend secret values into Kitaru's public string mapping."""
    raw_values = getattr(secret_response, "secret_values", None)
    if not isinstance(raw_values, Mapping):
        raise KitaruRuntimeError(
            f"Secret `{display_name}` does not contain readable key/value pairs."
        )

    normalized_values: dict[str, str] = {}
    for key, value in raw_values.items():
        key_string = str(key).strip()
        if not key_string:
            continue
        if value is None:
            continue
        normalized_values[key_string] = str(value)
    return normalized_values


def _get_secret_response_exact(
    name_or_id: str,
    *,
    client: Any | None = None,
    client_factory: Callable[[], Any] | None = None,
) -> Any:
    """Fetch one backend secret response by exact name or exact ID.

    Args:
        name_or_id: Secret name or ID to fetch.
        client: Optional already-created backend client. CLI code passes this
            to preserve its existing client lifecycle and tests.
        client_factory: Factory used when ``client`` is not provided. Defaults
            to the current backend client object, so tests can patch the module
            symbol normally.

    Returns:
        The raw backend secret response.

    Raises:
        KitaruUsageError: If ``name_or_id`` is empty.
        KitaruRuntimeError: If the secret is not found.
        KitaruBackendError: If the backend lookup fails unexpectedly.
    """
    normalized_name_or_id = _normalize_name_or_id(name_or_id)

    try:
        resolved_client = (
            client if client is not None else (client_factory or _ZenMLClient)()
        )
        return resolved_client.get_secret(
            name_id_or_prefix=normalized_name_or_id,
            allow_partial_name_match=False,
            allow_partial_id_match=False,
        )
    except (KeyError, _ZenMLKeyError) as exc:
        raise KitaruRuntimeError(
            f"Secret `{normalized_name_or_id}` was not found."
        ) from exc
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to load secret `{normalized_name_or_id}`: {exc}"
        ) from exc


def _secret_from_response(secret_response: Any) -> Secret:
    """Convert a backend secret response into Kitaru's public Secret model."""
    raw_name = getattr(secret_response, "name", None)
    name = str(raw_name).strip() if raw_name is not None else ""
    if not name:
        raise KitaruBackendError("Backend returned a secret without a readable name.")

    raw_id = getattr(secret_response, "id", None)
    if raw_id is None:
        raise KitaruBackendError(f"Backend returned secret `{name}` without an ID.")
    secret_id = str(raw_id)
    if not secret_id:
        raise KitaruBackendError(f"Backend returned secret `{name}` without an ID.")

    return Secret(
        name=name,
        id=secret_id,
        values=_normalize_secret_values(secret_response, display_name=name),
    )


def _read_secret_values(secret_name: str) -> dict[str, str]:
    """Read non-empty secret key/value pairs for internal credential injection."""
    secret_response = _get_secret_response_exact(secret_name)
    secret = _secret_from_response(secret_response)
    if not secret.values:
        raise KitaruRuntimeError(
            f"Secret `{secret_name}` does not contain readable key/value pairs."
        )
    return dict(secret.values)


def get_secret(name_or_id: str) -> Secret:
    """Read a stored secret by exact name or ID.

    Args:
        name_or_id: Secret name or ID. Partial name and partial ID matches are
            disabled so the lookup resolves exactly one intended secret.

    Returns:
        A Kitaru-native ``Secret`` model with normalized string values.

    Raises:
        KitaruUsageError: If ``name_or_id`` is empty.
        KitaruRuntimeError: If the secret is not found or has unreadable values.
        KitaruBackendError: If the backend lookup returns an invalid response or
            fails unexpectedly.
    """
    secret_response = _get_secret_response_exact(name_or_id)
    secret = _secret_from_response(secret_response)
    _analytics.track(_AnalyticsEvent.SECRET_READ, {"key_count": len(secret.values)})
    return secret


__all__ = ["Secret", "get_secret"]
