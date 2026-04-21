"""Public helpers for reading and managing Kitaru-managed secrets."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict
from zenml.client import Client as _ZenMLClient
from zenml.exceptions import EntityExistsError as _ZenMLEntityExistsError
from zenml.exceptions import ZenKeyError as _ZenMLKeyError

from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import KitaruBackendError, KitaruRuntimeError, KitaruUsageError

_SECRET_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class Secret(BaseModel):
    """Kitaru-native view of a stored secret.

    The model intentionally exposes only stable Kitaru SDK fields and hides the
    underlying backend response object.

    Attributes:
        name: Secret name.
        id: Backend secret ID, normalized to a string.
        values: Readable secret key/value pairs, normalized to strings.
        private: Whether the backend marks the secret as private, when known.
    """

    name: str
    id: str
    values: dict[str, str]
    private: bool | None = None

    model_config = ConfigDict(frozen=True)

    def get(self, key: str, default: str | None = None) -> str | None:
        """Return a secret value by key, or a default when the key is absent."""
        return self.values.get(key, default)


class SecretSummary(BaseModel):
    """Metadata-only view of a stored secret.

    This model is safe to return from write/delete operations because it lists
    key names but never includes raw secret values.
    """

    name: str
    id: str
    private: bool
    keys: list[str]
    has_missing_values: bool = False

    model_config = ConfigDict(frozen=True)

    @property
    def visibility(self) -> str:
        """Return the user-facing visibility label for this secret."""
        return "private" if self.private else "public"


def _normalize_name_or_id(name_or_id: str) -> str:
    """Normalize and validate a secret name or ID input."""
    normalized = name_or_id.strip()
    if not normalized:
        raise KitaruUsageError("Secret name or ID cannot be empty.")
    return normalized


def _normalize_secret_write_values(values: Mapping[str, Any]) -> dict[str, str]:
    """Validate and normalize values for secret creation."""
    if not isinstance(values, Mapping):
        raise KitaruUsageError("Secret values must be provided as a mapping.")
    if not values:
        raise KitaruUsageError("Provide at least one secret value.")

    normalized_values: dict[str, str] = {}
    for raw_key, raw_value in values.items():
        key = str(raw_key).strip()
        if not _SECRET_KEY_PATTERN.fullmatch(key):
            raise KitaruUsageError(
                "Invalid secret key "
                f"`{raw_key}`. Use env-var style names "
                "(letters, numbers, underscores; cannot start with a number)."
            )
        if raw_value is None:
            raise KitaruUsageError(f"Secret value for key `{key}` cannot be None.")

        value = str(raw_value)
        if value == "":
            raise KitaruUsageError(f"Secret value for key `{key}` cannot be empty.")
        if key in normalized_values:
            raise KitaruUsageError(f"Duplicate secret key `{key}`.")

        normalized_values[key] = value

    return normalized_values


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


def _secret_keys_from_response(secret_response: Any) -> list[str]:
    """Return stable key names from a backend secret response."""
    raw_values = getattr(secret_response, "values", None)
    if isinstance(raw_values, Mapping):
        return sorted(str(key) for key in raw_values)

    raw_secret_values = getattr(secret_response, "secret_values", None)
    if isinstance(raw_secret_values, Mapping):
        return sorted(str(key) for key in raw_secret_values)

    return []


def _get_secret_response_exact(
    name_or_id: str,
    *,
    client: Any | None = None,
) -> Any:
    """Fetch one backend secret response by exact name or exact ID.

    Args:
        name_or_id: Secret name or ID to fetch.
        client: Optional already-created backend client. CLI code passes this
            to preserve its existing client lifecycle and tests.

    Returns:
        The raw backend secret response.

    Raises:
        KitaruUsageError: If ``name_or_id`` is empty.
        KitaruRuntimeError: If the secret is not found.
        KitaruBackendError: If the backend lookup fails unexpectedly.
    """
    normalized_name_or_id = _normalize_name_or_id(name_or_id)

    try:
        resolved_client = client if client is not None else _ZenMLClient()
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
    secret_id = str(raw_id) if raw_id is not None else ""
    if not secret_id:
        raise KitaruBackendError(f"Backend returned secret `{name}` without an ID.")

    raw_private = getattr(secret_response, "private", None)
    private = bool(raw_private) if raw_private is not None else None

    return Secret(
        name=name,
        id=secret_id,
        values=_normalize_secret_values(secret_response, display_name=name),
        private=private,
    )


def _secret_summary_from_response(secret_response: Any) -> SecretSummary:
    """Convert a backend secret response into safe metadata."""
    raw_name = getattr(secret_response, "name", None)
    name = str(raw_name).strip() if raw_name is not None else ""
    if not name:
        raise KitaruBackendError("Backend returned a secret without a readable name.")

    raw_id = getattr(secret_response, "id", None)
    secret_id = str(raw_id) if raw_id is not None else ""
    if not secret_id:
        raise KitaruBackendError(f"Backend returned secret `{name}` without an ID.")

    return SecretSummary(
        name=name,
        id=secret_id,
        private=bool(getattr(secret_response, "private", False)),
        keys=_secret_keys_from_response(secret_response),
        has_missing_values=bool(getattr(secret_response, "has_missing_values", False)),
    )


def _read_secret_values(secret_name: str) -> dict[str, str]:
    """Read non-empty secret key/value pairs for internal credential injection."""
    secret_response = _get_secret_response_exact(secret_name)
    values = _normalize_secret_values(secret_response, display_name=secret_name)
    if not values:
        raise KitaruRuntimeError(
            f"Secret `{secret_name}` does not contain readable key/value pairs."
        )
    return values


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
    track(AnalyticsEvent.SECRET_READ, {"key_count": len(secret.values)})
    return secret


def create_secret(
    name: str,
    values: Mapping[str, Any],
    *,
    private: bool = False,
) -> SecretSummary:
    """Create a secret and return metadata without raw secret values.

    New secrets are public by default. Pass ``private=True`` to create a
    private backend secret.
    """
    normalized_name = _normalize_name_or_id(name)
    normalized_values = _normalize_secret_write_values(values)

    try:
        secret_response = _ZenMLClient().create_secret(
            name=normalized_name,
            values=normalized_values,
            private=private,
        )
    except _ZenMLEntityExistsError as exc:
        raise KitaruRuntimeError(f"Secret `{normalized_name}` already exists.") from exc
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to create secret `{normalized_name}`: {exc}"
        ) from exc

    summary = _secret_summary_from_response(secret_response)
    track(
        AnalyticsEvent.SECRET_UPSERTED,
        {"operation": "created", "key_count": len(normalized_values)},
    )
    return summary


def delete_secret(name_or_id: str) -> SecretSummary:
    """Delete a secret by exact name or ID and return deleted metadata."""
    normalized_name_or_id = _normalize_name_or_id(name_or_id)
    try:
        client = _ZenMLClient()
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to delete secret `{normalized_name_or_id}`: {exc}"
        ) from exc

    secret_response = _get_secret_response_exact(normalized_name_or_id, client=client)
    summary = _secret_summary_from_response(secret_response)

    try:
        client.delete_secret(name_id_or_prefix=summary.id)
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to delete secret `{normalized_name_or_id}`: {exc}"
        ) from exc

    return summary


__all__ = ["Secret", "SecretSummary", "create_secret", "delete_secret", "get_secret"]
