"""Shared secret helpers for CLI-facing command layers."""

from __future__ import annotations

from typing import Any

from zenml.models import SecretResponse

from kitaru.errors import KitaruRuntimeError, KitaruUsageError
from kitaru.secrets import _get_secret_response_exact


def resolve_secret_exact(client: Any, name_or_id: str) -> SecretResponse:
    """Fetch one secret by exact name or exact ID.

    Lower-level secret lookup raises Kitaru-specific exceptions. CLI command
    surfaces historically reported these as ``ValueError`` through the shared
    CLI error boundary, so this helper preserves that behavior for every
    command that needs exact secret validation.
    """
    try:
        return _get_secret_response_exact(name_or_id, client=client)
    except (KitaruRuntimeError, KitaruUsageError) as exc:
        raise ValueError(str(exc)) from exc
