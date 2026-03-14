"""Shared runtime-scope parsing helpers."""

from __future__ import annotations

from uuid import UUID

from kitaru.errors import KitaruStateError


def _parse_scope_uuid(
    scope_id: str,
    *,
    scope_name: str,
    api_name: str = "log",
) -> UUID:
    """Parse a runtime scope identifier as a UUID.

    Args:
        scope_id: Raw scope identifier from runtime context.
        scope_name: Human-readable scope name for error messages.
        api_name: Calling Kitaru API name.

    Returns:
        Parsed UUID.

    Raises:
        KitaruStateError: If the scope identifier is not a valid UUID.
    """
    try:
        return UUID(scope_id)
    except ValueError as exc:
        raise KitaruStateError(
            f"kitaru.{api_name}() found an invalid {scope_name} ID in runtime "
            f"scope: {scope_id!r}."
        ) from exc
