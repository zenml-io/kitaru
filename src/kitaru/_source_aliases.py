"""Shared Kitaru source-alias constants and naming helpers.

Kitaru registers two distinct names for each flow/checkpoint with ZenML:

1. **Registration name** — the plain, user-facing name passed to ZenML via
   ``pipeline(name=...)`` / ``step(name=...)``.  This is what appears in the
   ZenML UI, API responses, and stored run metadata.

2. **Source alias** — an internal prefixed name (``__kitaru_pipeline_source_*``
   / ``__kitaru_checkpoint_source_*``) used as a module-level attribute so that
   ZenML's source-resolution mechanism can reload the underlying ``Pipeline`` /
   step object.  This name is never user-facing.

This module is internal — it is not part of the public API surface.
"""

from __future__ import annotations

import re

PIPELINE_SOURCE_ALIAS_PREFIX = "__kitaru_pipeline_source_"
CHECKPOINT_SOURCE_ALIAS_PREFIX = "__kitaru_checkpoint_source_"

# Matches either alias prefix followed by a Python-identifier suffix.
_ALIAS_PATTERN = re.compile(
    r"(?:__kitaru_pipeline_source_|__kitaru_checkpoint_source_)"
    r"([A-Za-z_][A-Za-z0-9_]*)"
)


def _normalize_callable_name(raw_name: str, *, fallback: str) -> str:
    """Sanitize a callable name into a valid Python identifier fragment."""
    normalized = re.sub(r"\W", "_", raw_name)
    if not normalized:
        normalized = fallback
    if normalized[0].isdigit():
        normalized = f"{fallback}_{normalized}"
    return normalized


def build_pipeline_registration_name(name: str) -> str:
    """Build the plain ZenML pipeline name for a flow function."""
    return _normalize_callable_name(name, fallback="flow")


def build_checkpoint_registration_name(name: str) -> str:
    """Build the plain ZenML step name for a checkpoint function."""
    return _normalize_callable_name(name, fallback="checkpoint")


def build_pipeline_source_alias(name: str) -> str:
    """Build the internal source alias for a flow (pipeline) function."""
    return PIPELINE_SOURCE_ALIAS_PREFIX + _normalize_callable_name(
        name, fallback="flow"
    )


def build_checkpoint_source_alias(name: str) -> str:
    """Build the internal source alias for a checkpoint (step) function."""
    return CHECKPOINT_SOURCE_ALIAS_PREFIX + _normalize_callable_name(
        name, fallback="checkpoint"
    )


def normalize_flow_name(value: object | None) -> str | None:
    """Strip the pipeline alias prefix from a flow name.

    Returns ``None`` for ``None``, empty, or whitespace-only input.
    """
    if value is None:
        return None

    name = str(value).strip()
    if not name:
        return None

    if name.startswith(PIPELINE_SOURCE_ALIAS_PREFIX):
        name = name.removeprefix(PIPELINE_SOURCE_ALIAS_PREFIX)

    return name or None


def normalize_checkpoint_name(step_name: str) -> str:
    """Strip the checkpoint alias prefix from a checkpoint (step) name."""
    if step_name.startswith(CHECKPOINT_SOURCE_ALIAS_PREFIX):
        return step_name.removeprefix(CHECKPOINT_SOURCE_ALIAS_PREFIX)
    return step_name


def normalize_aliases_in_text(text: str) -> str:
    """Replace all alias-prefixed names with their user-facing names in free text."""
    return _ALIAS_PATTERN.sub(r"\1", text)
