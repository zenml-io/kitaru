"""Artifact helpers for explicit named artifacts.

``kitaru.save()`` persists a named artifact inside a checkpoint.
``kitaru.load()`` retrieves a named artifact from a previous execution.

Both are valid only inside a checkpoint.

Example::

    @kitaru.checkpoint
    def research(topic: str) -> str:
        context = gather_sources(topic)
        kitaru.save("sources", context, type="context")
        return summarize(context)

    # In a later execution:
    @kitaru.checkpoint
    def refine(exec_id: str) -> str:
        old_sources = kitaru.load(exec_id, "sources")
        return improve(old_sources)

Note: This is scaffolding. Artifact helpers are not yet implemented.
"""

from __future__ import annotations

from typing import Any

from kitaru.runtime import _not_implemented


def save(
    name: str,
    value: Any,
    *,
    type: str = "output",
    tags: list[str] | None = None,
) -> None:
    """Persist a named artifact inside the current checkpoint.

    Args:
        name: Artifact name (unique within the checkpoint).
        value: The value to persist. Must be serializable.
        type: Artifact type for categorization (e.g. ``"prompt"``,
            ``"response"``, ``"context"``, ``"output"``, ``"blob"``).
        tags: Optional tags for filtering and discovery.
    """
    _not_implemented("save")


def load(exec_id: str, name: str) -> Any:
    """Load a named artifact from a previous execution.

    Args:
        exec_id: The execution ID to load from.
        name: The artifact name to retrieve.

    Returns:
        The materialized artifact value.
    """
    _not_implemented("load")
