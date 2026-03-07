"""Wait primitive for durable suspension.

``kitaru.wait()`` suspends a running flow until external input arrives.
The execution remains in ``waiting`` status and can be resumed later
via the client API or CLI.

Wait is valid only directly inside a flow, not inside a checkpoint.

Example::

    approval = kitaru.wait(
        schema=bool,
        name="human_approval",
        question="Approve this draft?",
    )

Note: This is scaffolding. The wait primitive is not yet implemented.
It also requires ZenML server-side support (feature/pause-pipeline-runs).
"""

from __future__ import annotations

from typing import Any

from kitaru.runtime import _not_implemented


def wait(
    *,
    schema: Any = bool,
    name: str | None = None,
    question: str | None = None,
    timeout: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> Any:
    """Suspend the current flow until external input arrives.

    Args:
        schema: Expected type of the input value. Defaults to bool.
        name: Display name for this wait point.
        question: Human-readable prompt describing what input is needed.
        timeout: Resource-retention timeout in seconds (not expiration).
        metadata: Additional metadata to attach to the wait record.

    Returns:
        The validated input value once the execution is resumed.
    """
    _not_implemented("wait")
