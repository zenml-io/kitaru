"""Checkpoint decorator for durable work boundaries.

A checkpoint is a unit of work inside a flow whose outcome is
persisted. Successful outputs become artifacts; failures are recorded
for retry. Checkpoints are the replay boundaries that make durable
execution possible.

Example::

    @kitaru.checkpoint
    def fetch_data(url: str) -> str:
        return requests.get(url).text

Note: This is scaffolding. The checkpoint decorator is not yet implemented.
"""

from __future__ import annotations

from typing import Any

from kitaru.runtime import _not_implemented


def checkpoint(*args: Any, **kwargs: Any) -> Any:
    """Mark a function as a durable checkpoint.

    Can be used as a bare decorator or with arguments::

        @kitaru.checkpoint
        def my_step(): ...

        @kitaru.checkpoint(retries=3, type="llm_call")
        def my_step(): ...

    Args:
        retries: Number of checkpoint-level retries on failure.
        type: Checkpoint type for dashboard visualization
            (e.g. ``"llm_call"``, ``"tool_call"``).
    """
    _not_implemented("checkpoint")
