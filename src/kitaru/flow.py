"""Flow decorator for defining durable executions.

A flow is the outer orchestration boundary in Kitaru. It marks the
top-level function whose execution becomes durable, replayable, and
observable.

Example::

    @kitaru.flow
    def my_agent(query: str) -> str:
        data = fetch_data(query)
        return summarize(data)

Note: This is scaffolding. The flow decorator is not yet implemented.
"""

from __future__ import annotations

from typing import Any

from kitaru.runtime import _not_implemented


def flow(*args: Any, **kwargs: Any) -> Any:
    """Mark a function as a durable flow.

    Can be used as a bare decorator or with arguments::

        @kitaru.flow
        def my_flow(): ...

        @kitaru.flow(stack="remote", retries=2)
        def my_flow(): ...

    Args:
        stack: Execution stack to use.
        image: Container image for remote execution.
        cache: Whether to cache checkpoint outputs. Defaults to True.
        retries: Number of flow-level retries on failure.
    """
    _not_implemented("flow")
