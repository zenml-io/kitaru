"""Structured metadata logging.

``kitaru.log()`` attaches structured key-value metadata to the current
checkpoint or execution. It is context-sensitive: inside a checkpoint
it attaches to that checkpoint; inside a flow but outside a checkpoint
it attaches to the execution.

Example::

    @kitaru.checkpoint
    def call_model(prompt: str) -> str:
        response = model.generate(prompt)
        kitaru.log(
            tokens=response.usage.total_tokens,
            cost=response.usage.cost,
            model=response.model,
        )
        return response.text

Note: This is scaffolding. The log primitive is not yet implemented.
"""

from __future__ import annotations

from typing import Any

from kitaru.runtime import _not_implemented


def log(**kwargs: Any) -> None:
    """Attach structured metadata to the current checkpoint or execution.

    Multiple calls within the same scope are merged. Standard keys
    include ``cost``, ``tokens``, ``latency``, ``model``, but
    arbitrary keys are accepted.

    Args:
        **kwargs: Key-value pairs to attach as metadata.
    """
    _not_implemented("log")
