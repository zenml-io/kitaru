"""LLM call primitive for tracked model interactions.

``kitaru.llm()`` provides a thin convenience wrapper for making LLM
calls with automatic tracking of prompts, responses, token usage,
cost, and latency.

When called inside a flow (outside a checkpoint), it creates a
synthetic durable call boundary. When called inside a checkpoint,
it creates a tracked child event.

Example::

    response = kitaru.llm(
        "Summarize this article",
        model="fast",
        system="You are a helpful assistant.",
    )

Note: This is scaffolding. The LLM primitive is not yet implemented.
"""

from __future__ import annotations

from typing import Any

from kitaru.runtime import _not_implemented


def llm(
    prompt: str,
    *,
    model: str | None = None,
    system: str | None = None,
    temperature: float | None = None,
    max_tokens: int | None = None,
    name: str | None = None,
) -> Any:
    """Make a tracked LLM call.

    Args:
        prompt: The user prompt to send to the model.
        model: Model identifier or alias (e.g. ``"fast"``, ``"smart"``,
            ``"openai:gpt-4o"``). Resolved from stack config if omitted.
        system: System prompt.
        temperature: Sampling temperature.
        max_tokens: Maximum tokens in the response.
        name: Display name for this LLM call in the dashboard.

    Returns:
        The model response text.
    """
    _not_implemented("llm")
