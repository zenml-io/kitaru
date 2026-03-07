"""Kitaru: durable execution for AI agents.

Kitaru provides primitives for making AI agent workflows persistent,
replayable, and observable. Wrap your agent code with ``@workflow`` and
``@checkpoint`` decorators to get automatic durability.

Example:
    >>> import kitaru
    >>> @kitaru.workflow
    ... def my_agent():
    ...     result = fetch_data("https://example.com")
    ...     return process(result)
"""
