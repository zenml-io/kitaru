"""Shared types and helpers for the Kitaru PydanticAI adapter.

No ``from __future__ import annotations`` — ZenML step registration walks the
real ``_turn``'s annotations and rejects string forms.
"""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Literal, TypedDict

import kitaru

CheckpointRuntime = Literal['inline', 'isolated']


class CheckpointConfig(TypedDict, total=False):
    """Kwargs forwarded to the adapter's synthetic ``@kitaru.checkpoint(...)``."""

    runtime: CheckpointRuntime
    retries: int
    type: str


ToolCheckpointOverride = CheckpointConfig | Literal[False]
ToolCheckpointOverrides = Mapping[str, ToolCheckpointOverride]


def with_default_type(config: CheckpointConfig, default_type: str) -> CheckpointConfig:
    """Return ``config`` with ``type`` defaulted to ``default_type``."""
    if 'type' in config:
        return config
    return {**config, 'type': default_type}


def resolve_tool_checkpoint_config(
    tool_name: str,
    *,
    default: CheckpointConfig | None,
    by_name: ToolCheckpointOverrides | None,
) -> CheckpointConfig | None:
    """Pick the ``CheckpointConfig`` for ``tool_name``.

    Per-tool override in ``by_name`` wins (``False`` opts the tool out entirely);
    otherwise falls back to ``default``, or ``None`` when neither is set.
    """
    if by_name and tool_name in by_name:
        override = by_name[tool_name]
        return None if override is False else override
    return default


def materialize_step_output(value: Any) -> Any:
    """Unwrap a ZenML ``OutputArtifact`` handle to its concrete payload."""
    load = getattr(value, 'load', None)
    return load() if callable(load) else value


def reject_isolated_runtime(config: CheckpointConfig | dict[str, Any]) -> None:
    """Raise if ``config`` requests ``runtime='isolated'``.

    Adapter checkpoints wrap closures that capture live ``RunContext`` / tool
    / agent references. Those cannot survive the cross-process serialization
    that ``runtime='isolated'`` would trigger on remote stacks. Until
    :class:`KitaruRunContext` is wired through, accept only inline runtime.
    """
    from kitaru.errors import KitaruUsageError

    if config.get('runtime') == 'isolated':
        raise KitaruUsageError(
            "The PydanticAI adapter does not yet support `runtime='isolated'` — "
            'checkpoint closures capture live objects that cannot cross process '
            "boundaries. Use `runtime='inline'` or omit `runtime`."
        )


async def run_async_in_checkpoint(
    *,
    config: CheckpointConfig,
    step_name: str,
    body: Callable[[], Awaitable[Any]],
) -> Any:
    """Run an async ``body`` inside a ``@kitaru.checkpoint(**config)`` step.

    ``kitaru.checkpoint`` is sync-only; we bridge via ``asyncio.run`` inside a
    sync ``_turn`` dispatched to a worker thread so the caller's loop stays free.
    """
    reject_isolated_runtime(config)

    def _turn() -> Any:
        return asyncio.run(body())

    _turn.__name__ = step_name
    step_call = kitaru.checkpoint(**config)(_turn)
    step_output = await asyncio.to_thread(step_call)
    return materialize_step_output(step_output)
