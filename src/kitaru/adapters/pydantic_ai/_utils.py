"""Shared types and helpers for the Kitaru PydanticAI adapter.

No ``from __future__ import annotations`` — ZenML step registration walks the
real ``_turn``'s annotations and rejects string forms.
"""

import asyncio
import hashlib
import json
import sys
from collections.abc import Coroutine, Mapping
from typing import Any, Callable, Literal, TypedDict, cast

import kitaru
from kitaru._source_aliases import build_checkpoint_source_alias
from kitaru.errors import KitaruUsageError
from pydantic_core import to_jsonable_python

CheckpointRuntime = Literal['inline', 'isolated']


class CheckpointConfig(TypedDict, total=False):
    """Kwargs forwarded to the adapter's synthetic ``@kitaru.checkpoint(...)``."""

    runtime: CheckpointRuntime
    retries: int
    type: str


ToolCheckpointOverride = CheckpointConfig | Literal[False]
ToolCheckpointOverrides = Mapping[str, ToolCheckpointOverride]
_ALLOWED_CHECKPOINT_CONFIG_KEYS = frozenset({'runtime', 'retries', 'type'})

if f'src.{__name__}' not in sys.modules:
    sys.modules[f'src.{__name__}'] = sys.modules[__name__]


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


def checkpoint_cache_key(payload: Any) -> str:
    """Return a stable hash for synthetic checkpoint inputs."""
    try:
        normalized = to_jsonable_python(payload, serialize_unknown=True)
    except ValueError:
        normalized = {'repr': repr(payload), 'python_type': type(payload).__name__}
    encoded = json.dumps(normalized, sort_keys=True, separators=(',', ':'), default=repr).encode()
    return hashlib.sha256(encoded).hexdigest()


def validate_checkpoint_config(
    config: CheckpointConfig | None,
    *,
    context: str,
) -> CheckpointConfig | None:
    """Return a validated shallow copy of ``config``."""
    if config is None:
        return None
    unknown_keys = sorted(set(config) - _ALLOWED_CHECKPOINT_CONFIG_KEYS)
    if unknown_keys:
        unknown = ', '.join(unknown_keys)
        raise KitaruUsageError(
            f'Unsupported keys in {context}: {unknown}. '
            "Allowed keys are: 'runtime', 'retries', 'type'."
        )
    validated = cast(CheckpointConfig, dict(config))
    reject_isolated_runtime(validated)
    return validated


def validate_tool_checkpoint_overrides(
    overrides: ToolCheckpointOverrides | None,
    *,
    context: str,
) -> dict[str, ToolCheckpointOverride] | None:
    """Return validated tool checkpoint overrides."""
    if overrides is None:
        return None
    normalized: dict[str, ToolCheckpointOverride] = {}
    for tool_name, override in overrides.items():
        if override is False:
            normalized[tool_name] = False
            continue
        validated = validate_checkpoint_config(
            override, context=f'{context}[{tool_name!r}]'
        )
        normalized[tool_name] = validated if validated is not None else cast(CheckpointConfig, {})
    return normalized


def reject_isolated_runtime(config: CheckpointConfig | dict[str, Any]) -> None:
    """Raise if ``config`` requests ``runtime='isolated'``.

    Adapter checkpoints wrap closures that capture live ``RunContext`` / tool
    / agent references. Those cannot survive the cross-process serialization
    that ``runtime='isolated'`` would trigger on remote stacks. Until
    :class:`KitaruRunContext` is wired through, accept only inline runtime.
    """
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
    body: Callable[[], Coroutine[Any, Any, Any]],
    cache_key: str | None = None,
) -> Any:
    """Run an async ``body`` inside a ``@kitaru.checkpoint(**config)`` step.

    ``kitaru.checkpoint`` is sync-only; we bridge via ``asyncio.run`` inside a
    sync ``_turn`` dispatched to a worker thread so the caller's loop stays free.
    """
    reject_isolated_runtime(config)

    def _turn(_cache_key: str | None = None) -> Any:
        return asyncio.run(body())

    _turn.__name__ = step_name
    checkpoint_def = kitaru.checkpoint(**config)(_turn)
    step_obj = getattr(checkpoint_def, '_step', None)
    if step_obj is not None:
        alias = build_checkpoint_source_alias(_turn.__name__)
        for module_name in {__name__, f'src.{__name__}'}:
            module = sys.modules.get(module_name)
            if module is not None:
                setattr(module, alias, step_obj)
    step_call = checkpoint_def
    step_output = await asyncio.to_thread(step_call, cache_key)
    return materialize_step_output(step_output)
