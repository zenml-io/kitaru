"""Shared types and helpers for the Kitaru PydanticAI adapter.

No ``from __future__ import annotations`` — ZenML step registration walks the
real ``_turn``'s annotations and rejects string forms.
"""

import asyncio
import hashlib
import json
import sys
from collections.abc import Coroutine, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Callable, Literal, TypedDict, cast

from kitaru._source_aliases import build_checkpoint_source_alias
from kitaru.checkpoint import _synthetic_checkpoint
from kitaru.errors import KitaruUsageError
from pydantic_core import to_jsonable_python

CheckpointRuntime = Literal["inline", "isolated"]
CheckpointStrategy = Literal["calls", "turn"]
_ALLOWED_CHECKPOINT_STRATEGIES = ("calls", "turn")


class CheckpointConfig(TypedDict, total=False):
    """Kwargs forwarded to the adapter's synthetic ``@kitaru.checkpoint(...)``."""

    cache: bool | None
    runtime: CheckpointRuntime
    retries: int
    type: str


ToolCheckpointOverride = CheckpointConfig | Literal[False]
ToolCheckpointOverrides = Mapping[str, ToolCheckpointOverride]
_ALLOWED_CHECKPOINT_CONFIG_KEYS = frozenset({"cache", "runtime", "retries", "type"})


@dataclass(frozen=True)
class AdapterCheckpointArtifactRefs:
    """Display references for artifacts owned by a synthetic checkpoint.

    In granular adapter checkpoints, some event payloads now live in structural
    step slots rather than manual artifacts. These mappings let the event log
    say "prompt -> messages" or "result -> output" without creating duplicate
    manual artifacts inside the step.
    """

    input_artifacts: Mapping[str, str]
    output_artifacts: Mapping[str, str]


@dataclass(frozen=True)
class AdapterStreamingFallbackCheckpoint:
    """Private marker for adapter-owned streamed calls-strategy fallback turns."""

    allow_sync_tool_body_waits: bool


_ADAPTER_CHECKPOINT_ARTIFACT_REFS: ContextVar[AdapterCheckpointArtifactRefs | None] = (
    ContextVar("kitaru_adapter_checkpoint_artifact_refs", default=None)
)
_ADAPTER_STREAMING_FALLBACK_CHECKPOINT: ContextVar[
    AdapterStreamingFallbackCheckpoint | None
] = ContextVar("kitaru_adapter_streaming_fallback_checkpoint", default=None)

if f"src.{__name__}" not in sys.modules:
    sys.modules[f"src.{__name__}"] = sys.modules[__name__]


def with_default_type(config: CheckpointConfig, default_type: str) -> CheckpointConfig:
    """Return ``config`` with ``type`` defaulted to ``default_type``."""
    if "type" in config:
        return config
    return {**config, "type": default_type}


@contextmanager
def adapter_checkpoint_artifact_refs(
    *,
    input_artifacts: Mapping[str, str] | None = None,
    output_artifacts: Mapping[str, str] | None = None,
) -> Iterator[AdapterCheckpointArtifactRefs]:
    """Expose structural/canonical artifact slot names inside a checkpoint."""
    refs = AdapterCheckpointArtifactRefs(
        input_artifacts=input_artifacts or {},
        output_artifacts=output_artifacts or {},
    )
    token = _ADAPTER_CHECKPOINT_ARTIFACT_REFS.set(refs)
    try:
        yield refs
    finally:
        _ADAPTER_CHECKPOINT_ARTIFACT_REFS.reset(token)


def get_adapter_checkpoint_artifact_refs() -> AdapterCheckpointArtifactRefs | None:
    """Return granular checkpoint artifact slot references, if any."""
    return _ADAPTER_CHECKPOINT_ARTIFACT_REFS.get()


@contextmanager
def adapter_streaming_fallback_checkpoint(
    *, allow_sync_tool_body_waits: bool
) -> Iterator[AdapterStreamingFallbackCheckpoint]:
    """Mark an adapter-owned whole-turn checkpoint created for streaming fallback."""
    marker = AdapterStreamingFallbackCheckpoint(
        allow_sync_tool_body_waits=allow_sync_tool_body_waits
    )
    token = _ADAPTER_STREAMING_FALLBACK_CHECKPOINT.set(marker)
    try:
        yield marker
    finally:
        _ADAPTER_STREAMING_FALLBACK_CHECKPOINT.reset(token)


def get_adapter_streaming_fallback_checkpoint() -> (
    AdapterStreamingFallbackCheckpoint | None
):
    """Return the active streamed calls-strategy fallback marker, if any."""
    return _ADAPTER_STREAMING_FALLBACK_CHECKPOINT.get()


def has_explicit_tool_checkpoint_opt_out(
    tool_name: str,
    by_name: ToolCheckpointOverrides | None,
) -> bool:
    """Return whether ``tool_name`` has an explicit per-tool ``False`` override."""
    return bool(by_name and by_name.get(tool_name) is False)


def has_any_explicit_tool_checkpoint_opt_out(
    by_name: ToolCheckpointOverrides | None,
) -> bool:
    """Return whether any named tool has an explicit per-tool ``False`` override."""
    return bool(
        by_name
        and any(
            has_explicit_tool_checkpoint_opt_out(tool_name, by_name)
            for tool_name in by_name
        )
    )


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
    if has_explicit_tool_checkpoint_opt_out(tool_name, by_name):
        return None
    if by_name and tool_name in by_name:
        override = by_name[tool_name]
        return cast(CheckpointConfig, override)
    return default


def materialize_step_output(value: Any) -> Any:
    """Unwrap a ZenML ``OutputArtifact`` handle to its concrete payload."""
    load = getattr(value, "load", None)
    return load() if callable(load) else value


def checkpoint_input_value(value: Any) -> Any:
    """Return a JSON-friendly value for synthetic checkpoint input artifacts."""
    try:
        return to_jsonable_python(value, serialize_unknown=True)
    except ValueError:
        return {"repr": repr(value), "python_type": type(value).__name__}


def checkpoint_cache_key(payload: Any) -> str:
    """Return a stable hash for synthetic checkpoint inputs."""
    normalized = checkpoint_input_value(payload)
    encoded = json.dumps(
        normalized, sort_keys=True, separators=(",", ":"), default=repr
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def validate_checkpoint_strategy(value: str) -> CheckpointStrategy:
    """Return a supported PydanticAI checkpoint strategy."""
    if value in _ALLOWED_CHECKPOINT_STRATEGIES:
        return cast(CheckpointStrategy, value)
    accepted = ", ".join(repr(strategy) for strategy in _ALLOWED_CHECKPOINT_STRATEGIES)
    raise KitaruUsageError(
        f"Unsupported PydanticAI checkpoint_strategy {value!r}. "
        f"Accepted values are: {accepted}."
    )


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
        unknown = ", ".join(repr(key) for key in unknown_keys)
        allowed = ", ".join(
            repr(key) for key in sorted(_ALLOWED_CHECKPOINT_CONFIG_KEYS)
        )
        raise KitaruUsageError(
            f"Unsupported keys in {context}: {unknown}. Allowed keys are: {allowed}."
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
            override, context=f"{context}[{tool_name!r}]"
        )
        normalized[tool_name] = (
            validated if validated is not None else cast(CheckpointConfig, {})
        )
    return normalized


def reject_isolated_runtime(config: CheckpointConfig | dict[str, Any]) -> None:
    """Raise if ``config`` requests ``runtime='isolated'``.

    Adapter checkpoints wrap closures that capture live ``RunContext`` / tool
    / agent references. Those cannot survive the cross-process serialization
    that ``runtime='isolated'`` would trigger on remote stacks. Until
    :class:`KitaruRunContext` is wired through, accept only inline runtime.
    """
    if config.get("runtime") == "isolated":
        raise KitaruUsageError(
            "The PydanticAI adapter does not yet support `runtime='isolated'` — "
            "checkpoint closures capture live objects that cannot cross process "
            "boundaries. Use `runtime='inline'` or omit `runtime`."
        )


def _build_checkpoint_step(
    *,
    config: CheckpointConfig,
    step_name: str,
    body: Callable[[], Any],
    checkpoint_inputs: Mapping[str, Any] | None = None,
) -> Callable[..., Any]:
    reject_isolated_runtime(config)

    input_names = frozenset(checkpoint_inputs or {})
    if input_names == frozenset():

        def _turn_without_inputs(_cache_key: str | None = None) -> Any:
            return body()

        turn = _turn_without_inputs
    elif input_names == {"user_prompt"}:

        def _turn_with_user_prompt(
            user_prompt: Any,
            _cache_key: str | None = None,
        ) -> Any:
            return body()

        turn = _turn_with_user_prompt
    elif input_names == {"message_history"}:

        def _turn_with_message_history(
            message_history: Any,
            _cache_key: str | None = None,
        ) -> Any:
            return body()

        turn = _turn_with_message_history
    elif input_names == {"user_prompt", "message_history"}:

        def _turn_with_prompt_and_history(
            user_prompt: Any,
            message_history: Any,
            _cache_key: str | None = None,
        ) -> Any:
            return body()

        turn = _turn_with_prompt_and_history
    elif input_names == {"tool_args"}:

        def _turn_with_tool_args(
            tool_args: Any,
            _cache_key: str | None = None,
        ) -> Any:
            return body()

        turn = _turn_with_tool_args
    elif input_names == {"messages"}:

        def _turn_with_messages(
            messages: Any,
            _cache_key: str | None = None,
        ) -> Any:
            return body()

        turn = _turn_with_messages
    else:
        unsupported = ", ".join(sorted(input_names))
        raise KitaruUsageError(
            f"Unsupported synthetic checkpoint inputs: {unsupported}."
        )

    turn.__name__ = step_name
    checkpoint_def = _synthetic_checkpoint(
        **config,
        flow_result_candidate=False,
    )(turn)
    step_obj = getattr(checkpoint_def, "_step", None)
    if step_obj is not None:
        alias = build_checkpoint_source_alias(turn.__name__)
        for module_name in {__name__, f"src.{__name__}"}:
            module = sys.modules.get(module_name)
            if module is not None:
                setattr(module, alias, step_obj)
    return checkpoint_def


def run_sync_in_checkpoint(
    *,
    config: CheckpointConfig,
    step_name: str,
    body: Callable[[], Any],
    cache_key: str | None = None,
    checkpoint_inputs: Mapping[str, Any] | None = None,
) -> Any:
    """Run a sync ``body`` inside a ``@kitaru.checkpoint(**config)`` step."""
    checkpoint_def = _build_checkpoint_step(
        config=config,
        step_name=step_name,
        body=body,
        checkpoint_inputs=checkpoint_inputs,
    )
    return materialize_step_output(
        checkpoint_def(**(checkpoint_inputs or {}), _cache_key=cache_key)
    )


async def run_async_in_checkpoint(
    *,
    config: CheckpointConfig,
    step_name: str,
    body: Callable[[], Coroutine[Any, Any, Any]],
    cache_key: str | None = None,
    checkpoint_inputs: Mapping[str, Any] | None = None,
) -> Any:
    """Run an async ``body`` inside a ``@kitaru.checkpoint(**config)`` step.

    ``kitaru.checkpoint`` is sync-only; we bridge via ``asyncio.run`` inside a
    sync ``_turn`` dispatched to a worker thread so the caller's loop stays free.
    """
    checkpoint_def = _build_checkpoint_step(
        config=config,
        step_name=step_name,
        body=lambda: asyncio.run(body()),
        checkpoint_inputs=checkpoint_inputs,
    )
    step_output = await asyncio.to_thread(
        checkpoint_def,
        **(checkpoint_inputs or {}),
        _cache_key=cache_key,
    )
    return materialize_step_output(step_output)


def turn_cache_key(
    *,
    agent_name: str | None,
    user_prompt: Any,
    message_history: Any,
    deferred_tool_results: Any,
    output_type: Any,
    instructions: Any,
    deps: Any,
    model_settings: Any,
    usage_limits: Any,
    usage: Any,
    metadata: Any,
    infer_name: bool,
    toolsets: Any,
    builtin_tools: Any,
    event_stream_handler: Any,
    conversation_id: Any,
    output_retries: Any,
    capabilities: Any,
    spec: Any,
) -> str:
    """Stable cache key for a ``KitaruAgent`` turn checkpoint.

    Some turn inputs live in `_body`'s closure and aren't visible to ZenML's
    step cache. The visible ZenML inputs stay intentionally small
    (``user_prompt`` / ``message_history``), so the explicit key must include
    every run argument that can change how PydanticAI behaves. Falling back to
    ``repr`` for hard-to-serialize objects is acceptable here: a cache miss is
    safer than returning a stale agent result for a different run shape.
    """
    return checkpoint_cache_key(
        {
            "agent_name": agent_name,
            "user_prompt": user_prompt,
            "message_history": message_history,
            "deferred_tool_results": deferred_tool_results,
            "output_type": output_type,
            "instructions": instructions,
            "deps": deps,
            "model_settings": model_settings,
            "usage_limits": usage_limits,
            "usage": usage,
            "metadata": metadata,
            "infer_name": infer_name,
            "toolsets": toolsets,
            "builtin_tools": builtin_tools,
            "event_stream_handler": event_stream_handler,
            "conversation_id": conversation_id,
            "output_retries": output_retries,
            "capabilities": capabilities,
            "spec": spec,
        }
    )
