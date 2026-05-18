"""Shared validation and synthetic-checkpoint helpers for LangGraph.

No ``from __future__ import annotations`` here: this module builds synthetic
``@kitaru.checkpoint`` functions, and Kitaru/ZenML need real runtime
annotations rather than postponed string annotations.
"""

import asyncio
import hashlib
import json
import re
import sys
from collections.abc import Callable, Coroutine, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Literal, cast

from pydantic_core import to_jsonable_python
from typing_extensions import TypedDict

import kitaru
from kitaru._source_aliases import build_checkpoint_source_alias
from kitaru.errors import KitaruUsageError

CheckpointRuntime = Literal["inline", "isolated"]
GraphCheckpointStrategy = Literal["graph_call", "calls"]


class CheckpointConfig(TypedDict, total=False):
    """Kwargs forwarded to synthetic ``@kitaru.checkpoint(...)`` calls."""

    runtime: CheckpointRuntime
    retries: int
    type: str
    cache: bool


ToolCheckpointOverride = CheckpointConfig | Literal[False]
ToolCheckpointOverrides = Mapping[str, ToolCheckpointOverride]


@dataclass(frozen=True)
class AdapterCheckpointArtifactRefs:
    """Display references for artifacts owned by a synthetic checkpoint."""

    input_artifacts: Mapping[str, str]
    output_artifacts: Mapping[str, str]


_ADAPTER_CHECKPOINT_ARTIFACT_REFS: ContextVar[AdapterCheckpointArtifactRefs | None] = (
    ContextVar("kitaru_langgraph_adapter_checkpoint_artifact_refs", default=None)
)

_ALLOWED_CHECKPOINT_CONFIG_KEYS = frozenset({"runtime", "retries", "type", "cache"})
_VALID_CHECKPOINT_STRATEGIES = frozenset({"graph_call", "calls"})
_VALID_CHECKPOINT_STRATEGY_DISPLAY = ("graph_call", "calls")
_NON_WORD_PATTERN = re.compile(r"\W+")

# Synthetic checkpoint functions are registered dynamically from this module.
# ZenML may later import the same code through either ``kitaru...`` or
# ``src.kitaru...`` dotted paths, so expose both module keys to keep source
# alias lookup stable during local/worktree execution.
if f"src.{__name__}" not in sys.modules:
    sys.modules[f"src.{__name__}"] = sys.modules[__name__]


def validate_checkpoint_strategy(value: str) -> GraphCheckpointStrategy:
    """Validate the public LangGraph checkpoint vocabulary."""
    if value in _VALID_CHECKPOINT_STRATEGIES:
        return cast(GraphCheckpointStrategy, value)
    expected = "', '".join(_VALID_CHECKPOINT_STRATEGY_DISPLAY)
    raise KitaruUsageError(
        f"Unsupported LangGraph checkpoint strategy {value!r}. "
        f"Expected one of: '{expected}'."
    )


def validate_checkpoint_config(
    config: CheckpointConfig | None,
    *,
    context: str,
) -> CheckpointConfig | None:
    """Return a validated shallow copy of ``config``."""
    if config is None:
        return None
    if not isinstance(config, Mapping):
        raise KitaruUsageError(f"{context} must be a mapping.")
    unknown_keys = sorted(set(config) - _ALLOWED_CHECKPOINT_CONFIG_KEYS)
    if unknown_keys:
        unknown = ", ".join(unknown_keys)
        raise KitaruUsageError(
            f"Unsupported keys in {context}: {unknown}. Allowed keys are: "
            "'cache', 'runtime', 'retries', 'type'."
        )
    validated = cast(CheckpointConfig, dict(config))
    runtime = validated.get("runtime")
    if runtime == "isolated":
        raise KitaruUsageError(
            "The LangGraph adapter does not yet support `runtime='isolated'` "
            "because checkpoint closures capture live graph objects. Use "
            "`runtime='inline'` or omit `runtime`."
        )
    if runtime is not None and runtime != "inline":
        raise KitaruUsageError(
            f"Unsupported LangGraph checkpoint runtime {runtime!r}. "
            "Expected 'inline' or omit `runtime`."
        )
    retries = validated.get("retries")
    if retries is not None and (
        not isinstance(retries, int) or isinstance(retries, bool) or retries < 0
    ):
        raise KitaruUsageError(
            f"{context}.retries must be a non-negative integer when provided."
        )
    checkpoint_type = validated.get("type")
    if checkpoint_type is not None and (
        not isinstance(checkpoint_type, str) or not checkpoint_type.strip()
    ):
        raise KitaruUsageError(
            f"{context}.type must be a non-empty string when provided."
        )
    cache = validated.get("cache")
    if cache is not None and not isinstance(cache, bool):
        raise KitaruUsageError(f"{context}.cache must be a boolean when provided.")
    return validated


def validate_tool_checkpoint_overrides(
    overrides: ToolCheckpointOverrides | None,
    *,
    context: str,
) -> dict[str, ToolCheckpointOverride] | None:
    """Return validated per-tool checkpoint overrides."""
    if overrides is None:
        return None
    if not isinstance(overrides, Mapping):
        raise KitaruUsageError(f"{context} must be a mapping.")
    normalized: dict[str, ToolCheckpointOverride] = {}
    for tool_name, override in overrides.items():
        if not isinstance(tool_name, str) or not tool_name.strip():
            raise KitaruUsageError(
                f"{context} keys must be non-empty tool name strings."
            )
        if override is False:
            normalized[tool_name] = False
            continue
        validated = validate_checkpoint_config(
            override, context=f"{context}[{tool_name!r}]"
        )
        normalized[tool_name] = validated if validated is not None else {}
    return normalized


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


def merge_config(request: Any) -> dict[str, Any]:
    """Merge request config while forcing the stable LangGraph thread ID."""
    config = dict(request.config)
    base_configurable = config.get("configurable", {})
    if not isinstance(base_configurable, Mapping):
        raise KitaruUsageError("request.config['configurable'] must be a mapping.")
    configurable = {
        **dict(base_configurable),
        **dict(request.configurable),
        "thread_id": request.thread_id,
    }
    if request.checkpoint_id is not None:
        configurable["checkpoint_id"] = request.checkpoint_id
    if request.checkpoint_ns is not None:
        configurable["checkpoint_ns"] = request.checkpoint_ns
    config["configurable"] = configurable
    return config


def checkpoint_cache_key(payload: Any) -> str:
    """Return a stable hash for synthetic checkpoint inputs."""
    try:
        normalized = to_jsonable_python(payload, serialize_unknown=True)
    except ValueError:
        normalized = {"python_type": type(payload).__name__}
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        default=repr,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def materialize_step_output(value: Any) -> Any:
    """Unwrap a ZenML ``OutputArtifact`` handle to its concrete payload."""
    load = getattr(value, "load", None)
    return load() if callable(load) else value


def safe_step_name(value: str) -> str:
    """Normalize a graph name into a valid synthetic checkpoint name."""
    normalized = _NON_WORD_PATTERN.sub("_", value.strip()).strip("_")
    return normalized or "langgraph_call"


def _build_checkpoint_step(
    *,
    config: CheckpointConfig,
    step_name: str,
    body: Callable[[], Any],
    checkpoint_inputs: Mapping[str, Any] | None = None,
) -> Callable[..., Any]:
    input_names = frozenset(checkpoint_inputs or {})
    if input_names == frozenset():

        def _call_without_inputs(_cache_key: str | None = None) -> Any:
            return body()

        call = _call_without_inputs
    elif input_names == {"model_input"}:

        def _call_with_model_input(
            model_input: Any,
            _cache_key: str | None = None,
        ) -> Any:
            return body()

        call = _call_with_model_input
    elif input_names == {"tool_args"}:

        def _call_with_tool_args(
            tool_args: Any,
            _cache_key: str | None = None,
        ) -> Any:
            return body()

        call = _call_with_tool_args
    else:
        unsupported = ", ".join(sorted(input_names))
        raise KitaruUsageError(
            f"Unsupported synthetic checkpoint inputs: {unsupported}."
        )

    call.__name__ = safe_step_name(step_name)
    checkpoint_def = kitaru.checkpoint(**config)(call)
    step_obj = getattr(checkpoint_def, "_step", None)
    if step_obj is not None:
        alias = build_checkpoint_source_alias(call.__name__)
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
    """Run a sync body inside a synthetic ``@kitaru.checkpoint``."""
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
    """Run an async body inside a synthetic ``@kitaru.checkpoint``."""
    # Kitaru checkpoints are currently sync callables. The checkpoint body owns
    # a fresh event loop, while the caller awaits the materialized result from a
    # worker thread so an existing user event loop is not nested.
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
