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
from collections.abc import Callable, Coroutine, Mapping
from typing import Any, Literal, cast

from pydantic_core import to_jsonable_python
from typing_extensions import TypedDict

import kitaru
from kitaru._source_aliases import build_checkpoint_source_alias
from kitaru.errors import KitaruUsageError

CheckpointRuntime = Literal["inline", "isolated"]
GraphCheckpointStrategy = Literal["graph_call"]


class CheckpointConfig(TypedDict, total=False):
    """Kwargs forwarded to synthetic ``@kitaru.checkpoint(...)`` calls."""

    runtime: CheckpointRuntime
    retries: int
    type: str
    cache: bool


_ALLOWED_CHECKPOINT_CONFIG_KEYS = frozenset({"runtime", "retries", "type", "cache"})
_VALID_CHECKPOINT_STRATEGIES = frozenset({"graph_call"})
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
    raise KitaruUsageError(
        "Unsupported LangGraph checkpoint strategy "
        f"{value!r}. Expected `checkpoint_strategy='graph_call'`."
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
    *, config: CheckpointConfig, step_name: str, body: Callable[[], Any]
) -> Callable[..., Any]:
    def _call(_cache_key: str | None = None) -> Any:
        return body()

    _call.__name__ = safe_step_name(step_name)
    checkpoint_def = kitaru.checkpoint(**config)(_call)
    step_obj = getattr(checkpoint_def, "_step", None)
    if step_obj is not None:
        alias = build_checkpoint_source_alias(_call.__name__)
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
) -> Any:
    """Run a sync body inside a synthetic ``@kitaru.checkpoint``."""
    checkpoint_def = _build_checkpoint_step(
        config=config,
        step_name=step_name,
        body=body,
    )
    return materialize_step_output(checkpoint_def(cache_key))


async def run_async_in_checkpoint(
    *,
    config: CheckpointConfig,
    step_name: str,
    body: Callable[[], Coroutine[Any, Any, Any]],
    cache_key: str | None = None,
) -> Any:
    """Run an async body inside a synthetic ``@kitaru.checkpoint``."""
    # Kitaru checkpoints are currently sync callables. The checkpoint body owns
    # a fresh event loop, while the caller awaits the materialized result from a
    # worker thread so an existing user event loop is not nested.
    checkpoint_def = _build_checkpoint_step(
        config=config,
        step_name=step_name,
        body=lambda: asyncio.run(body()),
    )
    step_output = await asyncio.to_thread(checkpoint_def, cache_key)
    return materialize_step_output(step_output)
