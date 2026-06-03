"""Shared validation and synthetic-checkpoint helpers for Gemini.

No ``from __future__ import annotations`` here: this module builds synthetic
``@kitaru.checkpoint`` functions, and Kitaru/ZenML need real runtime
annotations rather than postponed string annotations.
"""

import asyncio
import hashlib
import json
import re
import sys
import time
from collections.abc import Callable, Coroutine, Mapping
from typing import Any, Literal, cast

from pydantic_core import to_jsonable_python
from typing_extensions import TypedDict

from kitaru._source_aliases import build_checkpoint_source_alias
from kitaru.checkpoint import _synthetic_checkpoint
from kitaru.errors import KitaruUsageError

CheckpointRuntime = Literal["inline", "isolated"]
CheckpointStrategy = Literal["interaction"]


class CheckpointConfig(TypedDict, total=False):
    """Kwargs forwarded to synthetic ``@kitaru.checkpoint(...)`` calls."""

    cache: bool | None
    runtime: CheckpointRuntime
    retries: int
    type: str


_ALLOWED_CHECKPOINT_CONFIG_KEYS = frozenset({"cache", "runtime", "retries", "type"})
_VALID_CHECKPOINT_STRATEGIES = frozenset({"interaction"})
_REJECTED_GRANULAR_STRATEGIES = frozenset(
    {
        "calls",
        "runner_call",
        "granular",
        "model_call",
        "tool_call",
        "client_tools",
        "antigravity_steps",
        "managed_agent_steps",
        "step",
        "run",
    }
)
_NON_WORD_PATTERN = re.compile(r"\W+")

if f"src.{__name__}" not in sys.modules:
    sys.modules[f"src.{__name__}"] = sys.modules[__name__]


def validate_checkpoint_strategy(value: str) -> CheckpointStrategy:
    """Validate the public Gemini checkpoint vocabulary."""
    if value in _VALID_CHECKPOINT_STRATEGIES:
        return cast(CheckpointStrategy, value)
    if value in _REJECTED_GRANULAR_STRATEGIES:
        raise KitaruUsageError(
            "Gemini Interactions adapter v0.1 only supports "
            "checkpoint_strategy='interaction'. It records one stable Google "
            "interaction response and does not checkpoint Gemini-internal model, "
            "managed-agent, Antigravity, tool, web, code, or sandbox steps."
        )
    raise KitaruUsageError(
        "Unsupported Gemini Interactions checkpoint strategy "
        f"{value!r}. Expected 'interaction'."
    )


def reject_isolated_runtime(config: CheckpointConfig | dict[str, Any]) -> None:
    """Raise if a checkpoint config requests ``runtime='isolated'``."""
    runtime = config.get("runtime")
    if runtime == "isolated":
        raise KitaruUsageError(
            "The Gemini Interactions adapter v0.1 does not support "
            "`runtime='isolated'` — checkpoint closures may capture a live "
            "Google client, callbacks, or in-process tool data. Use "
            "`runtime='inline'` or omit `runtime`."
        )
    if runtime is not None and runtime != "inline":
        raise KitaruUsageError(
            f"Unsupported Gemini checkpoint runtime {runtime!r}. Expected "
            "'inline' or omit `runtime`."
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
    reject_isolated_runtime(validated)
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
        raise KitaruUsageError(f"{context}.type must be a non-empty string.")
    cache = validated.get("cache")
    if cache is not None and not isinstance(cache, bool):
        raise KitaruUsageError(f"{context}.cache must be a boolean when provided.")
    return validated


def materialize_step_output(value: Any) -> Any:
    """Unwrap a ZenML ``OutputArtifact`` handle to its concrete payload."""
    load = getattr(value, "load", None)
    return load() if callable(load) else value


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


def elapsed_ms(started_at: float) -> float:
    """Return elapsed milliseconds with millisecond precision."""
    return round((time.perf_counter() - started_at) * 1000, 3)


def normalize_identifier(value: str | None, *, fallback: str) -> str:
    """Normalize free-form text into ``[A-Za-z0-9_]`` chunks."""
    normalized = _NON_WORD_PATTERN.sub("_", (value or "").strip()).strip("_")
    return normalized or fallback


def safe_step_name(value: str) -> str:
    """Normalize an SDK name into a valid synthetic checkpoint name."""
    return normalize_identifier(value, fallback="gemini_interaction")


def _build_checkpoint_step(
    *, config: CheckpointConfig, step_name: str, body: Callable[[], Any]
) -> Callable[..., Any]:
    reject_isolated_runtime(config)

    def _call(_cache_key: str | None = None) -> Any:
        return body()

    _call.__name__ = safe_step_name(step_name)
    checkpoint_def = _synthetic_checkpoint(
        **config,
        flow_result_candidate=False,
    )(_call)
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
    checkpoint_def = _build_checkpoint_step(
        config=config,
        step_name=step_name,
        body=lambda: asyncio.run(body()),
    )
    step_output = await asyncio.to_thread(checkpoint_def, cache_key)
    return materialize_step_output(step_output)
