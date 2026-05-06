"""Shared validation and synthetic-checkpoint helpers for the OpenAI adapter.

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

import kitaru
from kitaru._source_aliases import build_checkpoint_source_alias
from kitaru.errors import KitaruUsageError

CheckpointRuntime = Literal["inline", "isolated"]
CheckpointStrategy = Literal["calls", "runner_call"]


class CheckpointConfig(TypedDict, total=False):
    """Kwargs forwarded to synthetic ``@kitaru.checkpoint(...)`` calls."""

    runtime: CheckpointRuntime
    retries: int
    type: str


ToolCheckpointOverride = CheckpointConfig | Literal[False]
ToolCheckpointOverrides = Mapping[str, ToolCheckpointOverride]

_ALLOWED_CHECKPOINT_CONFIG_KEYS = frozenset({"runtime", "retries", "type"})
_VALID_CHECKPOINT_STRATEGIES = frozenset({"calls", "runner_call"})
_LEGACY_MODE_NAMES = frozenset({"step", "run"})
_NON_WORD_PATTERN = re.compile(r"\W+")

if f"src.{__name__}" not in sys.modules:
    sys.modules[f"src.{__name__}"] = sys.modules[__name__]


def validate_checkpoint_strategy(value: str) -> CheckpointStrategy:
    """Validate the public OpenAI checkpoint vocabulary."""
    if value in _VALID_CHECKPOINT_STRATEGIES:
        return cast(CheckpointStrategy, value)
    if value in _LEGACY_MODE_NAMES:
        raise KitaruUsageError(
            "Unsupported OpenAI Agents checkpoint strategy "
            f"{value!r}. Use `checkpoint_strategy='calls'` for model/tool "
            "call checkpoints or `checkpoint_strategy='runner_call'` for one "
            "checkpoint around `Runner.run(...)`."
        )
    expected = "', '".join(sorted(_VALID_CHECKPOINT_STRATEGIES))
    raise KitaruUsageError(
        f"Unsupported OpenAI Agents checkpoint strategy {value!r}. "
        f"Expected one of: '{expected}'."
    )


def reject_isolated_runtime(config: CheckpointConfig | dict[str, Any]) -> None:
    """Raise if a checkpoint config requests ``runtime='isolated'``."""
    runtime = config.get("runtime")
    if runtime == "isolated":
        raise KitaruUsageError(
            "The OpenAI Agents adapter does not yet support "
            "`runtime='isolated'` — checkpoint closures will capture live "
            "OpenAI SDK objects. Use `runtime='inline'` or omit `runtime`."
        )
    if runtime is not None and runtime != "inline":
        raise KitaruUsageError(
            "Unsupported OpenAI Agents checkpoint runtime "
            f"{runtime!r}. Expected 'inline' or omit `runtime`."
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
            f"Unsupported keys in {context}: {unknown}. "
            "Allowed keys are: 'runtime', 'retries', 'type'."
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
        raise KitaruUsageError(
            f"{context}.type must be a non-empty string when provided."
        )
    return validated


def validate_tool_checkpoint_overrides(
    overrides: ToolCheckpointOverrides | None,
    *,
    context: str,
) -> dict[str, ToolCheckpointOverride] | None:
    """Return validated per-tool checkpoint overrides."""
    if overrides is None:
        return None
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


def resolve_tool_checkpoint_config(
    tool_name: str,
    *,
    default: CheckpointConfig | None,
    by_name: ToolCheckpointOverrides | None,
) -> CheckpointConfig | None:
    """Pick the checkpoint config for a tool, honoring per-name opt-outs."""
    if by_name and tool_name in by_name:
        override = by_name[tool_name]
        return None if override is False else override
    return default


def materialize_step_output(value: Any) -> Any:
    """Unwrap a ZenML ``OutputArtifact`` handle to its concrete payload."""
    load = getattr(value, "load", None)
    return load() if callable(load) else value


def checkpoint_cache_key(payload: Any) -> str:
    """Return a stable hash for synthetic checkpoint inputs."""
    try:
        normalized = to_jsonable_python(payload, serialize_unknown=True)
    except ValueError:
        normalized = {"repr": repr(payload), "python_type": type(payload).__name__}
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


def safe_step_name(value: str) -> str:
    """Normalize an SDK/tool name into a valid synthetic checkpoint name."""
    normalized = _NON_WORD_PATTERN.sub("_", value.strip()).strip("_")
    return normalized or "openai_agents_call"


def _build_checkpoint_step(
    *, config: CheckpointConfig, step_name: str, body: Callable[[], Any]
) -> Callable[..., Any]:
    reject_isolated_runtime(config)

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
    """Run an async body inside a synthetic ``@kitaru.checkpoint``.

    ``kitaru.checkpoint`` is sync-only. We bridge by running the coroutine from
    a sync checkpoint function in a worker thread, matching the PydanticAI
    adapter's granular-checkpoint pattern.
    """
    checkpoint_def = _build_checkpoint_step(
        config=config,
        step_name=step_name,
        body=lambda: asyncio.run(body()),
    )
    step_output = await asyncio.to_thread(checkpoint_def, cache_key)
    return materialize_step_output(step_output)
