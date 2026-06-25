"""Synthetic-checkpoint helpers for the fast-agent adapter.

No ``from __future__ import annotations`` here: this module builds synthetic
``@kitaru.checkpoint`` functions, and Kitaru/ZenML need real runtime
annotations rather than postponed string annotations.
"""

import asyncio
import dataclasses
import hashlib
import json
import math
import re
import sys
from collections.abc import Callable, Coroutine, Mapping, Sequence
from typing import Any, Literal, TypedDict, cast

from pydantic_core import to_jsonable_python

from kitaru._source_aliases import build_checkpoint_source_alias
from kitaru.checkpoint import _synthetic_checkpoint
from kitaru.errors import KitaruUsageError

CheckpointRuntime = Literal["inline", "isolated"]


class CheckpointConfig(TypedDict, total=False):
    """Kwargs forwarded to synthetic ``@kitaru.checkpoint(...)`` calls."""

    cache: bool | None
    runtime: CheckpointRuntime
    retries: int
    type: str


_ALLOWED_CHECKPOINT_CONFIG_KEYS = frozenset({"cache", "runtime", "retries", "type"})
_NON_WORD_PATTERN = re.compile(r"\W+")

if f"src.{__name__}" not in sys.modules:
    sys.modules[f"src.{__name__}"] = sys.modules[__name__]


def with_default_type(config: CheckpointConfig, default_type: str) -> CheckpointConfig:
    """Return ``config`` with ``type`` defaulted to ``default_type``."""
    if "type" in config:
        return config
    return {**config, "type": default_type}


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
        unknown = ", ".join(repr(key) for key in unknown_keys)
        allowed = ", ".join(
            repr(key) for key in sorted(_ALLOWED_CHECKPOINT_CONFIG_KEYS)
        )
        raise KitaruUsageError(
            f"Unsupported keys in {context}: {unknown}. Allowed keys are: {allowed}."
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
    cache = validated.get("cache")
    if cache is not None and not isinstance(cache, bool):
        raise KitaruUsageError(f"{context}.cache must be a boolean when provided.")
    return validated


def reject_isolated_runtime(config: CheckpointConfig | dict[str, Any]) -> None:
    """Raise if ``config`` requests ``runtime='isolated'``."""
    runtime = config.get("runtime")
    if runtime == "isolated":
        raise KitaruUsageError(
            "The fast-agent adapter does not yet support `runtime='isolated'` — "
            "checkpoint closures capture live fast-agent model/tool objects. Use "
            "`runtime='inline'` or omit `runtime`."
        )
    if runtime is not None and runtime != "inline":
        raise KitaruUsageError(
            f"Unsupported fast-agent checkpoint runtime {runtime!r}. "
            "Expected 'inline' or omit `runtime`."
        )


def materialize_step_output(value: Any) -> Any:
    """Unwrap a ZenML ``OutputArtifact`` handle to its concrete payload."""
    load = getattr(value, "load", None)
    return load() if callable(load) else value


def safe_step_name(value: str) -> str:
    """Normalize an agent/tool name into a valid synthetic checkpoint name."""
    normalized = _NON_WORD_PATTERN.sub("_", value.strip()).strip("_")
    return normalized or "fast_agent_call"


def checkpoint_input_value(value: Any) -> Any:
    """Return a stable JSON-friendly value for checkpoint inputs and cache keys.

    Unknown live Python objects are represented by their type and visible public
    attributes, not by ``repr(obj)``. The default ``repr`` for ordinary objects
    often includes a memory address, which would turn two equivalent calls into
    two different cache identities.
    """
    return _stable_value(value, seen=set())


def checkpoint_cache_key(payload: Any) -> str:
    """Return a stable hash for synthetic checkpoint inputs."""
    normalized = checkpoint_input_value(payload)
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


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

        def _call_without_inputs(_cache_key: str | None = None) -> Any:
            return body()

        call = _call_without_inputs
    elif input_names == {"call_input"}:

        def _call_with_call_input(
            call_input: Any,
            _cache_key: str | None = None,
        ) -> Any:
            return body()

        call = _call_with_call_input
    else:
        unsupported = ", ".join(sorted(input_names))
        raise KitaruUsageError(
            f"Unsupported synthetic checkpoint inputs: {unsupported}."
        )

    call.__name__ = safe_step_name(step_name)
    checkpoint_def = _synthetic_checkpoint(
        **config,
        flow_result_candidate=False,
    )(call)
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


def _stable_value(value: Any, *, seen: set[int]) -> Any:
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return {"float": str(value)}
        return value
    if isinstance(value, bytes):
        try:
            return {"bytes_utf8": value.decode("utf-8")}
        except UnicodeDecodeError:
            return {"bytes_hex": value.hex()}
    if isinstance(value, type):
        return {"type": _type_name(value)}

    object_id = id(value)
    if object_id in seen:
        return {"cycle": _type_name(type(value))}
    seen.add(object_id)
    try:
        converted = _convert_structured_value(value, seen=seen)
        if converted is not _UNHANDLED:
            return converted
        try:
            return to_jsonable_python(value, serialize_unknown=False)
        except (TypeError, ValueError) as error:
            raise KitaruUsageError(
                "Cannot build a stable fast-agent checkpoint input for "
                f"{_type_name(type(value))}. Pass primitive, dataclass, "
                "Pydantic, mapping, sequence, or public-attribute inputs so "
                "Kitaru can safely replay the call without using object identity."
            ) from error
    finally:
        seen.remove(object_id)


def _convert_structured_value(value: Any, *, seen: set[int]) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _stable_value(item, seen=seen)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, tuple):
        return [_stable_value(item, seen=seen) for item in value]
    if isinstance(value, list):
        return [_stable_value(item, seen=seen) for item in value]
    if isinstance(value, set | frozenset):
        return sorted(
            (_stable_value(item, seen=seen) for item in value),
            key=lambda item: json.dumps(item, sort_keys=True, default=str),
        )
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return _stable_value(dataclasses.asdict(value), seen=seen)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            return _stable_value(model_dump(mode="json"), seen=seen)
        except TypeError:
            return _stable_value(model_dump(), seen=seen)
    public_attrs = _public_attributes(value)
    if public_attrs:
        return {
            "python_type": _type_name(type(value)),
            "attributes": _stable_value(public_attrs, seen=seen),
        }
    return _UNHANDLED


def _public_attributes(value: Any) -> dict[str, Any]:
    attrs = getattr(value, "__dict__", None)
    if isinstance(attrs, Mapping):
        return {
            str(key): item
            for key, item in attrs.items()
            if isinstance(key, str) and not key.startswith("_") and not callable(item)
        }
    slots = getattr(type(value), "__slots__", ())
    if isinstance(slots, str):
        slots = (slots,)
    if not isinstance(slots, Sequence):
        return {}
    public: dict[str, Any] = {}
    for slot in slots:
        if not isinstance(slot, str) or slot.startswith("_"):
            continue
        if hasattr(value, slot):
            item = getattr(value, slot)
            if not callable(item):
                public[slot] = item
    return public


def _type_name(value: type[Any]) -> str:
    return f"{value.__module__}.{value.__qualname__}"


class _Unhandled:
    pass


_UNHANDLED = _Unhandled()
