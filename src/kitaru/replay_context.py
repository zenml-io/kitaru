"""Replay runtime context transported to replay executions."""

from __future__ import annotations

import importlib
import json
import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

from kitaru._source_aliases import normalize_checkpoint_name
from kitaru.errors import KitaruRuntimeError

KITARU_REPLAY_CONTEXT_ENV = "KITARU_REPLAY_CONTEXT"


@dataclass(frozen=True)
class ReplayRuntimeContext:
    """Runtime replay overrides read by checkpoints during a replay run.

    ``code_overrides`` and ``model_overrides`` are keyed by the effective replay
    target identities resolved during planning: invocation ID, checkpoint call ID,
    and checkpoint name when available. Runtime consumers then look up the most
    specific identity they know instead of applying one global override to every
    tool or LLM call.
    """

    at: str
    output_mocks: dict[str, Any] = field(default_factory=dict)
    code_overrides: dict[str, str] = field(default_factory=dict)
    model_overrides: dict[str, str] = field(default_factory=dict)
    input_overrides: dict[str, dict[str, Any]] = field(default_factory=dict)
    tool_overrides: dict[str, str] = field(default_factory=dict)
    llm_model: str | None = None
    llm_model_at: str | None = None

    def to_json(self) -> str:
        payload = {
            "at": self.at,
            "output_mocks": self.output_mocks,
            "code_overrides": self.code_overrides,
            "model_overrides": self.model_overrides,
            "input_overrides": self.input_overrides,
        }
        if self.tool_overrides:
            payload["tool_overrides"] = self.tool_overrides
        if self.llm_model:
            payload["llm_model"] = self.llm_model
        if self.llm_model_at:
            payload["llm_model_at"] = self.llm_model_at
        return json.dumps(payload, default=str)

    @classmethod
    def from_json(cls, raw: str) -> ReplayRuntimeContext:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("Replay context payload must be a JSON object.")
        tool_overrides = dict(payload.get("tool_overrides") or {})
        code_overrides = dict(payload.get("code_overrides") or {})
        if tool_overrides:
            # Older serialized payloads stored code swaps under this key.
            code_overrides = {**tool_overrides, **code_overrides}
        llm_model = payload.get("llm_model")
        model_overrides = dict(payload.get("model_overrides") or {})
        llm_model_at = payload.get("llm_model_at")
        if llm_model and llm_model_at and llm_model_at not in model_overrides:
            model_overrides[str(llm_model_at)] = str(llm_model)
        return cls(
            at=str(payload.get("at") or ""),
            output_mocks=dict(payload.get("output_mocks") or {}),
            code_overrides=code_overrides,
            model_overrides=model_overrides,
            input_overrides=dict(payload.get("input_overrides") or {}),
            tool_overrides=tool_overrides,
            llm_model=llm_model,
            llm_model_at=llm_model_at,
        )


@lru_cache(maxsize=1)
def get_replay_runtime_context() -> ReplayRuntimeContext | None:
    """Return replay context from the environment, if this is a replay run."""
    raw = os.environ.get(KITARU_REPLAY_CONTEXT_ENV)
    if not raw:
        return None
    try:
        return ReplayRuntimeContext.from_json(raw)
    except (json.JSONDecodeError, ValueError, TypeError):
        return None


def is_replay() -> bool:
    """Return whether the current process is running inside a replay execution."""
    return KITARU_REPLAY_CONTEXT_ENV in os.environ


def _lookup_override(mapping: dict[str, str], *keys: str | None) -> str | None:
    for key in keys:
        if not key:
            continue
        if key in mapping:
            return mapping[key]
        normalized = normalize_checkpoint_name(key)
        if normalized in mapping:
            return mapping[normalized]
        base = normalized.removesuffix("_tool")
        if base in mapping:
            return mapping[base]
    return None


def resolve_tool_override(name: str, *, target: str | None = None) -> Any | None:
    """Import and return a targeted tool code override callable, if configured."""
    context = get_replay_runtime_context()
    if context is None:
        return None
    import_path = _lookup_override(context.code_overrides, target, name)
    if not import_path:
        return None

    import_path = str(import_path)
    module_path, _, attr = import_path.rpartition(".")
    target_label = target or name
    if not module_path or not attr:
        raise KitaruRuntimeError(
            "Replay code override for "
            f"{target_label!r} must be a dotted import path, got {import_path!r}."
        )

    try:
        module = importlib.import_module(module_path)
    except Exception as exc:
        raise KitaruRuntimeError(
            "Could not import replay code override "
            f"{import_path!r} for {target_label!r}: {exc}"
        ) from exc

    if not hasattr(module, attr):
        raise KitaruRuntimeError(
            f"Replay code override {import_path!r} for {target_label!r} does not exist."
        )
    resolved = getattr(module, attr)
    if not callable(resolved):
        raise KitaruRuntimeError(
            "Replay code override "
            f"{import_path!r} for {target_label!r} is not callable."
        )
    return resolved


def resolve_model_override(*targets: str | None) -> str | None:
    """Return a targeted model override for the current LLM checkpoint, if any."""
    context = get_replay_runtime_context()
    if context is None:
        return None
    return _lookup_override(context.model_overrides, *targets)


__all__ = [
    "KITARU_REPLAY_CONTEXT_ENV",
    "ReplayRuntimeContext",
    "get_replay_runtime_context",
    "is_replay",
    "resolve_model_override",
    "resolve_tool_override",
]
