"""Replay runtime context transported to replay executions."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

KITARU_REPLAY_CONTEXT_ENV = "KITARU_REPLAY_CONTEXT"


@dataclass(frozen=True)
class ReplayRuntimeContext:
    """Runtime replay overrides read by checkpoints during a replay run."""

    at: str
    output_mocks: dict[str, Any] = field(default_factory=dict)
    tool_overrides: dict[str, str] = field(default_factory=dict)
    llm_model: str | None = None
    llm_model_at: str | None = None
    input_overrides: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_json(self) -> str:
        payload = {
            "at": self.at,
            "output_mocks": self.output_mocks,
            "tool_overrides": self.tool_overrides,
            "llm_model": self.llm_model,
            "llm_model_at": self.llm_model_at,
            "input_overrides": self.input_overrides,
        }
        return json.dumps(payload, default=str)

    @classmethod
    def from_json(cls, raw: str) -> ReplayRuntimeContext:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("Replay context payload must be a JSON object.")
        return cls(
            at=str(payload.get("at") or ""),
            output_mocks=dict(payload.get("output_mocks") or {}),
            tool_overrides=dict(payload.get("tool_overrides") or {}),
            llm_model=payload.get("llm_model"),
            llm_model_at=payload.get("llm_model_at"),
            input_overrides=dict(payload.get("input_overrides") or {}),
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


def resolve_tool_override(name: str) -> Any | None:
    """Import and return a tool override callable, if configured."""
    context = get_replay_runtime_context()
    if context is None:
        return None
    import_path = context.tool_overrides.get(name)
    if not import_path:
        base = name.removesuffix("_tool")
        import_path = context.tool_overrides.get(base)
    if not import_path:
        return None
    module_path, _, attr = str(import_path).rpartition(".")
    if not module_path or not attr:
        return None
    import importlib

    module = importlib.import_module(module_path)
    resolved = getattr(module, attr, None)
    return resolved


__all__ = [
    "KITARU_REPLAY_CONTEXT_ENV",
    "ReplayRuntimeContext",
    "get_replay_runtime_context",
    "resolve_tool_override",
]
