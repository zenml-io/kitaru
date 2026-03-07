"""Internal runtime support for the Kitaru SDK.

This module provides shared utilities used across the SDK implementation.
It is not part of the public API surface.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import NoReturn


@dataclass(frozen=True)
class _FlowScope:
    """Internal runtime context for the currently executing flow."""

    name: str


@dataclass(frozen=True)
class _CheckpointScope:
    """Internal runtime context for the currently executing checkpoint."""

    name: str
    type: str | None


_CURRENT_FLOW_SCOPE: ContextVar[_FlowScope | None] = ContextVar(
    "kitaru_current_flow_scope",
    default=None,
)
_CURRENT_CHECKPOINT_SCOPE: ContextVar[_CheckpointScope | None] = ContextVar(
    "kitaru_current_checkpoint_scope",
    default=None,
)


@contextmanager
def _flow_scope(*, name: str) -> Iterator[None]:
    """Set flow runtime scope for the active execution context."""
    token = _CURRENT_FLOW_SCOPE.set(_FlowScope(name=name))
    try:
        yield
    finally:
        _CURRENT_FLOW_SCOPE.reset(token)


@contextmanager
def _checkpoint_scope(*, name: str, checkpoint_type: str | None) -> Iterator[None]:
    """Set checkpoint runtime scope for the active execution context."""
    token = _CURRENT_CHECKPOINT_SCOPE.set(
        _CheckpointScope(name=name, type=checkpoint_type)
    )
    try:
        yield
    finally:
        _CURRENT_CHECKPOINT_SCOPE.reset(token)


def _get_current_flow() -> _FlowScope | None:
    """Get the currently active flow scope, if any."""
    return _CURRENT_FLOW_SCOPE.get()


def _is_inside_flow() -> bool:
    """Check whether code is currently running inside a flow."""
    return _get_current_flow() is not None


def _get_current_checkpoint() -> _CheckpointScope | None:
    """Get the currently active checkpoint scope, if any."""
    return _CURRENT_CHECKPOINT_SCOPE.get()


def _is_inside_checkpoint() -> bool:
    """Check whether code is currently running inside a checkpoint."""
    return _get_current_checkpoint() is not None


def _not_implemented(name: str) -> NoReturn:
    """Raise NotImplementedError with a consistent message."""
    raise NotImplementedError(
        f"kitaru.{name}() is not yet implemented. "
        f"The Kitaru SDK is under active development."
    )
