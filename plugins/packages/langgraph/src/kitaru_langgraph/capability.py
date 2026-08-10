#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
"""Public capability contracts for the LangGraph adapter."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class LangGraphAdapterError(RuntimeError):
    """Base error raised by the LangGraph adapter."""


class UnsupportedInvocationError(LangGraphAdapterError):
    """Raised when an invocation mode is not recorded by the adapter."""


class UnsupportedCapabilityError(LangGraphAdapterError):
    """Raised when replay requests an unavailable adapter capability."""


class UnsupportedWorkerInterruptError(LangGraphAdapterError):
    """Raised when a worker invocation returns an interrupt."""


class ToolPolicyError(LangGraphAdapterError):
    """Raised when a replay tool policy cannot be applied."""


class ToolPolicyMissError(ToolPolicyError):
    """Raised when a replay tool lookup misses with fail behavior."""


class CapabilityOperation(StrEnum):
    """Operation whose availability can be inspected before execution."""

    RECORD = "record"
    REPLACE_INPUT = "replace_input"
    OVERRIDE_MODEL = "override_model"
    OVERRIDE_PROMPT = "override_prompt"
    OVERRIDE_SYSTEM_PROMPT = "override_system_prompt"
    OVERRIDE_MODEL_PARAMS = "override_model_params"
    SUBSTITUTE_TOOL_RESULT = "substitute_tool_result"


class CapabilityTargetKind(StrEnum):
    """Kind of agent boundary described by one capability target."""

    MAIN = "main"
    LOCAL_SUBAGENT = "local_subagent"
    OPAQUE = "opaque"


@dataclass(frozen=True)
class CapabilityTarget:
    """Immutable operation set for one observable agent boundary."""

    name: str
    kind: CapabilityTargetKind
    operations: frozenset[CapabilityOperation]

    def supports(self, operation: CapabilityOperation) -> bool:
        """Return whether this target supports an operation."""
        return operation in self.operations


@dataclass(frozen=True)
class LangGraphCapabilityView:
    """Immutable public view of capabilities actually injected by Kitaru."""

    targets: tuple[CapabilityTarget, ...]

    def get_target(self, name: str) -> CapabilityTarget | None:
        """Return a named target when it is declared."""
        return next((target for target in self.targets if target.name == name), None)


@dataclass(frozen=True)
class LocalSubagentFactorySpec:
    """Describe one local subagent built by an exact supported public factory.

    The ``factory`` must be the imported ``langchain.agents.create_agent`` or
    ``deepagents.create_deep_agent`` object, not a wrapper around either one.
    """

    name: str
    factory: Callable[..., Any]
    factory_kwargs: Mapping[str, Any] = field(default_factory=dict)
    description: str = "Kitaru-instrumented local subagent"

    def copied_kwargs(self) -> dict[str, Any]:
        """Return a mutable copy for one factory call."""
        return dict(self.factory_kwargs)


@dataclass(frozen=True)
class _CapabilityManifest:
    """Bind a public view to the exact middleware instance that created it."""

    middleware: object
    view: LangGraphCapabilityView


_DIRECT_OPERATIONS = frozenset(
    {CapabilityOperation.RECORD, CapabilityOperation.REPLACE_INPUT}
)
_FACTORY_OPERATIONS = frozenset(CapabilityOperation)


def _make_capability_manifest(
    middleware: object,
    *,
    local_subagents: tuple[str, ...] = (),
    opaque_targets: tuple[str, ...] = (),
) -> _CapabilityManifest:
    """Create a manifest from middleware injected by this adapter."""
    targets = [
        CapabilityTarget(
            name="main",
            kind=CapabilityTargetKind.MAIN,
            operations=_FACTORY_OPERATIONS,
        )
    ]
    targets.extend(
        CapabilityTarget(
            name=name,
            kind=CapabilityTargetKind.LOCAL_SUBAGENT,
            operations=_FACTORY_OPERATIONS,
        )
        for name in local_subagents
    )
    targets.extend(
        CapabilityTarget(
            name=name,
            kind=CapabilityTargetKind.OPAQUE,
            operations=frozenset({CapabilityOperation.RECORD}),
        )
        for name in opaque_targets
    )
    return _CapabilityManifest(
        middleware=middleware,
        view=LangGraphCapabilityView(targets=tuple(targets)),
    )


def _direct_capability_view() -> LangGraphCapabilityView:
    """Return the capability view for a directly wrapped runnable."""
    return LangGraphCapabilityView(
        targets=(
            CapabilityTarget(
                name="main",
                kind=CapabilityTargetKind.MAIN,
                operations=_DIRECT_OPERATIONS,
            ),
        )
    )


def _require_operation(
    view: LangGraphCapabilityView,
    operation: CapabilityOperation,
    *,
    target: str = "main",
) -> None:
    """Fail before delegation when an operation is not declared."""
    capability = view.get_target(target)
    if capability is None or not capability.supports(operation):
        raise UnsupportedCapabilityError(
            f"LangGraph target {target!r} does not support {operation.value!r}"
        )


def _validate_manifest(
    manifest: _CapabilityManifest | None, middleware: object | None
) -> LangGraphCapabilityView:
    """Return a bound manifest view or reject detached capability data."""
    if manifest is None:
        return _direct_capability_view()
    if middleware is None or manifest.middleware is not middleware:
        raise UnsupportedCapabilityError(
            "LangGraph capabilities are detached from Kitaru middleware"
        )
    return manifest.view
