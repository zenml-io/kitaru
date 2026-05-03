"""Synthetic ZenML memory steps."""

from typing import Any

from zenml.enums import StepType
from zenml.steps.step_decorator import step

from kitaru.memory import _operations as operations
from kitaru.memory import _scope as scope_mod
from kitaru.memory._constants import _MEMORY_STEP_EXTRA_PREFIX, _list
from kitaru.memory._models import MemoryEntry


def _memory_step(*, name: str, operation: str):
    """Build a private synthetic ZenML step for one memory operation."""
    extra = {
        "kitaru": {
            **_MEMORY_STEP_EXTRA_PREFIX["kitaru"],
            "operation": operation,
        },
    }
    return step(
        name=name,
        enable_cache=False,
        step_type=StepType.TOOL_CALL,
        extra=extra,
    )


@_memory_step(name="kitaru_memory_set", operation="set")
def _memory_set_step(scope: str, scope_type: str, key: str, value: Any) -> None:
    """Synthetic non-cacheable step for `memory.set()`."""
    operations._set_impl(scope_mod._coerce_memory_scope(scope, scope_type), key, value)


@_memory_step(name="kitaru_memory_get", operation="get")
def _memory_get_step(
    scope: str,
    scope_type: str,
    key: str,
    version: int | None = None,
    strict: bool = False,
) -> Any:
    """Synthetic non-cacheable step for `memory.get()`.

    Return type is ``Any`` (not ``Any | None``) because ZenML step
    introspection does not reliably handle union return types for
    materializer selection on synthetic memory steps.
    """
    return operations._get_impl(
        scope_mod._coerce_memory_scope(scope, scope_type),
        key,
        version,
        strict=strict,
    )


@_memory_step(name="kitaru_memory_list", operation="list")
def _memory_list_step(scope: str, scope_type: str) -> _list[MemoryEntry]:
    """Synthetic non-cacheable step for `memory.list()`."""
    return operations._list_impl(scope_mod._coerce_memory_scope(scope, scope_type))


@_memory_step(name="kitaru_memory_history", operation="history")
def _memory_history_step(scope: str, scope_type: str, key: str) -> _list[MemoryEntry]:
    """Synthetic non-cacheable step for `memory.history()`."""
    return operations._history_impl(
        scope_mod._coerce_memory_scope(scope, scope_type), key
    )


@_memory_step(name="kitaru_memory_delete", operation="delete")
def _memory_delete_step(
    scope: str,
    scope_type: str,
    key: str,
) -> Any:
    """Synthetic non-cacheable step for `memory.delete()`.

    Return type is ``Any`` (not ``MemoryEntry | None``) because ZenML
    step introspection does not reliably handle union return types for
    materializer selection on synthetic memory steps.
    """
    return operations._delete_impl(
        scope_mod._coerce_memory_scope(scope, scope_type), key
    )
