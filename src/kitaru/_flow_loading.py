"""Shared flow-target loading helpers used by CLI and MCP surfaces."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class _FlowHandleLike(Protocol):
    """Protocol for flow handles returned by `.run()` / `.deploy()`."""

    @property
    def exec_id(self) -> str: ...


@runtime_checkable
class _FlowTarget(Protocol):
    """Protocol for flow objects that support `.run()` and `.deploy()`."""

    def run(self, *args: Any, **kwargs: Any) -> _FlowHandleLike: ...

    def deploy(self, *args: Any, **kwargs: Any) -> _FlowHandleLike: ...


def _load_module_from_python_path(
    module_path: str,
    *,
    module_name_prefix: str,
) -> ModuleType:
    """Load a Python module from a filesystem path."""
    path = Path(module_path).expanduser().resolve()
    if not path.exists():
        raise ValueError(f"Flow module path does not exist: {module_path}")
    if path.suffix != ".py":
        raise ValueError(
            "Flow target file must be a Python file ending in `.py` "
            f"(received: {module_path})."
        )

    module_name = f"{module_name_prefix}{abs(hash(path))}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Unable to load Python module from path: {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_flow_target(
    target: str,
    *,
    module_name_prefix: str,
) -> _FlowTarget:
    """Load `<module_or_file>:<flow_name>` into a runnable flow object."""
    module_ref, separator, attr_name = target.partition(":")
    if separator != ":" or not module_ref or not attr_name:
        raise ValueError(
            "Flow target must use `<module_or_file>:<flow_name>` format "
            f"(received: {target!r})."
        )

    try:
        if module_ref.endswith(".py"):
            module = _load_module_from_python_path(
                module_ref, module_name_prefix=module_name_prefix
            )
        else:
            module = importlib.import_module(module_ref)
    except Exception as exc:
        raise ValueError(f"Unable to import flow module `{module_ref}`: {exc}") from exc

    try:
        flow_obj = getattr(module, attr_name)
    except AttributeError as exc:
        raise ValueError(
            f"Flow target `{target}` was not found: module `{module_ref}` "
            f"has no attribute `{attr_name}`."
        ) from exc

    if not isinstance(flow_obj, _FlowTarget):
        raise ValueError(
            f"Target `{target}` is not a Kitaru flow object. "
            "Expected an object created by `@flow` with `.run()` support."
        )

    return flow_obj
