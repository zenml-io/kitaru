#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Plugin code loading and the plumbing shared by the harness entrypoints."""

import asyncio
import importlib.machinery
import importlib.util
import os
import sys
from collections.abc import Callable, Coroutine
from pathlib import Path
from types import ModuleType
from typing import Any


class PluginLoadError(Exception):
    """Raised when registered code does not import."""


def load_plugin_module(module_name: str, path: Path) -> ModuleType:
    """Import registered code from a file the worker materialized.

    The cache stores content under its hash, so the file carries no
    suffix and the loader is named explicitly.

    Args:
        module_name: Name the code is registered under.
        path: Path of the code file.

    Raises:
        PluginLoadError: The file does not import.

    Returns:
        Imported module.
    """
    loader = importlib.machinery.SourceFileLoader(module_name, str(path))
    spec = importlib.util.spec_from_file_location(module_name, path, loader=loader)
    if spec is None:
        raise PluginLoadError(f"No import spec for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        loader.exec_module(module)
    except Exception as exc:
        raise PluginLoadError(f"{type(exc).__name__}: {exc}") from exc
    return module


def module_attribute(module: ModuleType, attribute: str, label: str) -> Any:
    """Return the callable attribute an imported module registers.

    Args:
        module: Imported module.
        attribute: Name of the attribute.
        label: Name the attribute is reported under.

    Raises:
        PluginLoadError: The attribute is missing or not callable.

    Returns:
        Attribute of the module.
    """
    try:
        value = getattr(module, attribute)
    except AttributeError as exc:
        raise PluginLoadError(
            f"Module {module.__name__!r} has no attribute {attribute!r}"
        ) from exc
    if not callable(value):
        raise PluginLoadError(
            f"{label} '{module.__name__}:{attribute}' is not callable"
        )
    return value


def required_env(name: str, error: type[Exception]) -> str:
    """Read an environment variable of the process contract.

    Args:
        name: Name of the variable.
        error: Error raised when the variable is not set.

    Raises:
        Exception: The variable is not set.

    Returns:
        Value of the variable.
    """
    value = os.environ.get(name)
    if not value:
        raise error(f"{name} is not set")
    return value


def run_harness(run: Callable[[], Coroutine[Any, Any, None]]) -> int:
    """Run a harness entrypoint and report what the process exits with.

    Args:
        run: Entrypoint coroutine function.

    Returns:
        Exit code.
    """
    try:
        asyncio.run(run())
    except Exception as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0
