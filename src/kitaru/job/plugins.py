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
"""Single-file plugin loading and entrypoint resolution."""

import importlib.machinery
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any


class PluginLoadError(Exception):
    """Raised when registered code does not import."""


def load_plugin_module(name: str, path: Path) -> ModuleType:
    """Import registered code from a file the worker materialized.

    The cache stores content under its hash, so the file carries no suffix
    and the loader is named explicitly.

    Args:
        name: Module name the code is registered under.
        path: Path of the code file.

    Raises:
        PluginLoadError: The file does not import.

    Returns:
        Imported module.
    """
    loader = importlib.machinery.SourceFileLoader(name, str(path))
    spec = importlib.util.spec_from_file_location(name, path, loader=loader)
    if spec is None:
        raise PluginLoadError(f"No import spec for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        loader.exec_module(module)
    except Exception as exc:
        raise PluginLoadError(f"{type(exc).__name__}: {exc}") from exc
    return module


def get_module_attribute(module: ModuleType, attribute: str, label: str) -> Any:
    """Return the entrypoint attribute an imported module registers.

    Args:
        module: Imported module.
        attribute: Name of the attribute.
        label: Plugin kind the attribute is reported under.

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
