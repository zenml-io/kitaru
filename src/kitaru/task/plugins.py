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
"""Foreign plugin code loading, the only module that imports foreign code."""

import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

from kitaru.source_refs import parse_source_ref

_SCRIPT_PLUGIN_MODULE_NAME = "kitaru._task_plugin"


class PluginLoadError(Exception):
    """Raised when foreign plugin code fails to load or resolve."""


def load_plugin_module(name: str, path: Path) -> ModuleType:
    """Import a single Python file as a module under a fixed name.

    Args:
        name: Module name the file is registered under in sys.modules.
        path: Path of the file to import.

    Raises:
        PluginLoadError: The file does not import.

    Returns:
        Imported module.
    """
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise PluginLoadError(f"Could not load a module spec from '{path}'")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        del sys.modules[name]
        raise PluginLoadError(f"Failed to import plugin file '{path}': {exc}") from exc
    return module


def get_module_attribute(module: ModuleType, attribute: str, label: str) -> Any:
    """Resolve a callable attribute on an imported module.

    Args:
        module: Module to resolve the attribute on.
        attribute: Attribute name.
        label: Plugin kind, named in the error message.

    Raises:
        PluginLoadError: The attribute is missing or not callable.

    Returns:
        Resolved attribute.
    """
    value = getattr(module, attribute, None)
    if value is None or not callable(value):
        raise PluginLoadError(
            f"{label} entrypoint '{attribute}' was not found or is not callable "
            f"on module '{module.__name__}'"
        )
    return value


def load_plugin_entrypoint(path: Path, entrypoint: str, label: str) -> Any:
    """Import a script plugin file and resolve its entrypoint.

    Args:
        path: Path of the materialized script plugin file.
        entrypoint: Attribute name of the entrypoint.
        label: Plugin kind, named in the error message.

    Raises:
        PluginLoadError: The file does not import, or the entrypoint is
            missing or not callable.

    Returns:
        Resolved entrypoint callable.
    """
    module = load_plugin_module(_SCRIPT_PLUGIN_MODULE_NAME, path)
    return get_module_attribute(module, entrypoint, label)


def load_source_ref(ref: str, label: str) -> Any:
    """Import an installed module and resolve a module:attribute reference.

    Args:
        ref: Module and attribute reference, as module:attribute.
        label: Plugin kind, named in the error message.

    Raises:
        PluginLoadError: The reference is malformed, the module does not
            import, or the attribute is missing or not callable.

    Returns:
        Resolved entrypoint callable.
    """
    try:
        module_name, attribute = parse_source_ref(ref)
    except ValueError as exc:
        raise PluginLoadError(str(exc)) from exc
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise PluginLoadError(
            f"Failed to import module '{module_name}': {exc}"
        ) from exc
    return get_module_attribute(module, attribute, label)
