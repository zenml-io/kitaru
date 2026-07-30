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
"""Foreign plugin code loading."""

import importlib
import importlib.machinery
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

from kitaru.source_refs import parse_source_ref

_PLUGIN_MODULE_NAME = "_kitaru_task_plugin"


class PluginLoadError(Exception):
    """Plugin loading failed."""


def load_plugin_module(name: str, path: Path) -> ModuleType:
    """Load a plugin module from a single Python file.

    Args:
        name: Module name to register.
        path: Plugin file path.

    Raises:
        PluginLoadError: The module cannot be loaded or imported.

    Returns:
        Loaded module.
    """
    try:
        # Why: cached blobs are named by digest with no .py suffix, so importlib
        # cannot pick a loader on its own.
        spec = importlib.util.spec_from_file_location(
            name, path, loader=importlib.machinery.SourceFileLoader(name, str(path))
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot create an import spec for {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    except Exception as exc:
        sys.modules.pop(name, None)
        raise PluginLoadError(f"Failed to load plugin file {path}: {exc}") from exc
    return module


def get_module_attribute(module: ModuleType, attribute: str, label: str) -> Any:
    """Resolve a callable plugin entrypoint.

    Args:
        module: Module containing the entrypoint.
        attribute: Entrypoint attribute name.
        label: Plugin kind used in errors.

    Raises:
        PluginLoadError: The attribute is missing or not callable.

    Returns:
        Callable plugin entrypoint.
    """
    try:
        entrypoint = getattr(module, attribute)
    except AttributeError as exc:
        raise PluginLoadError(
            f"{label} entrypoint {attribute!r} was not found in "
            f"module {module.__name__!r}."
        ) from exc
    if not callable(entrypoint):
        raise PluginLoadError(
            f"{label} entrypoint {attribute!r} in module "
            f"{module.__name__!r} is not callable."
        )
    return entrypoint


def load_plugin_entrypoint(path: Path, entrypoint: str, label: str) -> Any:
    """Load a callable entrypoint from a script plugin.

    Args:
        path: Plugin file path.
        entrypoint: Entrypoint attribute name.
        label: Plugin kind used in errors.

    Returns:
        Callable plugin entrypoint.
    """
    module = load_plugin_module(_PLUGIN_MODULE_NAME, path)
    return get_module_attribute(module, entrypoint, label)


def load_source_ref(ref: str, label: str) -> Any:
    """Load a callable entrypoint from an installed module reference.

    Args:
        ref: ``module:attribute`` reference.
        label: Plugin kind used in errors.

    Raises:
        PluginLoadError: The reference is invalid or cannot be loaded.

    Returns:
        Callable plugin entrypoint.
    """
    try:
        module_name, attribute = parse_source_ref(ref)
    except ValueError as exc:
        raise PluginLoadError(str(exc)) from exc
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise PluginLoadError(
            f"Failed to import {label} module {module_name!r}: {exc}"
        ) from exc
    return get_module_attribute(module, attribute, label)
