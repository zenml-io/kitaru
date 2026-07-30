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
"""Tests for foreign plugin code loading."""

import sys
import types
from pathlib import Path

import pytest

from kitaru.task.plugins import (
    PluginLoadError,
    get_module_attribute,
    load_plugin_entrypoint,
    load_plugin_module,
    load_source_ref,
)


def test_load_plugin_entrypoint_good_script(tmp_path: Path) -> None:
    """Load a callable entrypoint from a script file."""
    path = tmp_path / "plugin.py"
    path.write_text("def evaluate(session, **params):\n    return session\n")
    entrypoint = load_plugin_entrypoint(path, "evaluate", "Evaluator")
    assert entrypoint("session-value") == "session-value"


def test_load_plugin_entrypoint_import_error(tmp_path: Path) -> None:
    """Raise PluginLoadError when the script file fails to import."""
    path = tmp_path / "plugin.py"
    path.write_text("raise ValueError('boom')\n")
    with pytest.raises(PluginLoadError, match="Failed to import"):
        load_plugin_entrypoint(path, "evaluate", "Evaluator")


def test_load_plugin_entrypoint_missing_attribute(tmp_path: Path) -> None:
    """Raise PluginLoadError when the entrypoint attribute is missing."""
    path = tmp_path / "plugin.py"
    path.write_text("def other():\n    pass\n")
    with pytest.raises(PluginLoadError, match="Evaluator entrypoint 'evaluate'"):
        load_plugin_entrypoint(path, "evaluate", "Evaluator")


def test_load_plugin_entrypoint_non_callable(tmp_path: Path) -> None:
    """Raise PluginLoadError when the entrypoint attribute is not callable."""
    path = tmp_path / "plugin.py"
    path.write_text("evaluate = 'not callable'\n")
    with pytest.raises(PluginLoadError, match="Evaluator entrypoint 'evaluate'"):
        load_plugin_entrypoint(path, "evaluate", "Evaluator")


def test_load_plugin_module_registers_in_sys_modules(tmp_path: Path) -> None:
    """Register the imported script file under the requested module name."""
    path = tmp_path / "plugin.py"
    path.write_text("value = 42\n")
    module = load_plugin_module("kitaru._test_plugin_module", path)
    assert sys.modules["kitaru._test_plugin_module"] is module
    assert module.value == 42


def test_load_plugin_module_without_extension(tmp_path: Path) -> None:
    """Load a script file named after its content hash, with no .py suffix.

    This mirrors what the worker's content-addressed blob cache materializes
    a script plugin file as.
    """
    path = tmp_path / "ef6b4bdd7398a7f8d981c2a0141ca9f85e66d937c1458a2748ca595ef6ad582a"
    path.write_text("value = 42\n")
    module = load_plugin_module("kitaru._test_plugin_module_no_suffix", path)
    assert module.value == 42


def test_get_module_attribute_missing() -> None:
    """Raise PluginLoadError when the attribute does not exist on the module."""
    module = types.ModuleType("empty")
    with pytest.raises(PluginLoadError, match="Importer entrypoint 'parse'"):
        get_module_attribute(module, "parse", "Importer")


def test_load_source_ref_good() -> None:
    """Resolve a module:attribute reference against installed code."""
    entrypoint = load_source_ref("json:dumps", "Evaluator")
    assert entrypoint({"a": 1}) == '{"a": 1}'


def test_load_source_ref_bad_format() -> None:
    """Raise PluginLoadError when the reference is not module:attribute."""
    with pytest.raises(PluginLoadError, match="Invalid source reference"):
        load_source_ref("not-a-ref", "Evaluator")


def test_load_source_ref_missing_module() -> None:
    """Raise PluginLoadError when the module does not import."""
    with pytest.raises(PluginLoadError, match="Failed to import module"):
        load_source_ref("kitaru.does_not_exist:evaluate", "Evaluator")


def test_load_source_ref_missing_attribute() -> None:
    """Raise PluginLoadError when the attribute is missing on the module."""
    with pytest.raises(PluginLoadError, match="Evaluator entrypoint 'does_not_exist'"):
        load_source_ref("json:does_not_exist", "Evaluator")
