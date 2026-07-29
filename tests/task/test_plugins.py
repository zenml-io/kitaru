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
"""Tests for task plugin loading and source references."""

import sys
from types import ModuleType

import pytest

from kitaru.source_refs import parse_source_ref
from kitaru.task.plugins import (
    PluginLoadError,
    get_module_attribute,
    load_plugin_entrypoint,
    load_plugin_module,
    load_source_ref,
)


def test_load_plugin_entrypoint(tmp_path) -> None:
    """Import a script and resolve its callable entrypoint."""
    path = tmp_path / "plugin.py"
    path.write_text(
        "def evaluate(value):\n    return value + 1\n",
        encoding="utf-8",
    )
    evaluate = load_plugin_entrypoint(path, "evaluate", "Evaluator")
    assert evaluate(1) == 2


def test_load_plugin_module_registers_before_execution(tmp_path) -> None:
    """Register the module before executing code that inspects sys.modules."""
    path = tmp_path / "plugin.py"
    path.write_text(
        "import sys\nregistered = __name__ in sys.modules\n",
        encoding="utf-8",
    )
    module = load_plugin_module("test_registered_plugin", path)
    assert module.registered is True
    assert sys.modules["test_registered_plugin"] is module


def test_plugin_import_error_is_wrapped(tmp_path) -> None:
    """Wrap plugin import failures in the task-facing error type."""
    path = tmp_path / "broken.py"
    path.write_text("raise ValueError('broken import')\n", encoding="utf-8")
    with pytest.raises(PluginLoadError, match="broken import"):
        load_plugin_module("test_broken_plugin", path)
    assert "test_broken_plugin" not in sys.modules


@pytest.mark.parametrize(
    ("attribute", "message"),
    [
        ("missing", "was not found"),
        ("not_callable", "is not callable"),
    ],
)
def test_module_attribute_errors(attribute: str, message: str) -> None:
    """Reject missing and non-callable entrypoints."""
    module = ModuleType("example")
    module.__dict__["not_callable"] = 1
    with pytest.raises(PluginLoadError, match=message):
        get_module_attribute(module, attribute, "Importer")


@pytest.mark.parametrize(
    "ref",
    ["module", "module:", ":attribute", "module:attribute:extra"],
)
def test_parse_source_ref_rejects_invalid_format(ref: str) -> None:
    """Require exactly one colon and two nonempty components."""
    with pytest.raises(ValueError, match="module:attribute"):
        parse_source_ref(ref)


def test_load_source_ref(monkeypatch: pytest.MonkeyPatch) -> None:
    """Import and resolve an installed package entrypoint."""
    module = ModuleType("installed_plugin")

    def parse(payload, params):
        return iter(())

    module.__dict__["parse"] = parse
    monkeypatch.setitem(sys.modules, "installed_plugin", module)
    assert load_source_ref("installed_plugin:parse", "Importer") is parse


def test_load_source_ref_wraps_import_error() -> None:
    """Wrap unavailable package modules as plugin load failures."""
    with pytest.raises(PluginLoadError, match="Failed to import"):
        load_source_ref("definitely_missing_kitaru_plugin:parse", "Importer")
