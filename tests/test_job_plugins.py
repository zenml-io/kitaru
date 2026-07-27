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
"""Tests for plugin loading."""

from pathlib import Path

import pytest

from kitaru.job.plugins import PluginLoadError, get_module_attribute, load_plugin_module

PLUGIN_CODE = """
def entrypoint():
    return "ok"


NOT_CALLABLE = "text"
"""


def write_plugin(tmp_path: Path, code: str = PLUGIN_CODE) -> Path:
    """Write plugin code to a file without a suffix, as the cache does."""
    path = tmp_path / ("a" * 64)
    path.write_text(code)
    return path


def test_load_plugin_module_imports_a_suffixless_file(tmp_path: Path) -> None:
    """Import a cached code file and register it under the given name."""
    module = load_plugin_module("test_plugin_a", write_plugin(tmp_path))
    assert module.__name__ == "test_plugin_a"
    assert module.entrypoint() == "ok"


def test_load_plugin_module_raises_while_importing(tmp_path: Path) -> None:
    """Reject code that raises while importing."""
    path = write_plugin(tmp_path, "raise RuntimeError('boom')\n")
    with pytest.raises(PluginLoadError, match="RuntimeError: boom"):
        load_plugin_module("test_plugin_b", path)


def test_load_plugin_module_missing_file(tmp_path: Path) -> None:
    """Reject a code file that does not exist."""
    with pytest.raises(PluginLoadError, match="FileNotFoundError"):
        load_plugin_module("test_plugin_c", tmp_path / "absent")


def test_get_module_attribute_returns_the_attribute(tmp_path: Path) -> None:
    """Return the named attribute of the module."""
    module = load_plugin_module("test_plugin_d", write_plugin(tmp_path))
    assert get_module_attribute(module, "entrypoint", "Scorer")() == "ok"


def test_get_module_attribute_missing_attribute(tmp_path: Path) -> None:
    """Reject an attribute the module does not define."""
    module = load_plugin_module("test_plugin_e", write_plugin(tmp_path))
    with pytest.raises(PluginLoadError, match="has no attribute 'missing'"):
        get_module_attribute(module, "missing", "Scorer")


def test_get_module_attribute_not_callable(tmp_path: Path) -> None:
    """Reject an attribute that is not callable, naming the plugin kind."""
    module = load_plugin_module("test_plugin_f", write_plugin(tmp_path))
    with pytest.raises(PluginLoadError, match=r"Scorer .* is not callable"):
        get_module_attribute(module, "NOT_CALLABLE", "Scorer")
