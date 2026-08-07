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
"""Tests for the published default plugin catalog."""

from pathlib import Path

from kitaru.server.api.bootstrap import DEFAULT_PLUGIN_DEFINITIONS
from kitaru.source_refs import parse_source_ref
from kitaru.task.plugins import load_source_ref


def test_catalog_names_and_entrypoints_are_unique_and_loadable() -> None:
    """Expose one callable package entrypoint for each reserved plugin name."""
    definitions = DEFAULT_PLUGIN_DEFINITIONS
    names = [definition.name for definition in definitions]
    entrypoints = [definition.entrypoint for definition in definitions]
    requirements = [definition.requirement for definition in definitions]

    assert all(name.startswith("kitaru/") for name in names)
    assert len(names) == len(set(names))
    assert len(entrypoints) == len(set(entrypoints))
    for entrypoint in entrypoints:
        module, attribute = parse_source_ref(entrypoint)
        assert callable(load_source_ref(f"{module}:{attribute}", "Plugin"))
    requirements_file = Path(__file__).parents[1] / "default-requirements.txt"
    bundled_requirements = {
        line for line in requirements_file.read_text().splitlines() if line
    }
    assert set(requirements) == bundled_requirements
