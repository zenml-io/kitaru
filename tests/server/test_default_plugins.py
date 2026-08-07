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
"""Tests for default plugin registration."""

from types import SimpleNamespace

import pytest

from conftest import FakeBlobRepository, FakePluginRepository
from kitaru.server.api import bootstrap
from kitaru.server.api.bootstrap import (
    DefaultPluginDefinition,
    _load_default_plugin_definitions,
    register_default_plugins,
)
from kitaru.server.domain.names import RESERVED_PLUGIN_NAME_PREFIX
from kitaru.server.domain.plugin import PackagePluginSource, PluginKind

DEFINITIONS = (
    DefaultPluginDefinition(
        kind=PluginKind.IMPORTER,
        name=f"{RESERVED_PLUGIN_NAME_PREFIX}importer",
        description="Test importer.",
        provider="langfuse",
        logo_url="https://example.com/langfuse.svg",
        entrypoint="package.importer:parse",
        requirement="kitaru-langfuse-importer==1.0.0",
        display_version="1.0.0",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_PLUGIN_NAME_PREFIX}evaluator",
        description="Test evaluator.",
        provider=None,
        entrypoint="package.evaluator:evaluate",
        requirement="kitaru-evaluator==1.0.0",
        display_version="1.0.0",
    ),
)


@pytest.fixture
def blob_repository() -> FakeBlobRepository:
    """Provide a fake blob repository."""
    return FakeBlobRepository()


@pytest.fixture
def repository(blob_repository: FakeBlobRepository) -> FakePluginRepository:
    """Provide a fake plugin repository wired to the fake blob repository."""
    return FakePluginRepository(blob_repository=blob_repository)


async def test_register_creates_default_plugins(
    repository: FakePluginRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create every default plugin ownerless with one version on first startup."""
    monkeypatch.setattr(
        bootstrap, "_load_default_plugin_definitions", lambda: DEFINITIONS
    )

    await register_default_plugins(repository)

    for definition in DEFINITIONS:
        plugin = await repository.get_by_name(definition.kind, definition.name)
        assert plugin.owner_id is None
        assert plugin.description == definition.description
        assert plugin.provider == definition.provider
        assert plugin.logo_url == definition.logo_url
        assert plugin.latest_version == 1
        version = await repository.get_version(plugin.id, 1)
        assert isinstance(version.source, PackagePluginSource)
        assert version.source.entrypoint == definition.entrypoint
        assert version.source.requirement == definition.requirement
        assert version.display_version == "1.0.0"


async def test_register_is_idempotent(
    repository: FakePluginRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leave versions unchanged while the package requirement is unchanged."""
    monkeypatch.setattr(
        bootstrap, "_load_default_plugin_definitions", lambda: DEFINITIONS
    )
    await register_default_plugins(repository)

    await register_default_plugins(repository)

    for definition in DEFINITIONS:
        plugin = await repository.get_by_name(definition.kind, definition.name)
        assert plugin.latest_version == 1


async def test_register_creates_new_version_on_version_bump(
    repository: FakePluginRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create a version when the plugin distribution version changes."""
    monkeypatch.setattr(
        bootstrap, "_load_default_plugin_definitions", lambda: DEFINITIONS
    )
    await register_default_plugins(repository)

    bumped_name = DEFINITIONS[0].name
    bumped = tuple(
        definition.model_copy(
            update={
                "requirement": "kitaru-langfuse-importer==1.1.0",
                "display_version": "1.1.0",
            }
        )
        if definition.name == bumped_name
        else definition
        for definition in DEFINITIONS
    )
    monkeypatch.setattr(bootstrap, "_load_default_plugin_definitions", lambda: bumped)
    await register_default_plugins(repository)

    for definition in DEFINITIONS:
        plugin = await repository.get_by_name(definition.kind, definition.name)
        expected_version = 2 if definition.name == bumped_name else 1
        assert plugin.latest_version == expected_version


def test_loads_definitions_from_distribution_entrypoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Attach the catalog distribution name and version to every definition."""
    value = {
        "kind": "importer",
        "name": f"{RESERVED_PLUGIN_NAME_PREFIX}provider",
        "description": "Import provider traces.",
        "provider": "provider",
        "entrypoint": "kitaru_importer_provider.importer:parse",
    }
    catalog = SimpleNamespace(
        name="official",
        dist=SimpleNamespace(
            metadata={"Name": "kitaru-importer-provider"},
            version="2.3.0",
        ),
        load=lambda: lambda: [value],
    )
    monkeypatch.setattr(bootstrap, "entry_points", lambda **_: [catalog])

    [definition] = _load_default_plugin_definitions()

    assert definition.requirement == "kitaru-importer-provider==2.3.0"
    assert definition.display_version == "2.3.0"
