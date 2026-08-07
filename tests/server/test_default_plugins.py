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

import pytest

from conftest import FakeBlobRepository, FakePluginRepository
from kitaru.server.api import bootstrap
from kitaru.server.api.bootstrap import (
    DefaultPluginDefinition,
    register_default_plugins,
)
from kitaru.server.domain.names import RESERVED_PLUGIN_NAME_PREFIX
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource

DEFINITIONS = (
    DefaultPluginDefinition(
        kind=PluginKind.IMPORTER,
        name=f"{RESERVED_PLUGIN_NAME_PREFIX}importer",
        description="Test importer.",
        provider="langfuse",
        logo_url="https://example.com/langfuse.svg",
        entrypoint="parse",
        content=b"def parse(): ...",
        version=1,
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_PLUGIN_NAME_PREFIX}evaluator",
        description="Test evaluator.",
        provider=None,
        entrypoint="evaluate",
        content=b"def evaluate(): ...",
        version=1,
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
    blob_repository: FakeBlobRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create every default plugin ownerless with one version on first startup."""
    monkeypatch.setattr(bootstrap, "DEFAULT_PLUGIN_DEFINITIONS", DEFINITIONS)

    await register_default_plugins(repository, blob_repository)

    for definition in DEFINITIONS:
        plugin = await repository.get_by_name(definition.kind, definition.name)
        assert plugin.owner_id is None
        assert plugin.description == definition.description
        assert plugin.provider == definition.provider
        assert plugin.logo_url == definition.logo_url
        assert plugin.latest_version == 1
        version = await repository.get_version(plugin.id, 1)
        assert isinstance(version.source, ScriptPluginSource)
        assert version.source.entrypoint == definition.entrypoint
        blob = await blob_repository.get(version.source.blob_id)
        assert blob.owner_id is None
        assert blob.data == definition.content


async def test_register_is_idempotent(
    repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leave version numbers unchanged when the declared versions are unchanged."""
    monkeypatch.setattr(bootstrap, "DEFAULT_PLUGIN_DEFINITIONS", DEFINITIONS)
    await register_default_plugins(repository, blob_repository)

    await register_default_plugins(repository, blob_repository)

    for definition in DEFINITIONS:
        plugin = await repository.get_by_name(definition.kind, definition.name)
        assert plugin.latest_version == 1


async def test_register_creates_new_version_on_version_bump(
    repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create version 2 only for plugins whose declared version was bumped."""
    monkeypatch.setattr(bootstrap, "DEFAULT_PLUGIN_DEFINITIONS", DEFINITIONS)
    await register_default_plugins(repository, blob_repository)

    bumped_name = DEFINITIONS[0].name
    bumped = tuple(
        definition.model_copy(update={"version": 2})
        if definition.name == bumped_name
        else definition
        for definition in DEFINITIONS
    )
    monkeypatch.setattr(bootstrap, "DEFAULT_PLUGIN_DEFINITIONS", bumped)
    await register_default_plugins(repository, blob_repository)

    for definition in DEFINITIONS:
        plugin = await repository.get_by_name(definition.kind, definition.name)
        expected_version = 2 if definition.name == bumped_name else 1
        assert plugin.latest_version == expected_version
