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

from collections.abc import Callable

import pytest

from conftest import FakeBlobRepository, FakePluginRepository
from kitaru.server.application.services import default_plugins
from kitaru.server.application.services.default_plugins import (
    DEFAULT_PLUGIN_DEFINITIONS,
    register_default_plugins,
)
from kitaru.server.domain.plugin import ScriptPluginSource


def _source_reader(
    overrides: dict[str, bytes] | None = None,
) -> Callable[[str], bytes]:
    """Build a deterministic stand-in for reading plugin source files."""
    overrides = overrides or {}
    return lambda source_file: overrides.get(
        source_file, f"content-of-{source_file}".encode()
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
    monkeypatch.setattr(default_plugins, "_read_source", _source_reader())

    await register_default_plugins(repository, blob_repository)

    for definition in DEFAULT_PLUGIN_DEFINITIONS:
        plugin = await repository.get_by_name(definition.kind, definition.name)
        assert plugin.owner_id is None
        assert plugin.description == definition.description
        assert plugin.provider == definition.provider
        assert plugin.latest_version == 1
        version = await repository.get_version(plugin.id, 1)
        assert isinstance(version.source, ScriptPluginSource)
        assert version.source.entrypoint == definition.entrypoint


async def test_register_is_idempotent(
    repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leave version numbers unchanged when nothing about the source changed."""
    monkeypatch.setattr(default_plugins, "_read_source", _source_reader())
    await register_default_plugins(repository, blob_repository)

    await register_default_plugins(repository, blob_repository)

    for definition in DEFAULT_PLUGIN_DEFINITIONS:
        plugin = await repository.get_by_name(definition.kind, definition.name)
        assert plugin.latest_version == 1


async def test_register_creates_new_version_on_content_change(
    repository: FakePluginRepository,
    blob_repository: FakeBlobRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Create version 2 only for plugins whose source file changed."""
    monkeypatch.setattr(default_plugins, "_read_source", _source_reader())
    await register_default_plugins(repository, blob_repository)

    monkeypatch.setattr(
        default_plugins,
        "_read_source",
        _source_reader({"evaluators/basic.py": b"changed"}),
    )
    await register_default_plugins(repository, blob_repository)

    changed_names = {
        definition.name
        for definition in DEFAULT_PLUGIN_DEFINITIONS
        if definition.source_file == "evaluators/basic.py"
    }
    for definition in DEFAULT_PLUGIN_DEFINITIONS:
        plugin = await repository.get_by_name(definition.kind, definition.name)
        expected_version = 2 if definition.name in changed_names else 1
        assert plugin.latest_version == expected_version
