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
"""Tests for analyzer config resolution."""

import uuid

import pytest

from conftest import FakePluginRepository, create_plugin
from kitaru.server.application.models.replay_config import AnalyzerConfigInput
from kitaru.server.application.services.analyzer_resolution import (
    resolve_analyzer_config,
    validate_analyzers,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.plugin import (
    PackagePluginSource,
    PluginKind,
    PluginNotFound,
    PluginVersionNotFound,
)

OWNER_ID = uuid.uuid4()

SOURCE = PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score")


@pytest.fixture
def repository() -> FakePluginRepository:
    """Provide a fake plugin repository."""
    return FakePluginRepository()


async def test_resolve_latest_version(repository: FakePluginRepository) -> None:
    """Resolve an omitted version to the analyzer's latest version."""
    plugin = await create_plugin(
        repository, OWNER_ID, kind=PluginKind.ANALYZER, name="trends"
    )
    await repository.create_version(plugin.id, SOURCE, display_version="v1")
    second = await repository.create_version(plugin.id, SOURCE, display_version="v2")

    config = AnalyzerConfigInput(analyzer="trends")
    resolved = await resolve_analyzer_config(config, repository)
    assert resolved.analyzer == "trends"
    assert resolved.version == 2
    assert resolved.analyzer_version_id == second.id


async def test_resolve_explicit_version(repository: FakePluginRepository) -> None:
    """Resolve to the explicitly named version, not the latest."""
    plugin = await create_plugin(
        repository, OWNER_ID, kind=PluginKind.ANALYZER, name="trends"
    )
    first = await repository.create_version(plugin.id, SOURCE, display_version="v1")
    await repository.create_version(plugin.id, SOURCE, display_version="v2")

    config = AnalyzerConfigInput(analyzer="trends", version=1)
    resolved = await resolve_analyzer_config(config, repository)
    assert resolved.version == 1
    assert resolved.analyzer_version_id == first.id


async def test_resolve_missing_analyzer(repository: FakePluginRepository) -> None:
    """Raise when no analyzer plugin has the config's name."""
    config = AnalyzerConfigInput(analyzer="missing")
    with pytest.raises(PluginNotFound, match="Plugin missing was not found"):
        await resolve_analyzer_config(config, repository)


async def test_resolve_missing_version(repository: FakePluginRepository) -> None:
    """Raise when the explicit version has no matching plugin version."""
    await create_plugin(repository, OWNER_ID, kind=PluginKind.ANALYZER, name="trends")
    config = AnalyzerConfigInput(analyzer="trends", version=5)
    with pytest.raises(PluginVersionNotFound):
        await resolve_analyzer_config(config, repository)


async def test_resolve_no_versions_yet(repository: FakePluginRepository) -> None:
    """Raise when the analyzer plugin has no versions at all."""
    await create_plugin(repository, OWNER_ID, kind=PluginKind.ANALYZER, name="fresh")
    config = AnalyzerConfigInput(analyzer="fresh")
    with pytest.raises(PluginVersionNotFound):
        await resolve_analyzer_config(config, repository)


async def test_validate_analyzers_resolves_every_config(
    repository: FakePluginRepository,
) -> None:
    """Resolve every config in the list."""
    trends = await create_plugin(
        repository, OWNER_ID, kind=PluginKind.ANALYZER, name="trends"
    )
    outliers = await create_plugin(
        repository, OWNER_ID, kind=PluginKind.ANALYZER, name="outliers"
    )
    await repository.create_version(trends.id, SOURCE, display_version=None)
    await repository.create_version(outliers.id, SOURCE, display_version=None)

    resolved = await validate_analyzers(
        [
            AnalyzerConfigInput(analyzer="trends"),
            AnalyzerConfigInput(analyzer="outliers"),
        ],
        repository,
    )
    assert {config.analyzer for config in resolved} == {"trends", "outliers"}


async def test_validate_analyzers_rejects_duplicate_version(
    repository: FakePluginRepository,
) -> None:
    """Reject two configs resolving to the same analyzer version."""
    plugin = await create_plugin(
        repository, OWNER_ID, kind=PluginKind.ANALYZER, name="trends"
    )
    await repository.create_version(plugin.id, SOURCE, display_version="v1")

    with pytest.raises(ValidationError, match="appears more than once"):
        await validate_analyzers(
            [
                AnalyzerConfigInput(analyzer="trends", version=1),
                AnalyzerConfigInput(analyzer="trends"),
            ],
            repository,
        )
