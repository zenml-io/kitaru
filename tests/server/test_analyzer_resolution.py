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

from conftest import (
    FakeConnectionRepository,
    FakePluginRepository,
    create_connection,
    create_plugin,
)
from kitaru.server.application.models.replay_config import AnalyzerConfigInput
from kitaru.server.application.services.analyzer_resolution import (
    resolve_analyzer_config,
    validate_analyzers,
)
from kitaru.server.application.services.plugin_resolution import (
    check_unique_plugin_versions,
    resolve_plugin_credentials,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.connection import ConnectionNotFound
from kitaru.server.domain.plugin import (
    PackagePluginSource,
    PluginKind,
    PluginNotFound,
    PluginVersionNotFound,
)
from kitaru.server.domain.replay_config import AnalyzerConfig, EvaluatorConfig

OWNER_ID = uuid.uuid4()

SOURCE = PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score")


@pytest.fixture
def repository() -> FakePluginRepository:
    """Provide a fake plugin repository."""
    return FakePluginRepository()


@pytest.fixture
def connections() -> FakeConnectionRepository:
    """Provide a fake connection repository."""
    return FakeConnectionRepository()


async def test_resolve_latest_version(
    repository: FakePluginRepository, connections: FakeConnectionRepository
) -> None:
    """Resolve an omitted version to the analyzer's latest version."""
    plugin = await create_plugin(
        repository, OWNER_ID, kind=PluginKind.ANALYZER, name="trends"
    )
    await repository.create_version(plugin.id, SOURCE, display_version="v1")
    second = await repository.create_version(plugin.id, SOURCE, display_version="v2")

    config = AnalyzerConfigInput(analyzer="trends")
    resolved = await resolve_analyzer_config(config, repository, connections)
    assert resolved.analyzer == "trends"
    assert resolved.version == 2
    assert resolved.analyzer_version_id == second.id


async def test_resolve_explicit_version(
    repository: FakePluginRepository, connections: FakeConnectionRepository
) -> None:
    """Resolve to the explicitly named version, not the latest."""
    plugin = await create_plugin(
        repository, OWNER_ID, kind=PluginKind.ANALYZER, name="trends"
    )
    first = await repository.create_version(plugin.id, SOURCE, display_version="v1")
    await repository.create_version(plugin.id, SOURCE, display_version="v2")

    config = AnalyzerConfigInput(analyzer="trends", version=1)
    resolved = await resolve_analyzer_config(config, repository, connections)
    assert resolved.version == 1
    assert resolved.analyzer_version_id == first.id


async def test_resolve_missing_analyzer(
    repository: FakePluginRepository, connections: FakeConnectionRepository
) -> None:
    """Raise when no analyzer plugin has the config's name."""
    config = AnalyzerConfigInput(analyzer="missing")
    with pytest.raises(PluginNotFound, match="Plugin missing was not found"):
        await resolve_analyzer_config(config, repository, connections)


async def test_resolve_missing_version(
    repository: FakePluginRepository, connections: FakeConnectionRepository
) -> None:
    """Raise when the explicit version has no matching plugin version."""
    await create_plugin(repository, OWNER_ID, kind=PluginKind.ANALYZER, name="trends")
    config = AnalyzerConfigInput(analyzer="trends", version=5)
    with pytest.raises(PluginVersionNotFound):
        await resolve_analyzer_config(config, repository, connections)


async def test_resolve_no_versions_yet(
    repository: FakePluginRepository, connections: FakeConnectionRepository
) -> None:
    """Raise when the analyzer plugin has no versions at all."""
    await create_plugin(repository, OWNER_ID, kind=PluginKind.ANALYZER, name="fresh")
    config = AnalyzerConfigInput(analyzer="fresh")
    with pytest.raises(PluginVersionNotFound):
        await resolve_analyzer_config(config, repository, connections)


async def test_validate_analyzers_resolves_every_config(
    repository: FakePluginRepository, connections: FakeConnectionRepository
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
        connections,
    )
    assert {config.analyzer for config in resolved} == {"trends", "outliers"}


async def test_validate_analyzers_rejects_duplicate_version(
    repository: FakePluginRepository, connections: FakeConnectionRepository
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
            connections,
        )


async def test_resolve_named_connection(
    repository: FakePluginRepository, connections: FakeConnectionRepository
) -> None:
    """Resolve the connection explicitly named by the analyzer config."""
    plugin = await create_plugin(
        repository,
        OWNER_ID,
        kind=PluginKind.ANALYZER,
        name="trends",
        provider="langfuse",
    )
    await repository.create_version(plugin.id, SOURCE, display_version=None)
    connection = await create_connection(
        connections, OWNER_ID, uuid.uuid4(), name="named"
    )

    resolved = await resolve_analyzer_config(
        AnalyzerConfigInput(analyzer="trends", connection_id=connection.id),
        repository,
        connections,
    )

    assert resolved.connection_id == connection.id
    assert resolved.provider == "langfuse"


async def test_resolve_default_connection(
    repository: FakePluginRepository, connections: FakeConnectionRepository
) -> None:
    """Resolve the analyzer provider's default connection when none is named."""
    plugin = await create_plugin(
        repository,
        OWNER_ID,
        kind=PluginKind.ANALYZER,
        name="trends",
        provider="langfuse",
    )
    await repository.create_version(plugin.id, SOURCE, display_version=None)
    connection = await create_connection(
        connections, OWNER_ID, uuid.uuid4(), default=True
    )

    resolved = await resolve_analyzer_config(
        AnalyzerConfigInput(analyzer="trends"), repository, connections
    )

    assert resolved.connection_id == connection.id


async def test_resolve_analyzer_without_connection(
    repository: FakePluginRepository, connections: FakeConnectionRepository
) -> None:
    """Record no connection when the analyzer has no provider."""
    plugin = await create_plugin(
        repository, OWNER_ID, kind=PluginKind.ANALYZER, name="trends"
    )
    await repository.create_version(plugin.id, SOURCE, display_version=None)

    resolved = await resolve_analyzer_config(
        AnalyzerConfigInput(analyzer="trends"), repository, connections
    )

    assert resolved.connection_id is None


async def test_resolve_missing_connection(
    repository: FakePluginRepository, connections: FakeConnectionRepository
) -> None:
    """Raise when the analyzer config names an unknown connection."""
    plugin = await create_plugin(
        repository, OWNER_ID, kind=PluginKind.ANALYZER, name="trends"
    )
    await repository.create_version(plugin.id, SOURCE, display_version=None)

    with pytest.raises(ConnectionNotFound):
        await resolve_analyzer_config(
            AnalyzerConfigInput(analyzer="trends", connection_id=uuid.uuid4()),
            repository,
            connections,
        )


async def test_resolve_plugin_credentials_requires_credentials_without_a_connection(
    repository: FakePluginRepository, connections: FakeConnectionRepository
) -> None:
    """Report that a connection schema with no resolved connection needs credentials."""
    plugin = await create_plugin(
        repository,
        OWNER_ID,
        kind=PluginKind.ANALYZER,
        name="trends",
        provider="openai",
        connection_schema={"type": "object"},
    )

    connection_id, requires_credentials = await resolve_plugin_credentials(
        plugin, None, connections
    )

    assert connection_id is None
    assert requires_credentials is True


async def test_resolve_plugin_credentials_resolves_a_named_connection(
    repository: FakePluginRepository, connections: FakeConnectionRepository
) -> None:
    """Resolve the named connection and report no credentials are needed."""
    plugin = await create_plugin(
        repository,
        OWNER_ID,
        kind=PluginKind.ANALYZER,
        name="trends",
        provider="openai",
        connection_schema={"type": "object"},
    )
    connection = await create_connection(connections, OWNER_ID, uuid.uuid4())

    connection_id, requires_credentials = await resolve_plugin_credentials(
        plugin, connection.id, connections
    )

    assert connection_id == connection.id
    assert requires_credentials is False


async def test_resolve_plugin_credentials_without_a_schema(
    repository: FakePluginRepository, connections: FakeConnectionRepository
) -> None:
    """Report no credentials are needed when the plugin has no connection schema."""
    plugin = await create_plugin(
        repository, OWNER_ID, kind=PluginKind.ANALYZER, name="trends"
    )

    connection_id, requires_credentials = await resolve_plugin_credentials(
        plugin, None, connections
    )

    assert connection_id is None
    assert requires_credentials is False


async def test_resolve_plugin_credentials_missing_connection(
    repository: FakePluginRepository, connections: FakeConnectionRepository
) -> None:
    """Raise when the named connection does not exist."""
    plugin = await create_plugin(
        repository, OWNER_ID, kind=PluginKind.ANALYZER, name="trends"
    )

    with pytest.raises(ConnectionNotFound):
        await resolve_plugin_credentials(plugin, uuid.uuid4(), connections)


def test_check_unique_plugin_versions_accepts_distinct_versions() -> None:
    """Accept configs that resolve to distinct plugin versions."""
    configs = [
        AnalyzerConfig(analyzer="trends", version=1, analyzer_version_id=uuid.uuid4()),
        AnalyzerConfig(
            analyzer="outliers", version=1, analyzer_version_id=uuid.uuid4()
        ),
    ]
    check_unique_plugin_versions(configs, "analyzer")


def test_check_unique_plugin_versions_rejects_a_repeated_version() -> None:
    """Reject two configs resolving to the same plugin version."""
    version_id = uuid.uuid4()
    configs = [
        AnalyzerConfig(analyzer="trends", version=1, analyzer_version_id=version_id),
        AnalyzerConfig(analyzer="trends", version=2, analyzer_version_id=version_id),
    ]
    with pytest.raises(
        ValidationError,
        match="An analyzer version appears more than once in the analyzer list",
    ):
        check_unique_plugin_versions(configs, "analyzer")


def test_check_unique_plugin_versions_uses_the_given_label() -> None:
    """Name the given label, not a hardcoded plugin kind, in the error."""
    version_id = uuid.uuid4()
    configs = [
        EvaluatorConfig(
            evaluator="accuracy", version=1, evaluator_version_id=version_id
        ),
        EvaluatorConfig(
            evaluator="accuracy", version=2, evaluator_version_id=version_id
        ),
    ]
    with pytest.raises(
        ValidationError,
        match="An evaluator version appears more than once in the evaluator list",
    ):
        check_unique_plugin_versions(configs, "evaluator")
