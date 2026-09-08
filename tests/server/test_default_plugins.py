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
from pydantic import BaseModel, SecretStr

from conftest import FakeBlobRepository, FakePluginRepository
from kitaru.server.api import bootstrap
from kitaru.server.api.bootstrap import (
    DEFAULT_PLUGIN_DEFINITIONS,
    DefaultPluginDefinition,
    register_default_plugins,
)
from kitaru.server.domain.names import RESERVED_NAMESPACE
from kitaru.server.domain.plugin import PackagePluginSource, PluginKind


class SampleConnection(BaseModel):
    """Sample connection."""

    TEST_API_KEY: SecretStr


class RotatedSampleConnection(BaseModel):
    """Rotated sample connection."""

    TEST_API_KEY: SecretStr
    TEST_BASE_URL: str = "https://example.com"


DEFINITIONS = (
    DefaultPluginDefinition(
        kind=PluginKind.IMPORTER,
        name=f"{RESERVED_NAMESPACE}/importer",
        description="Test importer.",
        provider="langfuse",
        logo_url="https://example.com/langfuse.svg",
        entrypoint="package.importer:parse",
        requirement="kitaru-langfuse-importer==1.0.0",
        display_version="1.0.0",
        connection_schema=SampleConnection,
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_NAMESPACE}/evaluator",
        description="Test evaluator.",
        provider=None,
        entrypoint="package.evaluator:evaluate",
        requirement="kitaru-evaluator==1.0.0",
        display_version="1.0.0",
    ),
)


def test_post_import_analyzer_is_registered_from_plugin_package() -> None:
    """The default analyzer resolves its independently versioned distribution."""
    definition = next(
        item
        for item in DEFAULT_PLUGIN_DEFINITIONS
        if item.kind is PluginKind.ANALYZER
        and item.name == "kitaru/post-import-insights"
    )
    assert (
        definition.entrypoint
        == "kitaru_post_import_insights.analyzer:analyze_post_import_sessions"
    )
    assert definition.requirement == "kitaru-post-import-insights==0.1.0"
    assert definition.display_version == "0.1.0"
    assert definition.provider is None
    assert definition.connection_schema is None


def test_openai_post_import_analyzer_has_separate_entrypoint_and_credentials() -> None:
    """Expose OpenAI generation as a selectable provider-backed analyzer."""
    definition = next(
        item
        for item in DEFAULT_PLUGIN_DEFINITIONS
        if item.kind is PluginKind.ANALYZER
        and item.name == "kitaru/openai-post-import-insights"
    )
    assert definition.entrypoint == (
        "kitaru_post_import_insights.analyzer:analyze_openai_post_import_sessions"
    )
    assert definition.requirement == "kitaru-post-import-insights==0.1.0"
    assert definition.display_version == "0.1.0"
    assert definition.provider == "openai"
    assert definition.connection_schema is not None
    schema = definition.connection_schema.model_json_schema()
    assert schema["required"] == ["OPENAI_API_KEY"]
    assert schema["properties"]["OPENAI_API_KEY"]["writeOnly"] is True


async def test_register_keeps_both_post_import_analyzers_independent(
    repository: FakePluginRepository,
) -> None:
    """Register both plugins idempotently even though they share a distribution."""
    await register_default_plugins(repository)
    await register_default_plugins(repository)

    deterministic = await repository.get_by_name(
        PluginKind.ANALYZER, "kitaru/post-import-insights"
    )
    openai = await repository.get_by_name(
        PluginKind.ANALYZER, "kitaru/openai-post-import-insights"
    )
    assert deterministic.id != openai.id
    assert deterministic.latest_version == openai.latest_version == 1
    assert deterministic.connection_schema is None
    assert openai.connection_schema is not None


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
    monkeypatch.setattr(bootstrap, "DEFAULT_PLUGIN_DEFINITIONS", DEFINITIONS)

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
    monkeypatch.setattr(bootstrap, "DEFAULT_PLUGIN_DEFINITIONS", DEFINITIONS)
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
    monkeypatch.setattr(bootstrap, "DEFAULT_PLUGIN_DEFINITIONS", DEFINITIONS)
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
    monkeypatch.setattr(bootstrap, "DEFAULT_PLUGIN_DEFINITIONS", bumped)
    await register_default_plugins(repository)

    for definition in DEFINITIONS:
        plugin = await repository.get_by_name(definition.kind, definition.name)
        expected_version = 2 if definition.name == bumped_name else 1
        assert plugin.latest_version == expected_version


def test_default_definitions_have_unique_identities() -> None:
    """Keep every built-in kind and name pair unique."""
    identities = {
        (definition.kind, definition.name) for definition in DEFAULT_PLUGIN_DEFINITIONS
    }

    assert len(identities) == len(DEFAULT_PLUGIN_DEFINITIONS)


async def test_register_stores_the_connection_schema(
    repository: FakePluginRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Store the JSON Schema of a definition's connection model on the plugin."""
    monkeypatch.setattr(bootstrap, "DEFAULT_PLUGIN_DEFINITIONS", DEFINITIONS)

    await register_default_plugins(repository)

    importer = await repository.get_by_name(DEFINITIONS[0].kind, DEFINITIONS[0].name)
    assert importer.connection_schema == SampleConnection.model_json_schema()
    assert importer.connection_schema is not None
    key = importer.connection_schema["properties"]["TEST_API_KEY"]
    assert key["format"] == "password"
    assert key["writeOnly"] is True
    evaluator = await repository.get_by_name(DEFINITIONS[1].kind, DEFINITIONS[1].name)
    assert evaluator.connection_schema is None


async def test_register_refreshes_a_changed_connection_schema(
    repository: FakePluginRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Update the stored schema of an existing plugin when the definition changes."""
    monkeypatch.setattr(bootstrap, "DEFAULT_PLUGIN_DEFINITIONS", DEFINITIONS)
    await register_default_plugins(repository)

    rotated = tuple(
        definition.model_copy(update={"connection_schema": RotatedSampleConnection})
        if definition.name == DEFINITIONS[0].name
        else definition
        for definition in DEFINITIONS
    )
    monkeypatch.setattr(bootstrap, "DEFAULT_PLUGIN_DEFINITIONS", rotated)
    await register_default_plugins(repository)

    importer = await repository.get_by_name(DEFINITIONS[0].kind, DEFINITIONS[0].name)
    assert importer.connection_schema == RotatedSampleConnection.model_json_schema()
    assert importer.latest_version == 1


def test_default_importer_schemas_mark_secrets_write_only() -> None:
    """Mark every secret property of a built-in connection schema write-only."""
    for definition in DEFAULT_PLUGIN_DEFINITIONS:
        if definition.connection_schema is None:
            continue
        schema = definition.connection_schema.model_json_schema()
        secrets = [
            name for name, prop in schema["properties"].items() if prop.get("writeOnly")
        ]
        assert secrets
        for name in secrets:
            assert schema["properties"][name]["format"] == "password"
