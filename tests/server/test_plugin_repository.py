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
"""Contract tests for plugin and plugin version repositories."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import FakePluginRepository, pg_session, postgres_available
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
)
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.models.plugin import PluginFilter, PluginVersionFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    PackagePluginSource,
    Plugin,
    PluginKind,
    PluginNotFound,
    PluginVersionNotFound,
)

Setup = tuple[PluginRepository, uuid.UUID]

SOURCE_V1 = PackagePluginSource(
    requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"
)
SOURCE_V2 = PackagePluginSource(
    requirement="kitaru-scorer==2.0.0", entrypoint="pkg:score"
)


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each plugin repository implementation plus an owner id."""
    if request.param == "fake":
        yield FakePluginRepository(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        yield SQLPluginRepository(session), owner.id


def _plugin(
    owner_id: uuid.UUID,
    kind: PluginKind = PluginKind.EVALUATOR,
    name: str = "accuracy",
    provider: str | None = None,
) -> Plugin:
    return Plugin(owner_id=owner_id, kind=kind, name=name, provider=provider)


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new plugin with both timestamps set and latest_version at 0."""
    repository, owner_id = setup
    plugin = await repository.create(_plugin(owner_id))
    assert plugin.name == "accuracy"
    assert plugin.kind is PluginKind.EVALUATOR
    assert plugin.latest_version == 0
    assert plugin.created is not None
    assert plugin.updated is not None


async def test_create_duplicate_name_same_kind(setup: Setup) -> None:
    """Reject a second plugin with the same (kind, name)."""
    repository, owner_id = setup
    await repository.create(_plugin(owner_id, name="accuracy"))
    with pytest.raises(
        DuplicatePluginName, match="Evaluator name 'accuracy' is already registered"
    ):
        await repository.create(_plugin(owner_id, name="accuracy"))


async def test_evaluator_and_importer_share_a_name(setup: Setup) -> None:
    """Let an evaluator and an importer register the same name."""
    repository, owner_id = setup
    evaluator = await repository.create(
        _plugin(owner_id, kind=PluginKind.EVALUATOR, name="shared")
    )
    importer = await repository.create(
        _plugin(owner_id, kind=PluginKind.IMPORTER, name="shared")
    )
    assert evaluator.id != importer.id


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown plugin id."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(PluginNotFound, match=f"Plugin {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query_scoped_to_kind(setup: Setup) -> None:
    """Query only plugins of the requested kind."""
    repository, owner_id = setup
    await repository.create(
        _plugin(owner_id, kind=PluginKind.EVALUATOR, name="accuracy")
    )
    await repository.create(
        _plugin(owner_id, kind=PluginKind.IMPORTER, name="langfuse-import")
    )
    plugins, next_cursor = await repository.query(
        PluginFilter(kind=PluginKind.IMPORTER)
    )
    assert next_cursor is None
    assert [plugin.name for plugin in plugins] == ["langfuse-import"]


async def test_query_provider_filter(setup: Setup) -> None:
    """Filter importers by provider."""
    repository, owner_id = setup
    await repository.create(
        _plugin(
            owner_id, kind=PluginKind.IMPORTER, name="langfuse", provider="langfuse"
        )
    )
    await repository.create(
        _plugin(
            owner_id, kind=PluginKind.IMPORTER, name="braintrust", provider="braintrust"
        )
    )
    plugins, next_cursor = await repository.query(
        PluginFilter(kind=PluginKind.IMPORTER, provider="langfuse")
    )
    assert next_cursor is None
    assert [plugin.name for plugin in plugins] == ["langfuse"]


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, owner_id = setup
    created = await repository.create(_plugin(owner_id))
    created.update_description("Scores accuracy")
    created.update_metadata({"team": "eval"})
    updated = await repository.update(created)
    assert updated.description == "Scores accuracy"
    assert updated.metadata == {"team": "eval"}
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown plugin id."""
    repository, owner_id = setup
    plugin = _plugin(owner_id)
    with pytest.raises(PluginNotFound, match=f"Plugin {plugin.id} was not found"):
        await repository.update(plugin)


async def test_delete(setup: Setup) -> None:
    """Delete a stored plugin."""
    repository, owner_id = setup
    created = await repository.create(_plugin(owner_id))
    await repository.delete(created.id)
    with pytest.raises(PluginNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown plugin id."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(PluginNotFound, match=f"Plugin {missing_id} was not found"):
        await repository.delete(missing_id)


async def test_create_version_numbers_sequentially_and_bumps_latest(
    setup: Setup,
) -> None:
    """Assign sequential version numbers and bump the plugin's latest_version."""
    repository, owner_id = setup
    plugin = await repository.create(_plugin(owner_id))
    first = await repository.create_version(plugin.id, SOURCE_V1, display_version="v1")
    second = await repository.create_version(plugin.id, SOURCE_V2, display_version="v2")
    assert first.version == 1
    assert second.version == 2
    reloaded = await repository.get(plugin.id)
    assert reloaded.latest_version == 2


async def test_create_version_plugin_not_found(setup: Setup) -> None:
    """Raise for an unknown plugin id."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(PluginNotFound, match=f"Plugin {missing_id} was not found"):
        await repository.create_version(missing_id, SOURCE_V1, display_version=None)


async def test_delete_cascades_versions(setup: Setup) -> None:
    """Cascade a plugin's versions when it is deleted."""
    repository, owner_id = setup
    plugin = await repository.create(_plugin(owner_id))
    await repository.create_version(plugin.id, SOURCE_V1, display_version=None)
    await repository.delete(plugin.id)
    with pytest.raises(PluginVersionNotFound):
        await repository.get_version(plugin.id, 1)


async def test_get_version_by_number(setup: Setup) -> None:
    """Load a plugin version addressed by its version number."""
    repository, owner_id = setup
    plugin = await repository.create(_plugin(owner_id))
    created = await repository.create_version(
        plugin.id, SOURCE_V1, display_version="v1"
    )
    loaded = await repository.get_version(plugin.id, created.version)
    assert loaded == created


async def test_get_version_not_found(setup: Setup) -> None:
    """Raise for an unknown version number."""
    repository, owner_id = setup
    plugin = await repository.create(_plugin(owner_id))
    with pytest.raises(
        PluginVersionNotFound, match=f"Version 1 of plugin {plugin.id} was not found"
    ):
        await repository.get_version(plugin.id, 1)


async def test_query_versions(setup: Setup) -> None:
    """Query a plugin's versions."""
    repository, owner_id = setup
    plugin = await repository.create(_plugin(owner_id))
    await repository.create_version(plugin.id, SOURCE_V1, display_version=None)
    await repository.create_version(plugin.id, SOURCE_V2, display_version=None)
    versions, next_cursor = await repository.query_versions(
        PluginVersionFilter(plugin_id=plugin.id)
    )
    assert next_cursor is None
    assert sorted(version.version for version in versions) == [1, 2]


async def test_update_version(setup: Setup) -> None:
    """Persist a display version change and renew the updated timestamp."""
    repository, owner_id = setup
    plugin = await repository.create(_plugin(owner_id))
    created = await repository.create_version(
        plugin.id, SOURCE_V1, display_version="v1"
    )
    created.update_display_version("v1.0.1")
    updated = await repository.update_version(created)
    assert updated.display_version == "v1.0.1"
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated
    loaded = await repository.get_version(plugin.id, created.version)
    assert loaded == updated


async def test_update_version_not_found(setup: Setup) -> None:
    """Raise for an unknown plugin version id."""
    repository, owner_id = setup
    plugin = await repository.create(_plugin(owner_id))
    created = await repository.create_version(
        plugin.id, SOURCE_V1, display_version="v1"
    )
    created.id = uuid.uuid4()
    with pytest.raises(PluginVersionNotFound):
        await repository.update_version(created)
