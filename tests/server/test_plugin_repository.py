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
"""Contract tests for plugin repositories."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeBlobRepository,
    FakePluginRepository,
    pg_session,
    postgres_available,
)
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.blob_repository import (
    SQLBlobRepository,
)
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
)
from kitaru.server.application.interfaces.plugin_repository import (
    PluginRepository,
)
from kitaru.server.application.models.plugins import (
    PluginFilter,
    PluginVersionFilter,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.blob import Blob, BlobNotFound
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    Plugin,
    PluginFormat,
    PluginKind,
    PluginNotFound,
    PluginVersion,
    PluginVersionIdNotFound,
    PluginVersionNotFound,
)

Setup = tuple[PluginRepository, uuid.UUID, uuid.UUID]

SCORER = PluginKind.SCORER
IMPORTER = PluginKind.IMPORTER


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each plugin repository implementation, an owner id, and a blob id."""
    if request.param == "fake":
        blobs = FakeBlobRepository()
        owner_id = uuid.uuid4()
        blob = await blobs.create(
            Blob(
                owner_id=owner_id,
                sha256="a" * 64,
                size=3,
                media_type="text/x-python",
                data=b"abc",
            )
        )
        yield FakePluginRepository(blobs), owner_id, blob.id
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id and blob_id columns have foreign keys, so store the
        # owning account and the code blob first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        blob = await SQLBlobRepository(session).create(
            Blob(
                owner_id=owner.id,
                sha256="a" * 64,
                size=3,
                media_type="text/x-python",
                data=b"abc",
            )
        )
        yield SQLPluginRepository(session), owner.id, blob.id


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new plugin with both timestamps set."""
    repository, owner_id, _ = setup
    plugin = await repository.create(
        Plugin(owner_id=owner_id, kind=SCORER, name="relevance")
    )
    assert plugin.kind is SCORER
    assert plugin.name == "relevance"
    assert plugin.provider is None
    assert plugin.metadata == {}
    assert plugin.latest_version == 0
    assert plugin.created is not None
    assert plugin.updated is not None


async def test_create_importer_fields(setup: Setup) -> None:
    """Round-trip the provider and configuration of an importer."""
    repository, owner_id, _ = setup
    plugin = await repository.create(
        Plugin(
            owner_id=owner_id,
            kind=IMPORTER,
            name="langfuse",
            provider="langfuse",
            metadata={"region": "eu"},
        )
    )
    assert plugin.provider == "langfuse"
    assert plugin.metadata == {"region": "eu"}
    assert await repository.get(plugin.id) == plugin


async def test_create_duplicate_name(setup: Setup) -> None:
    """Reject a second plugin of the same kind and name."""
    repository, owner_id, _ = setup
    await repository.create(Plugin(owner_id=owner_id, kind=SCORER, name="relevance"))
    with pytest.raises(
        DuplicatePluginName, match="Plugin name 'relevance' is already registered"
    ):
        await repository.create(
            Plugin(owner_id=owner_id, kind=SCORER, name="relevance")
        )


async def test_create_same_name_other_kind(setup: Setup) -> None:
    """Accept the same name for a plugin of another kind."""
    repository, owner_id, _ = setup
    await repository.create(Plugin(owner_id=owner_id, kind=SCORER, name="shared"))
    plugin = await repository.create(
        Plugin(owner_id=owner_id, kind=IMPORTER, name="shared")
    )
    assert plugin.name == "shared"


async def test_create_after_duplicate_failure(setup: Setup) -> None:
    """Keep the repository usable after a duplicate name failure."""
    repository, owner_id, _ = setup
    await repository.create(Plugin(owner_id=owner_id, kind=SCORER, name="relevance"))
    with pytest.raises(DuplicatePluginName):
        await repository.create(
            Plugin(owner_id=owner_id, kind=SCORER, name="relevance")
        )
    plugin = await repository.create(
        Plugin(owner_id=owner_id, kind=SCORER, name="grounding")
    )
    assert plugin.name == "grounding"


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown plugin id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(PluginNotFound, match=f"Plugin {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query(setup: Setup) -> None:
    """Query plugins with filters and pagination."""
    repository, owner_id, _ = setup
    for name in ["alpha", "beta", "gamma"]:
        await repository.create(Plugin(owner_id=owner_id, kind=SCORER, name=name))
    await repository.create(
        Plugin(owner_id=owner_id, kind=IMPORTER, name="langfuse", provider="langfuse")
    )

    plugins, total = await repository.query(PluginFilter(kind=SCORER))
    assert total == 3
    assert [plugin.name for plugin in plugins] == ["alpha", "beta", "gamma"]

    plugins, total = await repository.query(PluginFilter(kind=SCORER, name="beta"))
    assert total == 1
    assert plugins[0].name == "beta"

    plugins, total = await repository.query(
        PluginFilter(kind=SCORER, page=2, page_size=2)
    )
    assert total == 3
    assert [plugin.name for plugin in plugins] == ["gamma"]

    plugins, total = await repository.query(
        PluginFilter(kind=IMPORTER, provider="langfuse")
    )
    assert total == 1
    assert plugins[0].name == "langfuse"

    plugins, total = await repository.query(
        PluginFilter(kind=IMPORTER, provider="braintrust")
    )
    assert total == 0
    assert plugins == []


async def test_query_owner_filter(setup: Setup) -> None:
    """Query plugins filtered on the owner."""
    repository, owner_id, _ = setup
    await repository.create(Plugin(owner_id=owner_id, kind=SCORER, name="relevance"))

    plugins, total = await repository.query(
        PluginFilter(kind=SCORER, owner_id=owner_id)
    )
    assert total == 1

    plugins, total = await repository.query(
        PluginFilter(kind=SCORER, owner_id=uuid.uuid4())
    )
    assert total == 0
    assert plugins == []


async def test_delete(setup: Setup) -> None:
    """Delete a stored plugin."""
    repository, owner_id, _ = setup
    created = await repository.create(
        Plugin(owner_id=owner_id, kind=SCORER, name="relevance")
    )
    await repository.delete(created.id)
    with pytest.raises(PluginNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown plugin id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(PluginNotFound, match=f"Plugin {missing_id} was not found"):
        await repository.delete(missing_id)


async def test_delete_cascades_versions(setup: Setup) -> None:
    """Delete the versions of a deleted plugin."""
    repository, owner_id, blob_id = setup
    created = await repository.create(
        Plugin(owner_id=owner_id, kind=SCORER, name="relevance")
    )
    await repository.create_version(
        PluginVersion(
            plugin_id=created.id,
            format=PluginFormat.INLINE,
            blob_id=blob_id,
            entrypoint="score",
        )
    )
    await repository.delete(created.id)
    versions, total = await repository.query_versions(
        PluginVersionFilter(plugin_id=created.id)
    )
    assert total == 0
    assert versions == []


async def test_create_version_allocates_numbers(setup: Setup) -> None:
    """Number versions from one upward and track the latest on the plugin."""
    repository, owner_id, blob_id = setup
    plugin = await repository.create(
        Plugin(owner_id=owner_id, kind=SCORER, name="relevance")
    )
    first = await repository.create_version(
        PluginVersion(
            plugin_id=plugin.id,
            format=PluginFormat.INLINE,
            blob_id=blob_id,
            entrypoint="score",
        )
    )
    second = await repository.create_version(
        PluginVersion(
            plugin_id=plugin.id,
            format=PluginFormat.INLINE,
            blob_id=blob_id,
            entrypoint="score",
        )
    )
    assert first.version == 1
    assert second.version == 2
    assert first.format is PluginFormat.INLINE
    assert first.blob_id == blob_id
    assert first.entrypoint == "score"
    assert first.created is not None
    assert (await repository.get(plugin.id)).latest_version == 2


async def test_create_version_plugin_not_found(setup: Setup) -> None:
    """Raise for an unknown plugin id."""
    repository, _, blob_id = setup
    missing_id = uuid.uuid4()
    with pytest.raises(PluginNotFound, match=f"Plugin {missing_id} was not found"):
        await repository.create_version(
            PluginVersion(
                plugin_id=missing_id,
                format=PluginFormat.INLINE,
                blob_id=blob_id,
                entrypoint="score",
            )
        )


async def test_create_version_blob_not_found(setup: Setup) -> None:
    """Raise for an unknown blob id."""
    repository, owner_id, _ = setup
    plugin = await repository.create(
        Plugin(owner_id=owner_id, kind=SCORER, name="relevance")
    )
    missing_id = uuid.uuid4()
    with pytest.raises(BlobNotFound, match=f"Blob {missing_id} was not found"):
        await repository.create_version(
            PluginVersion(
                plugin_id=plugin.id,
                format=PluginFormat.INLINE,
                blob_id=missing_id,
                entrypoint="score",
            )
        )


async def test_get_version(setup: Setup) -> None:
    """Load a plugin version by version number."""
    repository, owner_id, blob_id = setup
    plugin = await repository.create(
        Plugin(owner_id=owner_id, kind=SCORER, name="relevance")
    )
    created = await repository.create_version(
        PluginVersion(
            plugin_id=plugin.id,
            format=PluginFormat.INLINE,
            blob_id=blob_id,
            entrypoint="score",
        )
    )
    assert await repository.get_version(plugin.id, 1) == created


async def test_get_version_not_found(setup: Setup) -> None:
    """Raise for a version number the plugin does not have."""
    repository, owner_id, _ = setup
    plugin = await repository.create(
        Plugin(owner_id=owner_id, kind=SCORER, name="relevance")
    )
    with pytest.raises(
        PluginVersionNotFound, match=f"Plugin {plugin.id} has no version 1"
    ):
        await repository.get_version(plugin.id, 1)


async def test_get_version_by_id(setup: Setup) -> None:
    """Load a plugin version by id."""
    repository, owner_id, blob_id = setup
    plugin = await repository.create(
        Plugin(owner_id=owner_id, kind=SCORER, name="relevance")
    )
    created = await repository.create_version(
        PluginVersion(
            plugin_id=plugin.id,
            format=PluginFormat.INLINE,
            blob_id=blob_id,
            entrypoint="score",
        )
    )
    assert await repository.get_version_by_id(created.id) == created


async def test_get_version_by_id_not_found(setup: Setup) -> None:
    """Raise for an unknown plugin version id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        PluginVersionIdNotFound, match=f"Plugin version {missing_id} was not found"
    ):
        await repository.get_version_by_id(missing_id)


async def test_query_versions(setup: Setup) -> None:
    """Query plugin versions in version order with pagination."""
    repository, owner_id, blob_id = setup
    plugin = await repository.create(
        Plugin(owner_id=owner_id, kind=SCORER, name="relevance")
    )
    other = await repository.create(
        Plugin(owner_id=owner_id, kind=SCORER, name="grounding")
    )
    for _ in range(3):
        await repository.create_version(
            PluginVersion(
                plugin_id=plugin.id,
                format=PluginFormat.INLINE,
                blob_id=blob_id,
                entrypoint="score",
            )
        )
    await repository.create_version(
        PluginVersion(
            plugin_id=other.id,
            format=PluginFormat.INLINE,
            blob_id=blob_id,
            entrypoint="score",
        )
    )

    versions, total = await repository.query_versions(
        PluginVersionFilter(plugin_id=plugin.id)
    )
    assert total == 3
    assert [version.version for version in versions] == [1, 2, 3]

    versions, total = await repository.query_versions(
        PluginVersionFilter(plugin_id=plugin.id, page=2, page_size=2)
    )
    assert total == 3
    assert [version.version for version in versions] == [3]


async def test_get_many(setup: Setup) -> None:
    """Load plugins by id and omit ids that do not resolve."""
    repository, owner_id, _ = setup
    first = await repository.create(
        Plugin(owner_id=owner_id, kind=SCORER, name="relevance")
    )
    second = await repository.create(
        Plugin(owner_id=owner_id, kind=SCORER, name="grounding")
    )
    loaded = await repository.get_many([first.id, second.id, uuid.uuid4()])
    assert set(loaded) == {first.id, second.id}
    assert loaded[first.id].name == "relevance"
    assert await repository.get_many([]) == {}


async def test_get_versions_by_ids(setup: Setup) -> None:
    """Load plugin versions by id and omit ids that do not resolve."""
    repository, owner_id, blob_id = setup
    plugin = await repository.create(
        Plugin(owner_id=owner_id, kind=SCORER, name="relevance")
    )
    created = await repository.create_version(
        PluginVersion(
            plugin_id=plugin.id,
            format=PluginFormat.INLINE,
            blob_id=blob_id,
            entrypoint="score",
        )
    )
    loaded = await repository.get_versions_by_ids([created.id, uuid.uuid4()])
    assert loaded == {created.id: created}
    assert await repository.get_versions_by_ids([]) == {}
