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
"""Tests for plugin use cases."""

import uuid

import pytest

from conftest import FakeBlobRepository, FakePluginRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.plugins import (
    PluginFilter,
    PluginVersionFilter,
)
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.domain.account import Account
from kitaru.server.domain.blob import Blob, BlobNotFound
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    InvalidPlugin,
    InvalidPluginVersion,
    PluginFormat,
    PluginKind,
    PluginNotFound,
    PluginVersionNotFound,
)

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))
FOREIGN_ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="bob"))

SCORER = PluginKind.SCORER
IMPORTER = PluginKind.IMPORTER


@pytest.fixture
def blob_repository() -> FakeBlobRepository:
    """Provide a fake blob repository."""
    return FakeBlobRepository()


@pytest.fixture
def repository(blob_repository: FakeBlobRepository) -> FakePluginRepository:
    """Provide a fake plugin repository."""
    return FakePluginRepository(blob_repository)


@pytest.fixture
def service(
    repository: FakePluginRepository, blob_repository: FakeBlobRepository
) -> PluginService:
    """Provide a scorer-bound plugin service backed by the fake repositories."""
    return PluginService(
        repository=repository, blob_repository=blob_repository, kind=SCORER
    )


@pytest.fixture
def importer_service(
    repository: FakePluginRepository, blob_repository: FakeBlobRepository
) -> PluginService:
    """Provide an importer-bound plugin service backed by the fake repositories."""
    return PluginService(
        repository=repository, blob_repository=blob_repository, kind=IMPORTER
    )


async def create_blob(
    repository: FakeBlobRepository, sha256: str = "a" * 64
) -> uuid.UUID:
    """Store a code blob in the fake repository.

    Args:
        repository: Fake blob repository.
        sha256: Hash of the content.

    Returns:
        Id of the stored blob.
    """
    blob = await repository.create(
        Blob(
            owner_id=ACTOR.account.id,
            sha256=sha256,
            size=3,
            media_type="text/x-python",
            data=b"abc",
        )
    )
    return blob.id


async def test_create_scorer(service: PluginService) -> None:
    """Create a scorer owned by the caller."""
    plugin = await service.create_plugin(
        name="relevance", provider=None, metadata={}, actor=ACTOR
    )
    assert plugin.kind is SCORER
    assert plugin.name == "relevance"
    assert plugin.owner_id == ACTOR.account.id
    assert plugin.provider is None
    assert plugin.metadata == {}
    assert plugin.latest_version == 0
    assert plugin.created is not None
    assert plugin.updated is not None


async def test_create_importer(importer_service: PluginService) -> None:
    """Create an importer carrying a provider and configuration."""
    plugin = await importer_service.create_plugin(
        name="langfuse",
        provider="langfuse",
        metadata={"region": "eu"},
        actor=ACTOR,
    )
    assert plugin.kind is IMPORTER
    assert plugin.provider == "langfuse"
    assert plugin.metadata == {"region": "eu"}


async def test_create_scorer_with_provider_rejected(service: PluginService) -> None:
    """Reject a provider on a scorer."""
    with pytest.raises(InvalidPlugin, match="Scorers do not carry a provider"):
        await service.create_plugin(
            name="relevance", provider="langfuse", metadata={}, actor=ACTOR
        )


async def test_create_scorer_with_metadata_rejected(service: PluginService) -> None:
    """Reject metadata on a scorer."""
    with pytest.raises(InvalidPlugin, match="Scorers do not carry metadata"):
        await service.create_plugin(
            name="relevance",
            provider=None,
            metadata={"region": "eu"},
            actor=ACTOR,
        )


async def test_create_plugin_duplicate_name(service: PluginService) -> None:
    """Reject a second plugin of the same kind and name."""
    await service.create_plugin(
        name="relevance", provider=None, metadata={}, actor=ACTOR
    )
    with pytest.raises(
        DuplicatePluginName, match="Plugin name 'relevance' is already registered"
    ):
        await service.create_plugin(
            name="relevance", provider=None, metadata={}, actor=ACTOR
        )


async def test_create_plugin_same_name_other_kind(
    service: PluginService, importer_service: PluginService
) -> None:
    """Accept the same name for a plugin of another kind."""
    await service.create_plugin(name="shared", provider=None, metadata={}, actor=ACTOR)
    importer = await importer_service.create_plugin(
        name="shared", provider=None, metadata={}, actor=ACTOR
    )
    assert importer.name == "shared"


async def test_get_plugin(service: PluginService) -> None:
    """Load a stored plugin by id."""
    created = await service.create_plugin(
        name="relevance", provider=None, metadata={}, actor=ACTOR
    )
    assert await service.get_plugin(created.id, actor=ACTOR) == created


async def test_get_plugin_other_kind(
    service: PluginService, importer_service: PluginService
) -> None:
    """Report a plugin of another kind as not found."""
    created = await service.create_plugin(
        name="relevance", provider=None, metadata={}, actor=ACTOR
    )
    with pytest.raises(PluginNotFound, match=f"Plugin {created.id} was not found"):
        await importer_service.get_plugin(created.id, actor=ACTOR)


async def test_get_plugin_not_found(service: PluginService) -> None:
    """Raise for an unknown plugin id."""
    missing_id = uuid.uuid4()
    with pytest.raises(PluginNotFound, match=f"Plugin {missing_id} was not found"):
        await service.get_plugin(missing_id, actor=ACTOR)


async def test_get_plugin_foreign_owner(service: PluginService) -> None:
    """Read a plugin owned by another account."""
    created = await service.create_plugin(
        name="relevance", provider=None, metadata={}, actor=ACTOR
    )
    assert await service.get_plugin(created.id, actor=FOREIGN_ACTOR) == created


async def test_list_plugins(
    service: PluginService, importer_service: PluginService
) -> None:
    """List plugins of one kind with filters and pagination."""
    for name in ["alpha", "beta", "gamma"]:
        await service.create_plugin(name=name, provider=None, metadata={}, actor=ACTOR)
    await importer_service.create_plugin(
        name="langfuse", provider="langfuse", metadata={}, actor=ACTOR
    )

    plugins, total = await service.list_plugins(PluginFilter(kind=SCORER), actor=ACTOR)
    assert total == 3
    assert [plugin.name for plugin in plugins] == ["alpha", "beta", "gamma"]

    plugins, total = await service.list_plugins(
        PluginFilter(kind=SCORER, name="beta"), actor=ACTOR
    )
    assert total == 1
    assert plugins[0].name == "beta"

    plugins, total = await service.list_plugins(
        PluginFilter(kind=SCORER, page=2, page_size=2), actor=ACTOR
    )
    assert total == 3
    assert [plugin.name for plugin in plugins] == ["gamma"]

    plugins, total = await service.list_plugins(
        PluginFilter(kind=IMPORTER), actor=ACTOR
    )
    assert total == 1
    assert plugins[0].name == "langfuse"


async def test_list_plugins_provider_filter(importer_service: PluginService) -> None:
    """List importers filtered on the provider."""
    await importer_service.create_plugin(
        name="one", provider="langfuse", metadata={}, actor=ACTOR
    )
    await importer_service.create_plugin(
        name="two", provider="braintrust", metadata={}, actor=ACTOR
    )

    plugins, total = await importer_service.list_plugins(
        PluginFilter(kind=IMPORTER, provider="braintrust"), actor=ACTOR
    )
    assert total == 1
    assert plugins[0].name == "two"


async def test_delete_plugin(service: PluginService) -> None:
    """Delete a stored plugin."""
    created = await service.create_plugin(
        name="relevance", provider=None, metadata={}, actor=ACTOR
    )
    await service.delete_plugin(created.id, actor=ACTOR)
    with pytest.raises(PluginNotFound):
        await service.get_plugin(created.id, actor=ACTOR)


async def test_delete_plugin_other_kind(
    service: PluginService, importer_service: PluginService
) -> None:
    """Report a plugin of another kind as not found when deleting it."""
    created = await service.create_plugin(
        name="relevance", provider=None, metadata={}, actor=ACTOR
    )
    with pytest.raises(PluginNotFound):
        await importer_service.delete_plugin(created.id, actor=ACTOR)
    assert await service.get_plugin(created.id, actor=ACTOR) == created


async def test_delete_plugin_removes_versions(
    service: PluginService, blob_repository: FakeBlobRepository
) -> None:
    """Delete the versions of a deleted plugin."""
    blob_id = await create_blob(blob_repository)
    created = await service.create_plugin(
        name="relevance", provider=None, metadata={}, actor=ACTOR
    )
    await service.create_version(
        created.id,
        format=PluginFormat.INLINE,
        blob_id=blob_id,
        entrypoint="score",
        actor=ACTOR,
    )
    await service.delete_plugin(created.id, actor=ACTOR)
    with pytest.raises(PluginNotFound):
        await service.list_versions(
            PluginVersionFilter(plugin_id=created.id), actor=ACTOR
        )


async def test_create_version_allocates_numbers(
    service: PluginService, blob_repository: FakeBlobRepository
) -> None:
    """Number versions from one upward and track the latest on the plugin."""
    blob_id = await create_blob(blob_repository)
    plugin = await service.create_plugin(
        name="relevance", provider=None, metadata={}, actor=ACTOR
    )
    first = await service.create_version(
        plugin.id,
        format=PluginFormat.INLINE,
        blob_id=blob_id,
        entrypoint="score",
        actor=ACTOR,
    )
    second = await service.create_version(
        plugin.id,
        format=PluginFormat.INLINE,
        blob_id=blob_id,
        entrypoint="score",
        actor=ACTOR,
    )
    assert first.version == 1
    assert second.version == 2
    assert first.plugin_id == plugin.id
    assert first.format is PluginFormat.INLINE
    assert first.blob_id == blob_id
    assert first.entrypoint == "score"
    assert first.created is not None
    reloaded = await service.get_plugin(plugin.id, actor=ACTOR)
    assert reloaded.latest_version == 2


async def test_create_version_plugin_not_found(service: PluginService) -> None:
    """Raise for an unknown plugin id."""
    missing_id = uuid.uuid4()
    with pytest.raises(PluginNotFound):
        await service.create_version(
            missing_id,
            format=PluginFormat.INLINE,
            blob_id=uuid.uuid4(),
            entrypoint="score",
            actor=ACTOR,
        )


async def test_create_version_blob_not_found(service: PluginService) -> None:
    """Raise for an unknown blob id."""
    plugin = await service.create_plugin(
        name="relevance", provider=None, metadata={}, actor=ACTOR
    )
    missing_id = uuid.uuid4()
    with pytest.raises(BlobNotFound, match=f"Blob {missing_id} was not found"):
        await service.create_version(
            plugin.id,
            format=PluginFormat.INLINE,
            blob_id=missing_id,
            entrypoint="score",
            actor=ACTOR,
        )


async def test_create_version_empty_entrypoint(
    service: PluginService, blob_repository: FakeBlobRepository
) -> None:
    """Reject an empty entrypoint."""
    blob_id = await create_blob(blob_repository)
    plugin = await service.create_plugin(
        name="relevance", provider=None, metadata={}, actor=ACTOR
    )
    with pytest.raises(InvalidPluginVersion, match="Entrypoint must not be empty"):
        await service.create_version(
            plugin.id,
            format=PluginFormat.INLINE,
            blob_id=blob_id,
            entrypoint="",
            actor=ACTOR,
        )


async def test_get_version(
    service: PluginService, blob_repository: FakeBlobRepository
) -> None:
    """Load a plugin version by version number."""
    blob_id = await create_blob(blob_repository)
    plugin = await service.create_plugin(
        name="relevance", provider=None, metadata={}, actor=ACTOR
    )
    created = await service.create_version(
        plugin.id,
        format=PluginFormat.INLINE,
        blob_id=blob_id,
        entrypoint="score",
        actor=ACTOR,
    )
    assert await service.get_version(plugin.id, 1, actor=ACTOR) == created


async def test_get_version_not_found(service: PluginService) -> None:
    """Raise for a version number the plugin does not have."""
    plugin = await service.create_plugin(
        name="relevance", provider=None, metadata={}, actor=ACTOR
    )
    with pytest.raises(
        PluginVersionNotFound, match=f"Plugin {plugin.id} has no version 1"
    ):
        await service.get_version(plugin.id, 1, actor=ACTOR)


async def test_list_versions(
    service: PluginService, blob_repository: FakeBlobRepository
) -> None:
    """List the versions of a plugin in version order."""
    blob_id = await create_blob(blob_repository)
    plugin = await service.create_plugin(
        name="relevance", provider=None, metadata={}, actor=ACTOR
    )
    for _ in range(3):
        await service.create_version(
            plugin.id,
            format=PluginFormat.INLINE,
            blob_id=blob_id,
            entrypoint="score",
            actor=ACTOR,
        )

    versions, total = await service.list_versions(
        PluginVersionFilter(plugin_id=plugin.id), actor=ACTOR
    )
    assert total == 3
    assert [version.version for version in versions] == [1, 2, 3]

    versions, total = await service.list_versions(
        PluginVersionFilter(plugin_id=plugin.id, page=2, page_size=2),
        actor=ACTOR,
    )
    assert total == 3
    assert [version.version for version in versions] == [3]


async def test_list_versions_plugin_not_found(service: PluginService) -> None:
    """Raise for an unknown plugin id."""
    with pytest.raises(PluginNotFound):
        await service.list_versions(
            PluginVersionFilter(plugin_id=uuid.uuid4()), actor=ACTOR
        )
