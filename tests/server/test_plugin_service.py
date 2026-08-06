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
"""Tests for plugin registry use cases."""

import uuid
from typing import Any

import pytest

from conftest import FakeBlobRepository, FakePluginRepository, create_blob
from kitaru.analytics.events import AnalyticsEvent
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.plugin import (
    PluginFilter,
    PluginUpdate,
    PluginVersionFilter,
)
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import Account
from kitaru.server.domain.blob import BlobNotFound
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    InvalidPluginProvider,
    PackagePluginSource,
    PluginKind,
    PluginNotFound,
    PluginVersionNotFound,
    ReservedPluginName,
    ScriptPluginSource,
)

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


class _RecordingAnalytics(ServerAnalytics):
    """Analytics tracker recording track calls instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self.tracked: list[tuple[uuid.UUID, AnalyticsEvent | str, dict[str, Any]]] = []

    def track(
        self,
        user_id: uuid.UUID,
        event: AnalyticsEvent | str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Record a track call instead of buffering it.

        Args:
            user_id: User id.
            event: Event name.
            properties: Event properties.
        """
        self.tracked.append((user_id, event, properties or {}))


@pytest.fixture
def blob_repository() -> FakeBlobRepository:
    """Provide a fake blob repository."""
    return FakeBlobRepository()


@pytest.fixture
def repository(blob_repository: FakeBlobRepository) -> FakePluginRepository:
    """Provide a fake plugin repository wired to the fake blob repository."""
    return FakePluginRepository(blob_repository=blob_repository)


@pytest.fixture
def evaluator_service(
    repository: FakePluginRepository, blob_repository: FakeBlobRepository
) -> PluginService:
    """Provide a plugin service bound to the evaluator kind."""
    return PluginService(
        kind=PluginKind.EVALUATOR,
        repository=repository,
        blob_repository=blob_repository,
    )


@pytest.fixture
def importer_service(
    repository: FakePluginRepository, blob_repository: FakeBlobRepository
) -> PluginService:
    """Provide a plugin service bound to the importer kind, sharing the repository."""
    return PluginService(
        kind=PluginKind.IMPORTER, repository=repository, blob_repository=blob_repository
    )


async def test_create_plugin(evaluator_service: PluginService) -> None:
    """Create a plugin owned by the caller."""
    plugin = await evaluator_service.create_plugin(
        name="accuracy",
        description="Scores accuracy",
        provider=None,
        metadata={"team": "eval"},
        actor=ACTOR,
    )
    assert plugin.kind is PluginKind.EVALUATOR
    assert plugin.name == "accuracy"
    assert plugin.description == "Scores accuracy"
    assert plugin.owner_id == ACTOR.account.id
    assert plugin.metadata == {"team": "eval"}
    assert plugin.latest_version == 0
    assert plugin.created is not None
    assert plugin.updated is not None


async def test_create_plugin_duplicate_name(evaluator_service: PluginService) -> None:
    """Reject a second evaluator with the same name."""
    await evaluator_service.create_plugin(
        name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
    )
    with pytest.raises(
        DuplicatePluginName, match="Evaluator name 'accuracy' is already registered"
    ):
        await evaluator_service.create_plugin(
            name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
        )


async def test_create_plugin_reserved_name(evaluator_service: PluginService) -> None:
    """Reject a name that uses the reserved default-plugin prefix."""
    with pytest.raises(
        ReservedPluginName,
        match="Plugin name 'kitaru/accuracy' uses the reserved prefix 'kitaru/'",
    ):
        await evaluator_service.create_plugin(
            name="kitaru/accuracy",
            description=None,
            provider=None,
            metadata={},
            actor=ACTOR,
        )
    plugins, _ = await evaluator_service.list_plugins(
        PluginFilter(kind=PluginKind.EVALUATOR), actor=ACTOR
    )
    assert plugins == []


async def test_create_plugin_evaluator_rejects_provider(
    evaluator_service: PluginService,
) -> None:
    """Reject a provider on an evaluator plugin."""
    with pytest.raises(InvalidPluginProvider):
        await evaluator_service.create_plugin(
            name="accuracy",
            description=None,
            provider="langfuse",
            metadata={},
            actor=ACTOR,
        )


async def test_create_plugin_importer_allows_provider(
    importer_service: PluginService,
) -> None:
    """Store the provider on an importer plugin."""
    plugin = await importer_service.create_plugin(
        name="langfuse-import",
        description=None,
        provider="langfuse",
        metadata={},
        actor=ACTOR,
    )
    assert plugin.provider == "langfuse"


async def test_evaluator_and_importer_share_a_name(
    evaluator_service: PluginService, importer_service: PluginService
) -> None:
    """Let an evaluator and an importer register the same name."""
    evaluator = await evaluator_service.create_plugin(
        name="shared", description=None, provider=None, metadata={}, actor=ACTOR
    )
    importer = await importer_service.create_plugin(
        name="shared", description=None, provider=None, metadata={}, actor=ACTOR
    )
    assert evaluator.id != importer.id
    assert evaluator.name == importer.name == "shared"


async def test_get_plugin(evaluator_service: PluginService) -> None:
    """Get a plugin by id."""
    created = await evaluator_service.create_plugin(
        name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
    )
    loaded = await evaluator_service.get_plugin(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_plugin_not_found(evaluator_service: PluginService) -> None:
    """Raise for an unknown plugin id."""
    missing_id = uuid.uuid4()
    with pytest.raises(PluginNotFound, match=f"Plugin {missing_id} was not found"):
        await evaluator_service.get_plugin(missing_id, actor=ACTOR)


async def test_list_plugins_scoped_to_kind(
    evaluator_service: PluginService, importer_service: PluginService
) -> None:
    """List only plugins matching the service's bound kind."""
    await evaluator_service.create_plugin(
        name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
    )
    await importer_service.create_plugin(
        name="langfuse-import",
        description=None,
        provider=None,
        metadata={},
        actor=ACTOR,
    )

    evaluators, next_cursor = await evaluator_service.list_plugins(
        PluginFilter(kind=PluginKind.EVALUATOR), actor=ACTOR
    )
    assert next_cursor is None
    assert [plugin.name for plugin in evaluators] == ["accuracy"]

    importers, next_cursor = await importer_service.list_plugins(
        PluginFilter(kind=PluginKind.IMPORTER), actor=ACTOR
    )
    assert next_cursor is None
    assert [plugin.name for plugin in importers] == ["langfuse-import"]


async def test_update_plugin_description(evaluator_service: PluginService) -> None:
    """Update the description, leaving metadata unchanged."""
    created = await evaluator_service.create_plugin(
        name="accuracy",
        description="old",
        provider=None,
        metadata={"team": "eval"},
        actor=ACTOR,
    )
    updated = await evaluator_service.update_plugin(
        created.id, PluginUpdate(description="new"), actor=ACTOR
    )
    assert updated.description == "new"
    assert updated.metadata == {"team": "eval"}


async def test_update_plugin_explicit_null_clears_description(
    evaluator_service: PluginService,
) -> None:
    """Clear the description when it is explicitly set to null."""
    created = await evaluator_service.create_plugin(
        name="accuracy", description="old", provider=None, metadata={}, actor=ACTOR
    )
    updated = await evaluator_service.update_plugin(
        created.id, PluginUpdate(description=None), actor=ACTOR
    )
    assert updated.description is None


async def test_update_plugin_omitted_description_stays_unchanged(
    evaluator_service: PluginService,
) -> None:
    """Leave the description unchanged when the update omits it."""
    created = await evaluator_service.create_plugin(
        name="accuracy", description="old", provider=None, metadata={}, actor=ACTOR
    )
    updated = await evaluator_service.update_plugin(
        created.id, PluginUpdate(metadata={"team": "eval"}), actor=ACTOR
    )
    assert updated.description == "old"
    assert updated.metadata == {"team": "eval"}


async def test_update_plugin_not_found(evaluator_service: PluginService) -> None:
    """Raise for an unknown plugin id."""
    with pytest.raises(PluginNotFound):
        await evaluator_service.update_plugin(
            uuid.uuid4(), PluginUpdate(description="new"), actor=ACTOR
        )


async def test_delete_plugin(evaluator_service: PluginService) -> None:
    """Delete a stored plugin."""
    created = await evaluator_service.create_plugin(
        name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
    )
    await evaluator_service.delete_plugin(created.id, actor=ACTOR)
    with pytest.raises(PluginNotFound):
        await evaluator_service.get_plugin(created.id, actor=ACTOR)


async def test_delete_plugin_not_found(evaluator_service: PluginService) -> None:
    """Raise for an unknown plugin id."""
    with pytest.raises(PluginNotFound):
        await evaluator_service.delete_plugin(uuid.uuid4(), actor=ACTOR)


async def test_create_version_numbers_sequentially(
    evaluator_service: PluginService,
) -> None:
    """Assign sequential version numbers starting at 1."""
    plugin = await evaluator_service.create_plugin(
        name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
    )
    first = await evaluator_service.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"),
        display_version="v1",
        actor=ACTOR,
    )
    second = await evaluator_service.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-scorer==2.0.0", entrypoint="pkg:score"),
        display_version="v2",
        actor=ACTOR,
    )
    assert first.version == 1
    assert second.version == 2
    reloaded = await evaluator_service.get_plugin(plugin.id, actor=ACTOR)
    assert reloaded.latest_version == 2


async def test_create_version_plugin_not_found(
    evaluator_service: PluginService,
) -> None:
    """Raise for an unknown plugin id."""
    with pytest.raises(PluginNotFound):
        await evaluator_service.create_version(
            uuid.uuid4(),
            PackagePluginSource(
                requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"
            ),
            display_version=None,
            actor=ACTOR,
        )


async def test_create_version_script_source_checks_blob(
    evaluator_service: PluginService, blob_repository: FakeBlobRepository
) -> None:
    """Reject a script version naming an unknown blob."""
    plugin = await evaluator_service.create_plugin(
        name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
    )
    missing_blob_id = uuid.uuid4()
    with pytest.raises(BlobNotFound, match=f"Blob {missing_blob_id} was not found"):
        await evaluator_service.create_version(
            plugin.id,
            ScriptPluginSource(blob_id=missing_blob_id, entrypoint="score"),
            display_version=None,
            actor=ACTOR,
        )


async def test_create_version_script_source_with_known_blob(
    evaluator_service: PluginService, blob_repository: FakeBlobRepository
) -> None:
    """Accept a script version naming a stored blob."""
    plugin = await evaluator_service.create_plugin(
        name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
    )
    blob = await create_blob(blob_repository, ACTOR.account.id)
    version = await evaluator_service.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=blob.id, entrypoint="score"),
        display_version=None,
        actor=ACTOR,
    )
    assert isinstance(version.source, ScriptPluginSource)
    assert version.source.blob_id == blob.id


async def test_get_version(evaluator_service: PluginService) -> None:
    """Get a plugin version by version number."""
    plugin = await evaluator_service.create_plugin(
        name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
    )
    created = await evaluator_service.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"),
        display_version=None,
        actor=ACTOR,
    )
    loaded = await evaluator_service.get_version(
        plugin.id, created.version, actor=ACTOR
    )
    assert loaded == created


async def test_get_version_not_found(evaluator_service: PluginService) -> None:
    """Raise for an unknown version number."""
    plugin = await evaluator_service.create_plugin(
        name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
    )
    with pytest.raises(
        PluginVersionNotFound, match=f"Version 1 of plugin {plugin.id} was not found"
    ):
        await evaluator_service.get_version(plugin.id, 1, actor=ACTOR)


async def test_list_versions(evaluator_service: PluginService) -> None:
    """List a plugin's versions."""
    plugin = await evaluator_service.create_plugin(
        name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
    )
    await evaluator_service.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"),
        display_version=None,
        actor=ACTOR,
    )
    await evaluator_service.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-scorer==2.0.0", entrypoint="pkg:score"),
        display_version=None,
        actor=ACTOR,
    )
    versions, next_cursor = await evaluator_service.list_versions(
        PluginVersionFilter(plugin_id=plugin.id), actor=ACTOR
    )
    assert next_cursor is None
    assert sorted(version.version for version in versions) == [1, 2]


async def test_update_version_display_version(evaluator_service: PluginService) -> None:
    """Update a version's display version."""
    plugin = await evaluator_service.create_plugin(
        name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
    )
    created = await evaluator_service.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"),
        display_version="v1",
        actor=ACTOR,
    )
    updated = await evaluator_service.update_version(
        plugin.id, created.version, display_version="v1.0.1", actor=ACTOR
    )
    assert updated.display_version == "v1.0.1"


async def test_update_version_not_found(evaluator_service: PluginService) -> None:
    """Raise for an unknown version number."""
    plugin = await evaluator_service.create_plugin(
        name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
    )
    with pytest.raises(PluginVersionNotFound):
        await evaluator_service.update_version(
            plugin.id, 1, display_version="v1", actor=ACTOR
        )


async def test_create_version_tracks_plugin_registered(
    repository: FakePluginRepository, blob_repository: FakeBlobRepository
) -> None:
    """Fire PLUGIN_REGISTERED with the plugin kind and source type."""
    analytics = _RecordingAnalytics()
    service = PluginService(
        kind=PluginKind.EVALUATOR,
        repository=repository,
        blob_repository=blob_repository,
        analytics=analytics,
    )
    plugin = await service.create_plugin(
        name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
    )

    await service.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"),
        display_version="v1",
        actor=ACTOR,
    )

    assert len(analytics.tracked) == 1
    user_id, event, properties = analytics.tracked[0]
    assert user_id == ACTOR.account.id
    assert event == AnalyticsEvent.PLUGIN_REGISTERED
    assert properties == {"kind": "evaluator", "source_type": "package"}


async def test_create_version_without_analytics_tracker(
    evaluator_service: PluginService,
) -> None:
    """Register a plugin version normally when no analytics tracker is configured."""
    plugin = await evaluator_service.create_plugin(
        name="accuracy", description=None, provider=None, metadata={}, actor=ACTOR
    )
    version = await evaluator_service.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"),
        display_version="v1",
        actor=ACTOR,
    )
    assert version.version == 1
