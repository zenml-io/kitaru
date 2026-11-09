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
"""In-memory fakes for blobs, payloads, and plugins."""

import hashlib
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, NamedTuple

from kitaru.server.application.interfaces.blob_data_store import BlobDataStores
from kitaru.server.application.models.plugin import PluginFilter, PluginVersionFilter
from kitaru.server.application.models.worker import EphemeralWorkerSpec
from kitaru.server.application.payload_store import PayloadStore
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.blob import (
    Blob,
    BlobContentNotFound,
    BlobInUse,
    BlobNotFound,
    BlobStorageBackend,
)
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    DuplicatePluginVersion,
    Plugin,
    PluginKind,
    PluginNotFound,
    PluginSource,
    PluginVersion,
    PluginVersionIdNotFound,
    PluginVersionNotFound,
    ScriptPluginSource,
)
from kitaru_test_support.common import (
    _evaluate_filter_expression,
    _paginate_fake,
    _renewed_timestamp,
)
from kitaru_test_support.secrets_agents import FakeAgentRepository


class FakeBlobRepository:
    """In-memory blob repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._blobs: dict[uuid.UUID, Blob] = {}
        self._referenced: set[uuid.UUID] = set()

    async def create(self, blob: Blob) -> tuple[Blob, bool]:
        """Persist a new blob, deduping a concurrent identical upload.

        Args:
            blob: Blob to store.

        Returns:
            Stored blob and whether this call created it.
        """
        for other in self._blobs.values():
            if other.sha256 == blob.sha256 and other.media_type == blob.media_type:
                return other.model_copy(), False
        now = datetime.now(UTC)
        stored = blob.model_copy(update={"created": now})
        self._blobs[stored.id] = stored
        return stored.model_copy(), True

    async def get(self, blob_id: uuid.UUID) -> Blob:
        """Load a blob by id.

        Args:
            blob_id: Id of the blob.

        Raises:
            BlobNotFound: No blob has this id.

        Returns:
            Stored blob.
        """
        blob = self._blobs.get(blob_id)
        if blob is None:
            raise BlobNotFound(blob_id)
        return blob.model_copy()

    async def get_many(self, blob_ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, Blob]:
        """Bulk-load blobs by id, keyed by id, missing ids omitted.

        Args:
            blob_ids: Ids of the blobs to load.

        Returns:
            Stored blobs keyed by id.
        """
        wanted = set(blob_ids)
        return {
            blob.id: blob.model_copy()
            for blob in self._blobs.values()
            if blob.id in wanted
        }

    async def get_many_by_sha256s(
        self, sha256s: Sequence[str]
    ) -> dict[tuple[str, str], Blob]:
        """Bulk-load blobs by content hash, keyed by (sha256, media_type).

        Args:
            sha256s: Content hashes to look up.

        Returns:
            Stored blobs keyed by (sha256, media_type), hashes with no
            matching row omitted.
        """
        wanted = set(sha256s)
        return {
            (blob.sha256, blob.media_type): blob.model_copy()
            for blob in self._blobs.values()
            if blob.sha256 in wanted
        }

    async def delete(self, blob_id: uuid.UUID) -> None:
        """Delete a blob by id.

        Args:
            blob_id: Id of the blob.

        Raises:
            BlobNotFound: No blob has this id.
            BlobInUse: The blob is referenced by a plugin version.
        """
        if blob_id not in self._blobs:
            raise BlobNotFound(blob_id)
        if blob_id in self._referenced:
            raise BlobInUse(blob_id)
        del self._blobs[blob_id]

    def mark_referenced(self, blob_id: uuid.UUID) -> None:
        """Mark a blob as referenced by a plugin version, mirroring the FK restrict.

        Args:
            blob_id: Id of the referenced blob.
        """
        self._referenced.add(blob_id)


class FakeBlobDataStore:
    """In-memory blob content store."""

    def __init__(self) -> None:
        """Initialize the data store."""
        self._content: dict[str, bytes] = {}

    async def put(self, sha256: str, data: bytes) -> None:
        """Store content under its hash, idempotent on a repeat hash.

        Args:
            sha256: Content hash.
            data: Content bytes.
        """
        self._content.setdefault(sha256, data)

    async def put_many(self, data_by_sha256: Mapping[str, bytes]) -> None:
        """Store many contents under their hashes, idempotent per hash.

        Args:
            data_by_sha256: Content bytes keyed by their sha256.
        """
        for sha256, data in data_by_sha256.items():
            self._content.setdefault(sha256, data)

    async def get(self, sha256: str) -> bytes:
        """Load content by its hash.

        Args:
            sha256: Content hash.

        Raises:
            BlobContentNotFound: No content is stored under this hash.

        Returns:
            Content bytes.
        """
        data = self._content.get(sha256)
        if data is None:
            raise BlobContentNotFound(sha256)
        return data

    async def get_many(self, sha256s: Sequence[str]) -> dict[str, bytes]:
        """Load many contents by their hashes.

        Args:
            sha256s: Content hashes.

        Raises:
            BlobContentNotFound: A requested hash has no stored content.

        Returns:
            Content bytes keyed by their sha256.
        """
        data_by_sha256: dict[str, bytes] = {}
        for sha256 in sha256s:
            data = self._content.get(sha256)
            if data is None:
                raise BlobContentNotFound(sha256)
            data_by_sha256[sha256] = data
        return data_by_sha256

    async def delete(self, sha256: str) -> None:
        """Delete content by its hash, idempotent on a missing hash.

        Args:
            sha256: Content hash.
        """
        self._content.pop(sha256, None)


class FakeEphemeralWorkers:
    """In-memory ephemeral worker backend recording starts."""

    def __init__(self) -> None:
        """Initialize the backend."""
        self.starts: list[EphemeralWorkerSpec] = []
        self.error: Exception | None = None

    async def start(self, spec: EphemeralWorkerSpec) -> None:
        """Record a start, or raise the configured error.

        Args:
            spec: Ephemeral worker spec.

        Raises:
            Exception: The fake was configured to raise.
        """
        if self.error is not None:
            raise self.error
        self.starts.append(spec)


async def create_blob(
    repository: FakeBlobRepository,
    owner_id: uuid.UUID | None,
    content: bytes = b"blob-content",
    media_type: str = "application/octet-stream",
    data_store: FakeBlobDataStore | None = None,
) -> Blob:
    """Store a blob in the fake repository.

    Args:
        repository: Fake blob repository.
        owner_id: Id of the owning account.
        content: Blob content.
        media_type: Content media type.
        data_store: Fake data store to seed with the content, if given.

    Returns:
        Stored blob.
    """
    sha256 = hashlib.sha256(content).hexdigest()
    if data_store is not None:
        await data_store.put(sha256, content)
    blob, _ = await repository.create(
        Blob(
            owner_id=owner_id,
            sha256=sha256,
            size=len(content),
            media_type=media_type,
            stored_in=BlobStorageBackend.DATABASE,
        )
    )
    return blob


DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD_BYTES = 20 * 1024
DEFAULT_MAX_BLOB_SIZE_BYTES = 100 * 1024 * 1024


class BlobServiceFakes(NamedTuple):
    """Blob service backed by fresh fake blob storage."""

    service: BlobService
    blob_repository: FakeBlobRepository
    blob_data_store: FakeBlobDataStore


def build_blob_service() -> BlobServiceFakes:
    """Build a blob service backed by fresh fake blob storage.

    Returns:
        Blob service bound to fresh fakes, and the fakes themselves.
    """
    blob_repository = FakeBlobRepository()
    blob_data_store = FakeBlobDataStore()
    service = BlobService(
        repository=blob_repository,
        data_stores=BlobDataStores(
            {BlobStorageBackend.DATABASE: blob_data_store}, BlobStorageBackend.DATABASE
        ),
        max_size_bytes=DEFAULT_MAX_BLOB_SIZE_BYTES,
    )
    return BlobServiceFakes(service, blob_repository, blob_data_store)


class PayloadStoreFakes(NamedTuple):
    """Payload store backed by fresh fake blob storage."""

    store: PayloadStore
    blob_repository: FakeBlobRepository
    blob_data_store: FakeBlobDataStore


def build_payload_store(
    threshold_bytes: int = DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD_BYTES,
    blob_repository: FakeBlobRepository | None = None,
) -> PayloadStoreFakes:
    """Build a payload store backed by fake blob storage.

    Args:
        threshold_bytes: Serialized size above which a payload is offloaded.
        blob_repository: Blob registry to share, or None for a fresh one.

    Returns:
        Payload store bound to the fakes, and the fakes themselves.
    """
    if blob_repository is None:
        blob_repository = FakeBlobRepository()
    blob_data_store = FakeBlobDataStore()
    store = PayloadStore(
        repository=blob_repository,
        data_stores=BlobDataStores(
            {BlobStorageBackend.DATABASE: blob_data_store}, BlobStorageBackend.DATABASE
        ),
        threshold_bytes=threshold_bytes,
    )
    return PayloadStoreFakes(store, blob_repository, blob_data_store)


class FakePluginRepository:
    """In-memory plugin and plugin version repository."""

    def __init__(
        self,
        blob_repository: FakeBlobRepository | None = None,
        agent_repository: FakeAgentRepository | None = None,
    ) -> None:
        """Initialize the repository.

        Args:
            blob_repository: Blob repository, marked when a script version
                references one of its blobs, mirroring the FK restrict.
            agent_repository: Agent repository, checked against a plugin's
                agent id on create, mirroring the FK.
        """
        self._plugins: dict[uuid.UUID, Plugin] = {}
        self._versions: dict[uuid.UUID, PluginVersion] = {}
        self._blob_repository = blob_repository
        self._agent_repository = agent_repository

    def _check_duplicate_name(self, plugin: Plugin) -> None:
        for other in self._plugins.values():
            if (
                other.id != plugin.id
                and other.kind == plugin.kind
                and other.name == plugin.name
            ):
                raise DuplicatePluginName(plugin.kind, plugin.name)

    async def create(self, plugin: Plugin) -> Plugin:
        """Persist a new plugin.

        Args:
            plugin: Plugin to store.

        Raises:
            AgentNotFound: The plugin names an agent id and no agent has it.
            DuplicatePluginName: The (kind, name) pair is already registered.

        Returns:
            Stored plugin with timestamps set.
        """
        if (
            plugin.agent_id is not None
            and self._agent_repository is not None
            and not self._agent_repository.exists(plugin.agent_id)
        ):
            raise AgentNotFound(plugin.agent_id)
        self._check_duplicate_name(plugin)
        now = datetime.now(UTC)
        stored = plugin.model_copy(update={"created": now, "updated": now})
        self._plugins[stored.id] = stored
        return stored.model_copy()

    async def get(self, plugin_id: uuid.UUID) -> Plugin:
        """Load a plugin by id.

        Args:
            plugin_id: Id of the plugin.

        Raises:
            PluginNotFound: No plugin has this id.

        Returns:
            Stored plugin.
        """
        plugin = self._plugins.get(plugin_id)
        if plugin is None:
            raise PluginNotFound(plugin_id)
        return plugin.model_copy()

    async def get_by_name(self, kind: PluginKind, name: str) -> Plugin:
        """Load a plugin by kind and name.

        Args:
            kind: Plugin kind.
            name: Plugin name.

        Raises:
            PluginNotFound: No plugin has this kind and name.

        Returns:
            Stored plugin.
        """
        for plugin in self._plugins.values():
            if plugin.kind == kind and plugin.name == name:
                return plugin.model_copy()
        raise PluginNotFound(name)

    async def query(
        self, plugin_filter: PluginFilter
    ) -> tuple[list[Plugin], str | None]:
        """Query plugins matching a filter.

        Args:
            plugin_filter: Filter and pagination parameters.

        Returns:
            Page of matching plugins and the next cursor.
        """
        plugins = [
            plugin
            for plugin in self._plugins.values()
            if plugin.kind == plugin_filter.kind
        ]
        if plugin_filter.expression is not None:
            plugins = [
                plugin
                for plugin in plugins
                if _evaluate_filter_expression(plugin, plugin_filter.expression)
            ]
        page, next_cursor = _paginate_fake(plugins, plugin_filter)
        return [plugin.model_copy() for plugin in page], next_cursor

    async def update(self, plugin: Plugin) -> Plugin:
        """Persist changes to an existing plugin.

        Args:
            plugin: Plugin with modified fields.

        Raises:
            PluginNotFound: No plugin has this id.
            DuplicatePluginName: The (kind, name) pair is already registered.

        Returns:
            Stored plugin with the updated timestamp renewed.
        """
        stored = self._plugins.get(plugin.id)
        if stored is None:
            raise PluginNotFound(plugin.id)
        self._check_duplicate_name(plugin)
        now = _renewed_timestamp(stored.updated)
        updated = plugin.model_copy(update={"created": stored.created, "updated": now})
        self._plugins[plugin.id] = updated
        return updated.model_copy()

    async def delete(self, plugin_id: uuid.UUID) -> None:
        """Delete a plugin by id, cascading its versions.

        Args:
            plugin_id: Id of the plugin.

        Raises:
            PluginNotFound: No plugin has this id.
        """
        if plugin_id not in self._plugins:
            raise PluginNotFound(plugin_id)
        stale_ids = [
            version_id
            for version_id, version in self._versions.items()
            if version.plugin_id == plugin_id
        ]
        del self._plugins[plugin_id]
        for version_id in stale_ids:
            del self._versions[version_id]

    async def create_version(
        self,
        plugin_id: uuid.UUID,
        source: PluginSource,
        display_version: str | None,
    ) -> PluginVersion:
        """Persist a new plugin version with a server-assigned version number.

        Args:
            plugin_id: Id of the plugin.
            source: Plugin code source.
            display_version: Human-readable designator.

        Raises:
            PluginNotFound: No plugin has this id.
            DuplicatePluginVersion: The version number is already registered.

        Returns:
            Stored plugin version with timestamps set.
        """
        stored_plugin = self._plugins.get(plugin_id)
        if stored_plugin is None:
            raise PluginNotFound(plugin_id)
        version_number = stored_plugin.latest_version + 1
        if any(
            version.plugin_id == plugin_id and version.version == version_number
            for version in self._versions.values()
        ):
            raise DuplicatePluginVersion(plugin_id, version_number)
        self._plugins[plugin_id] = stored_plugin.model_copy(
            update={"latest_version": version_number}
        )
        now = datetime.now(UTC)
        version = PluginVersion(
            plugin_id=plugin_id,
            version=version_number,
            display_version=display_version,
            source=source,
            created=now,
            updated=now,
        )
        self._versions[version.id] = version
        if isinstance(source, ScriptPluginSource) and self._blob_repository is not None:
            self._blob_repository.mark_referenced(source.blob_id)
        return version.model_copy()

    async def get_version(self, plugin_id: uuid.UUID, version: int) -> PluginVersion:
        """Load a plugin version by plugin id and version number.

        Args:
            plugin_id: Id of the plugin.
            version: Version number.

        Raises:
            PluginVersionNotFound: No version with this number exists for
                this plugin.

        Returns:
            Stored plugin version.
        """
        for stored in self._versions.values():
            if stored.plugin_id == plugin_id and stored.version == version:
                return stored.model_copy()
        raise PluginVersionNotFound(plugin_id, version)

    async def get_version_by_id(self, plugin_version_id: uuid.UUID) -> PluginVersion:
        """Load a plugin version by id.

        Args:
            plugin_version_id: Id of the plugin version.

        Raises:
            PluginVersionIdNotFound: No plugin version has this id.

        Returns:
            Stored plugin version.
        """
        stored = self._versions.get(plugin_version_id)
        if stored is None:
            raise PluginVersionIdNotFound(plugin_version_id)
        return stored.model_copy()

    async def query_versions(
        self, version_filter: PluginVersionFilter
    ) -> tuple[list[PluginVersion], str | None]:
        """Query plugin versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.

        Returns:
            Page of matching plugin versions and the next cursor.
        """
        versions = [
            version
            for version in self._versions.values()
            if version.plugin_id == version_filter.plugin_id
        ]
        page, next_cursor = _paginate_fake(versions, version_filter)
        return [version.model_copy() for version in page], next_cursor

    async def update_version(self, plugin_version: PluginVersion) -> PluginVersion:
        """Persist changes to an existing plugin version.

        Args:
            plugin_version: Plugin version with modified fields.

        Raises:
            PluginVersionNotFound: No version has this id.

        Returns:
            Stored plugin version with the updated timestamp renewed.
        """
        stored = self._versions.get(plugin_version.id)
        if stored is None:
            raise PluginVersionNotFound(
                plugin_version.plugin_id, plugin_version.version
            )
        now = _renewed_timestamp(stored.updated)
        updated = plugin_version.model_copy(
            update={"created": stored.created, "updated": now}
        )
        self._versions[plugin_version.id] = updated
        return updated.model_copy()


async def create_plugin(
    repository: FakePluginRepository,
    owner_id: uuid.UUID | None,
    kind: PluginKind,
    name: str = "plugin",
    description: str | None = None,
    provider: str | None = None,
    metadata: dict[str, Any] | None = None,
    agent_id: uuid.UUID | None = None,
    connection_schema: dict[str, Any] | None = None,
) -> Plugin:
    """Store a plugin in the fake repository.

    Args:
        repository: Fake plugin repository.
        owner_id: Id of the owning account.
        kind: Plugin kind.
        name: Plugin name.
        description: Plugin description.
        provider: Source system.
        metadata: Arbitrary metadata.
        agent_id: Agent the plugin is scoped to, importers and analyzers
            must leave this unset.
        connection_schema: Connection schema.

    Returns:
        Stored plugin.
    """
    return await repository.create(
        Plugin(
            owner_id=owner_id,
            kind=kind,
            name=name,
            description=description,
            provider=provider,
            metadata=metadata or {},
            agent_id=agent_id,
            connection_schema=connection_schema,
        )
    )
