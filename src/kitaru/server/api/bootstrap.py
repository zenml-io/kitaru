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
"""Default account, default plugin, and server id bootstrap at server startup."""

import hashlib
import logging
import uuid
from importlib.metadata import version

from kitaru.base import FrozenModel
from kitaru.server.adapters.auth.passwords import BcryptPasswordHasher
from kitaru.server.adapters.permissions.admin_flag import AdminFlagPermissionProvider
from kitaru.server.application.interfaces.account_repository import AccountRepository
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.interfaces.server_settings_repository import (
    ServerSettingsRepository,
)
from kitaru.server.application.services.account_service import AccountService
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.blob import Blob
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    DuplicatePluginVersion,
    Plugin,
    PluginKind,
    PluginNotFound,
    ScriptPluginSource,
)

_SOURCE_MEDIA_TYPE = "text/x-python"

logger = logging.getLogger(__name__)


async def ensure_server_id(
    repository: ServerSettingsRepository, configured_id: uuid.UUID | None
) -> uuid.UUID:
    """Persist the server id on first startup and load it after.

    Args:
        repository: Server settings repository.
        configured_id: Configured server id, None generates one.

    Returns:
        Stored server id.
    """
    return await repository.ensure_server_id(configured_id or uuid.uuid4())


async def ensure_default_account(
    repository: AccountRepository,
    name: str,
    password: str | None,
    analytics: ServerAnalytics | None = None,
) -> None:
    """Create the default account when it does not exist.

    Args:
        repository: Account repository.
        name: Account name.
        password: Login password, hashed before storage.
        analytics: Analytics tracker, None skips tracking.
    """
    account_service = AccountService(
        repository=repository,
        password_hasher=BcryptPasswordHasher(),
        permission_service=PermissionService(AdminFlagPermissionProvider()),
        analytics=analytics,
    )
    await account_service.ensure_account(name, password)


class DefaultPluginDefinition(FrozenModel):
    """Default plugin definition."""

    kind: PluginKind
    name: str
    description: str
    provider: str | None
    logo_url: str | None = None
    entrypoint: str
    content: bytes
    version: int


DEFAULT_PLUGIN_DEFINITIONS: tuple[DefaultPluginDefinition, ...] = ()


async def _get_or_create_plugin(
    repository: PluginRepository, definition: DefaultPluginDefinition
) -> Plugin:
    """Load a default plugin, creating it ownerless on first startup.

    Args:
        repository: Plugin repository.
        definition: Default plugin definition.

    Returns:
        Stored plugin.
    """
    try:
        return await repository.get_by_name(definition.kind, definition.name)
    except PluginNotFound:
        pass
    try:
        plugin = await repository.create(
            Plugin(
                owner_id=None,
                kind=definition.kind,
                name=definition.name,
                description=definition.description,
                provider=definition.provider,
                logo_url=definition.logo_url,
                metadata={},
            )
        )
        logger.info("Created default plugin %s.", definition.name)
        return plugin
    except DuplicatePluginName:
        return await repository.get_by_name(definition.kind, definition.name)


async def register_default_plugins(
    repository: PluginRepository, blob_repository: BlobRepository
) -> None:
    """Create the built-in default plugins and their declared versions.

    Args:
        repository: Plugin repository.
        blob_repository: Blob repository.
    """
    installed_version = version("kitaru")
    for definition in DEFAULT_PLUGIN_DEFINITIONS:
        plugin = await _get_or_create_plugin(repository, definition)
        if plugin.latest_version >= definition.version:
            logger.debug("Default plugin %s is already current.", definition.name)
            continue
        blob, _ = await blob_repository.create(
            Blob(
                owner_id=None,
                sha256=hashlib.sha256(definition.content).hexdigest(),
                size=len(definition.content),
                media_type=_SOURCE_MEDIA_TYPE,
                data=definition.content,
            )
        )
        try:
            await repository.create_version(
                plugin.id,
                ScriptPluginSource(blob_id=blob.id, entrypoint=definition.entrypoint),
                installed_version,
            )
            logger.info("Created a new version for default plugin %s.", definition.name)
        except DuplicatePluginVersion:
            continue
