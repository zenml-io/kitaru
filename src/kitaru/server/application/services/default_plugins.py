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
"""Default plugin registration at server startup."""

import hashlib
import logging
from importlib.metadata import version
from pathlib import Path

from kitaru.base import FrozenModel
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.domain.blob import Blob
from kitaru.server.domain.names import RESERVED_PLUGIN_NAME_PREFIX
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    DuplicatePluginVersion,
    Plugin,
    PluginKind,
    PluginNotFound,
    ScriptPluginSource,
)

_PLUGINS_ROOT = Path(__file__).resolve().parents[5] / "plugins"
_SOURCE_MEDIA_TYPE = "text/x-python"

logger = logging.getLogger(__name__)


class DefaultPluginDefinition(FrozenModel):
    """Default plugin definition."""

    kind: PluginKind
    name: str
    description: str
    provider: str | None
    entrypoint: str
    source_file: str


DEFAULT_PLUGIN_DEFINITIONS: tuple[DefaultPluginDefinition, ...] = (
    DefaultPluginDefinition(
        kind=PluginKind.IMPORTER,
        name=f"{RESERVED_PLUGIN_NAME_PREFIX}langfuse",
        description="Import Langfuse JSON and JSONL trace exports.",
        provider="langfuse",
        entrypoint="parse",
        source_file="importers/langfuse.py",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_PLUGIN_NAME_PREFIX}cost",
        description="Report the total recorded session cost.",
        provider=None,
        entrypoint="cost",
        source_file="evaluators/basic.py",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_PLUGIN_NAME_PREFIX}latency",
        description="Measure session wall-clock duration.",
        provider=None,
        entrypoint="latency",
        source_file="evaluators/basic.py",
    ),
    DefaultPluginDefinition(
        kind=PluginKind.EVALUATOR,
        name=f"{RESERVED_PLUGIN_NAME_PREFIX}tool-call-patterns",
        description="Count repeated calls to the same tool.",
        provider=None,
        entrypoint="tool_call_patterns",
        source_file="evaluators/basic.py",
    ),
)


def _read_source(source_file: str) -> bytes:
    """Read one default plugin source file.

    Args:
        source_file: File path relative to the repository plugins directory.

    Returns:
        Source file content.
    """
    return (_PLUGINS_ROOT / source_file).read_bytes()


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
                metadata={},
            )
        )
        logger.info("Created default plugin %s.", definition.name)
        return plugin
    except DuplicatePluginName:
        return await repository.get_by_name(definition.kind, definition.name)


async def _version_is_current(
    repository: PluginRepository,
    blob_repository: BlobRepository,
    plugin: Plugin,
    definition: DefaultPluginDefinition,
    digest: str,
) -> bool:
    """Return whether a plugin's latest version already matches its packaged source.

    Args:
        repository: Plugin repository.
        blob_repository: Blob repository.
        plugin: Plugin the candidate version belongs to.
        definition: Default plugin definition.
        digest: sha256 digest of the packaged source.

    Returns:
        Whether the latest version's source and entrypoint already match.
    """
    if plugin.latest_version < 1:
        return False
    latest = await repository.get_version(plugin.id, plugin.latest_version)
    if not isinstance(latest.source, ScriptPluginSource):
        return False
    blob = await blob_repository.get_metadata(latest.source.blob_id)
    return blob.sha256 == digest and latest.source.entrypoint == definition.entrypoint


async def register_default_plugins(
    repository: PluginRepository, blob_repository: BlobRepository
) -> None:
    """Create or update the built-in default plugins for the installed version.

    Args:
        repository: Plugin repository.
        blob_repository: Blob repository.
    """
    installed_version = version("kitaru")
    for definition in DEFAULT_PLUGIN_DEFINITIONS:
        content = _read_source(definition.source_file)
        digest = hashlib.sha256(content).hexdigest()
        plugin = await _get_or_create_plugin(repository, definition)
        if await _version_is_current(
            repository, blob_repository, plugin, definition, digest
        ):
            logger.debug("Default plugin %s is already current.", definition.name)
            continue
        blob, _ = await blob_repository.create(
            Blob(
                owner_id=None,
                sha256=digest,
                size=len(content),
                media_type=_SOURCE_MEDIA_TYPE,
                data=content,
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
