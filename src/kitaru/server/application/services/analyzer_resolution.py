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
"""Analyzer config resolution against the plugin registry."""

import uuid

from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.replay_config import AnalyzerConfigInput
from kitaru.server.application.services.plugin_resolution import (
    resolve_plugin,
    resolve_plugin_version,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.plugin import PluginKind
from kitaru.server.domain.replay_config import AnalyzerConfig


async def resolve_analyzer_config(
    config: AnalyzerConfigInput,
    plugin_repository: PluginRepository,
    actor: AuthContext | None = None,
) -> AnalyzerConfig:
    """Resolve an analyzer config to a concrete plugin version.

    An omitted version resolves to the analyzer's latest version.

    Args:
        config: Analyzer config awaiting resolution.
        plugin_repository: Plugin repository, queried for the analyzer kind.
        actor: Caller context, unused, ownership is provenance only.

    Raises:
        PluginNotFound: No analyzer plugin has this name.
        PluginVersionNotFound: The resolved version has no matching plugin
            version.

    Returns:
        Resolved analyzer config carrying the concrete version and its id.
    """
    _ = actor
    plugin = await resolve_plugin(
        config.analyzer, PluginKind.ANALYZER, plugin_repository
    )
    plugin_version = await resolve_plugin_version(
        plugin, config.version, plugin_repository
    )
    return AnalyzerConfig(
        analyzer=config.analyzer,
        version=plugin_version.version,
        params=config.params,
        analyzer_version_id=plugin_version.id,
    )


async def validate_analyzers(
    configs: list[AnalyzerConfigInput],
    plugin_repository: PluginRepository,
    actor: AuthContext | None = None,
) -> list[AnalyzerConfig]:
    """Resolve every analyzer config, rejecting a repeated resolved version.

    Args:
        configs: Analyzer configs awaiting resolution.
        plugin_repository: Plugin repository, queried for the analyzer kind.
        actor: Caller context, unused, ownership is provenance only.

    Raises:
        PluginNotFound: A config names an unknown analyzer.
        PluginVersionNotFound: A config names an unknown version.
        ValidationError: Two configs resolve to the same analyzer version.

    Returns:
        Resolved analyzer configs.
    """
    resolved = [
        await resolve_analyzer_config(config, plugin_repository, actor)
        for config in configs
    ]
    seen_ids: set[uuid.UUID] = set()
    for analyzer_config in resolved:
        if analyzer_config.analyzer_version_id in seen_ids:
            raise ValidationError(
                "An analyzer version appears more than once in the analyzer list"
            )
        seen_ids.add(analyzer_config.analyzer_version_id)
    return resolved
