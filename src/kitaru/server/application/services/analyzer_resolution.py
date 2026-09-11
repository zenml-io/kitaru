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

from kitaru.server.application.interfaces.connection_repository import (
    ConnectionRepository,
)
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.plugin import AnalyzerConfigInput
from kitaru.server.application.services.plugin_resolution import (
    check_unique_plugin_versions,
    resolve_plugin_config,
)
from kitaru.server.domain.plugin import AnalyzerConfig, PluginKind


async def resolve_analyzer_config(
    config: AnalyzerConfigInput,
    plugin_repository: PluginRepository,
    connection_repository: ConnectionRepository,
    actor: AuthContext | None = None,
) -> AnalyzerConfig:
    """Resolve an analyzer config to a concrete plugin version.

    An omitted version resolves to the analyzer's latest version.

    Args:
        config: Analyzer config awaiting resolution.
        plugin_repository: Plugin repository, queried for the analyzer kind.
        connection_repository: Connection repository.
        actor: Caller context, unused, ownership is provenance only.

    Raises:
        PluginNotFound: No analyzer plugin has this name.
        PluginVersionNotFound: The resolved version has no matching plugin
            version.
        ConnectionNotFound: No connection has the named id.

    Returns:
        Resolved analyzer config carrying the concrete version and its id.
    """
    _ = actor
    resolved = await resolve_plugin_config(
        config.analyzer,
        config.version,
        PluginKind.ANALYZER,
        config.connection_id,
        plugin_repository,
        connection_repository,
    )
    return AnalyzerConfig(
        analyzer=config.analyzer,
        min_sessions=config.min_sessions,
        version=resolved.plugin_version.version,
        params=config.params,
        analyzer_version_id=resolved.plugin_version.id,
        provider=resolved.plugin.provider,
        connection_id=resolved.connection_id,
        requires_credentials=resolved.requires_credentials,
    )


async def validate_analyzers(
    configs: list[AnalyzerConfigInput],
    plugin_repository: PluginRepository,
    connection_repository: ConnectionRepository,
    actor: AuthContext | None = None,
) -> list[AnalyzerConfig]:
    """Resolve every analyzer config, rejecting a repeated resolved version.

    Args:
        configs: Analyzer configs awaiting resolution.
        plugin_repository: Plugin repository, queried for the analyzer kind.
        connection_repository: Connection repository.
        actor: Caller context, unused, ownership is provenance only.

    Raises:
        PluginNotFound: A config names an unknown analyzer.
        PluginVersionNotFound: A config names an unknown version.
        ConnectionNotFound: A config names an unknown connection.
        ValidationError: Two configs resolve to the same analyzer version.

    Returns:
        Resolved analyzer configs.
    """
    resolved = [
        await resolve_analyzer_config(
            config, plugin_repository, connection_repository, actor
        )
        for config in configs
    ]
    check_unique_plugin_versions(resolved, "analyzer")
    return resolved
