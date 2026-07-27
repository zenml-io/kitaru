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
"""Registry scorer resolution helpers."""

from kitaru.server.application.interfaces.plugin_repository import (
    PluginRepository,
)
from kitaru.server.application.services.plugin_resolution import (
    resolve_plugin_version,
)
from kitaru.server.domain.plugin import PluginKind, PluginVersion
from kitaru.server.domain.replay_config import (
    RegistryScorerConfig,
    ScoringPolicy,
)


async def resolve_registry_scorer(
    plugin_repository: PluginRepository, config: RegistryScorerConfig
) -> PluginVersion:
    """Resolve the registered version a registry scorer runs.

    Args:
        plugin_repository: Plugin repository.
        config: Registry scorer configuration.

    Raises:
        PluginNameNotFound: No scorer has the configured name.
        PluginVersionNotFound: The scorer has no such version.

    Returns:
        Resolved plugin version.
    """
    return await resolve_plugin_version(
        plugin_repository, PluginKind.SCORER, config.name, config.version
    )


async def validate_scoring_policy(
    plugin_repository: PluginRepository, policy: ScoringPolicy
) -> None:
    """Check that every registry scorer of a policy is registered.

    Args:
        plugin_repository: Plugin repository.
        policy: Scoring policy to validate.

    Raises:
        PluginNameNotFound: No scorer has a configured name.
        PluginVersionNotFound: A scorer has no such version.
    """
    for config in policy.scorers:
        if isinstance(config, RegistryScorerConfig):
            await resolve_registry_scorer(plugin_repository, config)
