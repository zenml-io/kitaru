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
"""Evaluator config resolution against the plugin registry."""

import uuid

from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.replay_config import EvaluatorConfigInput
from kitaru.server.application.services.plugin_resolution import (
    resolve_plugin,
    resolve_plugin_version,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.names import RESERVED_NAMESPACE
from kitaru.server.domain.plugin import PluginKind
from kitaru.server.domain.replay_config import EvaluatorConfig

OUTPUT_CONTRACT_EVALUATOR = f"{RESERVED_NAMESPACE}/output-contract"


def _validate_builtin_evaluator_params(config: EvaluatorConfigInput) -> None:
    """Validate parameters required by built-in evaluators."""
    if config.evaluator != OUTPUT_CONTRACT_EVALUATOR:
        return
    params = config.params
    if (
        "expected" not in params
        and params.get("required_paths") is None
        and params.get("type_requirements") is None
    ):
        raise ValidationError(
            f"Evaluator '{OUTPUT_CONTRACT_EVALUATOR}' requires at least one rule: "
            "expected, required_paths, or type_requirements"
        )


async def resolve_evaluator_config(
    config: EvaluatorConfigInput,
    plugin_repository: PluginRepository,
    actor: AuthContext | None = None,
) -> EvaluatorConfig:
    """Resolve an evaluator config to a concrete plugin version.

    An omitted version resolves to the evaluator's latest version.

    Args:
        config: Evaluator config awaiting resolution.
        plugin_repository: Plugin repository, queried for the evaluator kind.
        actor: Caller context, unused, ownership is provenance only.

    Raises:
        PluginNotFound: No evaluator plugin has this name.
        PluginVersionNotFound: The resolved version has no matching plugin
            version.

    Returns:
        Resolved evaluator config carrying the concrete version and its id.
    """
    _ = actor
    plugin = await resolve_plugin(
        config.evaluator, PluginKind.EVALUATOR, plugin_repository
    )
    plugin_version = await resolve_plugin_version(
        plugin, config.version, plugin_repository
    )
    _validate_builtin_evaluator_params(config)
    return EvaluatorConfig(
        evaluator=config.evaluator,
        version=plugin_version.version,
        params=config.params,
        evaluator_version_id=plugin_version.id,
    )


async def validate_evaluators(
    configs: list[EvaluatorConfigInput],
    plugin_repository: PluginRepository,
    actor: AuthContext | None = None,
) -> list[EvaluatorConfig]:
    """Resolve every evaluator config, rejecting a repeated resolved version.

    Args:
        configs: Evaluator configs awaiting resolution.
        plugin_repository: Plugin repository, queried for the evaluator kind.
        actor: Caller context, unused, ownership is provenance only.

    Raises:
        PluginNotFound: A config names an unknown evaluator.
        PluginVersionNotFound: A config names an unknown version.
        ValidationError: Two configs resolve to the same evaluator version.

    Returns:
        Resolved evaluator configs.
    """
    resolved = [
        await resolve_evaluator_config(config, plugin_repository, actor)
        for config in configs
    ]
    seen_ids: set[uuid.UUID] = set()
    for evaluator_config in resolved:
        if evaluator_config.evaluator_version_id in seen_ids:
            raise ValidationError(
                "An evaluator version appears more than once in the evaluator list"
            )
        seen_ids.add(evaluator_config.evaluator_version_id)
    return resolved
