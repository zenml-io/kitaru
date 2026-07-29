"""Evaluator configuration resolution."""

from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.plugin_resolution import (
    resolve_plugin,
    resolve_plugin_version,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.plugin import PluginKind
from kitaru.server.domain.replay_config import EvaluatorConfig


async def resolve_evaluator_config(
    config: EvaluatorConfig, repository: PluginRepository
) -> EvaluatorConfig:
    """Resolve an evaluator name and optional version to a version id."""
    plugin = await resolve_plugin(config.evaluator, PluginKind.EVALUATOR, repository)
    version = await resolve_plugin_version(plugin, config.version, repository)
    return config.model_copy(update={"evaluator_version_id": version.id})


async def validate_evaluators(
    configs: list[EvaluatorConfig], repository: PluginRepository
) -> list[EvaluatorConfig]:
    """Resolve evaluator configs and reject duplicate resolved versions."""
    resolved = [
        await resolve_evaluator_config(config, repository) for config in configs
    ]
    ids = [config.evaluator_version_id for config in resolved]
    if len(ids) != len(set(ids)):
        raise ValidationError("Evaluator versions must be unique")
    return resolved
