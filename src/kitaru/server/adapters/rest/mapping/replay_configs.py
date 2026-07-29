"""Replay configuration DTO conversions."""

from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig as EvaluatorConfigDTO,
)
from kitaru.api_models.v1.replay_config import ReplayOverride as ReplayOverrideDTO
from kitaru.api_models.v1.replay_config import ToolPolicy as ToolPolicyDTO
from kitaru.server.domain.replay_config import (
    EvaluatorConfig,
    ReplayConfig,
    ReplayOverride,
    ToolPolicy,
)


def replay_override_to_response(
    override: ReplayOverride | None,
) -> ReplayOverrideDTO | None:
    """Convert an optional replay override to its wire value."""
    if override is None:
        return None
    return ReplayOverrideDTO(
        model=override.model,
        system_prompt=override.system_prompt,
        prompt=override.prompt,
        model_params=override.model_params,
    )


def tool_policy_to_response(policy: ToolPolicy) -> ToolPolicyDTO:
    """Convert a tool policy to its wire value."""
    return ToolPolicyDTO.model_validate(policy.model_dump(mode="python"))


def evaluator_config_to_response(config: EvaluatorConfig) -> EvaluatorConfigDTO:
    """Convert a resolved evaluator configuration to its public form."""
    return EvaluatorConfigDTO(
        evaluator=config.evaluator,
        version=config.version,
        params=config.params,
    )


def replay_config_values(
    config: ReplayConfig,
) -> tuple[ReplayOverrideDTO | None, ToolPolicyDTO, list[EvaluatorConfigDTO]]:
    """Convert the three public fields of a persisted replay configuration."""
    return (
        replay_override_to_response(config.override),
        tool_policy_to_response(config.tool_policy),
        [evaluator_config_to_response(item) for item in config.evaluators],
    )
