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
"""Experiment configuration and CRUD CLI commands."""

from collections.abc import Sequence
from typing import Any

from kitaru.api_models.v1.experiment import (
    ExperimentCreateRequest,
    ExperimentUpdateRequest,
)
from kitaru.api_models.v1.replay_config import ReplayOverride, ToolPolicy
from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.registration import (
    list_params,
    page_result,
    parse_json_object,
    resolve_asset,
    resolve_evaluator_configs,
)


def parse_replay_override(value: str, *, option: str) -> ReplayOverride:
    """Parse an inline replay override using the existing API model."""
    return ReplayOverride.model_validate(parse_json_object(value, option=option))


def parse_tool_policy(value: str, *, option: str) -> ToolPolicy:
    """Parse an inline tool policy using the existing API model."""
    return ToolPolicy.model_validate(parse_json_object(value, option=option))


async def create_experiment(
    client: Any,
    name: str,
    *,
    agent: str,
    description: str | None,
    override: str | None,
    tool_policy: str | None,
    evaluators: Sequence[str],
    evaluator_params: Sequence[str] | None,
) -> CommandResult:
    """Create an experiment with exact evaluator versions."""
    resolved_agent = await resolve_asset(client.agents, agent, "Agent")
    fields: dict[str, Any] = {"name": name, "agent_id": resolved_agent.id}
    if description is not None:
        fields["description"] = description
    if override is not None:
        fields["override"] = parse_replay_override(override, option="--override")
    if tool_policy is not None:
        fields["tool_policy"] = parse_tool_policy(tool_policy, option="--tool-policy")
    configs, _, _ = await resolve_evaluator_configs(
        client, evaluators, evaluator_params or []
    )
    fields["evaluators"] = configs

    experiment = await client.experiments.create(ExperimentCreateRequest(**fields))
    return CommandResult(item=experiment.model_dump(mode="json"))


async def list_experiments(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> CommandResult:
    """List one server page of experiments."""
    params = list_params(
        "experiment", size=size, cursor=cursor, sort=sort, filter=filter
    )
    return page_result(await client.experiments.list(params), size=size)


async def get_experiment(client: Any, reference: str) -> CommandResult:
    """Get one experiment by exact UUID or case-sensitive name."""
    experiment = await resolve_asset(client.experiments, reference, "Experiment")
    return CommandResult(item=experiment.model_dump(mode="json"))


async def update_experiment(
    client: Any,
    reference: str,
    *,
    name: str | None,
    description: str | None,
    clear_description: bool,
    override: str | None,
    clear_override: bool,
    tool_policy: str | None,
    evaluators: Sequence[str] | None,
    evaluator_params: Sequence[str] | None,
) -> CommandResult:
    """Update only explicitly selected experiment fields."""
    if description is not None and clear_description:
        raise CLIError(
            "invalid_arguments",
            "--description and --clear-description cannot be used together.",
        )
    if override is not None and clear_override:
        raise CLIError(
            "invalid_arguments",
            "--override and --clear-override cannot be used together.",
        )
    if evaluator_params and evaluators is None:
        raise CLIError(
            "invalid_arguments",
            "--evaluator-params requires at least one --evaluator.",
        )

    fields: dict[str, Any] = {}
    if name is not None:
        fields["name"] = name
    if description is not None:
        fields["description"] = description
    elif clear_description:
        fields["description"] = None
    if override is not None:
        fields["override"] = parse_replay_override(override, option="--override")
    elif clear_override:
        fields["override"] = None
    if tool_policy is not None:
        fields["tool_policy"] = parse_tool_policy(tool_policy, option="--tool-policy")
    if evaluators is not None:
        configs, _, _ = await resolve_evaluator_configs(
            client, evaluators, evaluator_params or []
        )
        fields["evaluators"] = configs
    if not fields:
        raise CLIError("invalid_arguments", "Select at least one experiment update.")

    experiment = await resolve_asset(client.experiments, reference, "Experiment")
    updated = await client.experiments.update(
        experiment.id, ExperimentUpdateRequest(**fields)
    )
    return CommandResult(item=updated.model_dump(mode="json"))


async def delete_experiment(
    client: Any, reference: str, *, force: bool
) -> CommandResult:
    """Delete one exact experiment."""
    if not force:
        raise CLIError("invalid_arguments", "Deleting an experiment requires --force.")
    experiment = await resolve_asset(client.experiments, reference, "Experiment")
    await client.experiments.delete(experiment.id)
    return CommandResult(item={"id": str(experiment.id), "deleted": True})
