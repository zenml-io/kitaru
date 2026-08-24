#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Standalone replay CLI commands."""

import uuid
from collections.abc import Sequence
from typing import Any

from kitaru.api_models.v1.replay import ReplayCreateRequest
from kitaru.cli.output import CommandResult
from kitaru.cli.registration import (
    get_agent_version,
    list_params,
    page_result,
    parse_replay_override,
    parse_tool_policy,
    resolve_evaluator_configs,
)


async def create_replay(
    client: Any,
    baseline_session_id: uuid.UUID,
    *,
    evaluators: Sequence[str],
    evaluator_params: Sequence[str] | None,
    agent: str | None,
    override: str | None,
    tool_policy: str | None,
    evaluate_baselines: bool,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create one standalone replay with exact evaluator versions."""
    fields: dict[str, Any] = {"baseline_session_id": baseline_session_id}
    if agent is not None:
        _, agent_version = await get_agent_version(client, agent)
        fields["agent_version_id"] = agent_version.id
    if override is not None:
        fields["override"] = parse_replay_override(override, option="--override")
    if tool_policy is not None:
        fields["tool_policy"] = parse_tool_policy(tool_policy, option="--tool-policy")
    configs, _, _ = await resolve_evaluator_configs(
        client, evaluators, evaluator_params or []
    )
    fields["evaluators"] = configs
    fields["evaluate_baselines"] = evaluate_baselines

    replay = await client.replays.create(
        ReplayCreateRequest(**fields), idempotency_key=idempotency_key
    )
    return CommandResult(
        item=replay.model_dump(mode="json"),
        event="created",
        next_actions=[
            f"kitaru job watch {replay.job_id}",
            f"kitaru job cancel {replay.job_id}",
            f"kitaru replay get {replay.id}",
        ],
    )


async def list_replays(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> CommandResult:
    """List one server page of standalone and experiment-created replays."""
    params = list_params("replay", size=size, cursor=cursor, sort=sort, filter=filter)
    return page_result(await client.replays.list(params), size=size)


async def get_replay(client: Any, replay_id: uuid.UUID) -> CommandResult:
    """Get one replay without remapping its status."""
    replay = await client.replays.get(replay_id)
    return CommandResult(item=replay.model_dump(mode="json"))
