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
"""Experiment, experiment run, and replay commands."""

import uuid

from kitaru.api_models.v1.replay import BaselineEvaluationMode
from kitaru.cli import experiment_runs, experiments, invocation_context, replays
from kitaru.cli.command_registry import (
    ASSET_READ_ERRORS,
    ASSET_WRITE_ERRORS,
    COLLECTION_READ_ERRORS,
    IDEMPOTENCY_KEY_PARAMETER,
    JOB_WAIT_ERRORS,
    LIST_PARAMETERS,
    UUID_READ_ERRORS,
    WAIT_PARAMETERS,
    build_spec,
    experiment_app,
    experiment_run_app,
    register_command,
    replay_app,
)
from kitaru.cli.output import CommandResult
from kitaru.cli.schema import ParameterSpec


@register_command(
    experiment_app,
    build_spec(
        ("experiment", "create"),
        "Create an experiment with exact evaluator versions.",
        parameters=(
            ParameterSpec("NAME", "string", "argument", True, "New experiment name."),
            ParameterSpec(
                "--agent",
                "reference",
                "option",
                True,
                "Exact agent UUID or case-sensitive name.",
            ),
            ParameterSpec(
                "--description", "string", "option", False, "Experiment description."
            ),
            ParameterSpec(
                "--override",
                "JSON object",
                "option",
                False,
                "Replay override applied to every run.",
            ),
            ParameterSpec(
                "--tool-policy",
                "JSON object",
                "option",
                False,
                "Tool policy applied to every run; omitted uses the server default.",
            ),
            ParameterSpec(
                "--evaluator",
                "reference[]",
                "option",
                True,
                "Exact EVALUATOR@VERSION references.",
            ),
            ParameterSpec(
                "--evaluator-params",
                "EVALUATOR@VERSION=JSON_OBJECT[]",
                "option",
                False,
                "Parameters for a selected evaluator token.",
            ),
            ParameterSpec(
                "--evaluator-connection",
                "EVALUATOR@VERSION=CONNECTION[]",
                "option",
                False,
                "Connection for a selected evaluator token.",
            ),
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("creates_remote_state",),
        idempotency="non_idempotent_remote_create",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def experiment_create(
    name: str,
    /,
    *,
    agent: str,
    evaluator: list[str],
    description: str | None = None,
    override: str | None = None,
    tool_policy: str | None = None,
    evaluator_params: list[str] | None = None,
    evaluator_connection: list[str] | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create an experiment with exact evaluator versions."""
    async with invocation_context.open_asset_client() as client:
        return await experiments.create_experiment(
            client,
            name,
            agent=agent,
            description=description,
            override=override,
            tool_policy=tool_policy,
            evaluators=evaluator,
            evaluator_params=evaluator_params,
            evaluator_connections=evaluator_connection,
            idempotency_key=idempotency_key,
        )


@register_command(
    experiment_app,
    build_spec(
        ("experiment", "list"),
        "List experiments.",
        parameters=LIST_PARAMETERS,
        errors=ASSET_READ_ERRORS,
    ),
)
async def experiment_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of experiments."""
    async with invocation_context.open_asset_client() as client:
        return await experiments.list_experiments(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@register_command(
    experiment_app,
    build_spec(
        ("experiment", "get"),
        "Get an experiment by exact UUID or case-sensitive name.",
        parameters=(
            ParameterSpec(
                "EXPERIMENT",
                "reference",
                "argument",
                True,
                "Experiment UUID or name.",
            ),
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def experiment_get(experiment: str, /) -> CommandResult:
    """Get one exact experiment."""
    async with invocation_context.open_asset_client() as client:
        return await experiments.get_experiment(client, experiment)


@register_command(
    experiment_app,
    build_spec(
        ("experiment", "update"),
        "Update selected fields on an exact experiment.",
        parameters=(
            ParameterSpec(
                "EXPERIMENT",
                "reference",
                "argument",
                True,
                "Experiment UUID or name.",
            ),
            ParameterSpec("--name", "string", "option", False, "New experiment name."),
            ParameterSpec(
                "--description", "string", "option", False, "New description."
            ),
            ParameterSpec(
                "--clear-description",
                "boolean",
                "option",
                False,
                "Clear the description.",
            ),
            ParameterSpec(
                "--override",
                "JSON object",
                "option",
                False,
                "Replacement replay override.",
            ),
            ParameterSpec(
                "--clear-override",
                "boolean",
                "option",
                False,
                "Clear the replay override.",
            ),
            ParameterSpec(
                "--tool-policy",
                "JSON object",
                "option",
                False,
                "Replacement tool policy; cannot be cleared.",
            ),
            ParameterSpec(
                "--evaluator",
                "reference[]",
                "option",
                False,
                "Exact evaluator versions replacing the complete list.",
            ),
            ParameterSpec(
                "--evaluator-params",
                "EVALUATOR@VERSION=JSON_OBJECT[]",
                "option",
                False,
                "Parameters for a selected evaluator token.",
            ),
            ParameterSpec(
                "--evaluator-connection",
                "EVALUATOR@VERSION=CONNECTION[]",
                "option",
                False,
                "Connection for a selected evaluator token.",
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent replacement",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def experiment_update(
    experiment: str,
    /,
    *,
    name: str | None = None,
    description: str | None = None,
    clear_description: bool = False,
    override: str | None = None,
    clear_override: bool = False,
    tool_policy: str | None = None,
    evaluator: list[str] | None = None,
    evaluator_params: list[str] | None = None,
    evaluator_connection: list[str] | None = None,
) -> CommandResult:
    """Update selected fields on one exact experiment."""
    async with invocation_context.open_asset_client() as client:
        return await experiments.update_experiment(
            client,
            experiment,
            name=name,
            description=description,
            clear_description=clear_description,
            override=override,
            clear_override=clear_override,
            tool_policy=tool_policy,
            evaluators=evaluator,
            evaluator_params=evaluator_params,
            evaluator_connections=evaluator_connection,
        )


@register_command(
    experiment_app,
    build_spec(
        ("experiment", "delete"),
        "Delete an experiment and preserve server conflict behavior.",
        parameters=(
            ParameterSpec(
                "EXPERIMENT",
                "reference",
                "argument",
                True,
                "Experiment UUID or name.",
            ),
            ParameterSpec(
                "--force", "boolean", "option", False, "Confirm remote deletion."
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state", "deletes_remote_state"),
        idempotency="not_found after first removal",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def experiment_delete(
    experiment: str, /, *, force: bool = False
) -> CommandResult:
    """Delete one exact experiment."""
    async with invocation_context.open_asset_client() as client:
        return await experiments.delete_experiment(client, experiment, force=force)


@register_command(
    replay_app,
    build_spec(
        ("replay", "create"),
        "Create one standalone replay from an exact baseline session.",
        parameters=(
            ParameterSpec("BASELINE", "UUID", "argument", True, "Baseline session ID."),
            ParameterSpec(
                "--evaluator",
                "EVALUATOR@VERSION[]",
                "option",
                True,
                "Exact evaluator version; repeat for additional evaluators.",
            ),
            ParameterSpec(
                "--evaluator-params",
                "EVALUATOR@VERSION=JSON_OBJECT[]",
                "option",
                False,
                "Parameters for a selected evaluator token.",
            ),
            ParameterSpec(
                "--evaluator-connection",
                "EVALUATOR@VERSION=CONNECTION[]",
                "option",
                False,
                "Connection for a selected evaluator token.",
            ),
            ParameterSpec(
                "--agent",
                "AGENT@VERSION",
                "option",
                False,
                "Exact agent version; omit to use the baseline's recorded version.",
            ),
            ParameterSpec(
                "--override",
                "JSON object",
                "option",
                False,
                "Replay model, prompt, or model-parameter override.",
            ),
            ParameterSpec(
                "--tool-policy",
                "JSON object",
                "option",
                False,
                "Tool policy; omitting it uses the server default and may execute "
                "live tools.",
            ),
            ParameterSpec(
                "--baseline-evaluation-mode",
                "none|if_missing|force",
                "option",
                False,
                "How to score the baseline session.",
            ),
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("creates_remote_state",),
        idempotency="non_idempotent_replay_created_per_request",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def replay_create(
    baseline: uuid.UUID,
    /,
    *,
    evaluator: list[str],
    evaluator_params: list[str] | None = None,
    evaluator_connection: list[str] | None = None,
    agent: str | None = None,
    override: str | None = None,
    tool_policy: str | None = None,
    baseline_evaluation_mode: BaselineEvaluationMode = (
        BaselineEvaluationMode.IF_MISSING
    ),
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create one standalone replay without waiting for its job."""
    async with invocation_context.open_asset_client() as client:
        return await replays.create_replay(
            client,
            baseline,
            evaluators=evaluator,
            evaluator_params=evaluator_params,
            evaluator_connections=evaluator_connection,
            agent=agent,
            override=override,
            tool_policy=tool_policy,
            baseline_evaluation_mode=baseline_evaluation_mode,
            idempotency_key=idempotency_key,
        )


@register_command(
    replay_app,
    build_spec(
        ("replay", "list"),
        "List standalone and experiment-created replays.",
        parameters=LIST_PARAMETERS,
        errors=COLLECTION_READ_ERRORS,
    ),
)
async def replay_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of replays."""
    async with invocation_context.open_asset_client() as client:
        return await replays.list_replays(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@register_command(
    replay_app,
    build_spec(
        ("replay", "get"),
        "Get one replay by exact UUID.",
        parameters=(ParameterSpec("REPLAY", "UUID", "argument", True, "Replay ID."),),
        errors=UUID_READ_ERRORS,
    ),
)
async def replay_get(replay: uuid.UUID, /) -> CommandResult:
    """Get one replay without remapping its status."""
    async with invocation_context.open_asset_client() as client:
        return await replays.get_replay(client, replay)


@register_command(
    experiment_run_app,
    build_spec(
        ("experiment", "run", "start"),
        "Start an experiment run and optionally wait for terminal settlement.",
        parameters=(
            ParameterSpec(
                "EXPERIMENT",
                "reference",
                "argument",
                True,
                "Experiment UUID or name.",
            ),
            ParameterSpec(
                "--cohort-version",
                "UUID",
                "option",
                True,
                "Exact cohort-version ID.",
            ),
            ParameterSpec(
                "--agent",
                "AGENT@VERSION",
                "option",
                True,
                "Exact agent version reference.",
            ),
            ParameterSpec(
                "--baseline-evaluation-mode",
                "none|if_missing|force",
                "option",
                False,
                "How to score each baseline session.",
            ),
            *WAIT_PARAMETERS,
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("creates_remote_state",),
        idempotency="non_idempotent_run_created_per_request",
        errors=(*ASSET_WRITE_ERRORS, *JOB_WAIT_ERRORS),
        streams=True,
    ),
)
async def experiment_run_start(
    experiment: str,
    /,
    *,
    cohort_version: uuid.UUID,
    agent: str,
    baseline_evaluation_mode: BaselineEvaluationMode = (
        BaselineEvaluationMode.IF_MISSING
    ),
    wait: bool = False,
    interval: float | None = None,
    timeout: float | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Start one exact experiment run and optionally wait for it."""
    async with invocation_context.open_asset_client() as client:
        return await experiment_runs.start_run(
            client,
            experiment,
            cohort_version_id=cohort_version,
            agent_reference=agent,
            baseline_evaluation_mode=baseline_evaluation_mode,
            wait=wait,
            interval=interval,
            timeout=timeout,
            idempotency_key=idempotency_key,
        )


@register_command(
    experiment_run_app,
    build_spec(
        ("experiment", "run", "list"),
        "List experiment runs without deriving state from their jobs.",
        parameters=LIST_PARAMETERS,
        errors=ASSET_READ_ERRORS,
    ),
)
async def experiment_run_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of experiment runs."""
    async with invocation_context.open_asset_client() as client:
        return await experiment_runs.list_runs(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@register_command(
    experiment_run_app,
    build_spec(
        ("experiment", "run", "get"),
        "Get an experiment run and its aggregate progress by exact UUID.",
        parameters=experiment_runs.GET_PARAMETERS,
        errors=UUID_READ_ERRORS,
    ),
)
async def experiment_run_get(
    run: uuid.UUID, /, *, aggregate: bool = False
) -> CommandResult:
    """Get one experiment run without mapping terminal status to an error."""
    async with invocation_context.open_asset_client() as client:
        return await experiment_runs.get_run(client, run, aggregate=aggregate)


@register_command(
    experiment_run_app,
    build_spec(
        ("experiment", "run", "jobs"),
        "List one page of replay jobs backing an experiment run.",
        parameters=(
            ParameterSpec("RUN", "UUID", "argument", True, "Experiment-run ID."),
            *LIST_PARAMETERS,
        ),
        errors=UUID_READ_ERRORS,
    ),
)
async def experiment_run_jobs(
    run: uuid.UUID,
    /,
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of replay jobs for a run."""
    async with invocation_context.open_asset_client() as client:
        return await experiment_runs.list_run_jobs(
            client,
            run,
            size=size,
            cursor=cursor,
            sort=sort,
            filter=filter,
        )


@register_command(
    experiment_run_app,
    build_spec(
        ("experiment", "run", "watch"),
        "Poll a run until it completes, fails, or is canceled without changing it.",
        parameters=(
            ParameterSpec("RUN", "UUID", "argument", True, "Experiment-run ID."),
            ParameterSpec(
                "--interval", "positive float", "option", False, "Run polling interval."
            ),
            ParameterSpec(
                "--timeout", "positive float", "option", False, "Local wait timeout."
            ),
        ),
        errors=(*ASSET_READ_ERRORS, *JOB_WAIT_ERRORS),
        streams=True,
    ),
)
async def experiment_run_watch(
    run: uuid.UUID,
    /,
    *,
    interval: float = 2.0,
    timeout: float | None = None,
) -> CommandResult:
    """Watch one run locally without changing remote work."""
    async with invocation_context.open_asset_client() as client:
        return await experiment_runs.watch_run(
            client, run, interval=interval, timeout=timeout
        )


@register_command(
    experiment_run_app,
    build_spec(
        ("experiment", "run", "cancel"),
        "Request run cancellation once without waiting for settlement.",
        parameters=(
            ParameterSpec("RUN", "UUID", "argument", True, "Experiment-run ID."),
        ),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="server_rejects_settled_runs",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def experiment_run_cancel(run: uuid.UUID, /) -> CommandResult:
    """Request run cancellation and return after server acceptance."""
    async with invocation_context.open_asset_client() as client:
        return await experiment_runs.cancel_run(client, run)


@register_command(
    experiment_run_app,
    build_spec(
        ("experiment", "run", "delete"),
        "Delete a run and immediately delete all of its replay jobs and tasks.",
        parameters=(
            ParameterSpec("RUN", "UUID", "argument", True, "Experiment-run ID."),
            ParameterSpec(
                "--force", "boolean", "option", False, "Confirm remote deletion."
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state", "deletes_remote_state"),
        idempotency="not_found after first removal",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def experiment_run_delete(
    run: uuid.UUID, /, *, force: bool = False
) -> CommandResult:
    """Delete one run and its replay jobs and tasks immediately."""
    async with invocation_context.open_asset_client() as client:
        return await experiment_runs.delete_run(client, run, force=force)
