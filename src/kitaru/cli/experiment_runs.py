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
"""Experiment-run lifecycle CLI commands."""

import asyncio
import math
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from kitaru.api_models.v1.experiment_run import (
    ExperimentRunCreateRequest,
    ExperimentRunJobsListParams,
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.replay import BaselineEvaluationMode
from kitaru.cli.output import CLIError, CommandResult, emit_event
from kitaru.cli.receipts import get_wait_settings
from kitaru.cli.registration import (
    build_list_params,
    get_agent_version,
    list_params,
    page_result,
    resolve_asset,
)

_TERMINAL_STATUSES = {
    ExperimentRunStatus.COMPLETED,
    ExperimentRunStatus.FAILED,
    ExperimentRunStatus.CANCELED,
}


def _experiment_identity(experiment: Any) -> dict[str, Any]:
    """Return the bounded experiment identity used in run receipts."""
    return {"id": str(experiment.id), "name": experiment.name}


def _cohort_version_identity(version: Any) -> dict[str, Any]:
    """Return the bounded cohort-version identity used in run receipts."""
    return {
        "id": str(version.id),
        "cohort_id": str(version.cohort_id),
        "version": version.version,
    }


def _agent_version_identity(agent: Any, version: Any) -> dict[str, Any]:
    """Return the bounded agent-version identity used in run receipts."""
    return {
        "id": str(agent.id),
        "name": agent.name,
        "version_id": str(version.id),
        "version": version.version,
    }


def _run_receipt(
    run: ExperimentRunResponse,
    *,
    experiment: Mapping[str, Any],
    cohort_version: Mapping[str, Any],
    agent_version: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the stable bounded experiment-run receipt."""
    return {
        "operation": "experiment_run",
        "terminal": run.status in _TERMINAL_STATUSES,
        "experiment": dict(experiment),
        "cohort_version": dict(cohort_version),
        "agent_version": dict(agent_version),
        "run": run.model_dump(mode="json"),
    }


async def _get_run_identities(
    client: Any, run: ExperimentRunResponse
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Load the exact related resources needed for a watch receipt."""
    experiment = await client.experiments.get(run.experiment_id)
    cohort_version = await client.cohort_versions.get(run.cohort_version_id)
    agent_version = await client.agent_versions.get(run.agent_version_id)
    agent = await client.agents.get(agent_version.agent_id)
    return (
        _experiment_identity(experiment),
        _cohort_version_identity(cohort_version),
        _agent_version_identity(agent, agent_version),
    )


async def poll_run(
    client: Any,
    run_id: uuid.UUID,
    *,
    interval: float,
    timeout: float | None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    initial_run: ExperimentRunResponse | None = None,
) -> ExperimentRunResponse:
    """Poll an experiment run until it settles, emitting meaningful changes."""
    if not math.isfinite(interval) or interval <= 0:
        raise CLIError("invalid_arguments", "--interval must be positive and finite.")
    if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
        raise CLIError("invalid_arguments", "--timeout must be positive and finite.")

    deadline = None if timeout is None else clock() + timeout
    previous = (
        None
        if initial_run is None
        else initial_run.model_dump(mode="json", exclude={"updated"})
    )

    while True:
        if deadline is None:
            run: ExperimentRunResponse = await client.experiment_runs.get(run_id)
        else:
            remaining = deadline - clock()
            if remaining <= 0:
                raise _get_poll_timeout(run_id, previous)
            try:
                run = await asyncio.wait_for(
                    client.experiment_runs.get(run_id), timeout=remaining
                )
            except TimeoutError as error:
                raise _get_poll_timeout(run_id, previous) from error

        item = run.model_dump(mode="json")
        fingerprint = dict(item)
        fingerprint.pop("updated", None)
        if fingerprint != previous:
            emit_event("snapshot", item)
            previous = fingerprint

        if run.status in _TERMINAL_STATUSES:
            return run

        if deadline is None:
            await sleep(interval)
        else:
            remaining = deadline - clock()
            if remaining <= 0:
                raise _get_poll_timeout(run_id, previous)
            try:
                await asyncio.wait_for(
                    sleep(min(interval, remaining)), timeout=remaining
                )
            except TimeoutError as error:
                raise _get_poll_timeout(run_id, previous) from error


async def start_run(
    client: Any,
    experiment_reference: str,
    *,
    cohort_version_id: uuid.UUID,
    agent_reference: str,
    baseline_evaluation_mode: BaselineEvaluationMode,
    wait: bool,
    interval: float | None,
    timeout: float | None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Start one run and optionally wait locally for terminal settlement."""
    wait_settings = get_wait_settings(wait=wait, interval=interval, timeout=timeout)
    experiment = await resolve_asset(
        client.experiments, experiment_reference, "Experiment"
    )
    cohort_version = await client.cohort_versions.get(cohort_version_id)
    agent, agent_version = await get_agent_version(client, agent_reference)
    run = await client.experiments.start_run(
        experiment.id,
        ExperimentRunCreateRequest(
            cohort_version_id=cohort_version.id,
            agent_version_id=agent_version.id,
            baseline_evaluation_mode=baseline_evaluation_mode,
        ),
        idempotency_key=idempotency_key,
    )
    experiment_identity = _experiment_identity(experiment)
    cohort_version_identity = _cohort_version_identity(cohort_version)
    agent_version_identity = _agent_version_identity(agent, agent_version)
    receipt = _run_receipt(
        run,
        experiment=experiment_identity,
        cohort_version=cohort_version_identity,
        agent_version=agent_version_identity,
    )
    next_actions = [
        f"kitaru experiment run watch {run.id}",
        f"kitaru experiment run get {run.id}",
        f"kitaru experiment run jobs {run.id}",
        f"kitaru experiment run cancel {run.id}",
    ]
    if wait_settings is None:
        return CommandResult(
            item=receipt,
            event="created",
            next_actions=next_actions,
        )

    emit_event("created", {**receipt, "next_actions": next_actions})
    settled = await poll_run(
        client,
        run.id,
        interval=wait_settings[0],
        timeout=wait_settings[1],
        clock=clock,
        sleep=sleep,
        initial_run=run,
    )
    terminal_receipt = _run_receipt(
        settled,
        experiment=experiment_identity,
        cohort_version=cohort_version_identity,
        agent_version=agent_version_identity,
    )
    return _terminal_result(settled, terminal_receipt)


async def list_runs(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> CommandResult:
    """List one server page of experiment runs."""
    params = list_params(
        "experiment_run", size=size, cursor=cursor, sort=sort, filter=filter
    )
    return page_result(await client.experiment_runs.list(params), size=size)


async def get_run(client: Any, run_id: uuid.UUID) -> CommandResult:
    """Get one experiment run without mapping its terminal status to an error."""
    run = await client.experiment_runs.get(run_id)
    return CommandResult(item=run.model_dump(mode="json"))


async def list_run_jobs(
    client: Any,
    run_id: uuid.UUID,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> CommandResult:
    """List one page of jobs backing an experiment run."""
    params = build_list_params(
        ExperimentRunJobsListParams,
        size=size,
        cursor=cursor,
        sort=sort,
        filter=filter,
    )
    return page_result(
        await client.experiment_runs.list_jobs(run_id, params), size=size
    )


async def watch_run(
    client: Any,
    run_id: uuid.UUID,
    *,
    interval: float,
    timeout: float | None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> CommandResult:
    """Watch one run and map its terminal state through the receipt contract."""
    run = await poll_run(
        client,
        run_id,
        interval=interval,
        timeout=timeout,
        clock=clock,
        sleep=sleep,
    )
    (
        experiment_identity,
        cohort_version_identity,
        agent_version_identity,
    ) = await _get_run_identities(client, run)
    receipt = _run_receipt(
        run,
        experiment=experiment_identity,
        cohort_version=cohort_version_identity,
        agent_version=agent_version_identity,
    )
    return _terminal_result(run, receipt)


async def cancel_run(client: Any, run_id: uuid.UUID) -> CommandResult:
    """Request cancellation once without waiting for run settlement."""
    run = await client.experiment_runs.cancel(run_id)
    item = run.model_dump(mode="json")
    item["cancellation_requested"] = True
    next_action = (
        f"kitaru experiment run get {run_id}"
        if run.status is ExperimentRunStatus.CANCELED
        else f"kitaru experiment run watch {run_id}"
    )
    return CommandResult(item=item, next_actions=[next_action])


async def delete_run(client: Any, run_id: uuid.UUID, *, force: bool) -> CommandResult:
    """Delete one run and all of its jobs and tasks immediately."""
    if not force:
        raise CLIError(
            "invalid_arguments", "Deleting an experiment run requires --force."
        )
    run = await client.experiment_runs.get(run_id)
    await client.experiment_runs.delete(run_id)
    return CommandResult(
        item={
            "id": str(run_id),
            "deleted": True,
            "status": run.status.value,
        }
    )


def _terminal_result(
    run: ExperimentRunResponse, receipt: dict[str, Any]
) -> CommandResult:
    """Return a completed receipt or raise the stable remote terminal error."""
    next_actions = [
        f"kitaru experiment run get {run.id}",
        f"kitaru experiment run jobs {run.id}",
    ]
    if run.status is ExperimentRunStatus.COMPLETED:
        return CommandResult(
            item=receipt,
            event="terminal",
            next_actions=next_actions,
        )
    if run.status is ExperimentRunStatus.FAILED:
        kind = "remote_failed"
    elif run.status is ExperimentRunStatus.CANCELED:
        kind = "remote_canceled"
    else:
        raise ValueError("_terminal_result requires a terminal experiment run")

    emit_event("terminal", receipt)
    raise CLIError(
        kind,
        f"Experiment run {run.id} settled as {run.status.value}.",
        details={"receipt": receipt, "next_actions": next_actions},
    )


def _get_poll_timeout(run_id: uuid.UUID, previous: dict[str, Any] | None) -> CLIError:
    """Build the recoverable local run polling-timeout error."""
    return CLIError(
        "timeout",
        f"Timed out waiting for experiment run {run_id}; remote work continues.",
        details={
            "run_id": str(run_id),
            "last_status": previous["status"] if previous is not None else None,
            "remote_continues": True,
        },
        hint=f"Keep waiting with `kitaru experiment run watch {run_id}`.",
    )
