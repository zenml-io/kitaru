#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Import and session analysis CLI commands."""

import uuid
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from kitaru.api_models.v1.analysis import AnalysisCreateRequest
from kitaru.api_models.v1.imports import ImportAnalyzeRequest
from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.api_models.v1.session import SessionListParams
from kitaru.api_models.v1.task import TaskResponse
from kitaru.cli import receipts
from kitaru.cli.output import CLIError, CommandResult, emit_event
from kitaru.cli.registration import (
    build_list_params,
    list_params,
    page_result,
    resolve_analyzer_configs,
    resolve_asset,
)
from kitaru.cli.session_selection import get_cohort_version, parse_session_ids


async def list_imports(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> CommandResult:
    """List one server page of imports."""
    params = list_params("import", size=size, cursor=cursor, sort=sort, filter=filter)
    return page_result(await client.imports.list(params), size=size)


async def get_import(client: Any, import_id: uuid.UUID) -> CommandResult:
    """Get one import without remapping its status."""
    import_ = await client.imports.get(import_id)
    return CommandResult(item=import_.model_dump(mode="json"))


def _terminal_analysis_result(
    job: JobResponse,
    tasks: list[TaskResponse],
    *,
    operation: str,
    identity: dict[str, Any],
) -> CommandResult:
    """Map the settled analysis tasks of a job to a receipt."""
    task_entries = [
        {"id": str(task.id), "status": task.status.value, "error": task.error}
        for task in tasks
    ]
    receipt: dict[str, Any] = {
        **identity,
        "operation": operation,
        "terminal": True,
        "job": job.model_dump(mode="json"),
        "tasks": task_entries,
    }
    next_actions = [
        receipts.get_task_filter_action("insight", task.id) for task in tasks
    ]
    if job.status in {JobStatus.FAILED, JobStatus.CANCELED}:
        error = receipts.terminal_job_error(job, receipt)
        error.details["next_actions"] = next_actions
        raise error
    return CommandResult(item=receipt, next_actions=next_actions, event="terminal")


async def analyze_import(
    client: Any,
    import_id: uuid.UUID,
    *,
    analyzers: Sequence[str],
    analyzer_params: Sequence[str] | None,
    analyzer_connections: Sequence[str] | None,
    wait: bool,
    interval: float | None,
    timeout: float | None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create one analysis job over the sessions of an existing import."""
    wait_settings = receipts.get_wait_settings(
        wait=wait, interval=interval, timeout=timeout
    )
    analyzer_configs, analyzer_identity, _ = await resolve_analyzer_configs(
        client, analyzers, analyzer_params or [], analyzer_connections or []
    )
    identity: dict[str, Any] = {
        "import_id": str(import_id),
        "analyzers": analyzer_identity,
    }
    request = ImportAnalyzeRequest(analyzers=analyzer_configs)
    job = await client.imports.analyze(
        import_id, request, idempotency_key=idempotency_key
    )
    return await _finish_analysis_job(
        client,
        job,
        operation="import_analysis",
        identity=identity,
        wait_settings=wait_settings,
    )


async def analyze_sessions(
    client: Any,
    agent: str,
    sessions: Sequence[str] | None,
    *,
    sessions_file: Path | None,
    tag: str | None,
    cohort: str | None,
    import_id: uuid.UUID | None,
    started_after: datetime | None,
    started_before: datetime | None,
    filter: str | None,
    analyzers: Sequence[str],
    analyzer_params: Sequence[str] | None,
    analyzer_connections: Sequence[str] | None,
    wait: bool,
    interval: float | None,
    timeout: float | None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create one analysis job over the agent's sessions matching every selector."""
    wait_settings = receipts.get_wait_settings(
        wait=wait, interval=interval, timeout=timeout
    )
    resolved_agent = await resolve_asset(client.agents, agent, "Agent")
    session_ids = (
        parse_session_ids(list(sessions or []), sessions_file)
        if sessions or sessions_file is not None
        else None
    )
    normalized_tag = None if tag is None else tag.strip()
    if normalized_tag == "":
        raise CLIError("invalid_arguments", "--tag must not be empty.")
    cohort_version_id = None
    if cohort is not None:
        _, cohort_version = await get_cohort_version(client, cohort)
        cohort_version_id = cohort_version.id
    for option, value in (
        ("--started-after", started_after),
        ("--started-before", started_before),
    ):
        if value is not None and value.utcoffset() is None:
            raise CLIError(
                "invalid_arguments",
                f"{option} must include a timezone, such as Z or +02:00.",
            )
    session_filter = (
        None
        if filter is None
        else build_list_params(
            SessionListParams, size=1, cursor=None, sort="created:desc", filter=filter
        ).filter
    )
    analyzer_configs, analyzer_identity, _ = await resolve_analyzer_configs(
        client, analyzers, analyzer_params or [], analyzer_connections or []
    )
    request = AnalysisCreateRequest(
        agent_id=resolved_agent.id,
        analyzers=analyzer_configs,
        session_ids=session_ids,
        tag=normalized_tag,
        cohort_version_id=cohort_version_id,
        import_id=import_id,
        started_after=started_after,
        started_before=started_before,
        filter=session_filter,
    )
    identity: dict[str, Any] = {
        "agent_id": str(resolved_agent.id),
        "scope": request.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
            exclude={"agent_id", "analyzers"},
        ),
        "analyzers": analyzer_identity,
    }
    job = await client.analyses.create(request, idempotency_key=idempotency_key)
    return await _finish_analysis_job(
        client,
        job,
        operation="session_analysis",
        identity=identity,
        wait_settings=wait_settings,
    )


async def _finish_analysis_job(
    client: Any,
    job: JobResponse,
    *,
    operation: str,
    identity: dict[str, Any],
    wait_settings: tuple[float, float] | None,
) -> CommandResult:
    """Return the created receipt, or wait and map the settled tasks."""
    created = receipts.created_job_result(
        operation, job, identity=identity, next_actions=["kitaru insight list"]
    )
    if wait_settings is None:
        return created

    emit_event("created", {**created.item, "next_actions": created.next_actions})
    terminal_job, tasks = await receipts.wait_for_terminal_tasks(
        client,
        job.id,
        interval=wait_settings[0],
        timeout=wait_settings[1],
        initial_job=job,
    )
    return _terminal_analysis_result(
        terminal_job, tasks, operation=operation, identity=identity
    )
