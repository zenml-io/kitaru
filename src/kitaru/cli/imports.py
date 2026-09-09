#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Import CLI commands."""

import uuid
from collections.abc import Sequence
from typing import Any

from kitaru.api_models.v1.imports import ImportAnalyzeRequest
from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.api_models.v1.task import TaskResponse
from kitaru.cli import receipts
from kitaru.cli.output import CommandResult, emit_event
from kitaru.cli.registration import (
    list_params,
    page_result,
    resolve_analyzer_configs,
)


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
    identity: dict[str, Any],
) -> CommandResult:
    """Map the settled analysis tasks of a job to a receipt."""
    task_entries = [
        {"id": str(task.id), "status": task.status.value, "error": task.error}
        for task in tasks
    ]
    receipt: dict[str, Any] = {
        **identity,
        "operation": "import_analysis",
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
    created = receipts.created_job_result(
        "import_analysis",
        job,
        identity=identity,
        next_actions=["kitaru insight list"],
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
    return _terminal_analysis_result(terminal_job, tasks, identity=identity)
