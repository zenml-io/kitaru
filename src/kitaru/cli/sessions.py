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
"""Session import and inspection commands."""

import uuid
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from kitaru.api_models.v1.imports import ImportCreateRequest, ImportStats
from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.api_models.v1.session_node import SessionNodeListParams
from kitaru.api_models.v1.task import TaskKind, TaskResponse, TaskStatus
from kitaru.cli import receipts
from kitaru.cli.output import CLIError, CommandResult, emit_event
from kitaru.cli.registration import (
    get_agent_version,
    get_plugin_version,
    list_params,
    page_result,
    parse_json_object,
)
from kitaru.client.exceptions import APIError


def _read_payload(path: Path) -> bytes:
    """Read one regular local file without exposing its path in failures."""
    if not path.is_file():
        raise CLIError("invalid_arguments", "FILE must be an existing regular file.")
    try:
        return path.read_bytes()
    except OSError as error:
        reason = error.strerror or type(error).__name__
        raise CLIError(
            "invalid_arguments", f"FILE could not be read: {reason}."
        ) from None


def _blob_metadata(blob: Any) -> dict[str, Any]:
    """Return the bounded blob identity used by import receipts."""
    return {
        "id": str(blob.id),
        "sha256": blob.sha256,
        "size": blob.size,
        "media_type": blob.media_type,
    }


def _task_metadata(task: TaskResponse) -> dict[str, Any]:
    """Return bounded importer-task diagnostics without raw result data."""
    return {
        "id": str(task.id),
        "kind": task.kind.value,
        "status": task.status.value,
        "error": task.error,
    }


def _internal_receipt_error(
    message: str,
    job: JobResponse,
    tasks: list[TaskResponse],
) -> CLIError:
    """Build a bounded internal error for an invalid terminal task contract."""
    return CLIError(
        "internal_error",
        message,
        details={
            "job": job.model_dump(mode="json"),
            "tasks": [_task_metadata(task) for task in tasks],
        },
    )


def _get_import_stats(
    job: JobResponse, tasks: list[TaskResponse]
) -> tuple[TaskResponse, ImportStats | None]:
    """Validate the single importer task and its optional diagnostic result."""
    if len(tasks) != 1 or tasks[0].kind is not TaskKind.IMPORTER:
        raise _internal_receipt_error(
            "An import job must contain exactly one importer task.", job, tasks
        )

    task = tasks[0]
    stats = None
    if task.result is not None:
        try:
            stats = ImportStats.model_validate(task.result)
        except ValidationError as error:
            if job.status is JobStatus.COMPLETED:
                raise _internal_receipt_error(
                    "The importer task returned malformed import statistics.",
                    job,
                    tasks,
                ) from error

    if job.status is JobStatus.COMPLETED and (
        task.status is not TaskStatus.COMPLETED or stats is None
    ):
        raise _internal_receipt_error(
            "A completed import job must have one completed task "
            "with import statistics.",
            job,
            tasks,
        )
    return task, stats


def _terminal_import_result(
    job: JobResponse,
    tasks: list[TaskResponse],
    *,
    identity: dict[str, Any],
) -> CommandResult:
    """Validate an import task and map its settled outcome to a receipt."""
    task, stats = _get_import_stats(job, tasks)
    task_action = receipts.get_task_filter_action("session", task.id)
    receipt: dict[str, Any] = {
        **identity,
        "operation": "session_import",
        "terminal": True,
        "job": job.model_dump(mode="json"),
        "task": _task_metadata(task),
    }
    if stats is not None:
        receipt["stats"] = stats.model_dump(mode="json")

    if job.status in {JobStatus.FAILED, JobStatus.CANCELED}:
        error = receipts.terminal_job_error(job, receipt)
        error.details["next_actions"] = [task_action]
        raise error

    assert stats is not None
    if stats.failed:
        emit_event("terminal", receipt)
        raise CLIError(
            "partial_failure",
            f"The import completed with {stats.failed} failed item(s).",
            details={"receipt": receipt, "next_actions": [task_action]},
        )

    warnings = []
    if stats.skipped:
        warnings.append(f"{stats.skipped} duplicate session(s) were skipped.")
    return CommandResult(
        item=receipt,
        warnings=warnings,
        next_actions=[task_action],
        event="terminal",
    )


async def import_sessions(
    client: Any,
    path: Path,
    *,
    importer: str,
    agent: str,
    params: str | None,
    media_type: str,
    wait: bool,
    interval: float | None,
    timeout: float | None,
) -> CommandResult:
    """Upload a local payload and create one import job."""
    wait_settings = receipts.get_wait_settings(
        wait=wait, interval=interval, timeout=timeout
    )
    parsed_params = parse_json_object(params, option="--params")
    content = _read_payload(path)
    importer_parent, importer_version = await get_plugin_version(
        client.importers, importer, "Importer"
    )
    agent_parent, agent_version = await get_agent_version(client, agent)

    blob = await client.blobs.upload(content, media_type=media_type, filename=path.name)
    blob_identity = _blob_metadata(blob)
    identity = {
        "importer": {
            "id": str(importer_parent.id),
            "name": importer_parent.name,
            "version_id": str(importer_version.id),
            "version": importer_version.version,
        },
        "agent": {
            "id": str(agent_parent.id),
            "name": agent_parent.name,
            "version_id": str(agent_version.id),
            "version": agent_version.version,
        },
        "blob": blob_identity,
    }
    request = ImportCreateRequest(
        importer=importer_parent.name,
        version=importer_version.version,
        agent_id=agent_parent.id,
        agent_version_id=agent_version.id,
        payload_blob_id=blob.id,
        params=parsed_params,
    )
    try:
        job = await client.imports.create(request)
    except APIError as error:
        raise CLIError(
            "partial_failure",
            "The payload was uploaded, but the import job could not be created.",
            details={
                "operation": "session_import",
                "job_created": False,
                "blob": blob_identity,
                "error": {
                    "status_code": error.status_code,
                    "detail": error.detail,
                },
            },
            hint=f"The uploaded blob {blob.id} was not deleted.",
        ) from error
    except Exception as error:
        raise CLIError(
            "partial_failure",
            "The payload was uploaded, but the import job could not be created.",
            details={
                "operation": "session_import",
                "job_created": False,
                "blob": blob_identity,
                "error": {"type": type(error).__name__},
            },
            hint=f"The uploaded blob {blob.id} was not deleted.",
        ) from error

    created = receipts.created_job_result(
        "session_import",
        job,
        identity=identity,
        next_actions=["kitaru session list"],
    )
    if wait_settings is None:
        return created

    emit_event(
        "created",
        {**created.item, "next_actions": created.next_actions},
    )
    terminal_job, tasks = await receipts.wait_for_terminal_tasks(
        client,
        job.id,
        interval=wait_settings[0],
        timeout=wait_settings[1],
        initial_job=job,
    )
    return _terminal_import_result(terminal_job, tasks, identity=identity)


async def list_sessions(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> CommandResult:
    """List one bounded server page of sessions."""
    params = list_params("session", size=size, cursor=cursor, sort=sort, filter=filter)
    return page_result(await client.sessions.list(params), size=size)


async def get_session(client: Any, session_id: uuid.UUID) -> CommandResult:
    """Get one session by exact UUID."""
    item = await client.sessions.get(session_id)
    return CommandResult(item=item.model_dump(mode="json"))


async def list_session_nodes(
    client: Any,
    session_id: uuid.UUID,
    *,
    size: int,
    cursor: str | None,
    include_payloads: bool,
) -> CommandResult:
    """List one bounded server page of session nodes in index order."""
    params = SessionNodeListParams(
        size=size, cursor=cursor, include_payloads=include_payloads
    )
    return page_result(await client.sessions.list_nodes(session_id, params), size=size)
