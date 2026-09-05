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
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from kitaru.api_models.v1.filter import AndFilter, FilterCondition, FilterOp
from kitaru.api_models.v1.imports import ImportCreateRequest, ImportStats
from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.api_models.v1.replay_config import AnalyzerConfig, EvaluatorConfig
from kitaru.api_models.v1.session import (
    SessionListParams,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import SessionNodeListParams
from kitaru.api_models.v1.tag import (
    TagCreateRequest,
    TagLinkCreateRequest,
    TagListParams,
    TagResourceType,
    TagResponse,
)
from kitaru.api_models.v1.task import TaskKind, TaskResponse, TaskStatus
from kitaru.cli import receipts
from kitaru.cli.output import CLIError, CommandResult, emit_event
from kitaru.cli.references import (
    ParentKind,
    ReferenceResolutionError,
    resolve_parent,
)
from kitaru.cli.registration import (
    get_agent_version,
    get_plugin_version,
    list_params,
    page_result,
    parse_json_object,
    resolve_analyzer_configs,
    resolve_evaluator_configs,
)
from kitaru.cli.session_selection import get_cohort_version
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


def _normalize_import_tags(tags: list[str] | None, *, wait: bool) -> list[str]:
    """Normalize repeatable import tags before any remote mutation."""
    normalized = [tag.strip() for tag in tags or []]
    if any(not tag for tag in normalized):
        raise CLIError("invalid_arguments", "--tag must not be empty.")
    if len(set(normalized)) != len(normalized):
        raise CLIError("invalid_arguments", "Each --tag value must be unique.")
    if normalized and not wait:
        raise CLIError(
            "invalid_arguments",
            "--tag requires --wait so the imported sessions can be tagged.",
        )
    return normalized


async def _get_or_create_tag(client: Any, name: str) -> TagResponse:
    """Resolve one exact tag name, creating it when absent."""
    params = TagListParams(
        filter=FilterCondition(field="name", op=FilterOp.EQ, value=name)
    )
    async for tag in client.tags.iter(params):
        return tag
    try:
        return await client.tags.create(TagCreateRequest(name=name))
    except APIError as error:
        if error.status_code != 409:
            raise
    async for tag in client.tags.iter(params):
        return tag
    raise CLIError("internal_error", f"Tag {name!r} could not be resolved.")


async def _tag_imported_sessions(
    client: Any, task_id: uuid.UUID, names: list[str]
) -> int:
    """Apply every requested tag to sessions created by one import task."""
    tags = [await _get_or_create_tag(client, name) for name in names]
    params = SessionListParams(
        filter=FilterCondition(field="task_id", op=FilterOp.EQ, value=str(task_id))
    )
    count = 0
    async for session in client.sessions.iter(params):
        for tag in tags:
            await client.tags.create_link(
                tag.id,
                TagLinkCreateRequest(
                    resource_type=TagResourceType.SESSION,
                    resource_id=session.id,
                ),
            )
        count += 1
    return count


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
    tags: list[str] | None = None,
    evaluators: Sequence[str] | None = None,
    evaluator_params: Sequence[str] | None = None,
    analyzers: Sequence[str] | None = None,
    analyzer_params: Sequence[str] | None = None,
    media_type: str,
    wait: bool,
    interval: float | None,
    timeout: float | None,
    join_on: str | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Upload a local payload and create one import job."""
    tags = _normalize_import_tags(tags, wait=wait)
    if evaluator_params and not evaluators:
        raise CLIError(
            "invalid_arguments",
            "--evaluator-params requires at least one --evaluator.",
        )
    if analyzer_params and not analyzers:
        raise CLIError(
            "invalid_arguments",
            "--analyzer-params requires at least one --analyzer.",
        )
    wait_settings = receipts.get_wait_settings(
        wait=wait, interval=interval, timeout=timeout
    )
    parsed_params = parse_json_object(params, option="--params")
    if join_on is not None:
        if not join_on.startswith("/"):
            raise CLIError(
                "invalid_arguments",
                "--join-on must be an RFC 6901 JSON Pointer starting with '/'.",
            )
        for token in join_on[1:].split("/"):
            index = 0
            while index < len(token):
                if token[index] == "~":
                    if index + 1 >= len(token) or token[index + 1] not in "01":
                        raise CLIError(
                            "invalid_arguments",
                            "--join-on contains an invalid JSON Pointer escape.",
                        )
                    index += 2
                else:
                    index += 1
        if "join_on" in parsed_params:
            raise CLIError(
                "invalid_arguments",
                "--join-on cannot be combined with join_on in --params.",
            )
        parsed_params["join_on"] = join_on
    content = _read_payload(path)
    importer_parent, importer_version = await get_plugin_version(
        client.importers, importer, "Importer"
    )
    agent_parent, agent_version = await get_agent_version(client, agent)
    configs: list[EvaluatorConfig] = []
    evaluator_identity: list[dict[str, Any]] = []
    if evaluators:
        configs, evaluator_identity, _ = await resolve_evaluator_configs(
            client, evaluators, evaluator_params or []
        )
    analyzer_configs: list[AnalyzerConfig] = []
    analyzer_identity: list[dict[str, Any]] = []
    if analyzers:
        analyzer_configs, analyzer_identity, _ = await resolve_analyzer_configs(
            client, analyzers, analyzer_params or []
        )

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
    if tags:
        identity["tags"] = tags
    if evaluator_identity:
        identity["evaluators"] = evaluator_identity
    if analyzer_identity:
        identity["analyzers"] = analyzer_identity
    request = ImportCreateRequest(
        importer=importer_parent.name,
        version=importer_version.version,
        agent_id=agent_parent.id,
        agent_version_id=agent_version.id,
        payload_blob_id=blob.id,
        params=parsed_params,
        evaluators=configs,
        analyzers=analyzer_configs,
    )
    try:
        created_import = await client.imports.create(
            request, idempotency_key=idempotency_key
        )
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

    identity["import_id"] = str(created_import.id)
    assert created_import.job_id is not None
    job = await client.jobs.get(created_import.job_id)
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
    result = _terminal_import_result(terminal_job, tasks, identity=identity)
    if tags:
        task, _ = _get_import_stats(terminal_job, tasks)
        try:
            result.item["tagged_session_count"] = await _tag_imported_sessions(
                client, task.id, tags
            )
        except Exception as error:
            raise CLIError(
                "partial_failure",
                "The import completed, but its tags could not be applied.",
                details={"receipt": result.item, "error": type(error).__name__},
                hint="Retry the import without --tag or apply the tags manually.",
            ) from error
    return result


async def list_sessions(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
    status: SessionStatus | None = None,
    agent: str | None = None,
    origin: SessionOrigin | None = None,
    imported_from: str | None = None,
    tag: str | None = None,
    cohort: str | None = None,
    started_after: datetime | None = None,
    started_before: datetime | None = None,
    include_payloads: bool = False,
) -> CommandResult:
    """List one bounded server page of sessions."""
    params = list_params("session", size=size, cursor=cursor, sort=sort, filter=filter)
    assert isinstance(params, SessionListParams)
    if include_payloads:
        params = params.model_copy(update={"include_payloads": True})
    conditions = []
    if params.filter is not None:
        conditions.append(params.filter)
    if status is not None:
        conditions.append(
            FilterCondition(field="status", op=FilterOp.EQ, value=status.value)
        )
    if agent is not None:
        agent_id = await _get_agent_filter_id(client, agent)
        conditions.append(
            FilterCondition(field="agent_id", op=FilterOp.EQ, value=str(agent_id))
        )
    if origin is not None:
        conditions.append(
            FilterCondition(field="origin", op=FilterOp.EQ, value=origin.value)
        )
    if imported_from is not None:
        conditions.append(
            FilterCondition(field="imported_from", op=FilterOp.EQ, value=imported_from)
        )
    if tag is not None:
        normalized = tag.strip()
        if not normalized:
            raise CLIError("invalid_arguments", "--tag must not be empty.")
        conditions.append(
            FilterCondition(field="tag", op=FilterOp.EQ, value=normalized)
        )
    if cohort is not None:
        _, cohort_version = await get_cohort_version(client, cohort)
        conditions.append(
            FilterCondition(
                field="cohort_version_id",
                op=FilterOp.EQ,
                value=str(cohort_version.id),
            )
        )
    for option, field_operator, value in (
        ("--started-after", FilterOp.GE, started_after),
        ("--started-before", FilterOp.LT, started_before),
    ):
        if value is None:
            continue
        if value.tzinfo is None or value.utcoffset() is None:
            raise CLIError(
                "invalid_arguments",
                f"{option} must include a timezone, such as Z or +02:00.",
            )
        conditions.append(
            FilterCondition(field="started_at", op=field_operator, value=value)
        )
    if conditions:
        expression = (
            conditions[0]
            if len(conditions) == 1
            else AndFilter.model_validate({"and": conditions})
        )
        params = params.model_copy(update={"filter": expression})
    return page_result(await client.sessions.list(params), size=size)


async def _get_agent_filter_id(client: Any, reference: str) -> uuid.UUID:
    """Resolve a session agent filter through the shared bounded helper."""
    try:
        return uuid.UUID(reference)
    except ValueError:
        pass
    try:
        agent = await resolve_parent(client, ParentKind.AGENT, reference)
    except ReferenceResolutionError as error:
        raise CLIError(error.code, error.message, details=error.details) from error
    return agent.id


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
