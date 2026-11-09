#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""JSON wait timeouts retain the latest pending-work explanation."""

import io
import json
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from tests.cli.experiment_run_fakes import StubRunsClient

from kitaru.api_models.v1.job import JobKind, JobResponse, JobStatus
from kitaru.api_models.v1.task import TaskDiagnosticsResponse
from kitaru.cli import experiment_runs, jobs, receipts
from kitaru.cli.output import (
    CLIError,
    OutputContext,
    emit_error,
    reset_output_context,
    set_output_context,
)


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds


class _Tasks:
    def __init__(self, *, restore_coverage: bool) -> None:
        self.calls = 0
        self.restore_coverage = restore_coverage

    async def diagnostics(self, **_kwargs) -> TaskDiagnosticsResponse:
        self.calls += 1
        return TaskDiagnosticsResponse.model_validate(
            {
                "live_worker_count": 1,
                "pending_task_count": 1,
                "groups": [
                    {
                        "kind": "analyzer",
                        "labels": {},
                        "reasons": []
                        if self.restore_coverage and self.calls > 1
                        else ["claim_mismatch"],
                        "task_count": 1,
                        "task_ids": [],
                        "job_ids": [],
                    }
                ],
                "truncated": False,
            }
        )


@pytest.mark.parametrize("operation", ["job", "import", "run"])
@pytest.mark.parametrize("restore_coverage", [False, True])
async def test_json_timeout_retains_current_worker_diagnostics(
    operation: str, restore_coverage: bool
) -> None:
    now = datetime.now(UTC)
    job = JobResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        created=now,
        updated=now,
        kind=JobKind.IMPORT,
        status=JobStatus.RUNNING,
    )

    async def get_job(_job_id: uuid.UUID) -> JobResponse:
        return job

    task_resource = _Tasks(restore_coverage=restore_coverage)
    client = SimpleNamespace(jobs=SimpleNamespace(get=get_job), tasks=task_resource)
    run_client = StubRunsClient()
    run_resource = SimpleNamespace(
        experiment_runs=run_client.experiment_runs, tasks=task_resource
    )
    clock = _Clock()
    stdout, stderr = io.StringIO(), io.StringIO()
    token = set_output_context(
        OutputContext(
            command=f"{operation}.wait",
            mode="json",
            debug=False,
            traceback=False,
            stdout=stdout,
            stderr=stderr,
            rich=False,
        )
    )
    try:
        with pytest.raises(CLIError) as raised:
            kwargs: dict[str, Any] = {
                "interval": 1,
                "timeout": 2 if restore_coverage else 1,
                "clock": clock,
                "sleep": clock.sleep,
            }
            if operation == "run":
                await experiment_runs.poll_run(
                    run_resource, run_client.run_id, **kwargs
                )
            elif operation == "import":
                await receipts.wait_for_terminal_tasks(client, job.id, **kwargs)
            else:
                await jobs.poll_job(client, job.id, **kwargs)
        assert emit_error(raised.value) == 7
    finally:
        reset_output_context(token)

    assert stdout.getvalue() == ""
    error = json.loads(stderr.getvalue())["error"]
    assert error["kind"] == "timeout"
    details = error["details"]
    assert details["remote_continues"] is True
    assert details["task_diagnostics"]["pending_task_count"] == 1
    if restore_coverage:
        assert details["warnings"] == []
        assert details["task_diagnostics"]["groups"][0]["reasons"] == []
    else:
        assert details["task_diagnostics"]["groups"][0]["reasons"] == ["claim_mismatch"]
        assert "--claim analyzer" in " ".join(details["warnings"])
