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
"""Session import failure exit codes, --fail-on-failures, and follow-up receipts."""

import json
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from tests.cli.output_rendering import render_text
from tests.cli.test_sessions import StubImportClient, _job

from kitaru.api_models.v1.job import JobResponse, JobStatus
from kitaru.api_models.v1.task import (
    TaskKind,
    TaskOnFailure,
    TaskResponse,
    TaskStatus,
)
from kitaru.cli import app as app_module
from kitaru.cli import invocation_context, sessions
from kitaru.cli.output import CLIError


def _task(
    job: JobResponse,
    *,
    kind: TaskKind = TaskKind.IMPORTER,
    status: TaskStatus = TaskStatus.COMPLETED,
    on_failure: TaskOnFailure = TaskOnFailure.CONTINUE,
    plugin_version_id: uuid.UUID | None = None,
    input_session_id: uuid.UUID | None = None,
    error: str | None = None,
    result: Any = None,
) -> TaskResponse:
    """Build one settled task response for import receipt tests."""
    now = datetime(2026, 8, 3, tzinfo=UTC)
    return TaskResponse(
        id=uuid.uuid4(),
        job_id=job.id,
        kind=kind,
        status=status,
        on_failure=on_failure,
        attempt=0 if status is TaskStatus.SKIPPED else 1,
        labels={},
        plugin_version_id=plugin_version_id,
        input_session_id=input_session_id,
        error=error,
        result=result,
        created=now,
        updated=now,
    )


def _run_waited_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    job: JobResponse,
    tasks: list[TaskResponse],
    *,
    client: StubImportClient | None = None,
    extra_args: list[str] | None = None,
    output_mode: str | None = None,
) -> tuple[int, str, str]:
    """Run `kitaru session import --wait` against a stubbed terminal job."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    stub = client or StubImportClient()

    @asynccontextmanager
    async def fake_open_client():
        yield stub

    async def wait_for_terminal_tasks(*args: Any, **kwargs: Any) -> Any:
        assert args[1] == stub.job.id
        return job, tasks

    monkeypatch.setattr(invocation_context, "open_asset_client", fake_open_client)
    monkeypatch.setattr(
        sessions.receipts, "wait_for_terminal_tasks", wait_for_terminal_tasks
    )
    argv = [
        "session",
        "import",
        str(payload),
        "--importer",
        "jsonl@2",
        "--agent",
        "assistant@3",
        "--wait",
        *(extra_args or []),
    ]
    if output_mode is not None:
        argv = ["--output", output_mode, *argv]
    exit_code = app_module.main(argv)
    captured = capsys.readouterr()
    return exit_code, captured.out, captured.err


def test_all_failed_import_exits_nonzero_with_bounded_stats_and_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """When every imported record fails, `--wait` fails with remote_failed."""
    job = _job(JobStatus.COMPLETED)
    failures = [
        {
            "line": index,
            "external_id": f"trace-{index}",
            "error": (
                f"Langfuse export has no project identity for trace-{index}; "
                "provide source_instance or project_id."
            ),
        }
        for index in range(1, 21)
    ]
    importer = _task(
        job,
        result={"created": 0, "skipped": 0, "failed": 90, "failures": failures},
    )
    client = StubImportClient()
    skipped_analyzer = _task(
        job,
        kind=TaskKind.ANALYZER,
        status=TaskStatus.SKIPPED,
        plugin_version_id=client.analyzer_version.id,
        result={
            "reason": "insufficient_sessions",
            "eligible_sessions": 0,
            "min_sessions": 5,
        },
    )

    exit_code, stdout, stderr = _run_waited_import(
        tmp_path,
        monkeypatch,
        capsys,
        job,
        [importer, skipped_analyzer],
        client=client,
        extra_args=["--analyzer", "clustering@2"],
        output_mode="json",
    )

    assert exit_code == 8
    assert stdout == ""
    document = json.loads(stderr)
    assert document["ok"] is False
    error = document["error"]
    assert error["kind"] == "remote_failed"
    assert "all 90 item(s) failed to import" in error["message"]
    assert "line 1 (trace-1)" in error["message"]
    details = error["details"]
    assert details["job_id"] == str(job.id)
    assert details["task_id"] == str(importer.id)
    assert details["stats"]["created"] == 0
    assert details["stats"]["skipped"] == 0
    assert details["stats"]["failed"] == 90
    assert len(details["failures"]) == 5
    assert details["failures"][0]["line"] == 1
    assert details["failures"][0]["external_id"] == "trace-1"
    assert details["receipt"]["analyzer_tasks"] == [
        {
            "id": str(skipped_analyzer.id),
            "status": "skipped",
            "error": None,
            "analyzer": "clustering@2",
            "analyzer_version_id": str(client.analyzer_version.id),
            "skip_reason": "0 eligible sessions, needs at least 5",
            "eligible_sessions": 0,
            "min_sessions": 5,
        }
    ]
    assert details["warnings"] == [
        "Analyzer clustering@2 was skipped: 0 eligible sessions, needs at least 5."
    ]
    assert str(importer.id) in details["next_actions"][0]


def test_partial_failure_exits_zero_by_default_and_nonzero_with_fail_on_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Partial failures warn by default and fail under `--fail-on-failures`."""
    job = _job(JobStatus.COMPLETED)
    importer = _task(
        job,
        result={
            "created": 2,
            "skipped": 1,
            "failed": 3,
            "failures": [
                {"line": 4, "external_id": "trace-4", "error": "missing model"},
                {"line": 7, "external_id": None, "error": "invalid json"},
            ],
        },
    )

    exit_code, stdout, stderr = _run_waited_import(
        tmp_path, monkeypatch, capsys, job, [importer], output_mode="json"
    )
    assert exit_code == 0
    assert stderr == ""
    ok_doc = json.loads(stdout)
    assert ok_doc["ok"] is True
    assert ok_doc["item"]["stats"]["failed"] == 3
    assert "3 item(s) failed to import." in ok_doc["warnings"]
    assert "1 duplicate session(s) were skipped." in ok_doc["warnings"]

    strict_code, strict_out, strict_err = _run_waited_import(
        tmp_path,
        monkeypatch,
        capsys,
        job,
        [importer],
        extra_args=["--fail-on-failures"],
        output_mode="json",
    )
    assert strict_code == 8
    assert strict_out == ""
    err_doc = json.loads(strict_err)
    assert err_doc["ok"] is False
    assert err_doc["error"]["kind"] == "remote_failed"
    assert (
        "3 item(s) failed to import (created 2, skipped 1)"
        in err_doc["error"]["message"]
    )
    assert "line 4 (trace-4): missing model" in err_doc["error"]["message"]
    assert err_doc["error"]["details"]["stats"]["created"] == 2
    assert err_doc["error"]["details"]["stats"]["failed"] == 3
    assert len(err_doc["error"]["details"]["failures"]) == 2


def test_duplicates_plus_failures_exit_zero_by_default_and_fail_strictly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Skipped duplicates prevent the all-failed exit unless `--fail-on-failures`."""
    job = _job(JobStatus.COMPLETED)
    importer = _task(
        job,
        result={
            "created": 0,
            "skipped": 2,
            "failed": 1,
            "failures": [{"line": 3, "external_id": "t-3", "error": "bad row"}],
        },
    )

    exit_code, stdout, stderr = _run_waited_import(
        tmp_path, monkeypatch, capsys, job, [importer], output_mode="json"
    )
    assert exit_code == 0
    assert stderr == ""
    assert json.loads(stdout)["ok"] is True

    strict_code, strict_out, strict_err = _run_waited_import(
        tmp_path,
        monkeypatch,
        capsys,
        job,
        [importer],
        extra_args=["--fail-on-failures"],
        output_mode="json",
    )
    assert strict_code == 8
    assert strict_out == ""
    assert json.loads(strict_err)["error"]["kind"] == "remote_failed"


def test_empty_import_without_failures_exits_zero_even_with_fail_on_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Zero created, zero skipped, and zero failed is not a failure."""
    job = _job(JobStatus.COMPLETED)
    importer = _task(
        job, result={"created": 0, "skipped": 0, "failed": 0, "failures": []}
    )

    exit_code, stdout, stderr = _run_waited_import(
        tmp_path,
        monkeypatch,
        capsys,
        job,
        [importer],
        extra_args=["--fail-on-failures"],
        output_mode="json",
    )
    assert exit_code == 0
    assert stderr == ""
    assert json.loads(stdout)["ok"] is True


async def test_fail_on_failures_requires_wait_before_any_remote_call(
    tmp_path: Path,
) -> None:
    """Reject `--fail-on-failures` without `--wait` locally."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    with pytest.raises(CLIError, match="--fail-on-failures requires --wait") as exc:
        await sessions.import_sessions(
            client,
            payload,
            importer="jsonl@2",
            agent="assistant@3",
            params=None,
            media_type="application/jsonl",
            wait=False,
            interval=None,
            timeout=None,
            fail_on_failures=True,
        )

    assert exc.value.kind == "invalid_arguments"
    assert exc.value.exit_code == 2
    assert client.uploads == []
    assert client.requests == []


def test_fail_on_failures_without_wait_rejected_via_argv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The CLI rejects `--fail-on-failures` without `--wait` with exit code 2."""
    payload = tmp_path / "input.jsonl"
    payload.write_bytes(b'{"x":1}')
    client = StubImportClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(invocation_context, "open_asset_client", fake_open_client)

    exit_code = app_module.main(
        [
            "--output",
            "json",
            "session",
            "import",
            str(payload),
            "--importer",
            "jsonl@2",
            "--agent",
            "assistant@3",
            "--fail-on-failures",
        ]
    )
    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    error = json.loads(captured.err)["error"]
    assert error["kind"] == "invalid_arguments"
    assert "--fail-on-failures requires --wait" in error["message"]
    assert client.uploads == []


def test_waited_import_reports_skipped_analyzer_with_and_without_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Skipped analyzer tasks appear in the receipt and emit actionable warnings."""
    job = _job(JobStatus.COMPLETED)
    importer = _task(job, result={"created": 3, "skipped": 0, "failed": 0})
    client = StubImportClient()
    skipped_with_counts = _task(
        job,
        kind=TaskKind.ANALYZER,
        status=TaskStatus.SKIPPED,
        plugin_version_id=client.analyzer_version.id,
        result={
            "reason": "insufficient_sessions",
            "eligible_sessions": 3,
            "min_sessions": 5,
        },
    )

    exit_code, stdout, stderr = _run_waited_import(
        tmp_path,
        monkeypatch,
        capsys,
        job,
        [importer, skipped_with_counts],
        client=client,
        extra_args=["--analyzer", "clustering@2"],
        output_mode="json",
    )

    assert exit_code == 0
    assert stderr == ""
    doc = json.loads(stdout)
    assert doc["item"]["analyzer_tasks"] == [
        {
            "id": str(skipped_with_counts.id),
            "status": "skipped",
            "error": None,
            "analyzer": "clustering@2",
            "analyzer_version_id": str(client.analyzer_version.id),
            "skip_reason": "3 eligible sessions, needs at least 5",
            "eligible_sessions": 3,
            "min_sessions": 5,
        }
    ]
    assert doc["warnings"] == [
        "Analyzer clustering@2 was skipped: 3 eligible sessions, needs at least 5."
    ]

    skipped_without_payload = _task(
        job,
        kind=TaskKind.ANALYZER,
        status=TaskStatus.SKIPPED,
        plugin_version_id=None,
        result=None,
    )
    fallback_result = sessions._terminal_import_result(
        job,
        [importer, skipped_without_payload],
        identity={
            "analyzers": [
                {
                    "id": str(client.analyzer.id),
                    "name": "kitaru/post-import-insights",
                    "version_id": str(client.analyzer_version.id),
                    "version": 1,
                }
            ]
        },
    )
    assert fallback_result.item["analyzer_tasks"] == [
        {
            "id": str(skipped_without_payload.id),
            "status": "skipped",
            "error": None,
            "analyzer": "kitaru/post-import-insights@1",
        }
    ]
    assert fallback_result.warnings == [
        "Analyzer kitaru/post-import-insights@1 was skipped."
    ]


def test_waited_import_reports_completed_analyzers_with_and_without_insights() -> None:
    """Completed analyzers report insight counts, actions, and 0-insight warnings."""
    job = _job(JobStatus.COMPLETED)
    importer = _task(job, result={"created": 6, "skipped": 0, "failed": 0})
    v1 = uuid.uuid4()
    v2 = uuid.uuid4()
    v3 = uuid.uuid4()
    with_insights = _task(
        job,
        kind=TaskKind.ANALYZER,
        status=TaskStatus.COMPLETED,
        plugin_version_id=v1,
        result=[
            {
                "name": "latency-spike",
                "title": "Latency spike",
                "data": {"type": "text", "content": "raw-card-body-must-not-leak"},
            },
            {
                "name": "retry-loop",
                "title": "Retry loop",
                "data": {"type": "text", "content": "another-raw-body"},
            },
        ],
    )
    zero_insights = _task(
        job,
        kind=TaskKind.ANALYZER,
        status=TaskStatus.COMPLETED,
        plugin_version_id=v2,
        result=[],
    )
    zero_with_reason = _task(
        job,
        kind=TaskKind.ANALYZER,
        status=TaskStatus.COMPLETED,
        plugin_version_id=v3,
        result={"insights": [], "reason": "no_eligible_candidates"},
    )
    identity = {
        "analyzers": [
            {
                "id": str(uuid.uuid4()),
                "name": "kitaru/post-import-insights",
                "version_id": str(v1),
                "version": 1,
            },
            {
                "id": str(uuid.uuid4()),
                "name": "custom/empty-list",
                "version_id": str(v2),
                "version": 3,
            },
            {
                "id": str(uuid.uuid4()),
                "name": "custom/empty-reason",
                "version_id": str(v3),
                "version": 1,
            },
        ]
    }

    result = sessions._terminal_import_result(
        job,
        [importer, with_insights, zero_insights, zero_with_reason],
        identity=identity,
    )

    serialized = json.dumps(result.item)
    assert "raw-card-body-must-not-leak" not in serialized
    assert result.item["analyzer_tasks"] == [
        {
            "id": str(with_insights.id),
            "status": "completed",
            "error": None,
            "analyzer": "kitaru/post-import-insights@1",
            "analyzer_version_id": str(v1),
            "insight_count": 2,
        },
        {
            "id": str(zero_insights.id),
            "status": "completed",
            "error": None,
            "analyzer": "custom/empty-list@3",
            "analyzer_version_id": str(v2),
            "insight_count": 0,
        },
        {
            "id": str(zero_with_reason.id),
            "status": "completed",
            "error": None,
            "analyzer": "custom/empty-reason@1",
            "analyzer_version_id": str(v3),
            "insight_count": 0,
            "reason": "no_eligible_candidates",
        },
    ]
    assert result.warnings == [
        "Analyzer custom/empty-list@3 completed with 0 insights.",
        "Analyzer custom/empty-reason@1 completed with 0 insights "
        "(no_eligible_candidates).",
    ]
    assert str(importer.id) in result.next_actions[0]
    assert any(
        "kitaru insight list" in action and str(with_insights.id) in action
        for action in result.next_actions
    )


def test_waited_import_reports_failed_and_canceled_analyzers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Failed or canceled analyzers surface errors on both completed and failed jobs."""
    v1 = uuid.uuid4()
    v2 = uuid.uuid4()
    identity = {
        "analyzers": [
            {
                "id": str(uuid.uuid4()),
                "name": "kitaru/openai-post-import-insights",
                "version_id": str(v1),
                "version": 1,
            },
            {
                "id": str(uuid.uuid4()),
                "name": "custom/slow",
                "version_id": str(v2),
                "version": 2,
            },
        ]
    }

    # Case 1: job completed (e.g. on_failure=ignore), analyzer failed & canceled.
    completed_job = _job(JobStatus.COMPLETED)
    importer = _task(completed_job, result={"created": 6, "skipped": 0, "failed": 0})
    failed_analyzer = _task(
        completed_job,
        kind=TaskKind.ANALYZER,
        status=TaskStatus.FAILED,
        on_failure=TaskOnFailure.IGNORE,
        plugin_version_id=v1,
        error="OpenAI API key missing",
    )
    canceled_analyzer = _task(
        completed_job,
        kind=TaskKind.ANALYZER,
        status=TaskStatus.CANCELED,
        on_failure=TaskOnFailure.IGNORE,
        plugin_version_id=v2,
    )
    result = sessions._terminal_import_result(
        completed_job,
        [importer, failed_analyzer, canceled_analyzer],
        identity=identity,
    )
    assert [t["status"] for t in result.item["analyzer_tasks"]] == [
        "failed",
        "canceled",
    ]
    assert result.warnings == [
        "Analyzer kitaru/openai-post-import-insights@1 failed: OpenAI API key missing",
        "Analyzer custom/slow@2 was canceled.",
    ]

    # Case 2: job failed because an analyzer failed (on_failure=continue).
    failed_job = _job(JobStatus.FAILED)
    client = StubImportClient()
    importer_ok = _task(failed_job, result={"created": 6, "skipped": 0, "failed": 0})
    analyzer_crash = _task(
        failed_job,
        kind=TaskKind.ANALYZER,
        status=TaskStatus.FAILED,
        plugin_version_id=client.analyzer_version.id,
        error="Model returned invalid JSON",
    )
    exit_code, stdout, stderr = _run_waited_import(
        tmp_path,
        monkeypatch,
        capsys,
        failed_job,
        [importer_ok, analyzer_crash],
        client=client,
        extra_args=["--analyzer", "clustering@2"],
        output_mode="json",
    )
    assert exit_code == 8
    assert stdout == ""
    error = json.loads(stderr)["error"]
    assert error["kind"] == "remote_failed"
    assert error["details"]["receipt"]["analyzer_tasks"] == [
        {
            "id": str(analyzer_crash.id),
            "status": "failed",
            "error": "Model returned invalid JSON",
            "analyzer": "clustering@2",
            "analyzer_version_id": str(client.analyzer_version.id),
        }
    ]
    assert error["details"]["warnings"] == [
        "Analyzer clustering@2 failed: Model returned invalid JSON"
    ]


def test_waited_import_bounds_evaluator_tasks_and_prioritizes_failures() -> None:
    """Evaluator follow-up tasks report bounded summaries, counts, and warnings."""
    job = _job(JobStatus.COMPLETED)
    importer = _task(job, result={"created": 25, "skipped": 0, "failed": 0})
    evaluator_version_id = uuid.uuid4()
    identity = {
        "evaluators": [
            {
                "id": str(uuid.uuid4()),
                "name": "quality",
                "version_id": str(evaluator_version_id),
                "version": 3,
            }
        ]
    }
    completed_tasks = [
        _task(
            job,
            kind=TaskKind.EVALUATOR,
            status=TaskStatus.COMPLETED,
            plugin_version_id=evaluator_version_id,
            input_session_id=uuid.uuid4(),
            result=[
                {
                    "name": "accuracy",
                    "score": 0.9,
                    "explanation": "raw-eval-explanation-must-not-leak",
                },
                {"name": "helpfulness", "score": 1.0},
            ],
        )
        for _ in range(18)
    ]
    failed_tasks = [
        _task(
            job,
            kind=TaskKind.EVALUATOR,
            status=TaskStatus.FAILED,
            on_failure=TaskOnFailure.IGNORE,
            plugin_version_id=evaluator_version_id,
            input_session_id=uuid.uuid4(),
            error=f"judge timeout #{index}",
        )
        for index in range(6)
    ]
    skipped_task = _task(
        job,
        kind=TaskKind.EVALUATOR,
        status=TaskStatus.SKIPPED,
        plugin_version_id=evaluator_version_id,
        input_session_id=uuid.uuid4(),
        result={"reason": "session_missing_output"},
    )

    result = sessions._terminal_import_result(
        job,
        [importer, *completed_tasks, skipped_task, *failed_tasks],
        identity=identity,
    )

    assert "raw-eval-explanation-must-not-leak" not in json.dumps(result.item)
    assert result.item["evaluator_summary"] == {
        "total_tasks": 25,
        "completed_tasks": 18,
        "skipped_tasks": 1,
        "failed_tasks": 6,
        "canceled_tasks": 0,
        "evaluation_count": 36,
        "truncated": True,
    }
    assert len(result.item["evaluator_tasks"]) == 20
    # Non-completed tasks are prioritized ahead of completed tasks when truncating.
    assert [t["status"] for t in result.item["evaluator_tasks"][:7]] == [
        "failed",
        "failed",
        "failed",
        "failed",
        "failed",
        "failed",
        "skipped",
    ]
    assert result.item["evaluator_tasks"][7]["evaluation_count"] == 2
    assert (
        result.warnings[0]
        == "7 evaluator task(s) did not complete (6 failed, 1 skipped)."
    )
    # 1 summary warning + 5 bounded per-task warnings.
    assert len(result.warnings) == 6
    assert "kitaru evaluation list" in result.next_actions


@pytest.mark.parametrize("rich", [True, False])
def test_human_text_output_renders_follow_up_tasks_and_warnings(rich: bool) -> None:
    """Both rich and plain text modes render analyzer and evaluator outcomes clearly."""
    job = _job(JobStatus.COMPLETED)
    importer = _task(job, result={"created": 3, "skipped": 0, "failed": 0})
    analyzer_version_id = uuid.uuid4()
    evaluator_version_id = uuid.uuid4()
    session_id = uuid.uuid4()
    skipped_analyzer = _task(
        job,
        kind=TaskKind.ANALYZER,
        status=TaskStatus.SKIPPED,
        plugin_version_id=analyzer_version_id,
        result={
            "reason": "insufficient_sessions",
            "eligible_sessions": 3,
            "min_sessions": 5,
        },
    )
    completed_evaluator = _task(
        job,
        kind=TaskKind.EVALUATOR,
        status=TaskStatus.COMPLETED,
        plugin_version_id=evaluator_version_id,
        input_session_id=session_id,
        result=[{"name": "accuracy", "score": 1.0, "explanation": "secret-payload"}],
    )
    result = sessions._terminal_import_result(
        job,
        [importer, skipped_analyzer, completed_evaluator],
        identity={
            "analyzers": [
                {
                    "id": str(uuid.uuid4()),
                    "name": "kitaru/post-import-insights",
                    "version_id": str(analyzer_version_id),
                    "version": 1,
                }
            ],
            "evaluators": [
                {
                    "id": str(uuid.uuid4()),
                    "name": "quality",
                    "version_id": str(evaluator_version_id),
                    "version": 3,
                }
            ],
        },
    )

    stdout, stderr = render_text("session.import", result, rich=rich, columns=140)

    assert "kitaru/post-import-insights@1" in stdout
    assert "skipped" in stdout
    assert "3 eligible sessions, needs at least 5" in stdout
    assert "quality@3" in stdout
    assert "secret-payload" not in stdout
    normalized_stderr = " ".join(stderr.split())
    assert (
        "Warning: Analyzer kitaru/post-import-insights@1 was skipped: "
        "3 eligible sessions, needs at least 5." in normalized_stderr
    )
    if rich:
        assert "Follow-up tasks" in stdout
        assert "1/1 completed (1 evaluation)" in stdout
