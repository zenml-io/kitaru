"""PostgreSQL proof for the canonical TypeScript returns workflow."""

import asyncio
import json
import os
import re
import socket
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
import uvicorn

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
EXAMPLE_DIR = Path(__file__).resolve().parents[1]
TESTS_DIR = REPOSITORY_ROOT / "tests"
sys.path.insert(0, str(TESTS_DIR))

from conftest import db_settings, drop_test_database, postgres_available  # noqa: E402
from kitaru.server.api.app import create_app  # noqa: E402
from kitaru.server.database.service import DatabaseService  # noqa: E402

REQUIRE_POSTGRES_ENVIRONMENT_VARIABLE = "KITARU_REQUIRE_POSTGRES"
WORKFLOW_EVENT_SCHEMA_VERSION = 1
WORKFLOW_MANIFEST_SCHEMA_VERSION = 2
WORKFLOW_TIMEOUT_SECONDS = 300
WORKER_TIMEOUT_SECONDS = 300
UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
EXPECTED_COUNTS = {
    "baseline_sessions": 10,
    "baseline_passes": 8,
    "baseline_failures": 2,
    "target_sessions": 2,
    "control_sessions": 3,
    "experiment_runs": 2,
    "replays": 5,
    "replay_passes": 5,
}
EXPECTED_HANDOFFS = (
    ("baseline_evaluation", 1, "evaluation"),
    ("experiment_runs", 5, "replay"),
)
WORKFLOW_HANDOFF_KEYS = {
    "event",
    "schema_version",
    "evidence_set_id",
    "phase",
    "manifest_relative_path",
    "jobs",
}
WORKFLOW_COMPLETED_KEYS = {
    "event",
    "schema_version",
    "evidence_set_id",
    "counts",
}
WORKFLOW_JOB_KEYS = {"job_id", "job_kind", "agent_version_id"}
WORKFLOW_MANIFEST_KEYS = {
    "evidence_set_id",
    "ids",
    "pending_operation",
    "phase",
    "provider",
    "schema_version",
    "server",
    "source_hashes",
    "stages",
}
WORKFLOW_SERVER_KEYS = {"account_id", "api_url", "auth_scheme", "version"}
WORKFLOW_PROVIDER_KEYS = {
    "fixture_version",
    "kind",
    "provider_call",
    "requested_model",
    "served_model",
    "synthetic_usage",
}
WORKFLOW_SOURCE_HASH_KEYS = {
    "baseline_instructions_sha256",
    "evaluator_sha256",
    "fixtures_sha256",
    "strict_instructions_sha256",
}
WORKFLOW_STAGE_KEYS = {
    "baseline",
    "review",
    "baseline_evaluation",
    "cohorts",
    "experiment_runs",
    "verification",
}
WORKFLOW_ID_KEYS = {
    "agent_id",
    "agent_versions",
    "annotation_ids",
    "baseline_sessions",
    "cohort_versions",
    "cohorts",
    "evaluation_ids",
    "evaluation_job_id",
    "evaluator_blob_id",
    "evaluator_id",
    "evaluator_version_id",
    "experiment_id",
    "experiment_run_ids",
    "investigation_id",
    "investigation_session_id",
    "replay_evaluation_ids",
    "replay_ids",
    "replay_job_ids",
    "replay_result_session_ids",
    "task_ids",
}
WORKER_LIFECYCLE_EVENT_KEYS = {
    "schema_version",
    "command",
    "ok",
    "event",
    "item",
}
WORKER_RESULT_EVENT_KEYS = WORKER_LIFECYCLE_EVENT_KEYS | {
    "warnings",
    "links",
    "next_actions",
}
SUBPROCESS_ENVIRONMENT_ALLOWLIST = {
    "HOME",
    "LANG",
    "LC_ALL",
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "TMPDIR",
    "UV_CACHE_DIR",
    "VIRTUAL_ENV",
    "WINDIR",
}


async def _require_postgres() -> None:
    """Require PostgreSQL for the documented command or skip ambient collection."""
    if await postgres_available():
        return
    message = (
        "PostgreSQL is not reachable on the configured Kitaru test database port. "
        "Start it with `docker compose -f ../../docker-compose.yml up -d --build` "
        "from this example directory, then rerun `pnpm test:e2e`."
    )
    if os.environ.get(REQUIRE_POSTGRES_ENVIRONMENT_VARIABLE) == "1":
        pytest.fail(message)
    pytest.skip(message)


def _available_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@asynccontextmanager
async def _network_server() -> AsyncIterator[str]:
    await _require_postgres()
    settings = db_settings()
    await DatabaseService.create_db(settings)
    port = _available_port()
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(settings),
            host="127.0.0.1",
            port=port,
            lifespan="on",
            log_level="error",
        )
    )
    task = asyncio.create_task(server.serve())
    try:
        for _ in range(200):
            if server.started:
                break
            if task.done():
                task.result()
            await asyncio.sleep(0.01)
        else:
            raise RuntimeError("Timed out starting the Kitaru test server")
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        try:
            await task
        finally:
            await drop_test_database(settings)


def _subprocess_environment(**updates: str) -> dict[str, str]:
    environment = {
        name: value
        for name in SUBPROCESS_ENVIRONMENT_ALLOWLIST
        if (value := os.environ.get(name)) is not None
    }
    environment.update(updates)
    return environment


def _run_checked(command: Sequence[str], *, cwd: Path) -> None:
    result = subprocess.run(
        command,
        cwd=cwd,
        env=_subprocess_environment(),
        text=True,
        capture_output=True,
        timeout=WORKFLOW_TIMEOUT_SECONDS,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(
            f"Command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


async def _build_typescript() -> None:
    commands = (
        (["pnpm", "--filter", "@zenml-io/kitaru", "build"], REPOSITORY_ROOT),
        (
            ["pnpm", "--filter", "@zenml-io/kitaru-vercel-ai", "build"],
            REPOSITORY_ROOT,
        ),
        (["pnpm", "--ignore-workspace", "build"], EXAMPLE_DIR),
    )
    for command, cwd in commands:
        await asyncio.to_thread(_run_checked, command, cwd=cwd)


async def _run_process(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    timeout: float,
) -> tuple[str, str]:
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=cwd,
        env=dict(environment),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            process.communicate(), timeout=timeout
        )
    except TimeoutError:
        process.kill()
        stdout_bytes, stderr_bytes = await process.communicate()
        pytest.fail(
            f"Command timed out after {timeout} seconds: {' '.join(command)}\n"
            f"stdout:\n{stdout_bytes.decode('utf-8', errors='replace')}\n"
            f"stderr:\n{stderr_bytes.decode('utf-8', errors='replace')}"
        )
    stdout = stdout_bytes.decode("utf-8", errors="replace")
    stderr = stderr_bytes.decode("utf-8", errors="replace")
    if process.returncode != 0:
        pytest.fail(
            f"Command failed ({process.returncode}): {' '.join(command)}\n"
            f"stdout:\n{stdout}\nstderr:\n{stderr}"
        )
    return stdout, stderr


def _parse_json_object(line: str, *, source: str) -> dict[str, Any]:
    try:
        value = json.loads(line)
    except json.JSONDecodeError as error:
        pytest.fail(f"{source} emitted invalid JSON: {line!r}: {error}")
    if not isinstance(value, dict):
        pytest.fail(f"{source} emitted a non-object JSON value: {value!r}")
    return value


def _parse_single_workflow_event(stdout: str) -> dict[str, Any]:
    lines = stdout.splitlines()
    assert len(lines) == 1, (
        "The TypeScript workflow must emit exactly one versioned JSON event per "
        f"invocation; received {lines!r}"
    )
    event = _parse_json_object(lines[0], source="TypeScript workflow")
    assert type(event.get("schema_version")) is int
    assert event["schema_version"] == WORKFLOW_EVENT_SCHEMA_VERSION
    event_name = event.get("event")
    if event_name == "kitaru.worker_handoff":
        assert set(event) == WORKFLOW_HANDOFF_KEYS
    elif event_name == "kitaru.workflow_completed":
        assert set(event) == WORKFLOW_COMPLETED_KEYS
    else:
        pytest.fail(f"TypeScript workflow emitted unknown event {event_name!r}")
    _uuid(event.get("evidence_set_id"), field="event.evidence_set_id")
    return event


def _uuid(value: object, *, field: str) -> uuid.UUID:
    assert isinstance(value, str) and UUID_PATTERN.fullmatch(value), (
        f"{field} must be a UUID, received {value!r}"
    )
    return uuid.UUID(value)


def _assert_handoff(
    event: Mapping[str, Any],
    *,
    expected_phase: str,
    expected_jobs: int,
    expected_kind: str,
    state_dir: Path,
) -> list[dict[str, Any]]:
    assert event["event"] == "kitaru.worker_handoff"
    assert event["phase"] == expected_phase
    assert event["manifest_relative_path"] == os.path.relpath(
        state_dir / "workflow.json", EXAMPLE_DIR
    )
    jobs = event["jobs"]
    assert isinstance(jobs, list) and len(jobs) == expected_jobs
    assert all(isinstance(job, dict) and set(job) == WORKFLOW_JOB_KEYS for job in jobs)
    job_ids = [str(_uuid(job["job_id"], field="job.job_id")) for job in jobs]
    assert job_ids == sorted(job_ids), "Workflow handoff jobs must be UUID-sorted"
    assert len(set(job_ids)) == len(job_ids)
    for job in jobs:
        assert job["job_kind"] == expected_kind
        if expected_kind == "evaluation":
            assert job["agent_version_id"] is None
        else:
            _uuid(job["agent_version_id"], field="job.agent_version_id")
    return jobs


def _parse_worker_events(stdout: str, *, job_id: str) -> list[dict[str, Any]]:
    lines = stdout.splitlines()
    assert lines, f"Worker for {job_id} emitted no JSONL lifecycle events"
    events = [_parse_json_object(line, source=f"worker for {job_id}") for line in lines]
    for event in events[:-1]:
        assert set(event) == WORKER_LIFECYCLE_EVENT_KEYS
    assert set(events[-1]) == WORKER_RESULT_EVENT_KEYS
    for event in events:
        assert event["schema_version"] == "1"
        assert event["command"] == "worker.start"
        assert event["ok"] is True
        assert isinstance(event["event"], str)
    assert [events[0]["event"], events[-1]["event"]] == ["starting", "stopped"]
    starting = events[0]["item"]
    stopped = events[-1]["item"]
    assert isinstance(starting, dict) and isinstance(stopped, dict)
    assert starting["job_id"] == job_id
    assert starting["concurrency"] == 1
    assert stopped["job_id"] == job_id
    assert stopped["status"] == "stopped"
    assert stopped["stop_reason"] == "completed"
    assert events[-1]["warnings"] == []
    assert events[-1]["links"] == {}
    assert events[-1]["next_actions"] == []
    return events


async def _run_job_worker(job_id: str, *, api_url: str, state_dir: Path) -> None:
    worker_state = state_dir / "workers" / job_id
    command = [
        "uv",
        "run",
        "kitaru",
        "--output",
        "jsonl",
        "worker",
        "start",
        "--job-id",
        job_id,
        "--name",
        f"vercel-returns-e2e-{job_id}",
        "--concurrency",
        "1",
        "--poll-interval",
        ".05",
        "--timeout",
        str(WORKER_TIMEOUT_SECONDS),
        "--blob-cache-root",
        str(worker_state / "blobs"),
        "--payload-cache-root",
        str(worker_state / "payloads"),
    ]
    stdout, _stderr = await _run_process(
        command,
        cwd=REPOSITORY_ROOT,
        environment=_subprocess_environment(
            KITARU_API_KEY="local-development-key",
            KITARU_API_URL=api_url,
        ),
        timeout=WORKER_TIMEOUT_SECONDS + 30,
    )
    _parse_worker_events(stdout, job_id=job_id)


async def _invoke_workflow(*, api_url: str, state_dir: Path) -> dict[str, Any]:
    stdout, _stderr = await _run_process(
        ["node", "dist/workflow.js", "--state-dir", str(state_dir)],
        cwd=EXAMPLE_DIR,
        environment=_subprocess_environment(
            KITARU_API_KEY="local-development-key",
            KITARU_API_URL=api_url,
        ),
        timeout=WORKFLOW_TIMEOUT_SECONDS,
    )
    return _parse_single_workflow_event(stdout)


def _read_manifest(state_dir: Path) -> dict[str, Any]:
    manifest_path = state_dir / "workflow.json"
    try:
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        pytest.fail(f"Workflow manifest is unreadable at {manifest_path}: {error}")
    if not isinstance(value, dict):
        pytest.fail(f"Workflow manifest must be an object, received {value!r}")
    return value


def _assert_manifest(
    manifest: Mapping[str, Any],
    *,
    api_url: str,
    evidence_set_id: str,
) -> None:
    assert set(manifest) == WORKFLOW_MANIFEST_KEYS
    assert manifest["schema_version"] == WORKFLOW_MANIFEST_SCHEMA_VERSION
    assert manifest["evidence_set_id"] == evidence_set_id
    assert manifest["phase"] == "completed"
    assert manifest["pending_operation"] is None

    server = manifest["server"]
    assert isinstance(server, dict) and set(server) == WORKFLOW_SERVER_KEYS
    assert server["api_url"] == api_url
    _uuid(server["account_id"], field="manifest.server.account_id")
    assert server["auth_scheme"] == "none"
    assert isinstance(server["version"], str) and server["version"]

    provider = manifest["provider"]
    assert isinstance(provider, dict) and set(provider) == WORKFLOW_PROVIDER_KEYS
    assert provider["kind"] == "deterministic"
    assert provider["provider_call"] is False
    assert provider["synthetic_usage"] is True
    assert provider["fixture_version"] == "returns-v1"
    assert provider["requested_model"] == "openai/gpt-5-nano"
    assert provider["served_model"] == "kitaru-returns-scripted-fixture"

    source_hashes = manifest["source_hashes"]
    assert isinstance(source_hashes, dict)
    assert set(source_hashes) == WORKFLOW_SOURCE_HASH_KEYS
    assert all(
        isinstance(name, str)
        and isinstance(digest, str)
        and SHA256_PATTERN.fullmatch(digest)
        for name, digest in source_hashes.items()
    )

    stages = manifest["stages"]
    assert isinstance(stages, dict) and set(stages) == WORKFLOW_STAGE_KEYS
    assert all(
        isinstance(stage, dict)
        and set(stage) == {"status"}
        and stage["status"] == "completed"
        for stage in stages.values()
    )

    ids = manifest["ids"]
    assert isinstance(ids, dict) and set(ids) == WORKFLOW_ID_KEYS
    _assert_manifest_ids(ids)


def _assert_manifest_ids(ids: Mapping[str, Any]) -> None:
    """Assert the complete remote-resource inventory written by the workflow."""
    singular_fields = (
        "agent_id",
        "evaluation_job_id",
        "evaluator_blob_id",
        "evaluator_id",
        "evaluator_version_id",
        "experiment_id",
        "investigation_id",
        "investigation_session_id",
    )
    for field in singular_fields:
        _uuid(ids[field], field=f"manifest.ids.{field}")

    for field, expected_keys in (
        ("agent_versions", {"baseline", "strict"}),
        ("cohort_versions", {"control", "target"}),
        ("cohorts", {"control", "target"}),
        ("experiment_run_ids", {"control", "target"}),
    ):
        value = ids[field]
        assert isinstance(value, dict) and set(value) == expected_keys
        for name, resource_id in value.items():
            _uuid(resource_id, field=f"manifest.ids.{field}.{name}")
        assert len(set(value.values())) == len(expected_keys)

    baseline_sessions = ids["baseline_sessions"]
    assert isinstance(baseline_sessions, dict)
    assert set(baseline_sessions) == {f"ticket-{index:03d}" for index in range(1, 11)}
    for ticket_id, session_id in baseline_sessions.items():
        _uuid(session_id, field=f"manifest.ids.baseline_sessions.{ticket_id}")
    assert len(set(baseline_sessions.values())) == 10
    expected_list_sizes = {
        "annotation_ids": 3,
        "evaluation_ids": 10,
        "replay_evaluation_ids": 5,
        "replay_ids": 5,
        "replay_job_ids": 5,
        "replay_result_session_ids": 5,
        "task_ids": 20,
    }
    for field, expected_size in expected_list_sizes.items():
        values = ids[field]
        assert isinstance(values, list) and len(values) == expected_size
        normalized = [
            str(_uuid(value, field=f"manifest.ids.{field}[]")) for value in values
        ]
        assert len(set(normalized)) == expected_size

    assert set(ids["evaluation_ids"]).isdisjoint(ids["replay_evaluation_ids"])
    assert set(baseline_sessions.values()).isdisjoint(ids["replay_result_session_ids"])


def _assert_advertised_jobs(
    manifest: Mapping[str, Any], jobs: Sequence[Mapping[str, Any]]
) -> None:
    ids = manifest["ids"]
    assert isinstance(ids, dict)
    evaluation_jobs = [job for job in jobs if job["job_kind"] == "evaluation"]
    replay_jobs = [job for job in jobs if job["job_kind"] == "replay"]
    assert [job["job_id"] for job in evaluation_jobs] == [ids["evaluation_job_id"]]
    assert sorted(job["job_id"] for job in replay_jobs) == sorted(ids["replay_job_ids"])
    agent_versions = ids["agent_versions"]
    assert isinstance(agent_versions, dict)
    assert all(
        job["agent_version_id"] == agent_versions["strict"] for job in replay_jobs
    )


async def test_typescript_canonical_improvement_loop(tmp_path: Path) -> None:
    """Run and resume the deterministic TypeScript workflow through its public SDK."""
    await _build_typescript()
    state_dir = tmp_path / ".state"

    async with _network_server() as api_url:
        evidence_set_id: str | None = None
        advertised_jobs: list[dict[str, Any]] = []
        for expected_phase, expected_jobs, expected_kind in EXPECTED_HANDOFFS:
            event = await _invoke_workflow(api_url=api_url, state_dir=state_dir)
            if evidence_set_id is None:
                evidence_set_id = event["evidence_set_id"]
            assert event["evidence_set_id"] == evidence_set_id
            jobs = _assert_handoff(
                event,
                expected_phase=expected_phase,
                expected_jobs=expected_jobs,
                expected_kind=expected_kind,
                state_dir=state_dir,
            )
            for job in jobs:
                job_id = job["job_id"]
                assert all(existing["job_id"] != job_id for existing in advertised_jobs)
                advertised_jobs.append(job)
                await _run_job_worker(job_id, api_url=api_url, state_dir=state_dir)

        completed = await _invoke_workflow(api_url=api_url, state_dir=state_dir)
        assert completed["event"] == "kitaru.workflow_completed"
        assert completed["evidence_set_id"] == evidence_set_id
        assert completed["counts"] == EXPECTED_COUNTS
        assert len(advertised_jobs) == 6

        manifest = _read_manifest(state_dir)
        assert evidence_set_id is not None
        _assert_manifest(
            manifest,
            api_url=api_url,
            evidence_set_id=evidence_set_id,
        )
        _assert_advertised_jobs(manifest, advertised_jobs)
