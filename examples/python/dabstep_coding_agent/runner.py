"""Run the V0 DABstep coding-agent loop in a disposable local workdir."""

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import tempfile
import threading
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from examples.python.dabstep_coding_agent.convert import convert_trace
from examples.python.dabstep_coding_agent.score import score_answer

from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.session_node import SessionNodeBatchRequest
from kitaru.client import KitaruAPIClient
from kitaru.task import get_task_inputs
from kitaru.task.task_io import write_task_result

NETWORK_PROBE = (
    'python -c "import urllib.request; '
    "urllib.request.urlopen('https://example.com', timeout=3)\""
)
NETWORK_FAILURE_SIGNALS = (
    "urlerror",
    "connection refused",
    "nodename nor servname provided",
    "network is unreachable",
)


@dataclass(frozen=True)
class CodexExecution:
    """Observed lifecycle of one Codex subprocess."""

    exit_code: int
    started_at: str
    ended_at: str


def run_preflight(gold_path: Path) -> dict[str, Any]:
    """Prove the local scorer accepts the answer and rejects a wrong control."""
    expected = _load_gold(gold_path)
    oracle = score_answer(expected, expected)
    wrong_control = score_answer("definitely-not-the-answer", expected)
    if not oracle["passed"] or wrong_control["passed"]:
        raise RuntimeError("DABstep scorer preflight failed")
    return {"oracle": oracle, "wrong_control": wrong_control}


def run_task(
    *,
    fixture_dir: Path,
    gold_path: Path,
    skill_path: Path,
    artifacts_dir: Path,
    model: str | None,
    timeout_seconds: int,
    context_free: bool,
) -> dict[str, Any]:
    """Run Codex once and retain a sanitized import payload outside its workdir.

    Args:
        fixture_dir: Public task fixture made by :mod:`prepare`.
        gold_path: Private expected answer, never copied into the workdir.
        skill_path: Versioned analysis instructions to overlay for this run.
        artifacts_dir: Restricted local staging directory for retained evidence.
        model: Optional Codex model name.
        timeout_seconds: Per-Codex invocation timeout.
        context_free: Run only the question, for the one-way contamination canary.

    Returns:
        The final wrapper receipt.
    """
    task = _load_task(fixture_dir / "task.json")
    expected = _load_gold(gold_path, task_id=str(task["task_id"]))
    fixture_sha256 = _fixture_sha256(fixture_dir)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    run_dir = artifacts_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix="kitaru-dabstep-") as temporary:
        workdir = Path(temporary) / "workdir"
        _create_agent_workdir(
            workdir,
            fixture_dir=fixture_dir,
            task=task,
            skill_path=skill_path,
            context_free=context_free,
        )
        _assert_agent_workdir_safe(workdir)
        probe_trace = run_dir / "network-probe.codex.jsonl"
        _run_network_probe(
            workdir=workdir,
            trace_path=probe_trace,
            model=model,
            timeout_seconds=timeout_seconds,
        )
        if not _probe_failed(probe_trace):
            raise RuntimeError(
                "Network-denial probe did not show a sandboxed outbound failure; "
                "refusing the run"
            )

        trace_path = run_dir / "agent.codex.jsonl"
        prompt = _agent_prompt(context_free)
        execution = _run_codex(
            workdir=workdir,
            prompt=prompt,
            trace_path=trace_path,
            model=model,
            timeout_seconds=timeout_seconds,
        )
        _raise_for_codex_failure(execution)
        answer_path = workdir / "answer.txt"
        if not answer_path.is_file():
            raise RuntimeError("Codex did not create answer.txt")
        answer = answer_path.read_text(encoding="utf-8").strip()
        score_receipt = score_answer(answer, expected)
        run_metadata = {
            "fixture_task_id": task["task_id"],
            "fixture_sha256": fixture_sha256,
            "context_free": context_free,
            "exit_code": execution.exit_code,
            "started_at": execution.started_at,
            "ended_at": execution.ended_at,
            "network_access": False,
            "network_probe_command": NETWORK_PROBE,
            "network_probe_passed": True,
            "metrics_scope": "task execution only; network probe excluded",
            "skill_name": skill_path.name,
            "skill_sha256": _sha256(skill_path),
            "skill_content": skill_path.read_text(encoding="utf-8"),
            "invocation_prompt": prompt,
            "model": model,
            "model_provider": "openai" if model else None,
            "codex_command": _command_summary(model),
        }
        session = convert_trace(
            trace_path,
            task={**task, "fixture_sha256": fixture_sha256},
            answer=answer,
            score_receipt=score_receipt,
            run_metadata=run_metadata,
        )
        (run_dir / "answer.txt").write_text(answer + "\n", encoding="utf-8")
        (run_dir / "score.json").write_text(
            json.dumps(score_receipt, indent=2) + "\n", encoding="utf-8"
        )
        (run_dir / "kitaru-session.jsonl").write_text(
            json.dumps(session) + "\n", encoding="utf-8"
        )
    return {
        "run_dir": str(run_dir),
        "session_path": str(run_dir / "kitaru-session.jsonl"),
        "task_id": task["task_id"],
        "score": score_receipt,
        "context_free": context_free,
    }


def _create_agent_workdir(
    workdir: Path,
    *,
    fixture_dir: Path,
    task: dict[str, Any],
    skill_path: Path,
    context_free: bool,
) -> None:
    workdir.mkdir(parents=True)
    task_text = f"# DABstep task {task['task_id']}\n\n{task['question']}\n"
    guidelines = task.get("guidelines")
    if guidelines:
        task_text += f"\n## Guidelines\n\n{guidelines}\n"
    (workdir / "task.md").write_text(task_text, encoding="utf-8")
    shutil.copy2(skill_path, workdir / "SKILL.md")
    if not context_free:
        shutil.copytree(fixture_dir / "context", workdir / "data")


def _assert_agent_workdir_safe(workdir: Path) -> None:
    forbidden_names = {"gold.json", "solution", "solutions", "scorer.py"}
    forbidden = [
        path for path in workdir.rglob("*") if path.name.lower() in forbidden_names
    ]
    if forbidden:
        names = ", ".join(str(path.relative_to(workdir)) for path in forbidden)
        raise RuntimeError(
            f"Agent-visible workdir contains forbidden material: {names}"
        )


def _run_codex(
    *,
    workdir: Path,
    prompt: str,
    trace_path: Path,
    model: str | None,
    timeout_seconds: int,
) -> CodexExecution:
    command = _codex_command(workdir, prompt, model)
    started_at = _timestamp()
    with trace_path.open("w", encoding="utf-8") as trace_file:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        capture = threading.Thread(
            target=_capture_codex_output,
            args=(process, trace_file),
            daemon=True,
        )
        capture.start()
        try:
            exit_code = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired as error:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            trace_file.write(
                json.dumps(
                    {
                        "type": "wrapper.error",
                        "message": f"timeout: {error}",
                        "_kitaru_observed_at": _timestamp(),
                    }
                )
                + "\n"
            )
            exit_code = 124
        finally:
            capture.join()
    return CodexExecution(
        exit_code=exit_code,
        started_at=started_at,
        ended_at=_timestamp(),
    )


def _capture_codex_output(process: subprocess.Popen[str], trace_file: Any) -> None:
    """Retain each Codex record with the time the wrapper observed it."""
    if process.stdout is None:
        return
    for line in process.stdout:
        observed_at = _timestamp()
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            record = {"type": "wrapper.output", "message": line.rstrip()}
        if isinstance(record, dict):
            record["_kitaru_observed_at"] = observed_at
            trace_file.write(json.dumps(record) + "\n")
            trace_file.flush()


def _run_network_probe(
    *,
    workdir: Path,
    trace_path: Path,
    model: str | None,
    timeout_seconds: int,
) -> None:
    """Verify command networking is denied in the same Codex sandbox."""
    execution = _run_codex(
        workdir=workdir,
        prompt=(
            "Use the shell tool as your first action to run this exact command:\n"
            f"{NETWORK_PROBE}\n"
            "The command must fail because sandbox command networking is disabled. "
            "Report the observed failure and do nothing else."
        ),
        trace_path=trace_path,
        model=model,
        timeout_seconds=timeout_seconds,
    )
    _raise_for_codex_failure(execution)


def _raise_for_codex_failure(execution: CodexExecution) -> None:
    """Stop a task before scoring when Codex did not exit successfully."""
    if execution.exit_code != 0:
        raise RuntimeError(f"Codex exited with status {execution.exit_code}")


def _timestamp() -> str:
    """Return a UTC timestamp accepted by the Kitaru API."""
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _codex_options(model: str | None) -> list[str]:
    """Build the persistent Codex options shared by execution and provenance."""
    options = [
        "--ignore-user-config",
        "--sandbox",
        "workspace-write",
        "-c",
        "sandbox_workspace_write.network_access=false",
        "--json",
        "--skip-git-repo-check",
    ]
    if model:
        options.extend(("--model", model))
    return options


def _codex_command(workdir: Path, prompt: str, model: str | None) -> list[str]:
    """Build a non-interactive Codex command with explicit network denial."""
    command = ["codex", "exec", *_codex_options(model), "--cd", str(workdir)]
    command.append(prompt)
    return command


def _probe_failed(trace_path: Path) -> bool:
    records = [
        json.loads(line)
        for line in trace_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return any(
        record.get("type") == "item.completed"
        and isinstance((item := record.get("item")), dict)
        and item.get("type") == "command_execution"
        and NETWORK_PROBE in str(item.get("command", ""))
        and item.get("status") == "failed"
        and isinstance(item.get("exit_code"), int)
        and item["exit_code"] != 0
        and any(
            signal in str(item.get("aggregated_output", "")).lower()
            for signal in NETWORK_FAILURE_SIGNALS
        )
        for record in records
    )


def _load_task(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or not isinstance(value.get("task_id"), str):
        raise ValueError(f"Invalid public fixture task file: {path}")
    return value


def _load_gold(path: Path, *, task_id: str | None = None) -> str:
    value = json.loads(path.read_text(encoding="utf-8"))
    answer = value.get("answer") if isinstance(value, dict) else None
    if not isinstance(answer, str):
        raise ValueError(f"Invalid private gold answer file: {path}")
    if task_id is not None and value.get("task_id") != task_id:
        raise ValueError(f"Private gold task ID does not match task {task_id!r}")
    return answer


def _fixture_sha256(fixture_dir: Path) -> str:
    """Hash every agent-visible fixture path and byte sequence."""
    digest = hashlib.sha256()
    for path in sorted(item for item in fixture_dir.rglob("*") if item.is_file()):
        digest.update(path.relative_to(fixture_dir).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _validate_task_inputs(inputs: Any, fixture_dir: Path) -> None:
    """Refuse a rerun whose recorded task differs from the fixed fixture."""
    fixture_task = _load_task(fixture_dir / "task.json")
    if not isinstance(inputs, dict):
        raise ValueError("Kitaru task inputs must be a DABstep task object")
    for field in ("task_id", "question"):
        if inputs.get(field) != fixture_task.get(field):
            raise ValueError(
                "Kitaru task inputs do not match the fixed DABstep fixture "
                f"for {field!r}"
            )
    expected_fixture_sha256 = inputs.get("fixture_sha256")
    if (
        expected_fixture_sha256 is not None
        and expected_fixture_sha256 != _fixture_sha256(fixture_dir)
    ):
        raise ValueError("Kitaru task inputs do not match the fixed DABstep fixture")


def _resolve_task_fixture(inputs: Any, fixtures_root: Path) -> tuple[Path, Path]:
    """Resolve one public fixture and private gold from recorded task inputs."""
    if not isinstance(inputs, dict):
        raise ValueError("Kitaru task inputs must be a DABstep task object")
    task_id = str(inputs.get("task_id", ""))
    if re.fullmatch(r"[0-9]+", task_id) is None:
        raise ValueError(f"Invalid DABstep task ID: {task_id!r}")
    task_root = fixtures_root / f"task-{task_id}"
    fixture_dir = task_root / "public"
    gold_path = task_root / "private" / "gold.json"
    if not (fixture_dir / "task.json").is_file() or not gold_path.is_file():
        raise ValueError(
            f"No prepared DABstep fixture found for task {task_id!r} under "
            f"{fixtures_root}"
        )
    return fixture_dir, gold_path


async def _record_replay_session(session_path: Path) -> str:
    """Create the task-linked Kitaru replay session and its visible nodes."""
    payload = json.loads(session_path.read_text(encoding="utf-8"))
    final_status = SessionStatus(payload["status"])
    request = SessionCreateRequest(
        origin=SessionOrigin.REPLAY,
        status=SessionStatus.IN_PROGRESS,
        name=payload.get("name"),
        inputs=payload["inputs"],
        outputs=None,
        error=None,
        started_at=payload.get("started_at"),
        ended_at=None,
        external_id=payload.get("external_id"),
        metadata=payload.get("metadata", {}),
        framework=payload.get("framework"),
    )
    async with KitaruAPIClient() as client:
        session = await client.sessions.create(request)
        try:
            nodes = payload.get("nodes", [])
            if nodes:
                await client.sessions.ingest_nodes(
                    session.id,
                    SessionNodeBatchRequest.model_validate({"nodes": nodes}),
                )
            await client.sessions.update(
                session.id,
                SessionUpdateRequest(
                    status=final_status,
                    outputs=payload["outputs"],
                    error=payload.get("error"),
                    ended_at=payload.get("ended_at"),
                ),
            )
        except Exception as error:
            with suppress(Exception):
                await client.sessions.update(
                    session.id,
                    SessionUpdateRequest(
                        status=SessionStatus.FAILED,
                        error=f"Replay session upload failed: {error}",
                        ended_at=_timestamp(),
                    ),
                )
            raise
    return str(session.id)


def run_kitaru_task(
    *,
    fixture_dir: Path | None,
    gold_path: Path | None,
    skill_path: Path,
    artifacts_dir: Path,
    model: str | None,
    timeout_seconds: int,
    fixtures_root: Path | None = None,
) -> dict[str, Any]:
    """Execute one task-mode rerun and record its task-linked session."""
    inputs = get_task_inputs()
    if fixtures_root is not None:
        fixture_dir, gold_path = _resolve_task_fixture(inputs, fixtures_root)
    if fixture_dir is None or gold_path is None:
        raise ValueError(
            "task-run requires --fixtures-root or both --fixture and --gold"
        )
    _validate_task_inputs(inputs, fixture_dir)
    receipt = run_task(
        fixture_dir=fixture_dir,
        gold_path=gold_path,
        skill_path=skill_path,
        artifacts_dir=artifacts_dir,
        model=model,
        timeout_seconds=timeout_seconds,
        context_free=False,
    )
    session_id = asyncio.run(_record_replay_session(Path(receipt["session_path"])))
    result = {
        "task_id": receipt["task_id"],
        "session_id": session_id,
        "score": receipt["score"],
        "skill_name": skill_path.name,
    }
    write_task_result(result)
    return result


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _command_summary(model: str | None) -> list[str]:
    options = _codex_options(model)
    return ["codex exec", *options]


def _agent_prompt(context_free: bool) -> str:
    context = (
        "Do not use any files except task.md and SKILL.md."
        if context_free
        else (
            "Read task.md, SKILL.md, and the public files under data/. Explore and run "
            "local analysis as needed."
        )
    )
    return f"""Solve the DABstep question in this workspace.

{context}

Do not use the network. Do not try to find a known DABstep answer online.
Write only your final answer to answer.txt in the workspace.
Do not include reasoning in answer.txt.
"""


def main() -> None:
    """Run a scorer preflight, contamination canary, or agent task."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    preflight = subparsers.add_parser("preflight")
    preflight.add_argument("--gold", type=Path, required=True)
    run = subparsers.add_parser("run")
    run.add_argument("--fixture", type=Path, required=True)
    run.add_argument("--gold", type=Path, required=True)
    run.add_argument("--skill", type=Path, required=True)
    run.add_argument("--artifacts", type=Path, required=True)
    run.add_argument("--model")
    run.add_argument("--timeout-seconds", type=int, default=900)
    run.add_argument("--context-free", action="store_true")
    task_run = subparsers.add_parser("task-run")
    task_run.add_argument("--fixture", type=Path)
    task_run.add_argument("--gold", type=Path)
    task_run.add_argument("--fixtures-root", type=Path)
    task_run.add_argument("--skill", type=Path, required=True)
    task_run.add_argument("--artifacts", type=Path, required=True)
    task_run.add_argument("--model")
    task_run.add_argument("--timeout-seconds", type=int, default=900)
    args = parser.parse_args()
    if args.command == "preflight":
        print(json.dumps(run_preflight(args.gold), indent=2))
        return
    if args.command == "task-run":
        print(
            json.dumps(
                run_kitaru_task(
                    fixture_dir=args.fixture,
                    gold_path=args.gold,
                    fixtures_root=args.fixtures_root,
                    skill_path=args.skill,
                    artifacts_dir=args.artifacts,
                    model=args.model,
                    timeout_seconds=args.timeout_seconds,
                ),
                indent=2,
            )
        )
        return
    receipt = run_task(
        fixture_dir=args.fixture,
        gold_path=args.gold,
        skill_path=args.skill,
        artifacts_dir=args.artifacts,
        model=args.model,
        timeout_seconds=args.timeout_seconds,
        context_free=args.context_free,
    )
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
