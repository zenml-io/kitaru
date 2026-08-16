"""Run the canonical CLI walkthrough against a real local Kitaru server."""

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

REPOSITORY_ROOT = Path(__file__).parents[2]
EXAMPLE_DIR = REPOSITORY_ROOT / "examples" / "pydantic_ai_ticket_resolver"
TRACE_PATH = EXAMPLE_DIR / "traces" / "langfuse-traces.jsonl"
TEST_ASSETS_DIR = EXAMPLE_DIR / "tests"
EVALUATOR_FIXTURE_PATH = TEST_ASSETS_DIR / "canonical_returns_evaluator.py"
CANDIDATE_AGENT_COMMAND = (
    "python -m examples.pydantic_ai_ticket_resolver.tests.canonical_returns_agent"
)
CLI = Path(sys.executable).with_name("kitaru")

pytestmark = pytest.mark.skipif(
    os.environ.get("KITARU_CANONICAL_E2E") != "1",
    reason="Set KITARU_CANONICAL_E2E=1 against an isolated local server.",
)


def _subprocess_environment() -> dict[str, str]:
    """Build the environment used by the CLI and worker."""
    environment = os.environ.copy()
    environment["KITARU_API_URL"] = os.environ["KITARU_CANONICAL_SERVER_URL"]
    environment["KITARU_API_KEY"] = os.environ["KITARU_CANONICAL_API_KEY"]
    return environment


def _run(
    command: list[str],
    *,
    cwd: Path = EXAMPLE_DIR,
    timeout: float = 300,
) -> subprocess.CompletedProcess[str]:
    """Run one walkthrough command with actionable failure output."""
    result = subprocess.run(
        command,
        cwd=cwd,
        env=_subprocess_environment(),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(
            f"Command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def _cli(*arguments: str, timeout: float = 300) -> dict[str, Any]:
    """Run one CLI command and return its JSON document."""
    result = _run([str(CLI), "--output", "json", *arguments], timeout=timeout)
    return json.loads(result.stdout)


def _items(*arguments: str) -> list[dict[str, Any]]:
    """Return typed-looking item dictionaries from one list command."""
    document = _cli(*arguments)
    items = document.get("items")
    assert isinstance(items, list), document
    return items


def _ticket_id(session: dict[str, Any]) -> str:
    """Read the latest imported ticket id from a session document."""
    inputs = session["inputs"]
    if isinstance(inputs, dict) and isinstance(inputs.get("turns"), list):
        inputs = inputs["turns"][-1]["inputs"]
    assert isinstance(inputs, dict)
    ticket_id = inputs.get("ticket_id")
    assert isinstance(ticket_id, str)
    return ticket_id


def _write_test_evaluator(path: Path) -> None:
    """Copy the test-only evaluator used by the deterministic smoke path."""
    shutil.copyfile(EVALUATOR_FIXTURE_PATH, path)


def _wait_for_worker(name: str, process: subprocess.Popen[str]) -> None:
    """Wait until the foreground worker has registered with the server."""
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"Worker exited before registration with {process.returncode}."
            )
        workers = _items("worker", "list")
        if any(
            item.get("name") == name and item.get("status") == "live"
            for item in workers
        ):
            return
        time.sleep(0.25)
    raise RuntimeError("Worker did not become live within 30 seconds.")


def test_canonical_example_completes_import_to_replay(tmp_path: Path) -> None:
    """Exercise the documented import, evaluation, cohort, and replay loop."""
    assert CLI.exists()
    _cli("status")
    _cli(
        "agent",
        "register",
        "returns-resolver",
        "--command",
        "python -m examples.pydantic_ai_ticket_resolver.agent",
        "--description",
        "Resolve one synthetic returns or delivery ticket.",
        "--display-version",
        "baseline-v1",
        "--working-dir",
        "../..",
        "--timeout-seconds",
        "180",
        "--tool",
        "lookup_order",
        "--tool",
        "get_return_policy",
        "--tool",
        "check_shipping",
        "--tool",
        "issue_refund",
        "--tool",
        "create_replacement",
        "--tool",
        "escalate_to_human",
    )

    worker_name = "canonical-example-ci-worker"
    worker_log_path = tmp_path / "worker.log"
    with worker_log_path.open("w+", encoding="utf-8") as worker_log:
        worker = subprocess.Popen(
            [
                str(CLI),
                "--output",
                "jsonl",
                "worker",
                "start",
                "--name",
                worker_name,
                "--concurrency",
                "4",
                "--poll-interval",
                "0.1",
                "--blob-cache-root",
                str(tmp_path / "blobs"),
                "--payload-cache-root",
                str(tmp_path / "payloads"),
                "--timeout",
                "300",
            ],
            cwd=EXAMPLE_DIR,
            env=_subprocess_environment(),
            text=True,
            stdout=worker_log,
            stderr=subprocess.STDOUT,
        )
        try:
            _wait_for_worker(worker_name, worker)
            _cli(
                "session",
                "import",
                str(TRACE_PATH),
                "--importer",
                "kitaru/langfuse@latest",
                "--agent",
                "returns-resolver@1",
                "--tag",
                "returns-baseline",
                "--params",
                '{"source_instance":"canonical-returns-example-ci"}',
                "--media-type",
                "application/x-ndjson",
                "--wait",
                "--timeout",
                "180",
            )

            baseline = _items(
                "session",
                "list",
                "--tag",
                "returns-baseline",
                "--origin",
                "imported",
                "--size",
                "20",
            )
            assert len(baseline) == 10
            sessions_by_ticket = {_ticket_id(item): item["id"] for item in baseline}

            _cli(
                "session",
                "evaluate",
                "--tag",
                "returns-baseline",
                "--evaluator",
                "kitaru/cost@latest",
                "--evaluator",
                "kitaru/latency@latest",
                "--evaluator",
                "kitaru/tool-call-patterns@latest",
                "--wait",
                "--timeout",
                "180",
            )

            reviewed_tickets = {
                "ticket-004": ("problematic", "escalate"),
                "ticket-007": ("problematic", "escalate"),
                "ticket-001": ("acceptable", "refund"),
                "ticket-009": ("acceptable", "refund"),
                "ticket-010": ("acceptable", "refund"),
            }
            terminal_tools = {
                "issue_refund",
                "create_replacement",
                "escalate_to_human",
            }
            evidence_nodes: dict[str, str] = {}
            investigation_arguments = [
                "investigation",
                "create",
                "refund-policy-review",
                "--agent",
                "returns-resolver",
                "--description",
                "Review risky refunds and nearby valid refund behavior.",
            ]
            for ticket_id in reviewed_tickets:
                session_id = sessions_by_ticket[ticket_id]
                nodes = _items(
                    "session",
                    "nodes",
                    session_id,
                    "--include-payloads",
                    "--size",
                    "100",
                )
                evidence = next(
                    node for node in nodes if node.get("tool_name") in terminal_tools
                )
                evidence_nodes[ticket_id] = evidence["id"]
                question = (
                    "Is this outcome acceptable, problematic, or uncertain, and "
                    "what should the agent have done in this case?"
                )
                investigation_arguments.extend(
                    [
                        "--session",
                        session_id,
                        "--session-question",
                        f"{session_id}:outcome={question}",
                    ]
                )

            investigation = _cli(*investigation_arguments)["item"]
            investigation_id = investigation["id"]
            linked_sessions = _items(
                "investigation",
                "session",
                "list",
                investigation_id,
                "--size",
                "20",
            )
            assert len(linked_sessions) == 5
            links_by_session = {
                item["session_id"]: item["id"] for item in linked_sessions
            }
            for ticket_id, (verdict, expected_action) in reviewed_tickets.items():
                session_id = sessions_by_ticket[ticket_id]
                investigation_session_id = links_by_session[session_id]
                selector = json.dumps(
                    {"node_id": evidence_nodes[ticket_id]}, separators=(",", ":")
                )
                _cli(
                    "annotation",
                    "create",
                    "--investigation-session",
                    investigation_session_id,
                    "--question-key",
                    "outcome",
                    "--selector",
                    selector,
                    "--value",
                    json.dumps("Reviewed against the automatic refund policy."),
                )
                _cli(
                    "annotation",
                    "create",
                    "--investigation-session",
                    investigation_session_id,
                    "--question-key",
                    "outcome",
                    "--value",
                    json.dumps({"action": expected_action}, separators=(",", ":")),
                )
                _cli(
                    "investigation",
                    "session",
                    "verdict",
                    investigation_id,
                    session_id,
                    verdict,
                )

            _cli("investigation", "update", investigation_id, "--status", "completed")
            completed_investigation = _cli("investigation", "get", investigation_id)[
                "item"
            ]
            assert completed_investigation["status"] == "completed"
            assert completed_investigation["completed_sessions"] == 5
            annotation_filter = json.dumps(
                {
                    "field": "investigation_id",
                    "op": "eq",
                    "value": investigation_id,
                },
                separators=(",", ":"),
            )
            annotations = _items(
                "annotation",
                "list",
                "--filter",
                annotation_filter,
                "--size",
                "100",
            )
            assert len(annotations) == 10
            assert sum(item["selector"] is not None for item in annotations) == 5
            assert sum(isinstance(item["value"], str) for item in annotations) == 5
            assert all(
                not (isinstance(item["value"], dict) and "judgment" in item["value"])
                for item in annotations
            )

            evaluator_path = tmp_path / "returns_policy.py"
            _write_test_evaluator(evaluator_path)
            _cli(
                "evaluator",
                "test",
                str(evaluator_path),
                "--entrypoint",
                "evaluate",
            )
            _cli(
                "evaluator",
                "register",
                "returns-policy",
                "--script",
                str(evaluator_path),
                "--entrypoint",
                "evaluate",
                "--description",
                "Check reviewed returns outcomes.",
                "--display-version",
                "1.0",
            )
            _cli(
                "session",
                "evaluate",
                "--tag",
                "returns-baseline",
                "--evaluator",
                "returns-policy@1",
                "--wait",
                "--timeout",
                "180",
            )
            policy_filter = '{"field":"name","op":"eq","value":"policy_correct"}'
            baseline_policy = _items(
                "evaluation", "list", "--filter", policy_filter, "--size", "100"
            )
            assert len(baseline_policy) == 10
            assert sum(item["passed"] is False for item in baseline_policy) == 2

            _cli(
                "cohort",
                "create",
                "unsafe-refund-baseline",
                "--agent",
                "returns-resolver",
                "--session",
                sessions_by_ticket["ticket-004"],
                "--session",
                sessions_by_ticket["ticket-007"],
            )
            _cli(
                "cohort",
                "create",
                "safe-refund-control",
                "--agent",
                "returns-resolver",
                "--session",
                sessions_by_ticket["ticket-001"],
                "--session",
                sessions_by_ticket["ticket-009"],
                "--session",
                sessions_by_ticket["ticket-010"],
            )

            _cli(
                "agent",
                "version",
                "register",
                "returns-resolver",
                "--command",
                CANDIDATE_AGENT_COMMAND,
                "--description",
                "Check approval and risk rules before issuing a refund.",
                "--display-version",
                "strict-policy-v2",
                "--working-dir",
                "../..",
                "--timeout-seconds",
                "180",
                "--tool",
                "lookup_order",
                "--tool",
                "get_return_policy",
                "--tool",
                "check_shipping",
                "--tool",
                "issue_refund",
                "--tool",
                "create_replacement",
                "--tool",
                "escalate_to_human",
            )
            _cli(
                "experiment",
                "create",
                "improve-returns-policy",
                "--agent",
                "returns-resolver",
                "--tool-policy",
                '{"default":{"type":"passthrough"},"tools":{}}',
                "--evaluator",
                "returns-policy@1",
                "--evaluator",
                "kitaru/cost@latest",
                "--evaluator",
                "kitaru/latency@latest",
                "--evaluator",
                "kitaru/tool-call-patterns@latest",
            )

            target_version = _cli(
                "cohort", "version", "get", "unsafe-refund-baseline@1"
            )["item"]["id"]
            control_version = _cli("cohort", "version", "get", "safe-refund-control@1")[
                "item"
            ]["id"]
            for cohort_version in (target_version, control_version):
                _cli(
                    "experiment",
                    "run",
                    "start",
                    "improve-returns-policy",
                    "--cohort-version",
                    cohort_version,
                    "--agent",
                    "returns-resolver@2",
                    "--evaluate-baselines",
                    "--wait",
                    "--timeout",
                    "300",
                    timeout=360,
                )

            runs = _items("experiment", "run", "list", "--size", "20")
            assert len(runs) == 2
            assert {item["status"] for item in runs} == {"completed"}
            assert sum(item["progress"]["completed"] for item in runs) == 5

            replayed = _items(
                "session",
                "list",
                "--agent",
                "returns-resolver",
                "--origin",
                "replay",
                "--size",
                "20",
            )
            assert len(replayed) == 5
            assert {item["status"] for item in replayed} == {"completed"}
            replay_ids = {item["id"] for item in replayed}
            all_policy = _items(
                "evaluation", "list", "--filter", policy_filter, "--size", "100"
            )
            replay_policy = [
                item for item in all_policy if item["session_id"] in replay_ids
            ]
            assert len(replay_policy) == 5
            assert all(item["passed"] is True for item in replay_policy)
        finally:
            worker.terminate()
            try:
                worker.wait(timeout=15)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=5)
            if sys.exc_info()[0] is not None:
                worker_log.flush()
                worker_log.seek(0)
                print(worker_log.read())
