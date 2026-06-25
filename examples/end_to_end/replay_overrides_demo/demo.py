"""Replay overrides demo.

Run from this directory:

    uv run python demo.py seed
    uv run python demo.py seed-batch --count 4
    uv run python demo.py flow-override
    uv run python demo.py inject-output
    uv run python demo.py code-swap
    uv run python demo.py model-override
    uv run python demo.py explicit-skip
    uv run python demo.py tagged-batch
    uv run python demo.py diff-report
    uv run python demo.py run-all
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from support_agent import (
    FINAL_DECISION_CHECKPOINT,
    FINAL_MODEL_INVOCATION,
    FLOW_NAME,
    REPLAY_POINT,
    support_copilot_flow,
)
from utils import decision_summary, diff_decisions, load_support_decision, write_json

from kitaru import FlowHandle, KitaruClient, ReplaySubmission, diff
from kitaru.diff import diff_matrix, serialize_diff_matrix, serialize_execution_diff

ROOT = Path(__file__).resolve().parent
FIXTURES = ROOT / "fixtures"
REPORTS = ROOT / "reports"
SCENARIOS_PATH = FIXTURES / "scenarios.json"
PROD_EXEC_ID_PATH = FIXTURES / "prod_exec_id"
BATCH_EXEC_IDS_PATH = FIXTURES / "batch_exec_ids"
REPLAY_RESULTS_PATH = FIXTURES / "replay_results.json"

BASELINE_MODEL = "openai:gpt-5-mini"
VARIANT_MODEL = "openai:gpt-5-nano"
BASELINE_PROMPT_PROFILE = "baseline"
VARIANT_PROMPT_PROFILE = "trimmed_permissions"
REPLAY_TAG = "replay-overrides-demo"

DEFAULT_PROMPT = (
    "I need to grant every engineer admin access to the production SSO settings "
    "so they can self-service identity provider changes. Can you enable that?"
)
DEFAULT_CUSTOMER = "acme-corp / alice@acme.example"


def _section(title: str) -> None:
    print(f"\n\033[1m{title}\033[0m")


def _load_scenarios() -> list[dict[str, str]]:
    payload = json.loads(SCENARIOS_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise RuntimeError(f"{SCENARIOS_PATH} must contain a JSON list.")
    scenarios: list[dict[str, str]] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise RuntimeError(f"Scenario {index} must be an object.")
        label = str(item.get("label", "")).strip()
        customer = str(item.get("customer", "")).strip()
        prompt = str(item.get("prompt", "")).strip()
        if not label or not customer or not prompt:
            raise RuntimeError(f"Scenario {index} needs label, customer, and prompt.")
        scenarios.append({"label": label, "customer": customer, "prompt": prompt})
    return scenarios


def _read_lines(path: Path) -> list[str]:
    if not path.is_file():
        return []
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _write_lines(path: Path, values: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(values) + "\n", encoding="utf-8")


def _wait_for_flow_completion(handle: FlowHandle) -> None:
    """Wait for a flow without asking Kitaru to extract its return value."""
    while True:
        status = handle.status
        if not status.is_finished:
            time.sleep(1)
            continue
        if not status.is_successful:
            raise RuntimeError(f"Execution {handle.exec_id} finished with {status}.")
        return


def _wait_for_execution_completion(client: KitaruClient, exec_id: str) -> str:
    """Wait for an execution and return the replay row status string."""
    while True:
        run = client.executions.get(exec_id)
        status = run.status
        if not status.is_finished:
            time.sleep(1)
            continue
        if status.is_successful:
            return "completed"
        return "failed"


def _wait_for_submission(
    client: KitaruClient,
    submission: ReplaySubmission,
) -> ReplaySubmission:
    """Wait for submitted replay rows without using flow-handle result extraction."""
    updated_results = [
        replace(
            row,
            status=_wait_for_execution_completion(client, row.replay_exec_id),
            handle=None,
        )
        for row in submission.results
    ]
    return ReplaySubmission.create(
        submission_id=submission.submission_id,
        tag=submission.tag,
        at=submission.at,
        wait=True,
        plan=submission.plan,
        results=updated_results,
        failures=submission.failures,
        skipped=submission.skipped,
        compare_url=submission.compare_url,
    )


def _default_exec_id(explicit: str | None = None) -> str:
    if explicit:
        return explicit
    saved = _read_lines(PROD_EXEC_ID_PATH)
    if saved:
        return saved[0]
    message = (
        "No execution ID given and fixtures/prod_exec_id does not exist. "
        "Run seed first."
    )
    raise SystemExit(message)


def _default_batch_ids(explicit: list[str] | None = None) -> list[str]:
    if explicit:
        return explicit
    saved = _read_lines(BATCH_EXEC_IDS_PATH)
    if saved:
        return saved
    message = (
        "No execution IDs given and fixtures/batch_exec_ids does not exist. "
        "Run seed-batch first."
    )
    raise SystemExit(message)


def _remember_replay(label: str, submission: ReplaySubmission) -> None:
    saved = {}
    if REPLAY_RESULTS_PATH.is_file():
        saved = json.loads(REPLAY_RESULTS_PATH.read_text(encoding="utf-8"))
    saved[label] = submission.to_json()
    write_json(REPLAY_RESULTS_PATH, saved)


def _first_replay_id(submission: ReplaySubmission) -> str:
    if not submission.results:
        raise RuntimeError(f"Replay produced no result rows: {submission.to_json()}")
    return submission.results[0].replay_exec_id


def _injected_decision() -> dict[str, Any]:
    return {
        "policy_label": "injected_support_decision",
        "risk_status": "safe_to_answer",
        "required_action": "answer_directly_with_safety_note",
        "summary": "Injected during replay to test a proposed final decision.",
    }


def _print_submission(label: str, submission: ReplaySubmission) -> None:
    print(f"   {label}: {submission.summary.to_json()}")
    for row in submission.results:
        print(
            f"   original={row.original_exec_id} "
            f"replay={row.replay_exec_id} status={row.status}"
        )
        if row.compare_url:
            print(f"   compare: {row.compare_url}")
    for failure in submission.failures:
        print(f"   failed {failure.original_exec_ref}: {failure.reason}")
    for skipped in submission.skipped:
        print(f"   skipped {skipped.original_exec_ref}: {skipped.reason}")
    if submission.compare_url:
        print(f"   batch compare: {submission.compare_url}")


def _replay_and_report(
    label: str,
    exec_id: str,
    *,
    flow_overrides: dict[str, Any] | None = None,
    checkpoint_overrides: dict[str, dict[str, Any]] | None = None,
    invocation_overrides: dict[str, dict[str, Any]] | None = None,
    skip: list[str] | None = None,
    tag: str | None = None,
) -> ReplaySubmission:
    client = KitaruClient()
    original = load_support_decision(client, exec_id)
    submission = client.executions.replay(
        exec_id,
        at=REPLAY_POINT,
        flow_overrides=flow_overrides,
        checkpoint_overrides=checkpoint_overrides,
        invocation_overrides=invocation_overrides,
        skip=skip,
        tag=tag,
        wait=False,
        on_error="fail",
    )
    submission = _wait_for_submission(client, submission)
    _print_submission(label, submission)
    replay_id = _first_replay_id(submission)
    try:
        replay_decision = load_support_decision(client, replay_id)
    except Exception as exc:
        replay_decision = None
        print(f"   replay decision unavailable: {exc}")
    if replay_decision is not None:
        print(f"   original: {decision_summary(original)}")
        print(f"   replay:   {decision_summary(replay_decision)}")
        print(f"   decision diff: {diff_decisions(original, replay_decision)}")
    report_path = REPORTS / f"{label}.json"
    write_json(
        report_path,
        {
            "label": label,
            "submission": submission.to_json(),
            "original_decision": original,
            "replay_decision": replay_decision,
            "decision_diff": (
                diff_decisions(original, replay_decision).to_json()
                if replay_decision is not None
                else None
            ),
        },
    )
    print(f"   report: {report_path}")
    _remember_replay(label, submission)
    return submission


def seed() -> str:
    """Seed one production-like support run and store its execution ID."""
    _section("Seed one production-like support run")
    handle = support_copilot_flow.run(
        prompt=DEFAULT_PROMPT,
        customer=DEFAULT_CUSTOMER,
        model=BASELINE_MODEL,
        prompt_profile=BASELINE_PROMPT_PROFILE,
    )
    _wait_for_flow_completion(handle)
    client = KitaruClient()
    decision = load_support_decision(client, handle.exec_id)
    _write_lines(PROD_EXEC_ID_PATH, [handle.exec_id])
    print(f"   exec_id={handle.exec_id}")
    print(f"   {decision_summary(decision)}")
    print(f"   wrote {PROD_EXEC_ID_PATH}")
    return handle.exec_id


def seed_batch(count: int = 4) -> list[str]:
    """Seed several original executions for batch replay."""
    scenarios = _load_scenarios()[:count]
    if len(scenarios) < count:
        raise RuntimeError(f"Only {len(scenarios)} scenarios are available.")
    _section(f"Seed {count} batch originals")
    exec_ids: list[str] = []
    for index, scenario in enumerate(scenarios, start=1):
        print(f"\n   [{index}/{count}] {scenario['label']}")
        handle = support_copilot_flow.run(
            prompt=scenario["prompt"],
            customer=scenario["customer"],
            model=BASELINE_MODEL,
            prompt_profile=BASELINE_PROMPT_PROFILE,
        )
        _wait_for_flow_completion(handle)
        exec_ids.append(handle.exec_id)
        print(f"   exec_id={handle.exec_id}")
    _write_lines(BATCH_EXEC_IDS_PATH, exec_ids)
    _write_lines(PROD_EXEC_ID_PATH, [exec_ids[0]])
    print(f"\n   wrote {BATCH_EXEC_IDS_PATH}")
    return exec_ids


def flow_override_replay(exec_id: str) -> ReplaySubmission:
    """Replay with flow-level model and prompt-profile overrides."""
    _section("Flow override replay")
    return _replay_and_report(
        "flow_override",
        exec_id,
        flow_overrides={
            "model": VARIANT_MODEL,
            "prompt_profile": VARIANT_PROMPT_PROFILE,
        },
    )


def invocation_output_inject(exec_id: str) -> ReplaySubmission:
    """Replay by injecting one checkpoint invocation output."""
    _section("Invocation output inject")
    return _replay_and_report(
        "invocation_output_inject",
        exec_id,
        invocation_overrides={
            FINAL_DECISION_CHECKPOINT: {"output": _injected_decision()}
        },
    )


def checkpoint_code_swap(exec_id: str) -> ReplaySubmission:
    """Replay with a checkpoint-scoped code override for the policy lookup."""
    _section("Checkpoint code swap")
    return _replay_and_report(
        "checkpoint_code_swap",
        exec_id,
        checkpoint_overrides={REPLAY_POINT: {"code": "mocks.lookup_policy"}},
    )


def invocation_model_override(exec_id: str) -> ReplaySubmission:
    """Replay with a targeted model override on the final model request."""
    _section("Invocation model override")
    return _replay_and_report(
        "invocation_model_override",
        exec_id,
        invocation_overrides={FINAL_MODEL_INVOCATION: {"model": VARIANT_MODEL}},
    )


def explicit_skip(exec_id: str) -> ReplaySubmission:
    """Replay while explicitly skipping the final publish checkpoint."""
    _section("Explicit skip")
    return _replay_and_report(
        "explicit_skip",
        exec_id,
        flow_overrides={"prompt_profile": VARIANT_PROMPT_PROFILE},
        skip=[FINAL_DECISION_CHECKPOINT],
    )


def tagged_batch_replay(exec_ids: list[str]) -> ReplaySubmission:
    """Replay many explicit IDs with one tag and collect per-parent errors."""
    _section("Tagged batch replay")
    client = KitaruClient()
    submission = client.executions.replay(
        exec_ids,
        at=REPLAY_POINT,
        flow_overrides={
            "model": VARIANT_MODEL,
            "prompt_profile": VARIANT_PROMPT_PROFILE,
        },
        tag=REPLAY_TAG,
        wait=False,
        on_error="collect",
    )
    submission = _wait_for_submission(client, submission)
    _print_submission("tagged_batch", submission)
    write_json(REPORTS / "tagged_batch.json", submission.to_json())
    _remember_replay("tagged_batch", submission)
    return submission


def diff_report(original_id: str, replay_ids: list[str]) -> str:
    """Write an execution diff report for one original and selected replays."""
    _section("Diff report")
    if not replay_ids and REPLAY_RESULTS_PATH.is_file():
        saved = json.loads(REPLAY_RESULTS_PATH.read_text(encoding="utf-8"))
        replay_ids = [
            row["replay_exec_id"]
            for payload in saved.values()
            for row in payload.get("results", [])
            if row.get("original_exec_id") == original_id
        ]
    if not replay_ids:
        raise SystemExit("Pass replay IDs or run one replay command first.")
    result = diff(original_id, *replay_ids)
    path = REPORTS / "diff_report.json"
    write_json(
        path,
        {
            "original_exec_id": original_id,
            "replay_exec_ids": replay_ids,
            "diff": serialize_execution_diff(result),
        },
    )
    print(f"   wrote {path}")
    for url in result.urls:
        print(f"   compare: {url}")
    return str(path)


def diff_matrix_report(exec_ids: list[str]) -> str:
    """Write a diff-matrix report for batch originals and their tagged replays."""
    _section("Diff-matrix report")
    result = diff_matrix(exec_ids)
    path = REPORTS / "diff_matrix.json"
    write_json(path, serialize_diff_matrix(result))
    print(f"   flow: {FLOW_NAME}")
    print(f"   wrote {path}")
    return str(path)


def run_all() -> None:
    """Run the full narrated demo with gpt-5-mini and gpt-5-nano."""
    exec_id = seed()
    flow_override_replay(exec_id)
    invocation_output_inject(exec_id)
    checkpoint_code_swap(exec_id)
    invocation_model_override(exec_id)
    explicit_skip(exec_id)
    batch_ids = seed_batch(count=int(os.environ.get("REPLAY_DEMO_BATCH_COUNT", "3")))
    tagged_batch_replay(batch_ids)
    diff_report(exec_id, [])
    diff_matrix_report(batch_ids)


def _parse_flag(argv: list[str], flag: str) -> tuple[list[str], str | None]:
    if flag not in argv:
        return argv, None
    index = argv.index(flag)
    if index + 1 >= len(argv):
        raise SystemExit(f"{flag} requires a value")
    return argv[:index] + argv[index + 2 :], argv[index + 1]


def main(argv: list[str]) -> None:
    load_dotenv(ROOT / ".env")
    command = argv[0] if argv else "run-all"
    rest = argv[1:]

    if command == "seed":
        seed()
    elif command == "seed-batch":
        rest, count_raw = _parse_flag(rest, "--count")
        seed_batch(count=int(count_raw or "4"))
    elif command == "flow-override":
        flow_override_replay(_default_exec_id(rest[0] if rest else None))
    elif command == "inject-output":
        invocation_output_inject(_default_exec_id(rest[0] if rest else None))
    elif command == "code-swap":
        checkpoint_code_swap(_default_exec_id(rest[0] if rest else None))
    elif command == "model-override":
        invocation_model_override(_default_exec_id(rest[0] if rest else None))
    elif command == "explicit-skip":
        explicit_skip(_default_exec_id(rest[0] if rest else None))
    elif command == "tagged-batch":
        tagged_batch_replay(_default_batch_ids(rest or None))
    elif command == "diff-report":
        original = _default_exec_id(rest[0] if rest else None)
        diff_report(original, rest[1:])
    elif command == "diff-matrix-report":
        diff_matrix_report(_default_batch_ids(rest or None))
    elif command == "run-all":
        run_all()
    else:
        raise SystemExit(
            "usage: python demo.py "
            "seed | seed-batch [--count N] | flow-override [EXEC_ID] | "
            "inject-output [EXEC_ID] | code-swap [EXEC_ID] | "
            "model-override [EXEC_ID] | explicit-skip [EXEC_ID] | "
            "tagged-batch [EXEC_ID ...] | diff-report [ORIGINAL_ID REPLAY_ID ...] | "
            "diff-matrix-report [EXEC_ID ...] | run-all"
        )


if __name__ == "__main__":
    main(sys.argv[1:])
