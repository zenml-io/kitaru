"""Durable Kitaru-flow wrapper for the imported-input verifier demo.

Runs the same engine as ``run_langfuse_pydanticai_demo.py`` but as a Kitaru
flow: every baseline/candidate lane is a checkpoint (so baseline lanes are
cached across candidate iterations), and the cohort plus all reports are
persisted as Kitaru artifacts on the execution.
"""

# NOTE: no `from __future__ import annotations` here. This module defines
# Kitaru @flow/@checkpoint functions and ZenML rejects string annotations.

import json
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

import kitaru
from examples.replay_verify_imported_cases.run_langfuse_pydanticai_demo import (
    select_runner,
)
from examples.replay_verify_imported_cases.tool_registry import SAFE_TOOL_NAMES
from kitaru import checkpoint, flow
from kitaru._replay_verify_imported_html import render_html_report
from kitaru._replay_verify_imported_models import (
    ImportedReplayCase,
    imported_case_from_mapping,
    to_plain_data,
)
from kitaru._replay_verify_imported_runner import (
    ImportedRunnerCallable,
    ImportedRunnerInvocation,
    ImportedRunnerOutput,
    verify_imported_cases,
)


def lane_id(role: str, case_id: str) -> str:
    """Stable checkpoint invocation id for one case lane."""
    safe = "".join(ch if ch.isalnum() else "_" for ch in case_id)
    return f"{role}_{safe}"


def lane_runner(runner_mode: str, role: str) -> ImportedRunnerCallable:
    """Resolve the runner callable for one lane.

    Resolved inside the lane (from serializable strings) because checkpoint
    inputs must be data, not callables.
    """
    if runner_mode == "deterministic":
        from examples.replay_verify_imported_cases.support_copilot_demo import (
            run_baseline_support_copilot_case,
            run_candidate_support_copilot_case,
        )

        if role == "baseline":
            return run_baseline_support_copilot_case
        return run_candidate_support_copilot_case
    if runner_mode == "live":
        # Imported lazily so the deterministic path works without pydantic_ai.
        from examples.replay_verify_imported_cases.support_copilot_live import (
            run_baseline_support_copilot_case_live,
            run_candidate_support_copilot_case_live,
        )

        if role == "baseline":
            return run_baseline_support_copilot_case_live
        return run_candidate_support_copilot_case_live
    msg = f"Unknown runner_mode {runner_mode!r}; use 'deterministic' or 'live'."
    raise ValueError(msg)


def ensure_unique_case_ids(case_ids: list[str]) -> None:
    """Reject cohorts with duplicate case ids before any lane runs.

    Duplicate ids would silently mis-map case payloads in the row lookup and
    collide on checkpoint invocation ids, so a custom cohort must fail loudly.
    """
    duplicates = sorted({cid for cid in case_ids if case_ids.count(cid) > 1})
    if duplicates:
        msg = f"Duplicate case_id values in cohort: {', '.join(duplicates)}"
        raise ValueError(msg)


def execute_lane_payload(
    *,
    role: str,
    runner_mode: str,
    case_payload: dict[str, Any],
    invocation_payload: dict[str, Any],
) -> dict[str, Any]:
    """Run one lane and return an output/error envelope.

    Never raises: a raising checkpoint would mark the ZenML step failed even
    though the engine handles lane failures. The flow-side executor re-raises
    from the error envelope so the engine's fail-closed path records it.
    """
    try:
        runner = lane_runner(runner_mode, role)
        case = imported_case_from_mapping(case_payload)
        invocation = ImportedRunnerInvocation(
            case_id=str(invocation_payload["case_id"]),
            role=invocation_payload["role"],
            runner_id=str(invocation_payload["runner_id"]),
            root_input=invocation_payload["root_input"],
            available_tools=tuple(invocation_payload["available_tools"]),
            config=invocation_payload["config"],
            comparison_fields=tuple(invocation_payload["comparison_fields"]),
            execution_mode=str(invocation_payload["execution_mode"]),
        )
        raw = runner(case, invocation)
        if isinstance(raw, ImportedRunnerOutput):
            output = {
                "payload": dict(raw.payload),
                "metadata": dict(raw.metadata),
                "unsafe_live_execution_count": raw.unsafe_live_execution_count,
            }
        else:
            output = {
                "payload": dict(raw),
                "metadata": {},
                "unsafe_live_execution_count": 0,
            }
        return {"output": output}
    except Exception as exc:
        # Deliberately no type-name prefix: the flow-side executor re-raises
        # this message as a RuntimeError and the engine's _call_runner then
        # prepends the exception type itself. Prefixing here would double up
        # ("RuntimeError: ValueError: ...") in the persisted report.
        return {"error": str(exc)}


# cache=True is what makes baseline lanes reusable across candidate
# iterations, but it also caches *error* envelopes: a transient live-mode
# failure (e.g. an LLM API timeout) replays as the same cached error on every
# rerun with identical inputs. Recovery: change an input (e.g. bump a config
# value) or clear the local ZenML cache; there is no per-call cache override
# on `.submit()` today.
@checkpoint(type="verify_lane", cache=True)
def run_case_lane(
    role: str,
    runner_mode: str,
    case_payload: dict[str, Any],
    invocation_payload: dict[str, Any],
) -> dict[str, Any]:
    """One durable baseline/candidate lane. Inputs are role-specific only, so
    baseline lanes cache across candidate iterations."""
    return execute_lane_payload(
        role=role,
        runner_mode=runner_mode,
        case_payload=case_payload,
        invocation_payload=invocation_payload,
    )


@checkpoint(cache=False)
def persist_verification_report(
    report_payload: dict[str, Any],
    html_text: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Persist the cohort and reports as Kitaru artifacts; return the summary."""
    summary = dict(report_payload["summary"])
    kitaru.save("imported_cases", rows, type="input")
    kitaru.save("fidelity_report", report_payload["cases"], type="context")
    kitaru.save("verification_report", report_payload, type="output")
    kitaru.save("verification_report_html", html_text, type="output")
    kitaru.log(
        overall_verdict=summary.get("overall_verdict"),
        eligible_count=summary.get("eligible_count"),
        stopped_count=summary.get("stopped_count"),
        candidate_vs_baseline_drift_count=summary.get(
            "candidate_vs_baseline_drift_count"
        ),
        unsafe_live_execution_count=summary.get("unsafe_live_execution_count"),
    )
    return summary


@flow
def replay_verify_cohort(
    case_file: str,
    runner_mode: str = "deterministic",
    baseline: str = "support-copilot-v1",
    candidate: str = "support-copilot-v2",
    baseline_model: str | None = None,
    candidate_model: str | None = None,
) -> dict[str, Any]:
    """Verify an imported cohort as a durable Kitaru execution."""
    selection = select_runner(
        runner_mode,
        baseline=baseline,
        candidate=candidate,
        baseline_model=baseline_model,
        candidate_model=candidate_model,
    )
    rows = [
        json.loads(line)
        for line in Path(case_file).read_text().splitlines()
        if line.strip()
    ]
    cases = [imported_case_from_mapping(row) for row in rows]
    ensure_unique_case_ids([case.case_id for case in cases])
    row_by_id = {case.case_id: row for case, row in zip(cases, rows, strict=True)}

    # Kitaru has no native run tags yet; metadata is the labeling mechanism.
    kitaru.log(replay_verify_role="verify", runner_mode=runner_mode)

    lane_futures: list[Any] = []

    def _lane_executor(
        _runner: ImportedRunnerCallable,
        case: ImportedReplayCase,
        invocation: ImportedRunnerInvocation,
    ) -> ImportedRunnerOutput:
        # `_runner` is intentionally ignored: callables are not serializable
        # checkpoint inputs, so the checkpoint re-resolves the same runner
        # from (runner_mode, role) strings via `lane_runner`.
        # `.submit()` (rather than a plain call) returns a step future, which
        # is the only handle the `after=` edge on the persist checkpoint
        # accepts. The immediate `.load()` blocks, so the engine still runs
        # lanes sequentially (baseline gates candidate).
        lane_future = run_case_lane.submit(
            invocation.role,
            runner_mode,
            row_by_id[case.case_id],
            to_plain_data(invocation),
            id=lane_id(invocation.role, case.case_id),
        )
        lane_futures.append(lane_future)
        envelope = lane_future.load()
        if "error" in envelope:
            raise RuntimeError(envelope["error"])
        output = envelope["output"]
        return ImportedRunnerOutput(
            payload=output["payload"],
            metadata=output["metadata"],
            unsafe_live_execution_count=output["unsafe_live_execution_count"],
        )

    report = verify_imported_cases(
        cases,
        baseline_runner=selection.baseline_runner,
        candidate_runner=selection.candidate_runner,
        baseline_config=selection.baseline_config,
        candidate_config=selection.candidate_config,
        report_name=selection.report_name,
        expected_runner_entrypoint=selection.expected_runner_entrypoint,
        allowed_tool_names=SAFE_TOOL_NAMES,
        lane_executor=_lane_executor,
    )
    # The lanes' outputs reach the persist checkpoint as plain data, so ZenML
    # sees no graph edge between them. Explicit `after=` edges make the persist
    # checkpoint the flow's single terminal step, which is what `.wait()`
    # extracts the flow result from.
    return persist_verification_report(
        report.to_dict(),
        render_html_report(report),
        rows,
        after=lane_futures or None,
    ).load()


def run_durable_demo(
    *,
    case_file: str | None = None,
    runner_mode: str = "deterministic",
    baseline: str = "support-copilot-v1",
    candidate: str = "support-copilot-v2",
    baseline_model: str | None = None,
    candidate_model: str | None = None,
) -> dict[str, Any]:
    """Run the durable verifier flow and return exec_id plus the summary."""
    selection = select_runner(
        runner_mode,
        baseline=baseline,
        candidate=candidate,
        baseline_model=baseline_model,
        candidate_model=candidate_model,
    )
    resolved_case_file = case_file or str(selection.case_file)
    handle = replay_verify_cohort.run(
        resolved_case_file,
        runner_mode=runner_mode,
        baseline=baseline,
        candidate=candidate,
        baseline_model=baseline_model,
        candidate_model=candidate_model,
    )
    summary = handle.wait()
    return {"exec_id": handle.exec_id, "summary": summary}
