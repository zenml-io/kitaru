"""Durable Kitaru-flow wrapper for the imported-input verifier demo.

Runs the same engine as ``run_langfuse_pydanticai_demo.py`` but as a Kitaru
flow: every baseline/candidate lane is a checkpoint (so baseline lanes are
cached across candidate iterations), and the cohort plus all reports are
persisted as Kitaru artifacts on the execution.
"""

# NOTE: no `from __future__ import annotations` here. This module defines
# Kitaru @flow/@checkpoint functions and ZenML rejects string annotations.

import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from kitaru._replay_verify_imported_models import (
    imported_case_from_mapping,
)
from kitaru._replay_verify_imported_runner import (
    ImportedRunnerCallable,
    ImportedRunnerInvocation,
    ImportedRunnerOutput,
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
        return {"error": f"{type(exc).__name__}: {exc}"}
