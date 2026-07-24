"""Generate the Synera mechanical-engineering imported-case cohort (JSONL).

These are FABRICATED, representative LangFuse-shaped traces -- no customer data,
no IP. They stand in for what Synera would export from their own LangFuse: a
mechanical-engineering design assistant answering requirements / geometry /
simulation / standards questions.

The cohort is calibrated so that:

- simulation cases are *eligible* and reveal the planted regression (the cheaper
  candidate config skips ``run_fea_simulation``) as tool-selection + risk drift,
- geometry / requirements / standards cases are *eligible* and agree (ship),
- two cases are deliberately *stopped* by the fidelity gate (missing observed
  output; stale standards-corpus version) so the demo shows the engine refusing
  to grade what it cannot faithfully replay.

Run this to (re)write ``fixtures/synera_imported_cases.jsonl``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from examples.replay_verify_synera_langgraph.synera_agent import (
    TOOL_FEA,
    TOOL_GEOMETRY,
    TOOL_PARSE,
    TOOL_STANDARDS,
)

# The standards corpus version this cohort is current against. The eligible RAG
# case matches it; the stopped RAG case is one index behind.
SYNERA_CORPUS_INDEX_VERSION = "synera-standards-2026-06-a"
STALE_CORPUS_INDEX_VERSION = "synera-standards-2026-05-a"

RUNNER_ENTRYPOINT = (
    "examples.replay_verify_synera_langgraph.synera_runner.run_synera_case"
)
BASELINE_ID = "synera-assistant-v3"
CANDIDATE_ID = "synera-assistant-v3-fast"
COHORT = "synera-design-partner-demo"

FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "synera_imported_cases.jsonl"

_TOOL_KIND = {
    TOOL_PARSE: "tool",
    TOOL_GEOMETRY: "tool",
    TOOL_FEA: "tool",
    TOOL_STANDARDS: "retrieval",
}


def _trace_contract(available_tools: list[str]) -> dict[str, Any]:
    return {
        "trace_contract_version": "trace-contract-v1",
        "app_name": "synera-design-assistant",
        "app_version": "2026-06-10",
        "model": "langgraph:deterministic-router",
        "prompt_version": "synera-assistant-v3",
        "prompt_hash": "synera-v3-demo-hash",
        "available_tools": available_tools,
        "application_tool_names": available_tools,
        "side_effect_policy": "safe",
        "tool_registry_version": "synera-tools-2026-06-10",
    }


def _runner_contract() -> dict[str, Any]:
    return {
        "entrypoint": RUNNER_ENTRYPOINT,
        "baseline_id": BASELINE_ID,
        "candidate_id": CANDIDATE_ID,
        "comparison_fields": [
            "policy_label",
            "risk_status",
            "tool_names",
            "retrieval_document_ids",
        ],
    }


def _recorded_calls(observed_tools: list[str]) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = [
        {
            "kind": "llm",
            "name": "synera-router",
            "input_payload": {"message": "engineering request"},
            "output_payload": {"intent": "routed"},
            "observation_id": "obs-router-llm",
            "model": "langgraph:deterministic-router",
        }
    ]
    for tool in observed_tools:
        calls.append(
            {
                "kind": _TOOL_KIND.get(tool, "tool"),
                "name": tool,
                "input_payload": {"tool": tool},
                "output_payload": {"side_effect_status": "safe"},
                "observation_id": f"obs-{tool}",
            }
        )
    return calls


def _case(
    *,
    case_id: str,
    intent: str,
    user_message: str,
    available_tools: list[str],
    observed_tools: list[str],
    discipline_label: str,
    risk_status: str,
    expected_demo_state: str,
    case_type: str,
    part_id: str | None = None,
    requirements: list[str] | None = None,
    retrieval_context: dict[str, Any] | None = None,
    observed_document_ids: list[str] | None = None,
    observed_output_override: Any = "__USE_DEFAULT__",
) -> dict[str, Any]:
    root_input: dict[str, Any] = {"intent": intent, "user_message": user_message}
    if part_id is not None:
        root_input["part_id"] = part_id
    if requirements is not None:
        root_input["requirements"] = requirements

    if observed_output_override != "__USE_DEFAULT__":
        observed_output = observed_output_override
    else:
        observed_output = {
            "policy_label": discipline_label,
            "risk_status": risk_status,
            "tool_names": observed_tools,
            "retrieval_document_ids": observed_document_ids or [],
        }

    case: dict[str, Any] = {
        "case_id": case_id,
        "source_ref": {
            "source_system": "fixture-jsonl",
            "source_id": f"trace-{case_id}",
            "observation_ids": [f"obs-{case_id}-root"],
            "raw_source_ref": "fabricated representative LangFuse export (no customer data)",
        },
        "root_input": root_input,
        "observed_output": observed_output,
        "recorded_calls": _recorded_calls(observed_tools),
        "trace_contract": _trace_contract(available_tools),
        "runner_contract": _runner_contract(),
        "tenant_context": {
            "tenant_id": "tenant-demo",
            "workspace_id": "synera-demo",
            "user_id": "engineer-100",
            "role": "engineer",
            "permission_scope": "tenant:tenant-demo:engineer",
        },
        "cohort": COHORT,
        "labels": {"case_type": case_type, "expected_demo_state": expected_demo_state},
    }
    if retrieval_context is not None:
        case["retrieval_context"] = retrieval_context
    return case


def build_cohort() -> list[dict[str, Any]]:
    """Build the deterministic Synera demo cohort."""
    sim_tools = [TOOL_PARSE, TOOL_GEOMETRY, TOOL_FEA]
    cases: list[dict[str, Any]] = [
        # --- Eligible simulation cases: candidate skips FEA -> drift -> HOLD ---
        _case(
            case_id="synera-sim-bracket-eligible",
            intent="simulation_request",
            user_message="Validate the mounting bracket for a 2.4 kN load",
            available_tools=sim_tools,
            observed_tools=sim_tools,
            discipline_label="structural_simulation",
            risk_status="safe",
            expected_demo_state="eligible",
            case_type="simulation_regression_target",
            part_id="bracket-A",
            requirements=["2.4kN load", "AlSi10Mg", "safety factor >= 1.5"],
        ),
        _case(
            case_id="synera-sim-housing-eligible",
            intent="simulation_request",
            user_message="Check the gearbox housing under thermal + pressure load",
            available_tools=sim_tools,
            observed_tools=sim_tools,
            discipline_label="structural_simulation",
            risk_status="safe",
            expected_demo_state="eligible",
            case_type="simulation_regression_target",
            part_id="housing-G",
            requirements=["120C", "6 bar", "cast iron"],
        ),
        # --- Eligible non-simulation cases: baseline == candidate -> SHIP ---
        _case(
            case_id="synera-geometry-eligible",
            intent="geometry_request",
            user_message="Generate a parametric flange for a 80mm bore",
            available_tools=[TOOL_PARSE, TOOL_GEOMETRY],
            observed_tools=[TOOL_PARSE, TOOL_GEOMETRY],
            discipline_label="cad_design",
            risk_status="safe",
            expected_demo_state="eligible",
            case_type="geometry_stable",
            part_id="flange-80",
            requirements=["80mm bore", "PN16"],
        ),
        _case(
            case_id="synera-requirements-eligible",
            intent="requirements_only",
            user_message="Extract the structural requirements from this RFQ",
            available_tools=[TOOL_PARSE],
            observed_tools=[TOOL_PARSE],
            discipline_label="requirements_engineering",
            risk_status="safe",
            expected_demo_state="eligible",
            case_type="requirements_stable",
            requirements=["fatigue life 10^6", "mass < 1.2kg"],
        ),
        _case(
            case_id="synera-standards-rag-eligible",
            intent="standards_question",
            user_message="Which weld inspection class applies to a fatigue-critical seam?",
            available_tools=[TOOL_STANDARDS],
            observed_tools=[TOOL_STANDARDS],
            discipline_label="standards_compliance",
            risk_status="safe",
            expected_demo_state="eligible",
            case_type="standards_rag_stable",
            observed_document_ids=["iso-5817-cls-b", "en-1090-2-weld"],
            retrieval_context={
                "query": "weld inspection class fatigue critical seam",
                "retriever_name": "synera-standards-retriever",
                "corpus_index_version": SYNERA_CORPUS_INDEX_VERSION,
                "top_k": 4,
                "returned_document_ids": ["iso-5817-cls-b", "en-1090-2-weld"],
                "returned_chunk_ids": ["iso-5817-cls-b#3", "en-1090-2-weld#7"],
                "chunk_hashes": ["sha256:weld-a", "sha256:weld-b"],
                "permission_scope": "tenant:tenant-demo:engineer",
            },
        ),
        # --- Stopped: fidelity gate refuses to grade what it can't replay ---
        _case(
            case_id="synera-missing-output-stopped",
            intent="simulation_request",
            user_message="Validate the suspension upright (trace had no recorded output)",
            available_tools=sim_tools,
            observed_tools=sim_tools,
            discipline_label="structural_simulation",
            risk_status="safe",
            expected_demo_state="stopped",
            case_type="broken_missing_output",
            part_id="upright-S",
            observed_output_override=None,
        ),
        _case(
            case_id="synera-standards-rag-stopped",
            intent="standards_question",
            user_message="What is the bolt preload spec? (retrieved from a stale corpus)",
            available_tools=[TOOL_STANDARDS],
            observed_tools=[TOOL_STANDARDS],
            discipline_label="standards_compliance",
            risk_status="safe",
            expected_demo_state="stopped",
            case_type="broken_incomplete_rag",
            observed_document_ids=["vdi-2230-preload"],
            retrieval_context={
                "query": "bolt preload spec",
                "retriever_name": "synera-standards-retriever",
                # Fully instrumented EXCEPT the corpus is one index behind, so
                # this case is Skipped purely for the stale-corpus reason.
                "corpus_index_version": STALE_CORPUS_INDEX_VERSION,
                "top_k": 4,
                "returned_document_ids": ["vdi-2230-preload"],
                "returned_chunk_ids": ["vdi-2230-preload#1"],
                "chunk_hashes": ["sha256:preload-a"],
                "permission_scope": "tenant:tenant-demo:engineer",
            },
        ),
    ]
    return cases


def write_cohort(path: Path = FIXTURE_PATH) -> Path:
    """Write the cohort as JSONL and return the path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(case, sort_keys=True) for case in build_cohort()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


if __name__ == "__main__":
    written = write_cohort()
    print(f"Wrote {len(build_cohort())} Synera imported cases to {written}")
