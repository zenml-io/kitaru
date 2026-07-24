"""LangGraph runner that plugs the Synera agent into the Replay Verify engine.

The engine (``verify_imported_cases``) calls a baseline and a candidate runner
for each imported case and compares their structured outputs. These runners
satisfy the same ``ImportedRunnerCallable`` contract as the shipped PydanticAI
support-copilot runner -- the only difference is that the agent under test is a
real LangGraph graph, which is Synera's stack.

Trust rule (identical to the support-copilot demo): the runner constructs its
answer from imported *input*, imported *tool availability*, and imported
*retrieval metadata* only. It never reads ``observed_output`` or any manifest
``expected_output`` to make a candidate look good.
"""

from __future__ import annotations

from examples.replay_verify_synera_langgraph.synera_agent import run_synera_graph
from kitaru._replay_verify_imported_models import ImportedReplayCase
from kitaru._replay_verify_imported_runner import (
    ImportedRunnerInvocation,
    ImportedRunnerOutput,
)

RUNNER_ENTRYPOINT = (
    "examples.replay_verify_synera_langgraph.synera_runner.run_synera_case"
)


def run_baseline_synera_case(
    case: ImportedReplayCase,
    invocation: ImportedRunnerInvocation,
) -> ImportedRunnerOutput:
    """Baseline lane: the correct configuration (runs FEA validation)."""
    return run_synera_case(case, invocation)


def run_candidate_synera_case(
    case: ImportedReplayCase,
    invocation: ImportedRunnerInvocation,
) -> ImportedRunnerOutput:
    """Candidate lane: behaviour is driven entirely by ``invocation.config``.

    With ``skip_fea_validation=True`` (the planted "cheaper config"), the agent
    drops the FEA step on simulation requests -- the regression the engine
    should catch as tool-selection + risk-status drift.
    """
    return run_synera_case(case, invocation)


def run_synera_case(
    case: ImportedReplayCase,
    invocation: ImportedRunnerInvocation,
) -> ImportedRunnerOutput:
    """Run the LangGraph agent from imported inputs and context only."""
    retrieval = case.retrieval_context
    retrieval_ids = list(retrieval.returned_document_ids) if retrieval else []

    final_state = run_synera_graph(
        root_input=case.root_input,
        config=invocation.config,
        available_tools=invocation.available_tools,
        retrieval_document_ids=retrieval_ids,
    )

    payload = {
        # Field names match DEFAULT_COMPARISON_FIELDS so the engine compares
        # discipline routing, risk, tool trajectory, and retrieval grounding.
        "policy_label": final_state["discipline_label"],
        "risk_status": final_state["risk_status"],
        "tool_names": final_state["tool_names"],
        "retrieval_document_ids": final_state.get("retrieval_document_ids", []),
        "response": final_state["response"],
        "tool_results": final_state.get("tool_results", []),
        "metadata": {
            "case_id": case.case_id,
            "runner_role": invocation.role,
            "runner_id": invocation.runner_id,
            "agent_id": invocation.config.get("agent_id"),
            "graph": "synera-mech-eng-assistant",
            "execution_mode": invocation.execution_mode,
        },
    }
    return ImportedRunnerOutput(payload=payload)
