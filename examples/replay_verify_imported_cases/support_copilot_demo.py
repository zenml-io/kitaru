"""Support-copilot runner for imported-input Replay Verify cases.

This module is intentionally deterministic so the demo can run without provider
credentials. It still follows the important trust rule: the runner constructs
answers from imported input, imported tool availability, and imported retrieval
metadata. It does not read ``observed_output`` or manifest ``expected_output`` to
make the candidate look good.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from examples.replay_verify_imported_cases.tool_registry import run_imported_tool
from kitaru._replay_verify_imported_models import ImportedReplayCase
from kitaru._replay_verify_imported_runner import (
    ImportedRunnerInvocation,
    ImportedRunnerOutput,
)

RUNNER_ENTRYPOINT = (
    "examples.replay_verify_imported_cases.support_copilot_demo."
    "run_support_copilot_case"
)


def run_baseline_support_copilot_case(
    case: ImportedReplayCase,
    invocation: ImportedRunnerInvocation,
) -> ImportedRunnerOutput:
    """Run the baseline support-copilot implementation for one imported case."""
    return run_support_copilot_case(case, invocation)


def run_candidate_support_copilot_case(
    case: ImportedReplayCase,
    invocation: ImportedRunnerInvocation,
) -> ImportedRunnerOutput:
    """Run the candidate support-copilot implementation for one imported case."""
    return run_support_copilot_case(case, invocation)


def run_support_copilot_case(
    case: ImportedReplayCase,
    invocation: ImportedRunnerInvocation,
) -> ImportedRunnerOutput:
    """Execute the demo agent from imported inputs and context only."""
    root = _root_mapping(case)
    intent = str(root.get("intent") or "model_only")
    tool_results = _run_tools_for_intent(intent, case, invocation)
    tool_names = [str(result["tool_name"]) for result in tool_results]
    retrieval_document_ids = _retrieval_document_ids(case, intent)
    payload = {
        "policy_label": _policy_label(intent),
        "risk_status": _risk_status(intent),
        "tool_names": tool_names,
        "retrieval_document_ids": retrieval_document_ids,
        "response": _response_text(
            intent=intent,
            root=root,
            tool_names=tool_names,
            retrieval_document_ids=retrieval_document_ids,
            invocation=invocation,
        ),
        "tool_results": tool_results,
        "metadata": {
            "case_id": case.case_id,
            "runner_role": invocation.role,
            "runner_id": invocation.runner_id,
            "agent_id": invocation.config.get("agent_id"),
            "prompt_version": invocation.config.get("prompt_version"),
            "execution_mode": invocation.execution_mode,
        },
    }
    return ImportedRunnerOutput(payload=payload)


def _run_tools_for_intent(
    intent: str,
    case: ImportedReplayCase,
    invocation: ImportedRunnerInvocation,
) -> list[dict[str, Any]]:
    tool_plan = {
        "subscription_lookup": ["lookup_subscription"],
        "billing_lookup": ["lookup_invoice"],
        "mocked_ticket": ["create_support_ticket"],
        "rag_policy_answer": ["search_knowledge_base"],
    }.get(intent, [])
    return [run_imported_tool(tool_name, case, invocation) for tool_name in tool_plan]


def _retrieval_document_ids(case: ImportedReplayCase, intent: str) -> list[str]:
    if intent != "rag_policy_answer" or case.retrieval_context is None:
        return []
    return list(case.retrieval_context.returned_document_ids)


def _policy_label(intent: str) -> str:
    labels = {
        "model_only": "support_policy",
        "subscription_lookup": "billing_policy",
        "billing_lookup": "billing_policy",
        "mocked_ticket": "escalation_policy",
        "rag_policy_answer": "knowledge_base_policy",
    }
    return labels.get(intent, "support_policy")


def _risk_status(intent: str) -> str:
    if intent == "mocked_ticket":
        return "needs_review"
    return "safe"


def _response_text(
    *,
    intent: str,
    root: Mapping[str, Any],
    tool_names: list[str],
    retrieval_document_ids: list[str],
    invocation: ImportedRunnerInvocation,
) -> str:
    agent_id = invocation.config.get("agent_id", invocation.runner_id)
    if intent == "subscription_lookup":
        tools = ", ".join(tool_names)
        return f"{agent_id}: subscription is active; tools used: {tools}."
    if intent == "mocked_ticket":
        return f"{agent_id}: created a mocked support ticket for review."
    if intent == "rag_policy_answer":
        docs = ", ".join(retrieval_document_ids) or "no imported documents"
        return f"{agent_id}: answered from imported documents {docs}."
    return f"{agent_id}: {root.get('user_message', 'Support request received')}"


def _root_mapping(case: ImportedReplayCase) -> Mapping[str, Any]:
    return case.root_input if isinstance(case.root_input, Mapping) else {}
