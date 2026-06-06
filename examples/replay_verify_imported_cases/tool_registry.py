"""Tool registry for the imported-input support-copilot demo.

The demo tools are deterministic and side-effect safe. Read-only tools return
small fixture payloads. The write-like tool returns a mocked result and records
that no live side effect happened.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from kitaru._replay_verify_imported_models import ImportedReplayCase
from kitaru._replay_verify_imported_runner import ImportedRunnerInvocation

SAFE_TOOL_NAMES = {
    "lookup_subscription",
    "lookup_invoice",
    "search_knowledge_base",
    "create_support_ticket",
}


def run_imported_tool(
    tool_name: str,
    case: ImportedReplayCase,
    invocation: ImportedRunnerInvocation,
) -> dict[str, Any]:
    """Run one deterministic demo tool from imported case context."""
    if tool_name not in invocation.available_tools:
        msg = f"Tool {tool_name!r} was not imported as available for {case.case_id}."
        raise ValueError(msg)
    if tool_name == "lookup_subscription":
        return _lookup_subscription(case)
    if tool_name == "lookup_invoice":
        return _lookup_invoice(case)
    if tool_name == "search_knowledge_base":
        return _search_knowledge_base(case)
    if tool_name == "create_support_ticket":
        return _create_support_ticket(case)
    msg = f"Demo tool {tool_name!r} is not registered."
    raise ValueError(msg)


def _lookup_subscription(case: ImportedReplayCase) -> dict[str, Any]:
    root = _root_mapping(case)
    return {
        "tool_name": "lookup_subscription",
        "account_id": root.get("account_id", "acct-demo"),
        "plan": root.get("plan", "team"),
        "status": "active",
        "side_effect_status": "safe",
        "executed_live": False,
    }


def _lookup_invoice(case: ImportedReplayCase) -> dict[str, Any]:
    root = _root_mapping(case)
    return {
        "tool_name": "lookup_invoice",
        "invoice_id": root.get("invoice_id", "INV-DEMO-1"),
        "status": "paid",
        "side_effect_status": "safe",
        "executed_live": False,
    }


def _search_knowledge_base(case: ImportedReplayCase) -> dict[str, Any]:
    retrieval = case.retrieval_context
    return {
        "tool_name": "search_knowledge_base",
        "query": retrieval.query if retrieval else None,
        "document_ids": retrieval.returned_document_ids if retrieval else [],
        "chunk_ids": retrieval.returned_chunk_ids if retrieval else [],
        "corpus_index_version": retrieval.corpus_index_version if retrieval else None,
        "side_effect_status": "safe",
        "executed_live": False,
    }


def _create_support_ticket(case: ImportedReplayCase) -> dict[str, Any]:
    root = _root_mapping(case)
    return {
        "tool_name": "create_support_ticket",
        "ticket_id": f"mock-{case.case_id}",
        "subject": root.get("ticket_subject", "Imported demo support ticket"),
        "side_effect_status": "mocked",
        "executed_live": False,
    }


def _root_mapping(case: ImportedReplayCase) -> Mapping[str, Any]:
    return case.root_input if isinstance(case.root_input, Mapping) else {}
