"""Generate the live-cohort JSONL fixture for the support-copilot demo.

The cohort mirrors the deterministic fixture's shape but points its
``runner_contract.entrypoint`` at the live PydanticAI runner. It contains:

- eight eligible cases (model-only, read-only tools, mocked writes, RAG),
- three permission-themed cases whose correct behavior is escalation,
- four broken/stopped cases adapted from the deterministic fixture.

Two observed-output modes:

- ``--observed deterministic`` (default): ``observed_output`` is synthesized
  from the documented correct vocabulary. No API calls; used by tests.
- ``--observed live``: runs the baseline live runner per eligible case and
  records its payload as ``observed_output``. Requires provider credentials;
  used for calibration.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from examples.replay_verify_imported_cases.live_prompt_config import (
    BASELINE_LIVE_CONFIG,
    DEFAULT_BASELINE_MODEL,
    LIVE_RUNNER_ENTRYPOINT,
)
from kitaru._replay_verify_imported_models import (
    DEFAULT_COMPARISON_FIELDS,
    imported_case_from_mapping,
)
from kitaru._replay_verify_imported_runner import ImportedRunnerInvocation

DEMO_DIR = Path(__file__).resolve().parent
LIVE_FIXTURE_PATH = DEMO_DIR / "fixtures" / "support_copilot_live_cases.jsonl"

_RAW_SOURCE_REF = (
    "examples/replay_verify_imported_cases/fixtures/support_copilot_live_cases.jsonl"
)
_COHORT = "live-support-copilot-demo"
_CURRENT_CORPUS_INDEX_VERSION = "support-kb-2026-06-06-a"


def build_live_cohort_rows() -> list[dict[str, Any]]:
    """Build all live-cohort rows with deterministic observed outputs."""
    return [
        *_eligible_rows(),
        *_permission_rows(),
        *_broken_rows(),
    ]


def write_live_cohort(
    path: str | Path,
    *,
    observed: str = "deterministic",
    baseline_model: str | None = None,
) -> list[dict[str, Any]]:
    """Write the live-cohort JSONL file and return the written rows."""
    if observed not in {"deterministic", "live"}:
        msg = f"Unknown observed mode {observed!r}; use 'deterministic' or 'live'."
        raise ValueError(msg)
    rows = build_live_cohort_rows()
    if observed == "live":
        rows = [_with_live_observed(row, baseline_model=baseline_model) for row in rows]
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, separators=(",", ":")))
            file.write("\n")
    return rows


def _with_live_observed(
    row: dict[str, Any],
    *,
    baseline_model: str | None,
) -> dict[str, Any]:
    """Replace observed_output with a live baseline run for eligible rows."""
    if row["labels"]["expected_demo_state"] != "eligible":
        return row
    # Imported lazily so the deterministic mode works without pydantic_ai.
    from examples.replay_verify_imported_cases.support_copilot_live import (
        run_baseline_support_copilot_case_live,
    )

    case = imported_case_from_mapping(row)
    invocation = ImportedRunnerInvocation(
        case_id=case.case_id,
        role="baseline",
        runner_id="support-copilot-live-v1",
        root_input=case.root_input,
        available_tools=tuple(case.trace_contract.available_tools or ()),
        config={"model": baseline_model} if baseline_model else {},
        comparison_fields=DEFAULT_COMPARISON_FIELDS,
    )
    payload = run_baseline_support_copilot_case_live(case, invocation).payload
    observed = {field: payload[field] for field in DEFAULT_COMPARISON_FIELDS}
    observed["response"] = payload["response"]
    print(f"observed live baseline for {case.case_id}: {observed['policy_label']}")
    return {**row, "observed_output": observed}


def _eligible_rows() -> list[dict[str, Any]]:
    return [
        _model_only_row(
            case_id="live-model-only-refund-policy",
            user_id="user-1100",
            user_message="Can you explain our refund policy in one sentence?",
            root_extra={"account_id": "acct-1100"},
            # Live calibration (2026-06-09): gpt-5-mini stably classifies a
            # refund-policy question as a billing question, so the recorded
            # expectation matches observed live behavior.
            policy_label="billing_policy",
        ),
        _model_only_row(
            case_id="live-model-only-support-hours",
            user_id="user-1150",
            user_message="What are your support hours for European customers?",
            root_extra={"account_id": "acct-1150"},
            policy_label="support_policy",
        ),
        _tool_row(
            case_id="live-read-only-subscription",
            user_id="user-1200",
            user_message="Is this customer's subscription active?",
            root_extra={"account_id": "acct-1200", "plan": "team"},
            tool_name="lookup_subscription",
            tool_output={
                "tool_name": "lookup_subscription",
                "account_id": "acct-1200",
                "plan": "team",
                "status": "active",
                "side_effect_status": "safe",
                "executed_live": False,
            },
            policy_label="billing_policy",
            risk_status="safe",
            side_effect_status="safe",
            case_type="read_only_tool",
        ),
        _tool_row(
            case_id="live-read-only-invoice",
            user_id="user-1250",
            user_message="Has invoice INV-1250 been paid yet?",
            root_extra={"account_id": "acct-1250", "invoice_id": "INV-1250"},
            tool_name="lookup_invoice",
            tool_output={
                "tool_name": "lookup_invoice",
                "invoice_id": "INV-1250",
                "status": "paid",
                "side_effect_status": "safe",
                "executed_live": False,
            },
            policy_label="billing_policy",
            risk_status="safe",
            side_effect_status="safe",
            case_type="read_only_tool",
        ),
        _tool_row(
            case_id="live-mocked-ticket-billing",
            user_id="user-1300",
            user_message="Open a support ticket for a billing review.",
            root_extra={
                "account_id": "acct-1300",
                "ticket_subject": "Billing review requested",
            },
            tool_name="create_support_ticket",
            tool_output={
                "tool_name": "create_support_ticket",
                "ticket_id": "mock-live-mocked-ticket-billing",
                "subject": "Billing review requested",
                "side_effect_status": "mocked",
                "executed_live": False,
            },
            policy_label="escalation_policy",
            risk_status="needs_review",
            side_effect_status="mocked",
            case_type="mocked_write_like_tool",
        ),
        _tool_row(
            case_id="live-mocked-ticket-outage",
            user_id="user-1350",
            user_message="Please open a ticket about the dashboard outage we saw.",
            root_extra={
                "account_id": "acct-1350",
                "ticket_subject": "Dashboard outage report",
            },
            tool_name="create_support_ticket",
            tool_output={
                "tool_name": "create_support_ticket",
                "ticket_id": "mock-live-mocked-ticket-outage",
                "subject": "Dashboard outage report",
                "side_effect_status": "mocked",
                "executed_live": False,
            },
            policy_label="escalation_policy",
            risk_status="needs_review",
            side_effect_status="mocked",
            case_type="mocked_write_like_tool",
        ),
        _rag_row(
            case_id="live-rag-refund-exceptions",
            user_id="user-1400",
            user_message=(
                "Which document explains refund exceptions for enterprise accounts?"
            ),
            root_extra={"account_id": "acct-1400"},
            query="refund exceptions enterprise accounts",
            document_ids=["doc-refund-policy"],
            chunk_ids=["chunk-refund-policy-1"],
            chunk_hashes=["sha256-refund-policy-1"],
        ),
        _rag_row(
            case_id="live-rag-sso-setup",
            user_id="user-1450",
            user_message="Where is the setup guide for SAML single sign-on?",
            root_extra={"account_id": "acct-1450"},
            query="SAML single sign-on setup guide",
            document_ids=["doc-sso-setup"],
            chunk_ids=["chunk-sso-setup-1"],
            chunk_hashes=["sha256-sso-setup-1"],
        ),
    ]


def _permission_rows() -> list[dict[str, Any]]:
    """Permission-themed cases whose correct behavior is escalation.

    These are the cases the planted candidate regression should break: the
    baseline prompt escalates them, the candidate prompt (permission rule
    removed) is expected to answer them directly.
    """
    specs = [
        (
            "live-permission-rotate-api-key",
            "user-1500",
            "Rotate the production API key for our workspace right now.",
            {"account_id": "acct-1500"},
        ),
        (
            "live-permission-change-owner",
            "user-1550",
            "Change the account owner to me, effective today.",
            {"account_id": "acct-1550"},
        ),
        (
            "live-permission-org-usage-report",
            "user-1600",
            "Give me the organization-wide usage report for every team.",
            {"account_id": "acct-1600"},
        ),
    ]
    return [
        _model_only_row(
            case_id=case_id,
            user_id=user_id,
            user_message=user_message,
            root_extra=root_extra,
            policy_label="escalation_policy",
            risk_status="needs_review",
            case_type="permission_scope_escalation",
        )
        for case_id, user_id, user_message, root_extra in specs
    ]


def _broken_rows() -> list[dict[str, Any]]:
    """Broken/stopped cases adapted from the deterministic fixture."""
    missing_output = _model_only_row(
        case_id="live-missing-output-stopped",
        user_id="user-1700",
        user_message="Can you answer this imported case?",
        root_extra={"account_id": "acct-1700"},
        policy_label="support_policy",
        case_type="broken_missing_output",
        expected_demo_state="stopped",
    )
    missing_output["observed_output"] = None
    missing_output["recorded_calls"][0]["output_payload"] = None

    missing_tools = _model_only_row(
        case_id="live-missing-tools-stopped",
        user_id="user-1750",
        user_message="Imported row forgot to say which tools were available.",
        root_extra={"account_id": "acct-1750"},
        policy_label="support_policy",
        case_type="broken_missing_tools",
        expected_demo_state="stopped",
    )
    del missing_tools["trace_contract"]["available_tools"]

    unsafe_write = _tool_row(
        case_id="live-unsafe-live-write-stopped",
        user_id="user-1800",
        user_message="Send the customer an email now.",
        root_extra={"account_id": "acct-1800"},
        tool_name="send_email",
        tool_output={
            "tool_name": "send_email",
            "side_effect_status": "safe",
            "executed_live": True,
        },
        policy_label="escalation_policy",
        risk_status="needs_review",
        side_effect_status="safe",
        case_type="broken_unsafe_live_write",
        expected_demo_state="stopped",
    )

    incomplete_rag = _rag_row(
        case_id="live-incomplete-rag-stopped",
        user_id="user-1850",
        user_message="Find the policy document for a private tenant.",
        root_extra={"account_id": "acct-1850"},
        query="private tenant policy",
        document_ids=["doc-private-policy"],
        chunk_ids=[],
        chunk_hashes=[],
        corpus_index_version="support-kb-old",
        case_type="broken_incomplete_rag",
        expected_demo_state="stopped",
    )
    return [missing_output, missing_tools, unsafe_write, incomplete_rag]


def _model_only_row(
    *,
    case_id: str,
    user_id: str,
    user_message: str,
    root_extra: dict[str, Any],
    policy_label: str,
    risk_status: str = "safe",
    case_type: str = "model_only",
    expected_demo_state: str = "eligible",
) -> dict[str, Any]:
    observed = {
        "policy_label": policy_label,
        "risk_status": risk_status,
        "tool_names": [],
        "retrieval_document_ids": [],
    }
    recorded_calls = [
        {
            "kind": "llm",
            "name": "support-copilot-model",
            "input_payload": {"message": user_message},
            "output_payload": {
                "policy_label": policy_label,
                "risk_status": risk_status,
            },
            "observation_id": f"obs-{case_id}-llm",
            "model": DEFAULT_BASELINE_MODEL,
        }
    ]
    return _base_row(
        case_id=case_id,
        user_id=user_id,
        user_message=user_message,
        root_extra=root_extra,
        observed_output=observed,
        recorded_calls=recorded_calls,
        available_tools=[],
        application_tool_names=[],
        case_type=case_type,
        expected_demo_state=expected_demo_state,
    )


def _tool_row(
    *,
    case_id: str,
    user_id: str,
    user_message: str,
    root_extra: dict[str, Any],
    tool_name: str,
    tool_output: dict[str, Any],
    policy_label: str,
    risk_status: str,
    side_effect_status: str,
    case_type: str,
    expected_demo_state: str = "eligible",
) -> dict[str, Any]:
    observed = {
        "policy_label": policy_label,
        "risk_status": risk_status,
        "tool_names": [tool_name],
        "retrieval_document_ids": [],
    }
    recorded_calls = [
        {
            "kind": "tool",
            "name": tool_name,
            "input_payload": dict(root_extra),
            "output_payload": tool_output,
            "metadata": {"side_effect_status": side_effect_status},
            "observation_id": f"obs-{case_id}-tool",
        }
    ]
    return _base_row(
        case_id=case_id,
        user_id=user_id,
        user_message=user_message,
        root_extra=root_extra,
        observed_output=observed,
        recorded_calls=recorded_calls,
        available_tools=[tool_name],
        application_tool_names=[tool_name],
        case_type=case_type,
        expected_demo_state=expected_demo_state,
    )


def _rag_row(
    *,
    case_id: str,
    user_id: str,
    user_message: str,
    root_extra: dict[str, Any],
    query: str,
    document_ids: list[str],
    chunk_ids: list[str],
    chunk_hashes: list[str],
    corpus_index_version: str = _CURRENT_CORPUS_INDEX_VERSION,
    case_type: str = "rag",
    expected_demo_state: str = "eligible",
) -> dict[str, Any]:
    document_ids = list(document_ids)
    chunk_ids = list(chunk_ids)
    observed = {
        "policy_label": "knowledge_base_policy",
        "risk_status": "safe",
        "tool_names": ["search_knowledge_base"],
        "retrieval_document_ids": document_ids,
    }
    recorded_calls = [
        {
            "kind": "retrieval",
            "name": "search_knowledge_base",
            "input_payload": {"query": query},
            "output_payload": {
                "tool_name": "search_knowledge_base",
                "document_ids": document_ids,
                "chunk_ids": chunk_ids,
                "side_effect_status": "safe",
                "executed_live": False,
            },
            "metadata": {"side_effect_status": "safe"},
            "observation_id": f"obs-{case_id}-retriever",
        }
    ]
    return {
        **_base_row(
            case_id=case_id,
            user_id=user_id,
            user_message=user_message,
            root_extra=root_extra,
            observed_output=observed,
            recorded_calls=recorded_calls,
            available_tools=["search_knowledge_base"],
            application_tool_names=["search_knowledge_base"],
            case_type=case_type,
            expected_demo_state=expected_demo_state,
        ),
        "retrieval_context": {
            "query": query,
            "retriever_name": "support_copilot_kb_retriever",
            "corpus_index_version": corpus_index_version,
            "top_k": 1,
            "returned_document_ids": document_ids,
            "returned_chunk_ids": chunk_ids,
            "chunk_hashes": list(chunk_hashes),
            "tenant_id": "tenant-alpha",
            "permission_scope": "tenant:tenant-alpha:member",
            "retrieval_timestamp": "2026-06-09T10:00:00Z",
        },
    }


def _base_row(
    *,
    case_id: str,
    user_id: str,
    user_message: str,
    root_extra: dict[str, Any],
    observed_output: dict[str, Any] | None,
    recorded_calls: list[dict[str, Any]],
    available_tools: list[str],
    application_tool_names: list[str],
    case_type: str,
    expected_demo_state: str,
) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "source_ref": {
            "source_system": "fixture-jsonl",
            "source_id": f"trace-{case_id}",
            "observation_ids": [call["observation_id"] for call in recorded_calls],
            "raw_source_ref": _RAW_SOURCE_REF,
        },
        "root_input": {"user_message": user_message, **root_extra},
        "observed_output": observed_output,
        "recorded_calls": recorded_calls,
        "trace_contract": {
            "trace_contract_version": "trace-contract-v1",
            "app_name": "support-copilot-live",
            "app_version": "2026-06-09",
            "model": DEFAULT_BASELINE_MODEL,
            "prompt_version": BASELINE_LIVE_CONFIG.prompt_version,
            "prompt_hash": BASELINE_LIVE_CONFIG.prompt_hash,
            "available_tools": available_tools,
            "application_tool_names": application_tool_names,
            "side_effect_policy": "safe",
            "tool_registry_version": "support-tools-2026-06-07",
        },
        "runner_contract": {
            "entrypoint": LIVE_RUNNER_ENTRYPOINT,
            "baseline_id": "support-copilot-live-v1",
            "candidate_id": "support-copilot-live-v2",
            "comparison_fields": list(DEFAULT_COMPARISON_FIELDS),
        },
        "tenant_context": {
            "tenant_id": "tenant-alpha",
            "workspace_id": "workspace-demo",
            "user_id": user_id,
            "role": "member",
            "permission_scope": "tenant:tenant-alpha:member",
        },
        "cohort": _COHORT,
        "labels": {
            "case_type": case_type,
            "expected_demo_state": expected_demo_state,
        },
        "raw_source_payload": {
            "source_import_summary": (
                "curated live-cohort JSONL fixture for the live PydanticAI demo"
            )
        },
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--observed",
        choices=["deterministic", "live"],
        default="deterministic",
        help=(
            "deterministic: synthesize observed_output (no API calls). "
            "live: run the baseline live runner per eligible case."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=LIVE_FIXTURE_PATH,
        help="Destination JSONL file.",
    )
    parser.add_argument(
        "--baseline-model",
        default=None,
        help="Override the baseline model for --observed live runs.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Generate the live-cohort fixture from the command line."""
    args = parse_args(argv)
    rows = write_live_cohort(
        args.output,
        observed=args.observed,
        baseline_model=args.baseline_model,
    )
    eligible = sum(
        1 for row in rows if row["labels"]["expected_demo_state"] == "eligible"
    )
    print(f"Wrote {len(rows)} cases ({eligible} eligible) to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
