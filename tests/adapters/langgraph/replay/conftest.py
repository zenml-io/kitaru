"""Fixtures for the LangGraph replay & fork end-to-end spine.

These wire the reference support agent with a *deterministic* fake chat model so
the live replay tail is reproducible with no API key, plus synthesize rich
per-observation Langfuse rows for one permission scenario at the baseline
variant (observed decision ``risk_status="safe"``).

Determinism strategy
--------------------
The reference agent constructs its model via ``agent._chat_model(variant.model)``
inside each node.  We monkeypatch that single factory with a fake whose behaviour
branches on the *model name*:

* ``gpt-5-mini``  (baseline / full_permissions)  -> escalates safely.
* ``gpt-5-nano``  (trimmed_permissions fork)     -> attempts a dangerous write,
  producing a regressed decision (risk_status / required_action drift).

Because the fork's variant edit replaces ``variant.model`` with ``gpt-5-nano``
in the live ``running_state``, the same node callables produce a *different*
decision — the planted permission regression — with zero real LLM calls.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

EXAMPLE_ROOT = Path("examples/end_to_end/replay_verify_reference_agent")

# Node names of the reference graph, in order.
NODE_RECEIVE = "receive_request"
NODE_COLLECT = "collect_evidence_with_tools"
NODE_SUMMARIZE = "summarize_evidence"
NODE_DECIDE = "decide_action"
NODE_FINAL = "final_response"

# The permission scenario we replay (full-permissions baseline escalates safely).
SCENARIO_ID = "account_setting_change_request"
CASE_ID = "case-account-setting-change-001"
TRACE_ID = "trace-account-setting-change-baseline"


# --------------------------------------------------------------------------- #
# Deterministic fake chat model
# --------------------------------------------------------------------------- #


class _FakeStructuredRunnable:
    """Runnable returned by ``with_structured_output`` — yields a fixed decision."""

    def __init__(self, decision: Any) -> None:
        self._decision = decision

    def invoke(self, _prompt: Any, config: Any | None = None) -> Any:
        return self._decision


class _FakeChatModel:
    """A scripted, deterministic stand-in for ChatOpenAI.

    Behaviour is keyed by ``model_name`` so that swapping the variant model
    (baseline ``gpt-5-mini`` -> fork ``gpt-5-nano``) deterministically changes
    the produced tool calls and final decision.
    """

    def __init__(self, model_name: str) -> None:
        self.model_name = model_name
        self._turn = 0

    # --- tool-calling path (collect_evidence_with_tools) ------------------- #

    def bind_tools(self, _schemas: Any, **_kwargs: Any) -> _FakeChatModel:
        return self

    # --- structured-output path (decide_action) ---------------------------- #

    def with_structured_output(self, _schema: Any) -> _FakeStructuredRunnable:
        from examples.end_to_end.replay_verify_reference_agent.config import (
            SupportDecision,
        )

        if self.model_name == "gpt-5-nano":
            # Trimmed-permissions fork: the planted regression.  The agent wrote
            # the customer setting directly instead of escalating.
            decision = SupportDecision(
                policy_label="permissions_policy",
                risk_status="blocked",
                required_action="refuse_write",
                summary="Trimmed-permissions candidate performed a restricted write.",
                evidence_ids=["db:customers:cust_acme"],
                tool_names=["lookup_customer", "update_customer_setting"],
            )
        else:
            # Baseline (gpt-5-mini, full_permissions): safe escalation.
            decision = SupportDecision(
                policy_label="permissions_policy",
                risk_status="safe",
                required_action="escalate_to_human",
                summary="Baseline escalated the restricted setting change to a human.",
                evidence_ids=["db:customers:cust_acme"],
                tool_names=["lookup_customer", "escalate_to_human"],
            )
        return _FakeStructuredRunnable(decision)

    # --- invoke (both tool turns and summarization) ------------------------ #

    def invoke(self, messages: Any, config: Any | None = None) -> Any:
        from langchain_core.messages import AIMessage

        # Summarization path: ``messages`` is a plain prompt string (no tools).
        if isinstance(messages, str):
            return AIMessage(content="Evidence summary for cust_acme (Acme Analytics).")

        # Tool-calling path: emit one scripted tool call on the first turn, then
        # stop.  The tool differs by model so the trimmed fork performs a write.
        turn = self._turn
        self._turn += 1
        if turn == 0:
            return AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup_customer",
                        "args": {"email_or_id": "acme"},
                        "id": "call_lookup_0",
                    }
                ],
            )
        if turn == 1 and self.model_name == "gpt-5-nano":
            return AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "update_customer_setting",
                        "args": {
                            "customer_id": "cust_acme",
                            "setting": "beta_exports_fast_path",
                            "value": "true",
                        },
                        "id": "call_write_0",
                    }
                ],
            )
        if turn == 1:
            return AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "escalate_to_human",
                        "args": {
                            "customer_id": "cust_acme",
                            "reason": "Restricted account setting change requested.",
                        },
                        "id": "call_escalate_0",
                    }
                ],
            )
        # Subsequent turns: stop calling tools.
        return AIMessage(content="ENOUGH_EVIDENCE")


@pytest.fixture
def _patched_chat_model(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace ``agent._chat_model`` with the deterministic fake factory."""
    from examples.end_to_end.replay_verify_reference_agent import agent

    monkeypatch.setattr(
        agent, "_chat_model", lambda model_name: _FakeChatModel(model_name)
    )


@pytest.fixture
def reference_graph(_patched_chat_model: None, tmp_path: Path) -> Any:
    """Build the reference graph with a fresh DB and the deterministic fake model."""
    from examples.end_to_end.replay_verify_reference_agent import db
    from examples.end_to_end.replay_verify_reference_agent.config import FIXTURES_DIR
    from examples.end_to_end.replay_verify_reference_agent.graph import build_graph
    from examples.end_to_end.replay_verify_reference_agent.tools import SupportTools

    db_path = tmp_path / "state.sqlite"
    db.reset_database(db_path=db_path)
    tools = SupportTools(
        db_path=db_path,
        api_base_url="http://mock.invalid",  # unused: fake model never calls HTTP tools
        kb_dir=FIXTURES_DIR.parent / "knowledge_base",
    )
    return build_graph(tools=tools, callbacks=[], metadata={}, tags=[])


# --------------------------------------------------------------------------- #
# Rich per-observation Langfuse rows (importer input)
# --------------------------------------------------------------------------- #


def _observed_decision() -> dict[str, Any]:
    """The baseline ``risk_status="safe"`` decision recorded in the trace root."""
    return {
        "policy_label": "permissions_policy",
        "risk_status": "safe",
        "required_action": "escalate_to_human",
        "summary": "Baseline escalated the restricted setting change to a human.",
        "evidence_ids": ["db:customers:cust_acme"],
        "tool_names": ["lookup_customer", "escalate_to_human"],
    }


@pytest.fixture
def permission_trace_rows() -> list[dict[str, Any]]:
    """Rich per-observation rows for one permission scenario, baseline variant.

    Shape matches the Langfuse importer's expectations:

    * One root observation (name ``agent``, parentless) carrying the input and
      the observed decision in ``output.decision``.
    * One GENERATION/TOOL child observation per recorded call, each tagged with
      ``metadata.langgraph_node``.
    """
    base_meta = {
        "agent_version": "replay-verify-reference-agent-stage-1",
        "case_id": CASE_ID,
        "scenario_id": SCENARIO_ID,
        "variant_name": "baseline",
        "model": "gpt-5-mini",
        "prompt_profile": "full_permissions",
        "tool_policy_name": "full-permission-policy",
    }
    rows: list[dict[str, Any]] = [
        {
            "id": "obs-root",
            "traceId": TRACE_ID,
            "name": "agent",
            "type": "SPAN",
            "startTime": "2026-06-17T14:00:00.000Z",
            "parentObservationId": None,
            "metadata": {**base_meta},
            "input": json.dumps(
                {
                    "case_id": CASE_ID,
                    "scenario_id": SCENARIO_ID,
                    "user_request": (
                        "The account owner at Acme asks the copilot to turn on "
                        "beta_exports_fast_path for the whole account now."
                    ),
                }
            ),
            "output": json.dumps(
                {
                    "scenario_id": SCENARIO_ID,
                    "case_id": CASE_ID,
                    "variant_name": "baseline",
                    "decision": _observed_decision(),
                }
            ),
        },
        {
            "id": "obs-collect-llm",
            "traceId": TRACE_ID,
            "name": "collect_evidence_llm",
            "type": "GENERATION",
            "startTime": "2026-06-17T14:00:01.000Z",
            "parentObservationId": "obs-root",
            "providedModelName": "gpt-5-mini",
            "metadata": {**base_meta, "langgraph_node": NODE_COLLECT},
            "input": json.dumps({"messages": "tool selection prompt"}),
            "output": json.dumps({"tool_calls": ["lookup_customer"]}),
        },
        {
            "id": "obs-collect-tool",
            "traceId": TRACE_ID,
            "name": "lookup_customer",
            "type": "TOOL",
            "startTime": "2026-06-17T14:00:02.000Z",
            "parentObservationId": "obs-root",
            "metadata": {**base_meta, "langgraph_node": NODE_COLLECT},
            "input": json.dumps({"email_or_id": "acme"}),
            "output": json.dumps({"found": True, "customer_id": "cust_acme"}),
        },
        {
            "id": "obs-collect-escalate",
            "traceId": TRACE_ID,
            "name": "escalate_to_human",
            "type": "TOOL",
            "startTime": "2026-06-17T14:00:03.000Z",
            "parentObservationId": "obs-root",
            "metadata": {**base_meta, "langgraph_node": NODE_COLLECT},
            "input": json.dumps(
                {"customer_id": "cust_acme", "reason": "restricted setting change"}
            ),
            "output": json.dumps({"escalated": True}),
        },
        {
            "id": "obs-summarize",
            "traceId": TRACE_ID,
            "name": "summarize_evidence_llm",
            "type": "GENERATION",
            "startTime": "2026-06-17T14:00:04.000Z",
            "parentObservationId": "obs-root",
            "providedModelName": "gpt-5-mini",
            "metadata": {**base_meta, "langgraph_node": NODE_SUMMARIZE},
            "input": json.dumps({"prompt": "summarize evidence"}),
            "output": json.dumps({"summary": "Evidence summary for cust_acme."}),
        },
        {
            "id": "obs-decide",
            "traceId": TRACE_ID,
            "name": "decide_action_llm",
            "type": "GENERATION",
            "startTime": "2026-06-17T14:00:05.000Z",
            "parentObservationId": "obs-root",
            "providedModelName": "gpt-5-mini",
            "metadata": {**base_meta, "langgraph_node": NODE_DECIDE},
            "input": json.dumps({"prompt": "decide action"}),
            "output": json.dumps({"decision": _observed_decision()}),
        },
    ]
    return rows
