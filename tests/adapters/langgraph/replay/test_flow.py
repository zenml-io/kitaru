"""Tests for the mirror @flow with cached-or-live checkpoint bodies.

These tests verify:
1. Seed run (playback=True): returns recorded node outputs, zero live calls.
2. Live run (playback=False): node callables ARE invoked.
"""

from __future__ import annotations

from kitaru._replay_verify_imported_models import RecordedCall
from kitaru.adapters.langgraph.replay._compiler import CompiledTopology
from kitaru.adapters.langgraph.replay._flow import ReplayContext, run_seed


def _topology(live_calls: list[str]) -> CompiledTopology:
    def make(name: str):
        def _node(state: dict) -> dict:
            live_calls.append(name)  # records any LIVE execution
            return {name: "live"}
        return _node

    nodes = ["receive_request", "collect_evidence_with_tools", "decide_action"]
    return CompiledTopology(
        nodes=nodes,
        callables={n: make(n) for n in nodes},
        fanout_node="collect_evidence_with_tools",
    )


def test_seed_run_serves_recorded_and_makes_no_live_calls(primed_zenml) -> None:
    live_calls: list[str] = []
    topo = _topology(live_calls)
    ctx = ReplayContext(
        topology=topo,
        recorded_by_node={
            "collect_evidence_with_tools": [
                RecordedCall(
                    kind="tool",
                    name="lookup_customer",
                    node="collect_evidence_with_tools",
                    call_index=0,
                    output_payload={"found": True},
                ),
            ],
        },
        node_output_by_node={
            "receive_request": {"tool_executions": []},
            "collect_evidence_with_tools": {
                "tool_executions": [{"name": "lookup_customer"}]
            },
            "decide_action": {
                "decision": {"policy_label": "billing_policy", "risk_status": "safe"}
            },
        },
        playback=True,
        variant=None,
        edits=[],
    )
    handle = run_seed(ctx)
    result = handle.wait()
    assert live_calls == []  # nothing executed live during seed playback
    assert result["decide_action"]["decision"]["risk_status"] == "safe"


def test_live_run_executes_tail_nodes(primed_zenml) -> None:
    live_calls: list[str] = []
    topo = _topology(live_calls)
    ctx = ReplayContext(
        topology=topo,
        recorded_by_node={},
        node_output_by_node={},
        playback=False,
        variant=None,
        edits=[],
    )
    from kitaru.adapters.langgraph.replay._flow import build_replay_flow

    flow_def = build_replay_flow(ctx)
    handle = flow_def.run(False)
    handle.wait()
    assert "decide_action" in live_calls  # tail executed live
