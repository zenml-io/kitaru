"""Tests for the mirror @flow with cached-or-live checkpoint bodies.

These tests verify:
1. Seed run (playback=True): returns recorded node outputs, zero live calls.
2. Live run (playback=False): node callables ARE invoked.
"""

from __future__ import annotations

import importlib

import pytest

from kitaru._replay_verify_imported_models import RecordedCall
from kitaru._source_aliases import (
    build_checkpoint_source_alias,
    build_pipeline_source_alias,
)
from kitaru.adapters.langgraph.replay._compiler import CompiledTopology
from kitaru.adapters.langgraph.replay._flow import (
    ReplayContext,
    _call_node_callable,
    run_seed,
)


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


def _ctx(live_calls: list[str], *, playback: bool) -> ReplayContext:
    return ReplayContext(
        topology=_topology(live_calls),
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
            "receive_request": {"receive_request": "recorded"},
            "collect_evidence_with_tools": {"collect_evidence_with_tools": "recorded"},
            "decide_action": {"decide_action": "recorded"},
        },
        playback=playback,
        variant=None,
        edits=[],
    )


def test_generated_replay_flow_registers_importable_source_aliases() -> None:
    live_calls: list[str] = []
    ctx = _ctx(live_calls, playback=True)
    from kitaru.adapters.langgraph.replay._flow import build_replay_flow

    flow_def = build_replay_flow(ctx)
    pipeline_alias = build_pipeline_source_alias("replay_flow")
    checkpoint_alias = build_checkpoint_source_alias("node_step")
    importable_modules = []

    for module_name in (
        "kitaru.adapters.langgraph.replay._flow",
        "src.kitaru.adapters.langgraph.replay._flow",
    ):
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError:
            continue
        importable_modules.append(module_name)
        assert getattr(module, pipeline_alias) is flow_def._pipeline
        assert getattr(module, checkpoint_alias) is not None

    assert importable_modules


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


def test_live_run_supports_state_config_node_callable(primed_zenml) -> None:
    configs: list[dict] = []

    def _node(state: dict, config: dict) -> dict:
        configs.append(config)
        return {"seen": state.get("seed"), "config": config}

    topo = CompiledTopology(
        nodes=["state_config_node"],
        callables={"state_config_node": _node},
        fanout_node=None,
    )
    ctx = ReplayContext(
        topology=topo,
        recorded_by_node={},
        node_output_by_node={},
        playback=False,
        variant=None,
        edits=[],
        root_state={"seed": "root"},
    )
    from kitaru.adapters.langgraph.replay._flow import build_replay_flow

    flow_def = build_replay_flow(ctx)
    result = flow_def.run(False).wait()

    assert configs == [{}]
    assert result == {"state_config_node": {"seen": "root", "config": {}}}


def test_node_callable_type_error_from_user_code_is_not_retried() -> None:
    calls: list[str] = []

    def _node(state: dict) -> dict:
        calls.append("one-arg")
        raise TypeError("user code failed")

    with pytest.raises(TypeError, match="user code failed"):
        _call_node_callable(_node, {}, accepts_config=False)

    assert calls == ["one-arg"]


def test_generated_replay_flow_refreshes_source_aliases_before_run(
    primed_zenml,
) -> None:
    first_live_calls: list[str] = []
    second_live_calls: list[str] = []
    from kitaru.adapters.langgraph.replay._flow import build_replay_flow

    first_flow = build_replay_flow(_ctx(first_live_calls, playback=True))
    second_flow = build_replay_flow(_ctx(second_live_calls, playback=True))
    pipeline_alias = build_pipeline_source_alias("replay_flow")
    modules = [
        importlib.import_module(name)
        for name in (
            "kitaru.adapters.langgraph.replay._flow",
            "src.kitaru.adapters.langgraph.replay._flow",
        )
    ]

    assert all(
        getattr(module, pipeline_alias) is second_flow._pipeline for module in modules
    )

    handle = first_flow.run(True)
    handle.wait()

    assert all(
        getattr(module, pipeline_alias) is first_flow._pipeline for module in modules
    )


def test_seeded_mirror_flow_replays_tail_without_dynamic_source_error(
    primed_zenml,
) -> None:
    live_calls: list[str] = []
    ctx = _ctx(live_calls, playback=True)
    from kitaru.adapters.langgraph.replay._flow import build_replay_flow

    flow_def = build_replay_flow(ctx)
    seed_handle = flow_def.run(True)
    seed_handle.wait()
    assert live_calls == []

    replay_submission = flow_def.replay(
        seed_handle.exec_id,
        at="collect_evidence_with_tools",
        cache=False,
        flow_overrides={"playback": False},
        wait=False,
    )
    assert len(replay_submission.results) == 1
    replay_handle = replay_submission.results[0].handle
    assert replay_handle is not None
    result = replay_handle.wait()

    assert "receive_request" not in live_calls
    assert live_calls == ["collect_evidence_with_tools", "decide_action"]
    assert result["receive_request"] == {"receive_request": "recorded"}
    assert result["decide_action"] == {"decide_action": "live"}


def test_rebuilt_mirror_flow_replays_seed_execution_without_dynamic_source_error(
    primed_zenml,
) -> None:
    seed_live_calls: list[str] = []
    seed_ctx = _ctx(seed_live_calls, playback=True)
    from kitaru.adapters.langgraph.replay._flow import build_replay_flow

    seed_flow = build_replay_flow(seed_ctx)
    seed_handle = seed_flow.run(True)
    seed_handle.wait()
    assert seed_live_calls == []

    fork_live_calls: list[str] = []
    fork_ctx = _ctx(fork_live_calls, playback=False)
    fork_flow = build_replay_flow(fork_ctx)
    fork_submission = fork_flow.replay(
        seed_handle.exec_id,
        at="collect_evidence_with_tools",
        cache=False,
        flow_overrides={"playback": False},
        wait=False,
    )
    assert len(fork_submission.results) == 1
    fork_handle = fork_submission.results[0].handle
    assert fork_handle is not None
    result = fork_handle.wait()

    assert fork_live_calls == ["collect_evidence_with_tools", "decide_action"]
    assert result["receive_request"] == {"receive_request": "recorded"}
    assert result["decide_action"] == {"decide_action": "live"}
