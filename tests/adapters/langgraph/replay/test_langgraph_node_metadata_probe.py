"""Upstream-behavior probe: justifies the importer's tree-based node attribution.

When a LangGraph node invokes an inner runnable with an EXPLICIT
``config={'metadata': ...}`` — exactly how the reference agent calls its model
(examples/end_to_end/replay_fork_demo/reference_agent/agent.py) — the inherited
``langgraph_node`` does NOT reach that inner run's callback metadata. Only the
node-level run carries it. Langfuse records each observation's own metadata, so
the LLM/tool observations a real trace produces lack ``langgraph_node`` and the
importer must recover the node from the observation tree instead.

If this test ever fails because the inner run suddenly *does* carry
``langgraph_node``, that is a signal the upstream behavior changed — revisit
whether ``build_node_map``'s tree walk is still required (it would still be
correct, just possibly no longer load-bearing).
"""

from __future__ import annotations

from typing import Any

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict


class _CaptureMetadata(BaseCallbackHandler):
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def on_chain_start(
        self,
        serialized,
        inputs,
        *,
        run_id,
        parent_run_id=None,
        tags=None,
        metadata=None,
        **kwargs,
    ) -> None:
        self.events.append(dict(metadata or {}))


class _S(TypedDict, total=False):
    x: int


def test_inner_run_with_explicit_metadata_drops_langgraph_node():
    capture = _CaptureMetadata()
    inner = RunnableLambda(lambda _x: {"ok": True}, name="fake_model")

    def decide_action(_state: _S) -> _S:
        # Mirrors agent.py: model.invoke(..., config={'metadata': <user dict>})
        inner.invoke(
            {"q": 1},
            config={"callbacks": [capture], "metadata": {"inner_marker": "yes"}},
        )
        return {"x": 1}

    builder = StateGraph(_S)
    builder.add_node("decide_action", decide_action)
    builder.add_edge(START, "decide_action")
    builder.add_edge("decide_action", END)
    graph = builder.compile()

    graph.invoke(
        {"x": 0},
        config={"callbacks": [capture], "metadata": {"trace_marker": "yes"}},
    )

    # The node-level run carries langgraph_node...
    assert any(md.get("langgraph_node") == "decide_action" for md in capture.events), (
        "expected the node-level run to carry langgraph_node"
    )

    # ...but the explicitly-configured inner run (our marker) does NOT — which is
    # exactly why the importer cannot trust a call observation's own metadata.
    inner_events = [md for md in capture.events if md.get("inner_marker") == "yes"]
    assert inner_events, "probe did not capture the inner run"
    assert all("langgraph_node" not in md for md in inner_events), (
        "inner run unexpectedly carried langgraph_node — revisit importer tree walk"
    )
