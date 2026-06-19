"""Tests for the LangGraph → CompiledTopology compiler (Task 7)."""
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from kitaru.adapters.langgraph.replay._compiler import compile_topology

import pytest
from kitaru.errors import KitaruUsageError


class _S(TypedDict, total=False):
    x: int


def _linear_graph():
    b = StateGraph(_S)
    b.add_node("a", lambda s: {"x": 1})
    b.add_node("b", lambda s: {"x": 2})
    b.add_node("c", lambda s: {"x": 3})
    b.add_edge(START, "a")
    b.add_edge("a", "b")
    b.add_edge("b", "c")
    b.add_edge("c", END)
    return b.compile()


def test_compile_returns_nodes_in_edge_order():
    topo = compile_topology(_linear_graph())
    assert topo.nodes == ["a", "b", "c"]
    assert set(topo.callables) == {"a", "b", "c"}
    assert callable(topo.callables["a"])


def test_branching_graph_is_rejected():
    b = StateGraph(_S)
    b.add_node("a", lambda s: {"x": 1})
    b.add_node("b", lambda s: {"x": 2})
    b.add_node("c", lambda s: {"x": 3})
    b.add_edge(START, "a")
    b.add_edge("a", "b")
    b.add_edge("a", "c")
    b.add_edge("b", END)
    b.add_edge("c", END)
    with pytest.raises(KitaruUsageError):
        compile_topology(b.compile())
