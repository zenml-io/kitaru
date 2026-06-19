"""Graph compiler: introspect a compiled LangGraph StateGraph into a CompiledTopology.

LangGraph internals used (verified against installed version):
- ``graph.get_graph()`` → ``langchain_core.runnables.graph.Graph``
  - ``.nodes`` → dict mapping node_id to ``Node`` (has ``.id``, ``.data``)
  - ``.edges`` → list of ``Edge`` namedtuples with ``.source`` and ``.target``
- ``graph.nodes`` → dict mapping node_id to ``PregelNode``
  - ``PregelNode.bound`` → ``RunnableCallable`` (has ``.func`` — the original callable)

v1 supports single linear chains only; branching/cyclic graphs raise KitaruUsageError.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from kitaru.errors import KitaruUsageError

_SENTINELS = frozenset({"__start__", "__end__"})


@dataclass(frozen=True)
class CompiledTopology:
    nodes: list[str]
    callables: dict[str, Callable]
    fanout_node: str | None


def compile_topology(graph: object, *, fanout_node: str | None = None) -> CompiledTopology:
    """Introspect a compiled LangGraph graph into an ordered linear topology.

    v1 supports a single linear chain (the reference-agent shape). The fan-out
    node (variable tool calls) is named explicitly by the caller.

    Raises:
        KitaruUsageError: if the graph branches, has cycles, or has multiple
            start edges.
    """
    drawable = graph.get_graph()  # langchain_core.runnables.graph.Graph

    # Collect real (non-sentinel) nodes and build successor map.
    real_nodes: set[str] = {n for n in drawable.nodes if n not in _SENTINELS}
    successors: dict[str, list[str]] = {n: [] for n in real_nodes}
    start_targets: list[str] = []

    for edge in drawable.edges:
        src: str = edge.source
        dst: str = edge.target
        if src in _SENTINELS:
            if dst in real_nodes:
                start_targets.append(dst)
            continue
        if dst in _SENTINELS or src not in real_nodes:
            continue
        successors[src].append(dst)

    if len(start_targets) != 1:
        raise KitaruUsageError(
            "Replay v1 supports a single linear graph; "
            f"found {len(start_targets)} start edges."
        )

    # Walk the chain in edge order, detecting cycles and branches.
    ordered: list[str] = []
    current: str | None = start_targets[0]
    seen: set[str] = set()
    while current is not None:
        if current in seen:
            raise KitaruUsageError("Cycle detected; replay v1 requires a linear graph.")
        seen.add(current)
        ordered.append(current)
        nexts = successors.get(current, [])
        if len(nexts) > 1:
            raise KitaruUsageError(
                f"Node {current!r} branches to {nexts!r}; replay v1 requires a linear graph."
            )
        current = nexts[0] if nexts else None

    callables = {name: _node_callable(graph, name) for name in ordered}
    return CompiledTopology(nodes=ordered, callables=callables, fanout_node=fanout_node)


def _node_callable(graph: object, name: str) -> Callable:
    """Recover the original callable for a node from the compiled graph.

    LangGraph stores nodes as ``PregelNode`` objects. The original function
    lives at ``PregelNode.bound.func`` (where ``.bound`` is a
    ``RunnableCallable`` wrapping the user function).

    Falls back through several attribute paths for robustness across versions.
    """
    pregel_nodes: dict = getattr(graph, "nodes", {})
    spec = pregel_nodes.get(name)
    if spec is None:
        raise KitaruUsageError(f"Node {name!r} not found in compiled graph.")

    # Primary path: PregelNode.bound.func (RunnableCallable → original function)
    bound = getattr(spec, "bound", None)
    if bound is not None:
        func = getattr(bound, "func", None)
        if func is not None and callable(func):
            return func

    # Fallback: .runnable attribute (older LangGraph versions)
    runnable = getattr(spec, "runnable", None)
    if runnable is not None and callable(runnable):
        return runnable

    raise KitaruUsageError(
        f"Node {name!r} has no recoverable callable. "
        f"PregelNode attrs: {[a for a in dir(spec) if not a.startswith('__')]}"
    )
