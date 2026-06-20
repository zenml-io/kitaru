"""Mirror @flow with cached-or-live checkpoint bodies for LangGraph replay.

One checkpoint per node; the seed run (playback=True) returns recorded outputs
from the trace-keyed cache — zero live calls.  A later native replay with
playback=False executes the real node callables live.

Result shape: ``{node_name: node_output_dict, ...}`` — one entry per node.

Design: each ``node_step`` checkpoint receives the accumulated results dict from
all prior nodes (threaded as a handle to ZenML for lineage) and returns a new
dict that extends it with this node's output.  The terminal step therefore holds
the complete results map, which ``handle.wait()`` can extract directly via the
terminal-step fallback path.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from kitaru import checkpoint, flow
from kitaru.adapters.langgraph.replay._compiler import CompiledTopology
from kitaru.adapters.langgraph.replay._edits import Edit, resolve_edits
from kitaru._replay_verify_imported_models import RecordedCall


@dataclass
class ReplayContext:
    """All context needed to build or run a mirror replay flow."""

    topology: CompiledTopology
    recorded_by_node: dict[str, list[RecordedCall]]
    node_output_by_node: dict[str, Any]
    playback: bool
    variant: dict[str, Any] | None = None
    edits: list[Edit] = field(default_factory=list)


def build_replay_flow(ctx: ReplayContext) -> Any:
    """Build a Kitaru @flow that mirrors a graph's nodes.

    Returns a flow definition (not an execution handle).  Call
    ``flow_def.run(playback)`` to launch it.

    One checkpoint is defined per *build* call; successive node invocations
    use unique ``id=<node>`` values to keep each node's step distinct inside
    ZenML.  Handles are threaded as inputs so ZenML tracks data lineage across
    nodes without early materialisation.

    Result: ``{node_name: node_output_dict}`` — one entry per node preserving
    per-node provenance for downstream cut/fork operations (Task 9).

    In playback mode the node callable is never invoked; the recorded node
    output is returned directly.  In live mode the real callable runs against
    the accumulated state from previous nodes.
    """

    # --- node checkpoint (one per build_replay_flow call; closed over ctx) ----

    @checkpoint(cache=False)
    def node_step(
        node: str,
        accumulated_results: dict[str, Any],
        playback: bool,
    ) -> dict[str, Any]:
        """Cached-or-live body for one graph node.

        ``accumulated_results`` maps ``node_name -> node_output`` for all nodes
        that ran before this one.  Returns a new dict that extends it with
        this node's output, so the terminal step's output is the complete map.

        In playback mode the node callable is never invoked.
        """
        if playback:
            node_out = ctx.node_output_by_node.get(node, {})
        else:
            # Live path: build the running AgentState from prior results.
            running_state: dict[str, Any] = {}
            for prior_out in accumulated_results.values():
                if isinstance(prior_out, dict):
                    running_state.update(prior_out)

            # Apply any edits/variant overrides (no-op when lists are empty).
            recorded: dict[str, Any] = {}
            calls = ctx.recorded_by_node.get(node, [])
            if calls and getattr(calls[0], "model", None) is not None:
                recorded = {"model": calls[0].model}
            resolve_edits(
                node=node,
                call_index=None,
                edits=ctx.edits,
                variant=ctx.variant,
                recorded=recorded,
            )
            callable_ = ctx.topology.callables[node]
            node_out = callable_(running_state)

        # Extend and return the full accumulated results dict so the terminal
        # step's output IS the complete {node_name: node_output} map.
        return {**accumulated_results, node: node_out}

    # --- flow ------------------------------------------------------------------

    @flow(cache=False)
    def replay_flow(playback: bool) -> dict[str, Any]:
        """Mirror flow: one checkpoint per graph node, result keyed by node.

        Each node_step receives the accumulated results dict from its predecessor
        (threaded via ZenML's step-future argument passing for data lineage).
        We call ``.load()`` in the flow body ONLY to extract the intermediate
        dict for passing to the next checkpoint; the step future is also kept
        as the ``after=`` dependency.

        The terminal step's checkpoint output is the complete
        ``{node_name: node_output}`` map, which ``handle.wait()`` returns via
        the terminal-step fallback extraction path.
        """
        nodes = ctx.topology.nodes

        # First node — empty accumulated results.
        prev_handle = node_step.submit(nodes[0], {}, playback, id=nodes[0])

        for node in nodes[1:]:
            # Load the previous accumulated-results dict to pass as the next
            # checkpoint's argument.  We do this in the flow body (not inside
            # a checkpoint) because we need the plain dict value to construct
            # the next call; this is the minimum required `.load()`.
            prev_out = prev_handle.load()
            cur_handle = node_step.submit(
                node, prev_out, playback, id=node, after=prev_handle
            )
            prev_handle = cur_handle

        # The terminal step returns the full results dict; load it here so
        # the flow can return a plain serialisable dict (not a StepFuture).
        return prev_handle.load()

    return replay_flow


def run_seed(ctx: ReplayContext) -> Any:
    """Convenience: build and run the mirror flow with playback=True.

    Returns a FlowHandle; call ``.wait()`` to get the result dict
    (``{node_name: node_output_dict}``).
    """
    ctx.playback = True
    replay_flow = build_replay_flow(ctx)
    return replay_flow.run(True)
