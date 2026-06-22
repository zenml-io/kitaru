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

import contextlib
import copy
import importlib
import sys
from dataclasses import dataclass, field
from functools import wraps
from typing import Any

import kitaru
from kitaru import checkpoint, flow
from kitaru._replay_verify_imported_models import RecordedCall
from kitaru._source_aliases import (
    build_checkpoint_source_alias,
    build_pipeline_source_alias,
)
from kitaru.adapters.langgraph.replay._compiler import CompiledTopology
from kitaru.adapters.langgraph.replay._edits import Edit, resolve_edits


def _json_safe(value: Any) -> Any:
    """Best-effort conversion to a JSON-friendly value for artifact display."""
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump()
        except Exception:
            return str(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _module_import_names() -> set[str]:
    """Return module names ZenML may use for this replay adapter."""
    names = {__name__}
    if __name__.startswith("src."):
        names.add(__name__.removeprefix("src."))
    else:
        names.add(f"src.{__name__}")
    return names


def _import_module_for_alias(module_name: str) -> Any:
    """Import a module the same ways ZenML may import dynamic sources."""
    module = sys.modules.get(module_name)
    if module is not None:
        return module

    with contextlib.suppress(ModuleNotFoundError):
        return importlib.import_module(module_name)

    with contextlib.suppress(Exception):
        from zenml.utils.source_utils import get_source_root, prepend_python_path

        with prepend_python_path(get_source_root()):
            return importlib.import_module(module_name)

    return None


def _register_generated_source_aliases(*, node_step: Any, replay_flow: Any) -> None:
    """Expose generated replay objects under Kitaru source-alias names.

    ``node_step`` and ``replay_flow`` are created inside ``build_replay_flow`` so
    they close over the trace-specific ``ReplayContext``.  ZenML later reloads
    dynamic pipelines by importing this module and looking up the Kitaru source
    alias.  Without these module attributes, replay/fork runs cannot find the
    generated objects that were built in the current process.
    """
    step_obj = getattr(node_step, "_step", None)
    pipeline_obj = getattr(replay_flow, "_pipeline", None)
    if step_obj is None or pipeline_obj is None:
        return

    aliases = {
        build_checkpoint_source_alias("node_step"): step_obj,
        build_pipeline_source_alias("replay_flow"): pipeline_obj,
    }
    for module_name in _module_import_names():
        module = _import_module_for_alias(module_name)
        if module is None:
            continue
        for alias, obj in aliases.items():
            setattr(module, alias, obj)


def _wrap_alias_registration_around_execution(
    *,
    node_step: Any,
    replay_flow: Any,
) -> None:
    """Refresh source aliases before this generated flow runs or replays."""
    original_run = replay_flow.run
    original_replay = replay_flow.replay

    @wraps(original_run)
    def run_with_aliases(*args: Any, **kwargs: Any) -> Any:
        _register_generated_source_aliases(node_step=node_step, replay_flow=replay_flow)
        return original_run(*args, **kwargs)

    @wraps(original_replay)
    def replay_with_aliases(*args: Any, **kwargs: Any) -> Any:
        _register_generated_source_aliases(node_step=node_step, replay_flow=replay_flow)
        return original_replay(*args, **kwargs)

    replay_flow.run = run_with_aliases
    replay_flow.replay = replay_with_aliases


@dataclass
class ReplayContext:
    """All context needed to build or run a mirror replay flow."""

    topology: CompiledTopology
    recorded_by_node: dict[str, list[RecordedCall]]
    node_output_by_node: dict[str, Any]
    # Not the runtime switch; the actual switch is the playback flow input
    # threaded to node_step.
    playback: bool
    variant: dict[str, Any] | None = None
    edits: list[Edit] = field(default_factory=list)
    root_state: dict[str, Any] = field(default_factory=dict)


def _apply_effective_variant(
    running_state: dict[str, Any],
    *,
    effective: dict[str, Any],
) -> None:
    """Overlay the effective variant params onto ``running_state['variant']``.

    The reference-agent node callables read ``state['variant']`` (an
    ``AgentVariant`` pydantic model or a plain dict).  A fork edit replaces the
    variant's model / prompt_profile / etc. so the *same* node callables produce
    a different decision.  This mutates the variant object in place via a
    structure-preserving copy and writes it back into ``running_state``.

    Only fields that already exist on the variant are overlaid, so unrelated
    edit/variant keys (e.g. ``value``) never leak onto the variant object.

    Important: ``running_state`` is a per-node shallow copy of ``ctx.root_state``
    so the dict itself is isolated; however the variant object inside it is
    shared.  All three branches below write a NEW object back into
    ``running_state['variant']`` (never mutating the original), so the shared
    proxy/model in ``ctx.root_state`` is never corrupted across nodes.
    """
    variant_obj = running_state.get("variant")
    if variant_obj is None or not effective:
        return

    if isinstance(variant_obj, dict):
        updates = {k: v for k, v in effective.items() if k in variant_obj}
        if updates:
            running_state["variant"] = {**variant_obj, **updates}
        return

    # Pydantic-style model: only overlay fields the model actually declares.
    model_copy = getattr(variant_obj, "model_copy", None)
    declared = getattr(type(variant_obj), "model_fields", None)
    if callable(model_copy) and isinstance(declared, dict):
        updates = {k: v for k, v in effective.items() if k in declared}
        if updates:
            running_state["variant"] = model_copy(update=updates)
        return

    # Fallback: plain objects (e.g. _VariantProxy / SimpleNamespace).
    # Work on a per-node shallow copy so we never mutate the shared proxy
    # in ctx.root_state, and restrict setattr to keys that existed at
    # construction time (recorded in __dict__) so stray edit keys like
    # 'value' cannot attach to the proxy.
    known_keys = set(vars(variant_obj))
    updates = {k: v for k, v in effective.items() if k in known_keys}
    if updates:
        local_copy = copy.copy(variant_obj)
        for key, value in updates.items():
            with contextlib.suppress(AttributeError, TypeError):
                setattr(local_copy, key, value)
        running_state["variant"] = local_copy


def build_replay_flow(ctx: ReplayContext) -> Any:
    """Build a Kitaru @flow that mirrors a graph's nodes.

    Returns a flow definition (not an execution handle).  Call
    ``flow_def.run(playback)`` to launch it.

    One checkpoint is defined per *build* call; successive node invocations
    use unique ``id=<node>`` values to keep each node's step distinct inside
    ZenML.  Handles are threaded as inputs so ZenML tracks data lineage across
    nodes without early materialization.

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
            # Live path: seed the running AgentState with the graph's root input
            # (scenario + variant — NOT recoverable from prior node OUTPUT dicts),
            # then overlay each prior node's output delta on top.
            running_state: dict[str, Any] = dict(ctx.root_state)
            for prior_out in accumulated_results.values():
                if isinstance(prior_out, dict):
                    running_state.update(prior_out)

            # Resolve effective edit/variant params (call > variant > recorded)
            # and apply the effective variant into running_state so the live
            # node callables run under the forked configuration.
            recorded: dict[str, Any] = {}
            calls = ctx.recorded_by_node.get(node, [])
            if calls and getattr(calls[0], "model", None) is not None:
                recorded = {"model": calls[0].model}
            effective = resolve_edits(
                node=node,
                call_index=None,
                edits=ctx.edits,
                variant=ctx.variant,
                recorded=recorded,
            )
            _apply_effective_variant(running_state, effective=effective)

            callable_ = ctx.topology.callables[node]
            node_out = callable_(running_state)

        # Surface THIS node's own output as an individual, JSON-clean named
        # artifact, so the dashboard shows e.g. `decide_action` / `final_response`
        # as viewable artifacts — not just the opaque accumulated-map blob that
        # the checkpoint must return for replay. Best-effort: never fail the run.
        with contextlib.suppress(Exception):
            if isinstance(node_out, dict) and node_out:
                kitaru.save(node, _json_safe(node_out), type="output")

        # Extend and return the full accumulated results dict so the terminal
        # step's output IS the complete {node_name: node_output} map (load-bearing
        # for native replay; the individual artifact above is for display).
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

    _register_generated_source_aliases(node_step=node_step, replay_flow=replay_flow)
    _wrap_alias_registration_around_execution(
        node_step=node_step,
        replay_flow=replay_flow,
    )
    return replay_flow


def run_seed(ctx: ReplayContext) -> Any:
    """Convenience: build and run the mirror flow with playback=True.

    Returns a FlowHandle; call ``.wait()`` to get the result dict
    (``{node_name: node_output_dict}``).
    """
    ctx.playback = True
    replay_flow = build_replay_flow(ctx)
    return replay_flow.run(True)
