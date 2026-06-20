"""KitaruReplayAgent: the end-to-end spine for LangGraph replay & fork.

Public contract::

    agent = KitaruReplayAgent(graph, fanout_node="...")
    case  = agent.import_trace(rows)
    seed  = agent.reconstruct(case)                       # seed exec_id
    repro = agent.replay(seed, from_=cut)                 # live tail, no edits
    fork  = agent.fork(seed, from_=cut, variant={...})    # live tail, forked
    report = agent.diff(case, repro, fork)                # DriftReport

``reconstruct`` builds a mirror @flow (one checkpoint per graph node) and runs
it in *playback* mode, returning recorded node outputs from the trace with zero
live calls.  ``replay`` re-executes the tail of that seed run *live* with
``flow.replay(from_=cut, cache=False, playback=False)`` — the head (skipped,
upstream of the cut) reuses the seed's recorded outputs, the tail runs the real
node callables.  ``fork`` rebuilds the flow with edits/variant and replays the
same seed run, so the forked tail re-executes under the edited configuration.

Drift semantics:

* reproduction drift = recorded (observed) decision  vs  replayed live decision
* fork drift         = replayed live decision         vs  forked live decision
"""
from __future__ import annotations

import warnings
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from types import SimpleNamespace
from typing import Any

from kitaru import KitaruClient
from kitaru._replay_verify_imported_models import ImportedReplayCase, RecordedCall
from kitaru.adapters.langgraph.replay._compiler import compile_topology
from kitaru.adapters.langgraph.replay._drift import DriftReport, compare_decisions
from kitaru.adapters.langgraph.replay._edits import Edit
from kitaru.adapters.langgraph.replay._flow import ReplayContext, build_replay_flow
from kitaru.adapters.langgraph.replay._importer import import_trace
from kitaru.adapters.langgraph.replay._protocol import LANGGRAPH_CAPS, Caps

_DECISION_NODES = ("decide_action", "final_response")


class _ReplayResult:
    """Lightweight result of a replay/fork: the exec_id plus the node-output map.

    The mirror flow's terminal checkpoint returns the complete
    ``{node_name: node_output}`` map, so the flow result already carries every
    node's live output — ``diff`` reads the decision straight off it without
    re-loading individual checkpoint artifacts.
    """

    __slots__ = ("exec_id", "node_outputs")

    def __init__(self, exec_id: str, node_outputs: Mapping[str, Any]) -> None:
        self.exec_id = exec_id
        self.node_outputs = dict(node_outputs)


class KitaruReplayAgent:
    """Reconstruct, replay, fork, and diff a LangGraph run from an imported trace."""

    def __init__(self, graph: object, *, fanout_node: str | None = None) -> None:
        self._graph = graph
        self._topology = compile_topology(graph, fanout_node=fanout_node)
        self._client = KitaruClient()
        self._flow_def: Any = None  # set during reconstruct
        self._seed_ctx: ReplayContext | None = None

    # ---- import ----------------------------------------------------------- #

    def import_trace(
        self, rows: Iterable[Mapping[str, Any]], *, trace_id: str | None = None
    ) -> ImportedReplayCase:
        return import_trace(rows, trace_id=trace_id)

    # ---- reconstruct (seed) ----------------------------------------------- #

    def reconstruct(
        self,
        case: ImportedReplayCase,
        *,
        root_state: Mapping[str, Any] | None = None,
    ) -> str:
        """Build the mirror flow and run it in playback mode; return seed exec_id.

        ``root_state`` carries the graph's ROOT input (scenario + variant) so the
        later live tail can run the real node callables.  When omitted it is
        reconstructed from the imported case via attribute-accessible proxies.
        """
        effective_root_state = (
            dict(root_state)
            if root_state is not None
            else _root_state_from_case(case)
        )
        ctx = ReplayContext(
            topology=self._topology,
            recorded_by_node=_recorded_by_node(case),
            node_output_by_node=_node_outputs_from_case(case, self._topology.nodes),
            playback=True,
            variant=None,
            edits=[],
            root_state=effective_root_state,
        )
        self._seed_ctx = ctx
        self._flow_def = build_replay_flow(ctx)
        handle = self._flow_def.run(True)
        handle.wait()
        return handle.exec_id

    # ---- reproduction replay (live tail, no edits) ------------------------ #

    def replay(self, seed_exec_id: str, *, from_: str) -> _ReplayResult:
        if self._flow_def is None:
            raise RuntimeError("reconstruct() must be called before replay().")
        handle = self._flow_def.replay(
            seed_exec_id, from_=from_, cache=False, playback=False
        )
        node_outputs = handle.wait()
        return _ReplayResult(handle.exec_id, _as_mapping(node_outputs))

    # ---- fork (live tail, edited configuration) --------------------------- #

    def fork(
        self,
        seed_exec_id: str,
        *,
        from_: str,
        edits: Sequence[Edit] = (),
        variant: dict[str, Any] | None = None,
    ) -> _ReplayResult:
        if self._seed_ctx is None:
            raise RuntimeError("reconstruct() must be called before fork().")
        # Rebuild the flow closure with edits/variant for the live tail.  The
        # seed run's recorded head is reused; only the tail re-executes, now
        # under the forked configuration.
        # NOTE: fork replays the seed execution through a STRUCTURALLY-IDENTICAL
        # REBUILT flow definition — Kitaru native replay matches by node names /
        # step ids, not object identity — so a fresh build_replay_flow(fork_ctx)
        # is correct to replay against the original seed's exec_id.
        fork_ctx = ReplayContext(
            topology=self._topology,
            recorded_by_node=self._seed_ctx.recorded_by_node,
            node_output_by_node=self._seed_ctx.node_output_by_node,
            playback=False,
            variant=variant,
            edits=list(edits),
            root_state=dict(self._seed_ctx.root_state),
        )
        fork_flow = build_replay_flow(fork_ctx)
        handle = fork_flow.replay(
            seed_exec_id, from_=from_, cache=False, playback=False
        )
        node_outputs = handle.wait()
        return _ReplayResult(handle.exec_id, _as_mapping(node_outputs))

    # ---- diff ------------------------------------------------------------- #

    def diff(
        self,
        case: ImportedReplayCase,
        replay_exec: _ReplayResult,
        fork_exec: _ReplayResult,
    ) -> DriftReport:
        trace_decision = _decision_of_observed(case)
        replay_decision = _decision_of_result(replay_exec)
        fork_decision = _decision_of_result(fork_exec)
        return DriftReport(
            reproduction=compare_decisions(trace_decision, replay_decision),
            fork=compare_decisions(replay_decision, fork_decision),
        )

    # ---- capabilities ----------------------------------------------------- #

    def capabilities(self) -> Caps:
        return LANGGRAPH_CAPS


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _recorded_by_node(case: ImportedReplayCase) -> dict[str, list[RecordedCall]]:
    grouped: dict[str, list[RecordedCall]] = defaultdict(list)
    for call in case.recorded_calls:
        grouped[call.node or call.name].append(call)
    return dict(grouped)


def _node_outputs_from_case(
    case: ImportedReplayCase, nodes: list[str]
) -> dict[str, Any]:
    """Recover each node's recorded output dict (used in playback mode).

    The terminal decision node carries the observed decision; other nodes get an
    empty delta.  Playback only needs the decision node to be correct because the
    seed run exists solely to give the later live replay a head to skip past.
    """
    observed = case.observed_output if isinstance(case.observed_output, Mapping) else {}
    decision = _decision_of_observed(case)
    outputs: dict[str, Any] = {n: {} for n in nodes}
    for node in nodes:
        if node in _DECISION_NODES and decision:
            outputs[node] = {"decision": dict(decision)}
    # Also surface the full observed output on the final node for completeness.
    if nodes and isinstance(observed, Mapping) and observed:
        final = nodes[-1]
        merged = dict(outputs.get(final, {}))
        merged.setdefault("final_output", dict(observed))
        if decision:
            merged["decision"] = dict(decision)
        outputs[final] = merged
    return outputs


def _decision_of_observed(case: ImportedReplayCase) -> dict[str, Any]:
    observed = case.observed_output
    if not isinstance(observed, Mapping):
        return {}
    decision = observed.get("decision")
    if isinstance(decision, Mapping):
        return dict(decision)
    # The observed output may itself be the decision (risk_status at top level).
    if "risk_status" in observed:
        return dict(observed)
    return {}


def _decision_of_result(result: _ReplayResult) -> dict[str, Any]:
    """Read the decision dict out of a replay/fork node-output map.

    Raises RuntimeError if node_outputs is empty or contains none of the known
    decision nodes with a non-empty decision — an empty result would make
    ``has_reproduction_drift`` vacuously False, defeating the whole test.
    """
    if not result.node_outputs:
        raise RuntimeError(
            f"Replay/fork result for exec_id={result.exec_id!r} has an empty "
            f"node_outputs map.  This likely means the replay flow returned an "
            f"unexpected shape (e.g. Kitaru's flow.replay() return contract "
            f"changed).  Cannot compute drift against an empty result."
        )

    for node in _DECISION_NODES:
        node_out = result.node_outputs.get(node)
        if not isinstance(node_out, Mapping):
            continue
        decision = node_out.get("decision")
        decision = _coerce_decision(decision)
        if decision:
            return decision
        # final_response may nest the decision under final_output.
        final_output = node_out.get("final_output")
        if isinstance(final_output, Mapping):
            nested = _coerce_decision(final_output.get("decision"))
            if nested:
                return nested

    present_nodes = sorted(result.node_outputs)
    raise RuntimeError(
        f"Replay/fork result for exec_id={result.exec_id!r} has node_outputs "
        f"({present_nodes}) but none of the decision nodes "
        f"{list(_DECISION_NODES)} contain a non-empty 'decision' key.  "
        f"Cannot compute drift — pass root_state= to reconstruct() or check "
        f"that the graph's decision node name matches _DECISION_NODES."
    )


def _coerce_decision(value: Any) -> dict[str, Any]:
    """Normalize a decision (pydantic model or mapping) into a plain dict."""
    if value is None:
        return {}
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump()
        return dict(dumped) if isinstance(dumped, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


# ---- root-state reconstruction from an imported case ---------------------- #


def _root_state_from_case(case: ImportedReplayCase) -> dict[str, Any]:
    """Rebuild attribute-accessible scenario + variant proxies from the case.

    The reference-agent node callables read ``state['scenario']`` and
    ``state['variant']`` via attribute access (and ``variant.allows_tool``).
    Imported traces only carry plain dicts, so we wrap them in proxies that
    expose the attributes/methods the callables need.  A fork edit overlays
    ``model`` / ``prompt_profile`` onto the variant proxy via attribute writes.
    """
    contract = getattr(case, "trace_contract", None)
    raw_config = dict(getattr(contract, "raw_config", {}) or {})
    root_input = case.root_input if isinstance(case.root_input, Mapping) else {}

    scenario = _scenario_proxy(case, raw_config, root_input)
    variant = _variant_proxy(raw_config)
    return {"scenario": scenario, "variant": variant}


def _scenario_proxy(
    case: ImportedReplayCase,
    raw_config: Mapping[str, Any],
    root_input: Mapping[str, Any],
) -> SimpleNamespace:
    return SimpleNamespace(
        scenario_id=str(
            root_input.get("scenario_id")
            or raw_config.get("scenario_id")
            or case.case_id
        ),
        case_id=str(
            root_input.get("case_id") or raw_config.get("case_id") or case.case_id
        ),
        scenario_set=str(raw_config.get("scenario_set") or "smoke"),
        title=str(raw_config.get("title") or ""),
        user_request=str(
            root_input.get("user_request") or raw_config.get("user_request") or ""
        ),
        customer_key=root_input.get("customer_key") or raw_config.get("customer_key"),
        expected_policy_label=str(raw_config.get("expected_policy_label") or "unknown"),
        expected_required_action=str(
            raw_config.get("expected_required_action") or "answer_directly"
        ),
        notes=str(raw_config.get("notes") or ""),
    )


class _VariantProxy(SimpleNamespace):
    """Variant proxy with the ``allows_tool`` method the agent callables use."""

    def allows_tool(self, name: str) -> bool:
        denied = getattr(self, "denied_tools", []) or []
        allowed = getattr(self, "allowed_tools", []) or []
        if name in denied:
            return False
        return not allowed or name in allowed


def _variant_proxy(raw_config: Mapping[str, Any]) -> _VariantProxy:
    missing: list[str] = []
    model_val = raw_config.get("model")
    if not model_val:
        missing.append("model")
        model_val = "gpt-5-mini"

    prompt_profile_val = raw_config.get("prompt_profile")
    if not prompt_profile_val:
        missing.append("prompt_profile")
        prompt_profile_val = "full_permissions"

    variant_name_val = raw_config.get("variant_name") or raw_config.get("name")
    if not variant_name_val:
        missing.append("variant_name")
        variant_name_val = "baseline"

    if missing:
        warnings.warn(
            f"Trace raw_config is missing field(s) {missing!r}; defaulting to "
            f"model={model_val!r}, prompt_profile={prompt_profile_val!r}, "
            f"variant_name={variant_name_val!r}.  These defaults may produce "
            f"incorrect reproduction.  Pass root_state= to reconstruct() to "
            f"override.",
            stacklevel=3,
        )

    return _VariantProxy(
        name=str(variant_name_val),
        model=str(model_val),
        prompt_profile=str(prompt_profile_val),
        tool_policy_name=str(
            raw_config.get("tool_policy_name") or "full-permission-policy"
        ),
        max_tool_calls=int(raw_config.get("max_tool_calls") or 6),
        allowed_tools=list(raw_config.get("allowed_tools") or []),
        denied_tools=list(raw_config.get("denied_tools") or []),
        dry_run_writes=bool(raw_config.get("dry_run_writes") or False),
    )
