"""High-level surface over the LangGraph replay machinery.

Keeps the call site as small as the PRD sketch:

    from kitaru.adapters.langgraph.replay import KitaruAdapter, import_langgraph_trace

    agent  = KitaruAdapter(graph, cut="decide")        # wrap your graph
    case   = import_langgraph_trace("langfuse:<id>")   # a recorded run you can fork
    replay = agent.replay(case)                        # cached head, live tail
    fork   = agent.fork(case, model="gpt-5-nano").run()# branch + edit, run forward
    fork.diff(replay)                                  # compare -> drift report

``cut`` is a node-level fork point (call-level cuts/edits are not built yet).
Agents whose state is JSON-native need no ``rehydrate``; agents with typed
(pydantic) state pass a hook that returns ``(root_state, node_outputs)``.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any

from kitaru.adapters.langgraph.replay._agent import (
    KitaruReplayAgent,
    _decision_from_replay_result,
    _recorded_decision_from_case,
)
from kitaru.adapters.langgraph.replay._drift import DriftReport, compare_decisions
from kitaru.adapters.langgraph.replay._importer import import_trace

RehydrateHook = Callable[[Any], "tuple[dict | None, dict | None]"]


def import_langgraph_trace(
    ref: str | None = None,
    *,
    rows: Any = None,
    trace_id: str | None = None,
) -> Any:
    """Import a recorded LangGraph run as a forkable Case.

    Pass ``"langfuse:<trace_id>"`` to fetch the rich observation rows live, or
    ``rows=`` if you already have them. Pass ``trace_id=`` when a row set
    contains multiple Langfuse traces.
    """
    if ref is not None and rows is not None:
        raise ValueError("Pass either ref or rows, not both.")
    if rows is not None:
        return import_trace(list(rows), trace_id=trace_id)
    if isinstance(ref, str) and ref.startswith("langfuse:"):
        ref_trace_id = ref.split(":", 1)[1]
        if trace_id is not None and trace_id != ref_trace_id:
            raise ValueError(
                "trace_id must match the langfuse:<id> reference; "
                f"got {trace_id!r} for {ref!r}."
            )
        return import_trace(
            _fetch_langfuse_rows(ref_trace_id),
            trace_id=ref_trace_id,
        )
    raise ValueError("Pass rows=… or a 'langfuse:<trace_id>' reference.")


def _fetch_langfuse_rows(trace_id: str, *, timeout: float = 120.0) -> list[dict]:
    """Poll Langfuse until the trace is fully ingested (count stops growing)."""
    from langfuse import Langfuse

    client = Langfuse()
    deadline = time.monotonic() + timeout
    rows: list[dict] = []
    last = -1
    while time.monotonic() < deadline:
        resp = client.api.observations.get_many(
            trace_id=trace_id,
            limit=100,
            fields="core,basic,io,metadata,model,usage",
        )
        rows = [json.loads(obs.model_dump_json()) for obs in resp.data]
        if rows and len(rows) == last:
            return rows  # stable across two polls -> ingestion done
        last = len(rows)
        time.sleep(3)
    if not rows:
        raise SystemExit(f"No rows for trace {trace_id} after {timeout:.0f}s.")
    return rows


class _RunResult:
    """One replay/fork execution; ``.diff(other)`` compares against another run."""

    def __init__(self, adapter: KitaruAdapter, case: Any, result: Any) -> None:
        self._adapter = adapter
        self._case = case
        self.result = result

    @property
    def decision(self) -> dict:
        """The decision this run produced."""
        return _decision_from_replay_result(self.result)

    @property
    def original_decision(self) -> dict:
        """The decision recorded in the imported trace."""
        return _recorded_decision_from_case(self._case)

    def diff(self, other: _RunResult) -> DriftReport:
        # self is the fork, `other` is the unchanged replay (PRD: fork.diff(replay)).
        return self._adapter._agent.diff(self._case, other.result, self.result)

    def vs_trace(self) -> DriftReport:
        """Compare this run against the original recorded trace (reproduction)."""
        return DriftReport(
            reproduction=compare_decisions(self.original_decision, self.decision),
            fork=[],
        )


class _Fork:
    """A pending fork; call ``.run()`` to execute it."""

    def __init__(
        self,
        adapter: KitaruAdapter,
        case: Any,
        *,
        at: str | None,
        variant: dict[str, Any],
    ) -> None:
        self._adapter = adapter
        self._case = case
        self._at = at
        self._variant = variant

    def run(self) -> _RunResult:
        seed = self._adapter._seed(self._case)
        cut = self._at or self._adapter._cut
        result = self._adapter._agent.fork(seed, at=cut, variant=self._variant or None)
        return _RunResult(self._adapter, self._case, result)


class KitaruAdapter:
    """Wrap a compiled LangGraph graph; replay and fork recorded runs of it."""

    def __init__(
        self,
        graph: Any,
        *,
        cut: str,
        fanout_node: str | None = None,
        rehydrate: RehydrateHook | None = None,
    ) -> None:
        self._agent = KitaruReplayAgent(graph, fanout_node=fanout_node)
        self._cut = cut
        self._rehydrate = rehydrate
        self._seeds: dict[int, str] = {}

    def _seed(self, case: Any) -> str:
        key = id(case)
        if key not in self._seeds:
            root_state, node_outputs = (
                self._rehydrate(case) if self._rehydrate else (None, None)
            )
            self._seeds[key] = self._agent.reconstruct(
                case, root_state=root_state, node_outputs=node_outputs
            )
        return self._seeds[key]

    def replay(self, case: Any, *, at: str | None = None) -> _RunResult:
        """Reproduce the run: cached before the cut, live after — no edits."""
        result = self._agent.replay(self._seed(case), at=at or self._cut)
        return _RunResult(self, case, result)

    def fork(self, case: Any, *, at: str | None = None, **edits: Any) -> _Fork:
        """Branch at the cut and change global config (e.g. ``model=…``)."""
        return _Fork(
            self, case, at=at, variant={k: v for k, v in edits.items() if v is not None}
        )
