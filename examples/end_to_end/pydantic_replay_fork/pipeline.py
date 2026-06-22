"""Kitaru durable execution adapter for the PydanticAI support-copilot (multi-step).

R1 of the REVISED TASK SEQUENCE — 2026-06-22.

Structure
---------
The flow ``support_copilot`` is composed of three explicit ``@checkpoint`` step
functions, each wrapping a raw ``pydantic_ai.Agent``:

  gather_context(prompt, customer) -> dict
      Triage / classify the incoming request.  Returns a ``GatherResult`` dict.

  decide(gather_out: dict) -> dict
      Produce the ``SupportDecision`` from the triage result.  This is the CUT
      (intermediate step).  The decide step's system prompt switches when
      ``prompt_profile="trimmed_permissions"`` is supplied — reconfiguring it
      flips the decision.

  finalize(decide_out: dict) -> dict
      Assemble the customer-facing answer from the decision.  This is the
      single terminal step in the ZenML DAG.

Chaining via artifacts
----------------------
``gather_context`` produces a ZenML artifact that is passed directly as the
argument to ``decide``.  ``decide`` produces an artifact that is passed to
``finalize``.  The ZenML DAG therefore has exactly one terminal (``finalize``),
so ``flow.run(...).wait()`` succeeds without ``_MultipleTerminalStepsOutputError``.

Each ``@checkpoint`` body uses raw ``pydantic_ai.Agent``  (not ``KitaruAgent``).
Inside an explicit ``@checkpoint``, ``KitaruAgent`` is a passthrough, so the raw
agent is correct and avoids the double-wrapping footgun.

CUT
---
``CUT = "decide"`` is the fixed checkpoint name for the intermediate step.  Use
``cut_of(exec_id)`` to resolve it per-run (validates that the checkpoint exists).

decision_of
-----------
Reads the ``SupportDecision`` dict from the ``decide`` (preferred) or
``finalize`` checkpoint artifact.  Raises loudly if not found (Task 3 loud-
failure behavior preserved).

reproduce / diff
----------------
Carry over Task 4 semantics: ``reproduce`` replays from CUT; ``diff`` uses
``_drift.compare_decisions``.
"""
from __future__ import annotations

from typing import Any

from kitaru import flow, KitaruClient
from kitaru.checkpoint import checkpoint

from .agent import (
    SupportDecision,
    build_decide_agent,
    build_finalize_agent,
    build_gather_agent,
)


# ---------------------------------------------------------------------------
# Module-level CUT constant
# ---------------------------------------------------------------------------

#: The fixed checkpoint name for the intermediate decide step.
#: R1 spec: CUT = "decide".
CUT: str = "decide"


# ---------------------------------------------------------------------------
# KitaruAdapterPA
# ---------------------------------------------------------------------------

class KitaruAdapterPA:
    """Durable execution adapter for the three-step PydanticAI support-copilot.

    Args:
        model: A PydanticAI-compatible model (real or ``TestModel`` for tests).
            The *same* model is used for all three steps.  Pass different models
            per step by extending this adapter.
        prompt_profile: System-prompt profile (``"baseline"`` or
            ``"trimmed_permissions"``).  Drives the ``decide`` step's behaviour.
        name: Stable name prefix for the step agents.  Stored on ``self.name``.
    """

    def __init__(
        self,
        *,
        model: Any,
        prompt_profile: str = "baseline",
        name: str = "support_copilot",
    ) -> None:
        self.name = name
        self._client = KitaruClient()

        # Build per-step raw pydantic_ai.Agent objects.
        # Raw agents are correct here — @checkpoint provides the Kitaru boundary.
        _gather_agent = build_gather_agent(
            model,
            prompt_profile=prompt_profile,
            name=f"{name}_gather",
        )
        _decide_agent = build_decide_agent(
            model,
            prompt_profile=prompt_profile,
            name=f"{name}_decide",
        )
        _finalize_agent = build_finalize_agent(
            model,
            prompt_profile=prompt_profile,
            name=f"{name}_finalize",
        )

        # Define the three @checkpoint step functions as closures over the agents.
        # Each function name == checkpoint name in the ZenML DAG.

        @checkpoint
        def gather_context(prompt: str, customer: str) -> dict:  # type: ignore[return]
            """Triage / classify the incoming support request."""
            result = _gather_agent.run_sync(
                f"Customer: {customer}\nRequest: {prompt}"
            )
            return result.output.model_dump()

        @checkpoint
        def decide(gather_out: dict) -> dict:  # type: ignore[return]
            """Produce the SupportDecision from the triage result (the CUT)."""
            triage_summary = (
                f"intent={gather_out.get('intent', 'unknown')} "
                f"category={gather_out.get('category', 'general')} "
                f"triage={gather_out.get('triage', 'medium')}"
            )
            result = _decide_agent.run_sync(triage_summary)
            return result.output.model_dump()

        @checkpoint
        def finalize(decide_out: dict) -> dict:  # type: ignore[return]
            """Assemble the customer-facing answer (single terminal step)."""
            decision_summary = (
                f"policy_label={decide_out.get('policy_label', 'unknown')} "
                f"risk_status={decide_out.get('risk_status', 'unknown')} "
                f"required_action={decide_out.get('required_action', 'unknown')} "
                f"summary={decide_out.get('summary', '')!r}"
            )
            result = _finalize_agent.run_sync(decision_summary)
            out = result.output.model_dump()
            # Ensure all SupportDecision fields are propagated so decision_of
            # can read them from the finalize artifact as a fallback.
            for key in ("policy_label", "risk_status", "required_action", "summary"):
                if key not in out or not out[key]:
                    out[key] = decide_out.get(key, "unknown")
            return out

        # Build the @flow that chains the three checkpoints.
        # finalize is the single terminal — ZenML sees exactly one output.
        @flow(cache=False)
        def support_copilot_flow(prompt: str, customer: str) -> dict:  # type: ignore[return]
            gathered = gather_context(prompt, customer)
            decided = decide(gathered)
            return finalize(decided)

        self._flow = support_copilot_flow

        # Cache for decisions read at run() time.
        self._results: dict[str, dict] = {}

    # -----------------------------------------------------------------------
    # Public interface
    # -----------------------------------------------------------------------

    def run(self, prompt: str, customer: str) -> str:
        """Run the three-step flow and return the exec_id.

        Blocks until the execution finishes.  The flow result (from the
        ``finalize`` terminal) is cached by exec_id to enable the fast path
        in ``decision_of``.
        """
        handle = self._flow.run(prompt, customer)
        result = handle.wait()
        exec_id = handle.exec_id
        # The flow result is the finalize step's dict (contains SupportDecision
        # fields propagated by finalize).  Cache it for fast decision_of lookup.
        if isinstance(result, dict) and "risk_status" in result:
            self._results[exec_id] = result
        return exec_id

    def cut_of(self, exec_id: str) -> str:
        """Return the CUT checkpoint name for *exec_id*.

        In the multi-step flow, CUT is always ``"decide"`` (the intermediate
        checkpoint).  This method validates that the checkpoint exists and raises
        loudly if the flow structure has changed.

        Raises:
            RuntimeError: If no ``"decide"`` checkpoint exists.
        """
        run = self._client.executions.get(exec_id)
        names = [c.name for c in run.checkpoints]
        if CUT not in names:
            raise RuntimeError(
                f"CUT checkpoint {CUT!r} not found in execution {exec_id}. "
                f"Checkpoints present: {names}. "
                "The flow may not be using the multi-step @checkpoint structure."
            )
        return CUT

    def decision_of(self, exec_id: str) -> dict:
        """Return the ``SupportDecision`` dict for *exec_id*.

        Lookup order:
        1. In-memory cache (populated at ``run()`` time).
        2. ``decide`` checkpoint artifact (preferred; produced by the CUT step).
        3. ``finalize`` checkpoint artifact (fallback; contains propagated fields).

        Raises:
            RuntimeError: If the decision cannot be found via any path.
        """
        # Fast path: decision cached at run() time.
        cached = self._results.get(exec_id)
        if cached is not None:
            return cached

        run = self._client.executions.get(exec_id)

        # Helper: extract a SupportDecision dict from an artifact value.
        def _extract(val: Any) -> dict | None:
            if isinstance(val, dict) and "risk_status" in val:
                return val
            # SupportDecision object (from pydantic BaseModel).
            if isinstance(val, SupportDecision):
                return val.model_dump()
            model_dump = getattr(val, "model_dump", None)
            if callable(model_dump):
                dumped = model_dump()
                if isinstance(dumped, dict) and "risk_status" in dumped:
                    return dumped
            return None

        # Scan checkpoints in priority order: decide first, then finalize.
        priority = [CUT, "finalize"]
        cp_by_name = {c.name: c for c in run.checkpoints}
        for cp_name in priority:
            cp = cp_by_name.get(cp_name)
            if cp is None:
                continue
            for art in cp.artifacts:
                if getattr(art, "direction", None) not in (None, "output"):
                    continue
                try:
                    val = art.load()
                except Exception:  # noqa: BLE001
                    continue
                extracted = _extract(val)
                if extracted is not None:
                    return extracted

        raise RuntimeError(
            f"Could not extract a SupportDecision from execution {exec_id!r}. "
            f"Searched checkpoints: {priority}. "
            f"Checkpoints present: {[c.name for c in run.checkpoints]}. "
            "Ensure the flow completed successfully and the 'decide' checkpoint "
            "produced a serializable SupportDecision dict as its artifact."
        )

    def reproduce(self, exec_id: str) -> str:
        """Re-run from the CUT checkpoint without edits; return the replay exec_id.

        The ``gather_context`` head is served from cache; ``decide`` and
        ``finalize`` re-run under the same agent configuration.
        """
        handle = self._flow.replay(exec_id, from_=self.cut_of(exec_id), cache=False)
        result = handle.wait()
        replay_id = handle.exec_id
        if isinstance(result, dict) and "risk_status" in result:
            self._results[replay_id] = result
        return replay_id

    def diff(self, baseline_exec: str, other_exec: str):
        """Return a DriftReport comparing the two executions' SupportDecision dicts."""
        from kitaru.adapters.langgraph.replay._drift import DriftReport, compare_decisions

        base = self.decision_of(baseline_exec)
        other = self.decision_of(other_exec)
        return DriftReport(reproduction=[], fork=compare_decisions(base, other))
