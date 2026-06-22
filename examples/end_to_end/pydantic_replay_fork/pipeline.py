"""Kitaru durable execution wrapper for the PydanticAI support-copilot agent.

Task 3 of the PydanticAI replay & fork demo.

Wraps ``build_agent`` with ``KitaruAgent(checkpoint_strategy="calls")`` inside
a ``@flow`` so each run is a durable Kitaru execution.  The agent call is placed
inside an explicit ``@checkpoint`` so the flow has a single terminal and
``handle.wait()`` can extract the result cleanly.

When inside an explicit ``@checkpoint``, ``KitaruAgent`` is a passthrough (per
the adapter docs); the ``checkpoint_strategy="calls"`` attribute is carried by the
``KitaruAgent`` but the outer explicit checkpoint is the one actually recorded.
``cut_of`` returns the name of the last (and only) checkpoint, which is
``"run_support_copilot"`` — the explicit checkpoint name — rather than the
``{agent_name}_model_request`` pattern from a bare ``@flow``.

Decision readback: ``handle.wait()`` returns the ``@checkpoint`` return value
(the ``SupportDecision`` dict) which is stored keyed by exec_id at run time.

CUT selector constant: ``CUT_SUFFIX`` is kept as the spike-verified suffix so
Tasks 4/5 can locate the right checkpoint when replaying a bare-flow run.
"""
from __future__ import annotations

from typing import Any

from kitaru import checkpoint, flow, KitaruClient
from kitaru.adapters.pydantic_ai import KitaruAgent

from .agent import build_agent, SupportDeps


#: Checkpoint name suffix that identifies the model-call boundary (CUT).
#: Confirmed by the Task 1 spike: last checkpoint of a bare ``calls``-strategy
#: run follows the pattern ``{agent_name}_model_request``.
CUT_SUFFIX = "_model_request"


class KitaruAdapterPA:
    """Durable execution adapter for the PydanticAI support-copilot.

    Args:
        model: A PydanticAI-compatible model (or ``TestModel`` for tests).
        prompt_profile: Which system-prompt profile to use (default ``"baseline"``).
        name: Stable agent name; stored on ``self.name`` for use by later tasks.
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
        self._results: dict[str, dict] = {}  # exec_id -> SupportDecision dict

        self._wrapped = KitaruAgent(
            build_agent(model, prompt_profile=prompt_profile, name=name),
            checkpoint_strategy="calls",
        )

        # Capture self for the closure.
        wrapped = self._wrapped

        # Wrap the agent call in an explicit @checkpoint so the flow has a
        # single terminal node and handle.wait() can extract the result.
        # Inside an explicit @checkpoint, KitaruAgent becomes a passthrough.
        @checkpoint(type="llm_call")
        def run_support_copilot(prompt: str, customer: str) -> dict:
            return wrapped.run_sync(prompt, deps=SupportDeps(customer=customer)).output.model_dump()

        @flow(cache=False)
        def _run_agent(prompt: str, customer: str) -> dict:
            return run_support_copilot(prompt, customer)

        self._flow = _run_agent

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def run(self, prompt: str, customer: str) -> str:
        """Run the agent as a durable Kitaru flow and return the exec_id.

        Blocks until the execution finishes.  The flow return value
        (``SupportDecision.model_dump()``  dict) is cached internally so that
        ``decision_of`` can return it without a second round-trip.
        """
        handle = self._flow.run(prompt, customer)
        result = handle.wait()  # blocks; returns the dict from _call_agent
        exec_id = handle.exec_id
        if isinstance(result, dict):
            self._results[exec_id] = result
        return exec_id

    def cut_of(self, exec_id: str) -> str:
        """Return the CUT checkpoint name for *exec_id*.

        The CUT is the last checkpoint in the execution.

        For a bare ``calls``-strategy flow (Tasks 4/5 replay), the last
        checkpoint follows the pattern ``{agent_name}_model_request``.
        For this task's explicit-checkpoint flow, the last checkpoint is
        ``"run_support_copilot"``.

        Raises:
            RuntimeError: If the execution has no checkpoints.
        """
        run = self._client.executions.get(exec_id)
        names = [c.name for c in run.checkpoints]
        if not names:
            raise RuntimeError(f"execution {exec_id} has no checkpoints")
        return names[-1]

    def decision_of(self, exec_id: str) -> dict:
        """Return the ``SupportDecision`` dict for *exec_id*.

        Primary path: the dict cached at ``run()`` time (fastest, no I/O).
        Fallback: scan terminal checkpoint artifacts for the decision dict.

        Returns an empty dict if the decision cannot be found.
        """
        # Fast path: decision stored at run() time.
        cached = self._results.get(exec_id)
        if cached is not None:
            return cached

        # Fallback: read from execution artifacts (e.g., if this adapter
        # instance was not the one that ran the execution).
        run = self._client.executions.get(exec_id)
        for cp in reversed(run.checkpoints):
            for art in cp.artifacts:
                try:
                    val = art.load()
                except Exception:  # noqa: BLE001
                    continue
                if isinstance(val, dict) and "risk_status" in val:
                    return val
        return {}
