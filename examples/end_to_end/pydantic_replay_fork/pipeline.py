"""Kitaru durable execution wrapper for the PydanticAI support-copilot agent.

Task 3 of the PydanticAI replay & fork demo.

Wraps ``build_agent`` with ``KitaruAgent(checkpoint_strategy="calls")`` inside
a bare ``@flow`` (no outer ``@checkpoint``).  Because the agent has no tools,
``KitaruAgent`` produces exactly ONE terminal checkpoint per run —
``{agent_name}_model_request`` — which is the CUT (checkpoint-under-test) that
Tasks 4/5 replay from.

Design rationale for the tool-free agent
-----------------------------------------
With tools registered, ``checkpoint_strategy="calls"`` creates sibling
checkpoints for each tool call *and* the final model request.  A bare ``@flow``
with multiple terminal checkpoints raises ``_MultipleTerminalStepsOutputError``
because the dynamic flow cannot pick a single output.  Wrapping the agent call
in an outer ``@checkpoint`` "fixes" this but turns KitaruAgent into a
passthrough, suppressing the per-call checkpoints and destroying the CUT.

The correct solution (chosen here) is to keep the agent tool-free.  Customer
context (plan, role) is folded into ``SupportDeps`` and injected into the prompt
so there is no need for a ``lookup_customer`` tool.  The result is a single
``{agent_name}_model_request`` checkpoint that is both the terminal and the CUT.
"""
from __future__ import annotations

from typing import Any

from kitaru import flow, KitaruClient
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

        # Bare @flow: no outer @checkpoint, so KitaruAgent's per-call
        # checkpoints are recorded normally.  The agent is tool-free, so there
        # is exactly one terminal checkpoint: ``{name}_model_request``.
        @flow(cache=False)
        def _run_agent(prompt: str, customer: str) -> dict:
            deps = SupportDeps(customer=customer)
            return wrapped.run_sync(prompt, deps=deps).output.model_dump()

        self._flow = _run_agent

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def run(self, prompt: str, customer: str) -> str:
        """Run the agent as a durable Kitaru flow and return the exec_id.

        Blocks until the execution finishes.  In a bare ``calls``-strategy
        flow the ``handle.wait()`` return value is the terminal checkpoint's
        stored ``ModelResponse`` (not the flow body's dict), so the cache is
        populated at run() time to enable the fast path in ``decision_of``.
        """
        handle = self._flow.run(prompt, customer)
        result = handle.wait()  # blocks until the execution finishes
        if isinstance(result, dict) and "risk_status" in result:
            self._results[handle.exec_id] = result
        return handle.exec_id

    def cut_of(self, exec_id: str) -> str:
        """Return the CUT checkpoint name for *exec_id*.

        The CUT is the checkpoint whose name ends with ``_model_request``
        (i.e. ``{agent_name}_model_request``).  This is robust against
        positional changes and matches the pattern validated by the Task 1 spike.

        Raises:
            RuntimeError: If no ``_model_request`` checkpoint exists.
        """
        run = self._client.executions.get(exec_id)
        for cp in run.checkpoints:
            if cp.name.endswith(CUT_SUFFIX):
                return cp.name
        names = [c.name for c in run.checkpoints]
        raise RuntimeError(
            f"No '{CUT_SUFFIX}' checkpoint found in execution {exec_id}. "
            f"Checkpoints present: {names}"
        )

    def decision_of(self, exec_id: str) -> dict:
        """Return the ``SupportDecision`` dict for *exec_id*.

        Scans the ``{agent_name}_model_request`` checkpoint artifact (the CUT)
        for the ``ModelResponse`` stored by ``KitaruAgent(checkpoint_strategy="calls")``.
        PydanticAI stores structured output as a ``final_result`` tool-call in the
        ``ModelResponse.parts``; this method extracts those args.

        Raises:
            RuntimeError: If the decision cannot be found in the cache or artifacts.
        """
        # Fast path: decision stored at run() time (e.g. from a prior explicit cache).
        cached = self._results.get(exec_id)
        if cached is not None:
            return cached

        # Scan the _model_request checkpoint artifact.
        # KitaruAgent(checkpoint_strategy="calls") stores the ModelResponse object
        # as the checkpoint output.  PydanticAI encodes structured output as a
        # ToolCallPart with tool_name="final_result" in ModelResponse.parts.
        run = self._client.executions.get(exec_id)
        for cp in run.checkpoints:
            if not cp.name.endswith(CUT_SUFFIX):
                continue
            for art in cp.artifacts:
                if art.direction != "output":
                    continue
                try:
                    val = art.load()
                except Exception:  # noqa: BLE001
                    continue
                # Direct dict with decision keys (e.g., saved explicitly).
                if isinstance(val, dict) and "risk_status" in val:
                    return val
                # Loaded ModelResponse object: extract final_result tool-call args.
                # ModelResponse.parts contains ToolCallPart objects with .tool_name
                # and .args attributes.
                parts = getattr(val, "parts", None)
                if parts is not None:
                    for part in parts:
                        args = getattr(part, "args", None)
                        if (
                            getattr(part, "tool_name", None) == "final_result"
                            and isinstance(args, dict)
                        ):
                            return args
                # Serialized ModelResponse dict (fallback for raw dict saves).
                if isinstance(val, dict) and "parts" in val:
                    for part in val.get("parts", []):
                        if (
                            isinstance(part, dict)
                            and part.get("part_kind") == "tool-call"
                            and part.get("tool_name") == "final_result"
                            and isinstance(part.get("args"), dict)
                        ):
                            return part["args"]
        raise RuntimeError(
            f"Could not extract a SupportDecision from execution {exec_id!r} "
            f"(no cached result, and no decision found in the CUT checkpoint artifact)."
        )
