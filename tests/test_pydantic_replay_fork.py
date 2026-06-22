"""Spike: validate PydanticAI fork mechanism and lock the CUT checkpoint selector.

Task 1 of the PydanticAI replay & fork demo (see
docs/superpowers/plans/2026-06-22-pydantic-replay-fork-demo.md).

Findings from the /tmp/pa_spike.py run:
- CUT selector pattern: ``{agent_name}_model_request``
- Mechanism A (fork-by-replay with a different agent) works.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pydantic_ai")

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

from kitaru import flow, KitaruClient
from kitaru.adapters.pydantic_ai import KitaruAgent


class _Decision(BaseModel):
    risk_status: str = "unknown"


def _make_flow(agent_name: str, risk: str):
    """Build a flow wrapping a KitaruAgent(calls strategy) returning a _Decision."""
    inner = Agent(
        TestModel(custom_output_args={"risk_status": risk}),
        name=agent_name,
        output_type=_Decision,
    )
    wrapped = KitaruAgent(inner, checkpoint_strategy="calls")

    @flow(cache=False)
    def run_agent(prompt: str) -> dict:
        return wrapped.run_sync(prompt).output.model_dump()

    return run_agent


def test_fork_by_replay_reexecutes_tail_under_new_agent(primed_zenml) -> None:
    """Mechanism A: replay from CUT using a fork flow re-runs the tail under the new agent.

    Spike verdict: A.
    CUT selector: ``{agent_name}_model_request`` (last checkpoint of a calls-strategy run).
    """
    del primed_zenml  # fixture used for ZenML store init side-effect

    # --- baseline run ---
    base_flow = _make_flow("forktest_agent", "needs_review")
    base_handle = base_flow.run("a permission request")
    base_exec_id = base_handle.exec_id
    # Wait for completion (ignore return value — it may be a ModelResponse from the
    # terminal calls-strategy checkpoint rather than the dict from the flow body)
    base_handle.wait()

    # --- discover checkpoint names ---
    client = KitaruClient()
    run = client.executions.get(base_exec_id)
    checkpoint_names = [c.name for c in run.checkpoints]

    assert checkpoint_names, "Expected at least one checkpoint on calls-strategy run"
    # The decision checkpoint follows the pattern: {agent_name}_model_request
    assert any("model_request" in name for name in checkpoint_names), (
        f"Expected a '*_model_request' checkpoint; got: {checkpoint_names}"
    )

    # CUT = the last checkpoint (the decision / model-call checkpoint)
    cut = checkpoint_names[-1]
    assert cut == "forktest_agent_model_request", (
        f"Unexpected CUT selector: {cut!r}. "
        "Update the constant in this test and in docs/superpowers/notes/."
    )

    # --- fork-by-replay (mechanism A) ---
    fork_flow = _make_flow("forktest_agent", "safe")
    fork_handle = fork_flow.replay(base_exec_id, from_=cut, cache=False)
    fork_exec_id = fork_handle.exec_id
    fork_result = fork_handle.wait()

    # The fork re-ran the model-call checkpoint under the fork agent.
    # The result from wait() is the ModelResponse stored by the calls-strategy
    # checkpoint; the args dict contains the Decision fields set by TestModel.
    # Verify the fork agent's output appears in the result.
    result_text = str(fork_result)
    assert "safe" in result_text, (
        f"Fork result did not contain 'safe'; fork did not re-run under the new agent. "
        f"Got: {fork_result!r}"
    )
    assert "needs_review" not in result_text, (
        f"Fork result still shows baseline 'needs_review'. Got: {fork_result!r}"
    )

    # --- lineage assertion (locks test to the replay path) ---
    # A fork-by-replay execution must record the source execution in original_exec_id.
    # This assertion is NON-VACUOUS: if the mechanism degraded to a fresh run (no
    # replay lineage), original_exec_id would be None and this assertion would fail.
    fork_exec = client.executions.get(fork_exec_id)
    assert fork_exec.original_exec_id == base_exec_id, (
        f"Replay lineage broken: expected original_exec_id={base_exec_id!r}, "
        f"got original_exec_id={fork_exec.original_exec_id!r}. "
        "The fork did not run via the replay path (mechanism A)."
    )
