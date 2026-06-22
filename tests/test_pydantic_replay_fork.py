"""Spike: validate PydanticAI fork mechanism and lock the CUT checkpoint selector.

Task 1 of the PydanticAI replay & fork demo (see
docs/superpowers/plans/2026-06-22-pydantic-replay-fork-demo.md).

Findings from the /tmp/pa_spike.py run:
- CUT selector pattern: ``{agent_name}_model_request``
- Mechanism A (fork-by-replay with a different agent) works.

Multi-step spike findings (2026-06-22,
docs/superpowers/notes/2026-06-22-pydantic-multistep-spike.md):
- Chosen structure: two explicit @checkpoint functions in one @flow (b2).
- CUT selector: ``"decide_step"`` (name of the second, terminal checkpoint).
- Replay from CUT: ``fork_flow.replay(exec_id, from_="decide_step", cache=False)``.
- Override first invocation of CUT's upstream: ``overrides={"checkpoint.gather_step": value}``.
- Global config change: build fork flow with a different agent closure.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pydantic_ai")

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

from kitaru import flow, KitaruClient
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.checkpoint import checkpoint


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


def test_build_agent_runs_and_returns_decision():
    """Task 2: PydanticAI support-copilot agent factory."""
    from pydantic_ai.models.test import TestModel
    import importlib, sys, pathlib
    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.agent import build_agent, SupportDeps, SupportDecision

    model = TestModel(custom_output_args={
        "policy_label": "permissions_policy", "risk_status": "needs_review",
        "required_action": "escalate_to_human", "summary": "s"})
    agent = build_agent(model, prompt_profile="baseline")
    out = agent.run_sync("Can I enable SSO?", deps=SupportDeps(customer="acme")).output
    assert isinstance(out, SupportDecision)
    assert out.risk_status == "needs_review"


def test_run_produces_durable_execution_with_call_checkpoints(primed_zenml):
    """Task 3: KitaruAdapterPA wraps the agent as a durable Kitaru execution.

    Asserts that the baseline run exposes a ``{agent_name}_model_request``
    checkpoint (the CUT) so Tasks 4/5 can replay/fork from it.  This assertion
    ensures the outer-checkpoint suppression regression cannot silently creep
    back in: if KitaruAgent is wrapped in an outer ``@checkpoint``, it becomes
    a passthrough and no ``_model_request`` checkpoint is recorded.
    """
    from pydantic_ai.models.test import TestModel
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.pipeline import KitaruAdapterPA
    from kitaru import KitaruClient

    model = TestModel(custom_output_args={
        "policy_label": "permissions_policy", "risk_status": "needs_review",
        "required_action": "escalate_to_human", "summary": "s"})
    adapter = KitaruAdapterPA(model=model)
    exec_id = adapter.run("Can I enable SSO?", customer="acme")
    run = KitaruClient().executions.get(exec_id)
    assert run.checkpoints                      # per-call checkpoints exist
    # CUT must be present: bare calls-strategy flow produces {agent_name}_model_request.
    # If this fails, an outer @checkpoint is suppressing the per-call checkpoints.
    assert any(c.name.endswith("_model_request") for c in run.checkpoints), (
        f"No '_model_request' checkpoint found. Checkpoints: {[c.name for c in run.checkpoints]}. "
        "Check that pipeline.py uses a bare @flow with no outer @checkpoint."
    )
    assert adapter.decision_of(exec_id)["risk_status"] == "needs_review"


def test_multistep_replay_from_intermediate_step(primed_zenml) -> None:
    """Multi-step spike: two @checkpoint functions chained in one @flow.

    Spike verdict (docs/superpowers/notes/2026-06-22-pydantic-multistep-spike.md):
    - Structure (b2): two sequential @checkpoint calls in @flow; each wraps a raw Agent.
    - CUT selector: ``"decide_step"`` (name of the second, terminal checkpoint).
    - Replay from CUT: gather_step served from cache; decide_step re-runs.
    - Override upstream step output: ``overrides={"checkpoint.gather_step": value}``
      injects a new dict into decide_step's input, composing with the global config change.

    NON-VACUOUS assertion: the locking assertion is ``received_triage == "critical"``
    (injected value reached decide) AND ``verdict == "reject"`` (fork agent drove it).
    If the override were silently dropped, decide would see ``triage='medium'`` from
    cache and the assertion would fail.  If the fork agent were not used, verdict would
    be ``'approved'`` and the assertion would fail.
    """
    del primed_zenml  # fixture used for ZenML store init side-effect

    # ---- helpers ----

    class _GatherOut(BaseModel):
        triage: str = "unknown"

    class _DecideOut(BaseModel):
        verdict: str = "pending"
        received_triage: str = "unset"

    def _build_flow(triage_val: str, verdict_val: str):
        """Build the two-@checkpoint flow with inline agent closures."""
        _gather_agent = Agent(
            TestModel(custom_output_args={"triage": triage_val}),
            name="ms_gather_agent",
            output_type=_GatherOut,
        )
        _decide_agent = Agent(
            TestModel(custom_output_args={"verdict": verdict_val, "received_triage": "n/a"}),
            name="ms_decide_agent",
            output_type=_DecideOut,
        )

        @checkpoint
        def gather_step(prompt: str) -> dict:  # type: ignore[return]
            return _gather_agent.run_sync(prompt).output.model_dump()

        @checkpoint
        def decide_step(triage_result: dict) -> dict:  # type: ignore[return]
            out = _decide_agent.run_sync(f"triage={triage_result['triage']}").output.model_dump()
            out["received_triage"] = triage_result["triage"]
            return out

        @flow(cache=False)
        def ms_flow(prompt: str) -> dict:  # type: ignore[return]
            gathered = gather_step(prompt)
            decided = decide_step(gathered)
            return decided

        return ms_flow

    # ---- baseline run ----
    base_flow = _build_flow("medium", "approved")
    base_handle = base_flow.run("analyze ticket")
    base_exec_id = base_handle.exec_id
    base_result = base_handle.wait()

    # Flow returns decide_step's output; gather_step's triage is recorded in received_triage.
    assert base_result == {"verdict": "approved", "received_triage": "medium"}, (
        f"Unexpected baseline result: {base_result!r}"
    )

    client = KitaruClient()
    base_run = client.executions.get(base_exec_id)
    cp_names = [c.name for c in base_run.checkpoints]

    assert cp_names == ["gather_step", "decide_step"], (
        f"Expected ['gather_step', 'decide_step']; got: {cp_names!r}. "
        "The two @checkpoint functions must produce a chained DAG."
    )

    CUT = "decide_step"

    # ---- fork: replay from CUT + inject override on gather + different agent ----
    # fork_flow uses a different agent: verdict="reject" (global config change)
    fork_flow = _build_flow("low", "reject")  # triage="low" never runs; "reject" is the fork model

    fork_handle = fork_flow.replay(
        base_exec_id,
        from_=CUT,
        cache=False,
        overrides={"checkpoint.gather_step": {"triage": "critical"}},  # inject new gather output
    )
    fork_exec_id = fork_handle.exec_id
    fork_result = fork_handle.wait()

    # gather was served from cache (skipped); decide re-ran under the fork agent
    # with the injected triage="critical" value.
    assert fork_result["received_triage"] == "critical", (
        f"Override not applied: decide_step should have received triage='critical', "
        f"got received_triage={fork_result.get('received_triage')!r}. "
        f"Full result: {fork_result!r}"
    )
    assert fork_result["verdict"] == "reject", (
        f"Fork agent not used: expected verdict='reject', got {fork_result.get('verdict')!r}. "
        f"Full result: {fork_result!r}"
    )

    # ---- lineage assertion (locks test to the replay path) ----
    fork_exec = client.executions.get(fork_exec_id)
    assert fork_exec.original_exec_id == base_exec_id, (
        f"Replay lineage broken: expected original_exec_id={base_exec_id!r}, "
        f"got original_exec_id={fork_exec.original_exec_id!r}. "
        "The fork did not run via the replay path."
    )


def test_reproduce_matches_original(primed_zenml):
    """Task 4: reproduce() re-runs from CUT; diff() reports no fork drift."""
    from pydantic_ai.models.test import TestModel
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.pipeline import KitaruAdapterPA

    model = TestModel(custom_output_args={"policy_label": "permissions_policy",
        "risk_status": "needs_review", "required_action": "escalate_to_human", "summary": "s"})
    adapter = KitaruAdapterPA(model=model)
    base = adapter.run("Can I enable SSO?", customer="acme")
    repro = adapter.reproduce(base)
    report = adapter.diff(base, repro)
    assert report.has_fork_drift is False   # repro vs base: no change
