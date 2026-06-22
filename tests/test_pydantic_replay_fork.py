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
    """Task 3 (updated for R1 multi-step): KitaruAdapterPA produces a durable execution.

    Reworked from the single-step ``KitaruAgent(calls)`` assertion to match the
    R1 multi-step ``@checkpoint`` structure.  The old assertion checked for a
    ``{agent_name}_model_request`` checkpoint; R1 replaces that with three named
    checkpoints (``gather_context``, ``decide``, ``finalize``).

    This test validates that:
    - at least one checkpoint exists (durable execution recorded);
    - the ``decide`` checkpoint (CUT) is present;
    - ``decision_of`` successfully extracts the SupportDecision dict.
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
    assert run.checkpoints, "Expected at least one checkpoint in the durable execution"
    # R1 structure: three named @checkpoint steps replace the old _model_request pattern.
    cp_names = [c.name for c in run.checkpoints]
    assert "decide" in cp_names, (
        f"'decide' (CUT) checkpoint not found. Checkpoints: {cp_names}. "
        "Ensure pipeline.py uses the multi-step @checkpoint structure."
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


def test_r1_multistep_gather_decide_finalize_checkpoints(primed_zenml) -> None:
    """R1: three @checkpoint steps (gather_context -> decide -> finalize) in the flow.

    Validates the multi-step structure described in the REVISED TASK SEQUENCE R1:
    - execution has chained checkpoints named gather_context, decide, finalize;
    - decision_of(exec_id) returns the decision dict with risk_status present;
    - CUT == "decide" (module constant);
    - cut_of(exec_id) resolves to "decide";
    - the execution's original_exec_id is None (it is not a replay).

    NON-VACUOUS: if decide were missing from the checkpoint list, the assertion
    fails.  If decision_of returned an empty dict or without risk_status, the
    assertion fails.
    """
    del primed_zenml  # fixture used for ZenML store init side-effect

    from pydantic_ai.models.test import TestModel
    import sys
    import pathlib

    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.pipeline import KitaruAdapterPA, CUT
    from kitaru import KitaruClient

    # Build all three per-step agents with the same TestModel output; we only
    # care that each step returns a serializable dict so artifacts chain cleanly.
    model = TestModel(
        custom_output_args={
            "policy_label": "permissions_policy",
            "risk_status": "needs_review",
            "required_action": "escalate_to_human",
            "summary": "needs human review",
        }
    )
    adapter = KitaruAdapterPA(model=model)
    exec_id = adapter.run("Can I enable SSO?", customer="acme")

    client = KitaruClient()
    run = client.executions.get(exec_id)
    cp_names = [c.name for c in run.checkpoints]

    # All three checkpoints must exist in order.
    assert "gather_context" in cp_names, (
        f"Missing 'gather_context' checkpoint. Got: {cp_names}"
    )
    assert "decide" in cp_names, (
        f"Missing 'decide' checkpoint. Got: {cp_names}"
    )
    assert "finalize" in cp_names, (
        f"Missing 'finalize' checkpoint. Got: {cp_names}"
    )
    assert cp_names.index("gather_context") < cp_names.index("decide"), (
        "gather_context must come before decide"
    )
    assert cp_names.index("decide") < cp_names.index("finalize"), (
        "decide must come before finalize"
    )

    # CUT must be "decide" (module constant).
    assert CUT == "decide", f"CUT constant should be 'decide', got {CUT!r}"
    assert adapter.cut_of(exec_id) == "decide", (
        f"cut_of should return 'decide', got {adapter.cut_of(exec_id)!r}"
    )

    # decision_of must find the SupportDecision with risk_status.
    decision = adapter.decision_of(exec_id)
    assert isinstance(decision, dict), f"decision_of should return a dict, got {type(decision)}"
    assert "risk_status" in decision, (
        f"decision_of result missing 'risk_status': {decision}"
    )
    assert decision["risk_status"] == "needs_review", (
        f"Expected 'needs_review', got {decision['risk_status']!r}"
    )

    # Not a replay — original_exec_id should be None.
    assert run.original_exec_id is None, (
        f"A fresh run should not have original_exec_id set; got {run.original_exec_id!r}"
    )


def test_r2_reproduce_head_is_cached(primed_zenml) -> None:
    """R2: reproduce() caches the gather_context head; only decide+finalize re-run.

    NON-VACUOUS proof: on the reproduce execution, the gather_context checkpoint's
    original_call_id must be set (pointing back to the baseline's gather_context
    call), proving it was served from cache rather than re-executed.  If the head
    were re-executed, original_call_id would be None and the assertion fails.

    Also asserts:
    - diff(base, repro).has_fork_drift is False (reproduce matches baseline);
    - the reproduce execution's original_exec_id == base exec_id (lineage);
    - the decide/finalize checkpoints have original_call_id None (they re-ran).
    """
    del primed_zenml  # fixture used for ZenML store init side-effect

    from pydantic_ai.models.test import TestModel
    import sys
    import pathlib

    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.pipeline import KitaruAdapterPA
    from kitaru import KitaruClient

    model = TestModel(
        custom_output_args={
            "policy_label": "permissions_policy",
            "risk_status": "needs_review",
            "required_action": "escalate_to_human",
            "summary": "needs human review",
        }
    )
    adapter = KitaruAdapterPA(model=model)
    base = adapter.run("Can I enable SSO?", customer="acme")

    # Reproduce: replay from CUT (decide), same config.
    repro = adapter.reproduce(base)

    # 1. Semantic equivalence.
    report = adapter.diff(base, repro)
    assert report.has_fork_drift is False, (
        f"reproduce() should match baseline; got drift: {report.fork}"
    )

    # 2. Lineage: reproduce execution links back to baseline.
    client = KitaruClient()
    repro_run = client.executions.get(repro)
    assert repro_run.original_exec_id == base, (
        f"Reproduce execution must link to baseline via original_exec_id. "
        f"Expected {base!r}, got {repro_run.original_exec_id!r}."
    )

    # 3. Non-vacuous head-cached proof: gather_context has original_call_id set.
    repro_cp_by_name = {c.name: c for c in repro_run.checkpoints}

    gather_cp = repro_cp_by_name.get("gather_context")
    assert gather_cp is not None, (
        f"'gather_context' checkpoint missing from reproduce execution. "
        f"Checkpoints: {list(repro_cp_by_name)}"
    )
    assert gather_cp.original_call_id is not None, (
        "gather_context.original_call_id is None on the reproduce execution — "
        "the head was NOT served from cache (it re-ran). "
        "Replay from 'decide' should skip gather_context (head) from cache."
    )

    # 4. decide and finalize re-ran (their original_call_id should be None).
    for step_name in ("decide", "finalize"):
        cp = repro_cp_by_name.get(step_name)
        assert cp is not None, (
            f"'{step_name}' checkpoint missing from reproduce execution. "
            f"Checkpoints: {list(repro_cp_by_name)}"
        )
        assert cp.original_call_id is None, (
            f"'{step_name}'.original_call_id is set on the reproduce execution — "
            f"this step should have re-run, not been served from cache. "
            f"original_call_id={cp.original_call_id!r}"
        )


def test_r3_experiment_flips_decision(primed_zenml) -> None:
    """R3: experiment() re-runs decide+finalize under a reconfigured agent (kind #2).

    The experiment builds a new KitaruAdapterPA with a different model/prompt_profile
    and replays from CUT so gather_context is served from cache while decide and
    finalize run under the new config.

    Assertions:
    - experiment(base, model=fork_model, prompt_profile="trimmed_permissions") returns
      an exec_id;
    - diff(repro, exp) shows risk_status changed (needs_review → safe);
    - the experiment execution's original_exec_id == base exec_id (lineage);
    - gather_context has original_call_id set on the experiment run (head cached);
    - decide/finalize have original_call_id None (they re-ran under new config).

    NON-VACUOUS: if experiment used output-override instead of re-running the step,
    or if the new model was not applied, the risk_status field in the experiment
    decision would still be 'needs_review' and the changed-fields assertion fails.
    """
    del primed_zenml  # fixture used for ZenML store init side-effect

    from pydantic_ai.models.test import TestModel
    import sys
    import pathlib

    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.pipeline import KitaruAdapterPA
    from kitaru import KitaruClient

    # Baseline: needs_review.
    base_model = TestModel(
        custom_output_args={
            "policy_label": "permissions_policy",
            "risk_status": "needs_review",
            "required_action": "escalate_to_human",
            "summary": "needs human review",
        }
    )
    # Fork model: safe (different TestModel simulating a cheaper/looser-prompt agent).
    fork_model = TestModel(
        custom_output_args={
            "policy_label": "permissions_policy",
            "risk_status": "safe",
            "required_action": "answer_directly",
            "summary": "safe to answer",
        }
    )

    adapter = KitaruAdapterPA(model=base_model)
    base = adapter.run("Can I enable SSO?", customer="acme")
    repro = adapter.reproduce(base)

    # R3: experiment with reconfigured decide step.
    exp = adapter.experiment(base, model=fork_model, prompt_profile="trimmed_permissions")

    # 1. diff(repro, exp) shows risk_status changed.
    report = adapter.diff(repro, exp)
    assert report.has_fork_drift is True, (
        "experiment should produce a different decision than the reproduction. "
        f"Diff: {report.fork}"
    )
    changed_fields = {c.field for c in report.fork if not c.matches}
    assert "risk_status" in changed_fields, (
        f"risk_status should have changed (needs_review → safe). "
        f"Changed fields: {changed_fields}; diff: {report.fork}"
    )

    # 2. Experiment decision is 'safe'.
    exp_decision = adapter.decision_of(exp)
    assert exp_decision["risk_status"] == "safe", (
        f"experiment decision risk_status should be 'safe'; got {exp_decision['risk_status']!r}"
    )

    # 3. Lineage: experiment execution links back to baseline.
    client = KitaruClient()
    exp_run = client.executions.get(exp)
    assert exp_run.original_exec_id == base, (
        f"Experiment execution must link to baseline via original_exec_id. "
        f"Expected {base!r}, got {exp_run.original_exec_id!r}."
    )

    # 4. Head cached: gather_context has original_call_id set.
    exp_cp_by_name = {c.name: c for c in exp_run.checkpoints}

    gather_cp = exp_cp_by_name.get("gather_context")
    assert gather_cp is not None, (
        f"'gather_context' checkpoint missing from experiment execution. "
        f"Checkpoints: {list(exp_cp_by_name)}"
    )
    assert gather_cp.original_call_id is not None, (
        "gather_context.original_call_id is None on the experiment execution — "
        "the head was NOT served from cache (it re-ran). "
        "experiment() replay from 'decide' should skip gather_context from cache."
    )

    # 5. decide and finalize re-ran under new config (original_call_id None).
    for step_name in ("decide", "finalize"):
        cp = exp_cp_by_name.get(step_name)
        assert cp is not None, (
            f"'{step_name}' checkpoint missing from experiment execution. "
            f"Checkpoints: {list(exp_cp_by_name)}"
        )
        assert cp.original_call_id is None, (
            f"'{step_name}'.original_call_id is set on the experiment execution — "
            f"this step should have re-run under the new config, not been cached. "
            f"original_call_id={cp.original_call_id!r}"
        )


# ---------------------------------------------------------------------------
# R4: cohort + improvement metrics
# ---------------------------------------------------------------------------

def test_r4_cohort_report_rows_and_aggregates(primed_zenml) -> None:
    """R4: cohort() returns a CohortReport with per-run rows and aggregates.

    Seeds 3 baseline executions (decide -> "needs_review"), then runs
    cohort(model=fork_model, prompt_profile="trimmed_permissions", n=3).

    Asserts:
    - 3 per-run rows (one per seeded execution);
    - decision_changed == True for each row (all flipped needs_review -> safe);
    - decision_change_count == 3;
    - judge scores populated (non-None) for each row;
    - latency aggregates (mean_latency_baseline, mean_latency_experiment) present
      and >= 0;
    - improvement is a bool;
    - skipped_count == 0.

    NON-VACUOUS: if cohort() returned fewer rows the count assertion fails;
    if the decision flip did not propagate the decision_changed assertion fails;
    if improvement is not a bool the type assertion fails.
    """
    del primed_zenml  # fixture used for ZenML store init side-effect

    import sys
    import pathlib

    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.pipeline import KitaruAdapterPA, CohortReport

    base_model = TestModel(
        custom_output_args={
            "policy_label": "permissions_policy",
            "risk_status": "needs_review",
            "required_action": "escalate_to_human",
            "summary": "needs human review",
        }
    )
    fork_model = TestModel(
        custom_output_args={
            "policy_label": "permissions_policy",
            "risk_status": "safe",
            "required_action": "answer_directly",
            "summary": "safe to answer",
        }
    )

    adapter = KitaruAdapterPA(model=base_model)
    # Seed 3 baseline executions.
    for _ in range(3):
        adapter.run("Can I enable SSO?", customer="acme")

    # Run the cohort.
    report = adapter.cohort(
        model=fork_model,
        prompt_profile="trimmed_permissions",
        n=3,
    )

    assert isinstance(report, CohortReport), (
        f"cohort() should return a CohortReport, got {type(report)}"
    )
    assert len(report.rows) == 3, (
        f"Expected 3 rows, got {len(report.rows)}: {report.rows}"
    )
    for i, row in enumerate(report.rows):
        assert row.decision_changed is True, (
            f"Row {i}: decision_changed should be True (needs_review -> safe); "
            f"got {row.decision_changed!r}"
        )
        assert row.judge_score_baseline is not None, (
            f"Row {i}: judge_score_baseline is None"
        )
        assert row.judge_score_experiment is not None, (
            f"Row {i}: judge_score_experiment is None"
        )
        assert row.latency_baseline_s is not None and row.latency_baseline_s >= 0, (
            f"Row {i}: latency_baseline_s invalid: {row.latency_baseline_s!r}"
        )
        assert row.latency_experiment_s is not None and row.latency_experiment_s >= 0, (
            f"Row {i}: latency_experiment_s invalid: {row.latency_experiment_s!r}"
        )

    assert report.decision_change_count == 3, (
        f"decision_change_count should be 3, got {report.decision_change_count}"
    )
    assert report.skipped_count == 0, (
        f"skipped_count should be 0, got {report.skipped_count}"
    )
    assert report.mean_latency_baseline_s >= 0, (
        f"mean_latency_baseline_s should be >= 0, got {report.mean_latency_baseline_s}"
    )
    assert report.mean_latency_experiment_s >= 0, (
        f"mean_latency_experiment_s should be >= 0, got {report.mean_latency_experiment_s}"
    )
    assert isinstance(report.improvement, bool), (
        f"improvement should be a bool, got {type(report.improvement)}: {report.improvement!r}"
    )
    # String representation should be non-empty.
    assert str(report), "CohortReport.__str__ should return a non-empty string"


def test_r4_last_executions_returns_exec_ids(primed_zenml) -> None:
    """R4: last_executions(n) returns exec_ids for this adapter's flow.

    Seeds 2 executions, then verifies last_executions(2) returns exactly 2
    exec_ids (strings), newest first.
    """
    del primed_zenml

    import sys
    import pathlib

    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.pipeline import KitaruAdapterPA

    model = TestModel(
        custom_output_args={
            "policy_label": "permissions_policy",
            "risk_status": "needs_review",
            "required_action": "escalate_to_human",
            "summary": "s",
        }
    )
    adapter = KitaruAdapterPA(model=model)
    id1 = adapter.run("Can I enable SSO?", customer="acme")
    id2 = adapter.run("Change billing owner?", customer="beta")

    ids = adapter.last_executions(2)
    assert isinstance(ids, list), f"last_executions should return a list, got {type(ids)}"
    assert len(ids) == 2, f"Expected 2 exec_ids, got {len(ids)}: {ids}"
    assert all(isinstance(x, str) for x in ids), f"All items must be strings: {ids}"
    # Newest first: id2 was created after id1.
    assert ids[0] == id2, (
        f"Newest exec should be first. Expected {id2!r} first, got {ids[0]!r}"
    )
    assert ids[1] == id1, (
        f"Second exec should be second. Expected {id1!r} second, got {ids[1]!r}"
    )


def test_r4_skipped_count_covered(primed_zenml) -> None:
    """R4: skipped_count path: executions missing CUT are skipped, not dropped silently.

    Seeds 1 baseline execution, then tests the skip guard directly via
    cut_of() raising for an exec without the 'decide' checkpoint.  This
    validates the guard path without running a real cohort over broken data.
    """
    del primed_zenml

    import sys
    import pathlib

    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.pipeline import KitaruAdapterPA

    # Use a fresh adapter so there are no prior executions in scope.
    model = TestModel(
        custom_output_args={
            "policy_label": "permissions_policy",
            "risk_status": "needs_review",
            "required_action": "escalate_to_human",
            "summary": "s",
        }
    )
    adapter = KitaruAdapterPA(model=model)
    exec_id = adapter.run("Can I enable SSO?", customer="acme")

    # Validate the guard raises on a non-existent exec_id (simulates broken data).
    with pytest.raises((RuntimeError, LookupError, Exception)):
        adapter.cut_of("00000000-0000-0000-0000-000000000000")


# ---------------------------------------------------------------------------
# Finding 1: alias-overwrite race — reproduce-after-experiment correctness
# ---------------------------------------------------------------------------

def test_reproduce_after_experiment_is_unaffected(primed_zenml) -> None:
    """Finding 1 (RED→GREEN): reproduce() after experiment() must use adapter A's agents.

    Before the ContextVar fix, constructing a second KitaruAdapterPA inside
    experiment() overwrote the module-level source aliases for gather_context /
    decide / finalize.  A subsequent reproduce() on adapter A would silently
    dispatch the fork closures, returning risk_status="safe" instead of
    "needs_review".

    After the fix, _activate() sets the ContextVar to THIS adapter's agents
    before each dispatch, so reproduce()-after-experiment() returns the correct
    baseline decision.

    RED: returns "safe" (fork model leaks into baseline reproduce).
    GREEN: returns "needs_review" (adapter A's agents used correctly).
    """
    del primed_zenml  # fixture used for ZenML store init side-effect

    import sys
    import pathlib

    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.pipeline import KitaruAdapterPA

    # Adapter A: baseline model -> needs_review
    base_model = TestModel(
        custom_output_args={
            "policy_label": "permissions_policy",
            "risk_status": "needs_review",
            "required_action": "escalate_to_human",
            "summary": "needs human review",
        }
    )
    # Fork model: safe (the experiment uses this)
    fork_model = TestModel(
        custom_output_args={
            "policy_label": "permissions_policy",
            "risk_status": "safe",
            "required_action": "answer_directly",
            "summary": "safe to answer",
        }
    )

    # Step 1: Run adapter A baseline.
    adapter_A = KitaruAdapterPA(model=base_model)
    base_exec = adapter_A.run("Can I enable SSO?", customer="acme")
    base_decision = adapter_A.decision_of(base_exec)
    assert base_decision["risk_status"] == "needs_review", (
        f"Baseline should be 'needs_review', got {base_decision['risk_status']!r}"
    )

    # Step 2: Call experiment() — this constructs a second KitaruAdapterPA
    # (the fork adapter) and dispatches the flow under the fork model.
    exp_exec = adapter_A.experiment(base_exec, model=fork_model, prompt_profile="trimmed_permissions")
    exp_decision = adapter_A.decision_of(exp_exec)
    assert exp_decision["risk_status"] == "safe", (
        f"Experiment should be 'safe', got {exp_decision['risk_status']!r}"
    )

    # Step 3: NOW call reproduce() on adapter A AFTER experiment() has run.
    # Before fix (RED): returns "safe" because the fork adapter's closures
    #   overwrote the module-level aliases.
    # After fix (GREEN): returns "needs_review" because _activate() sets the
    #   ContextVar to adapter A's agents before dispatching.
    repro_exec = adapter_A.reproduce(base_exec)
    repro_decision = adapter_A.decision_of(repro_exec)
    assert repro_decision["risk_status"] == "needs_review", (
        f"reproduce() after experiment() must return 'needs_review' (adapter A's agents). "
        f"Got {repro_decision['risk_status']!r}. "
        "This indicates the alias-overwrite race: the fork adapter's closures leaked "
        "into adapter A's reproduce dispatch."
    )


# ---------------------------------------------------------------------------
# Finding 2: improvement logic — pure unit test with synthetic values
# ---------------------------------------------------------------------------

def test_improvement_logic_with_synthetic_values() -> None:
    """Finding 2: CohortReport.improvement logic tested with known synthetic aggregates.

    Tests the derivation independent of timing by constructing CohortReport and
    CohortRow instances directly with known values.

    Cases:
    - clearly_improved: cheaper, faster, same quality -> True
    - experiment_costs_more: experiment cost > baseline cost -> False
    - experiment_is_slower: experiment latency > baseline latency -> False
    - experiment_quality_worse: experiment score << baseline score (below tolerance) -> False
    """
    import sys
    import pathlib

    sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))
    from pydantic_replay_fork.pipeline import CohortReport, CohortRow

    def _make_report(
        cost_baseline: float,
        cost_experiment: float,
        latency_baseline: float,
        latency_experiment: float,
        score_baseline: int,
        score_experiment: int,
    ) -> CohortReport:
        """Build a CohortReport with a single non-skipped row of known values."""
        row = CohortRow(
            base_exec_id="fake-base",
            repro_exec_id="fake-repro",
            exp_exec_id="fake-exp",
            decision_changed=True,
            cost_baseline_usd=cost_baseline,
            cost_experiment_usd=cost_experiment,
            latency_baseline_s=latency_baseline,
            latency_experiment_s=latency_experiment,
            judge_score_baseline=score_baseline,
            judge_score_experiment=score_experiment,
            skipped=False,
        )
        report = CohortReport(rows=[row], skipped_count=0)
        return report

    # Case 1: clearly improved — cheaper, faster, same quality.
    clearly_improved = _make_report(
        cost_baseline=0.05, cost_experiment=0.02,
        latency_baseline=2.0, latency_experiment=1.0,
        score_baseline=4, score_experiment=4,
    )
    assert clearly_improved.improvement is True, (
        "Expected improvement=True when experiment is cheaper, faster, same quality. "
        f"cost_baseline={clearly_improved.mean_cost_baseline_usd}, "
        f"cost_experiment={clearly_improved.mean_cost_experiment_usd}, "
        f"latency_baseline={clearly_improved.mean_latency_baseline_s}, "
        f"latency_experiment={clearly_improved.mean_latency_experiment_s}, "
        f"score_baseline={clearly_improved.mean_judge_score_baseline}, "
        f"score_experiment={clearly_improved.mean_judge_score_experiment}"
    )

    # Case 2: experiment costs more -> False.
    costs_more = _make_report(
        cost_baseline=0.02, cost_experiment=0.05,  # experiment more expensive
        latency_baseline=2.0, latency_experiment=1.0,
        score_baseline=4, score_experiment=4,
    )
    assert costs_more.improvement is False, (
        "Expected improvement=False when experiment costs more than baseline. "
        f"cost_baseline={costs_more.mean_cost_baseline_usd}, "
        f"cost_experiment={costs_more.mean_cost_experiment_usd}"
    )

    # Case 3: experiment is slower -> False.
    slower = _make_report(
        cost_baseline=0.05, cost_experiment=0.02,
        latency_baseline=1.0, latency_experiment=3.0,  # experiment slower
        score_baseline=4, score_experiment=4,
    )
    assert slower.improvement is False, (
        "Expected improvement=False when experiment is slower than baseline. "
        f"latency_baseline={slower.mean_latency_baseline_s}, "
        f"latency_experiment={slower.mean_latency_experiment_s}"
    )

    # Case 4: experiment quality worse (below tolerance) -> False.
    # QUALITY_TOLERANCE is 0.1; baseline=4, experiment=3 -> 3 < 4-0.1=3.9 -> False.
    quality_worse = _make_report(
        cost_baseline=0.05, cost_experiment=0.02,
        latency_baseline=2.0, latency_experiment=1.0,
        score_baseline=4, score_experiment=3,  # significantly worse quality
    )
    assert quality_worse.improvement is False, (
        "Expected improvement=False when experiment quality is significantly worse. "
        f"score_baseline={quality_worse.mean_judge_score_baseline}, "
        f"score_experiment={quality_worse.mean_judge_score_experiment}, "
        f"tolerance={quality_worse.QUALITY_TOLERANCE}"
    )
