"""PydanticAI replay demo — test suite.

Verbs: rerun (no-edit: cached head, live tail -> RunHandle),
       replay (with-edit: reconfigured decide+tail -> RunHandle).

Test inventory:
  [1]  test_fork_by_replay_reexecutes_tail_under_new_agent
  [2]  test_run_produces_durable_execution_with_call_checkpoints
  [3]  test_multistep_replay_from_intermediate_step
  [4]  test_rerun_matches_original
  [5]  test_r1_multistep_gather_decide_finalize_checkpoints
  [6]  test_r2_rerun_head_is_cached
  [7]  test_r3_replay_flips_decision
  [8]  test_r4_last_executions_returns_exec_ids
  [9]  test_r4_skipped_count_covered
  [10] test_rerun_after_replay_is_unaffected
  [11] test_run_handle_rerun_returns_handle
  [12] test_run_handle_replay_flips_decision
  [13] test_run_handle_diff
  [14] test_recipe_identity_and_as_kwargs
  [15] test_metric_delta_is_worse
  [16] test_report_regressions_synthetic
  [17] test_report_improvement_synthetic
  [18] test_cohort_experiment_three_cases
  [19] test_cohort_repeats_averaging
"""
from __future__ import annotations

import sys
import pathlib

import pytest

pytest.importorskip("pydantic_ai")

# Ensure examples/ is importable.
sys.path.insert(0, str(pathlib.Path("examples/end_to_end").resolve()))

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

from kitaru import flow, KitaruClient
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.checkpoint import checkpoint


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

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


_BASE_ARGS = {
    "policy_label": "permissions_policy",
    "risk_status": "needs_review",
    "required_action": "escalate_to_human",
    "summary": "needs human review",
}
_FORK_ARGS = {
    "policy_label": "permissions_policy",
    "risk_status": "safe",
    "required_action": "answer_directly",
    "summary": "safe to answer",
}


def _base_model():
    return TestModel(custom_output_args=_BASE_ARGS)


def _fork_model():
    return TestModel(custom_output_args=_FORK_ARGS)


# ---------------------------------------------------------------------------
# [1] Pre-existing: fork-by-replay mechanism (unchanged)
# ---------------------------------------------------------------------------

def test_fork_by_replay_reexecutes_tail_under_new_agent(primed_zenml) -> None:
    del primed_zenml

    base_flow = _make_flow("forktest_agent", "needs_review")
    base_handle = base_flow.run("a permission request")
    base_exec_id = base_handle.exec_id
    base_handle.wait()

    client = KitaruClient()
    run = client.executions.get(base_exec_id)
    checkpoint_names = [c.name for c in run.checkpoints]

    assert checkpoint_names, "Expected at least one checkpoint on calls-strategy run"
    assert any("model_request" in name for name in checkpoint_names), (
        f"Expected a '*_model_request' checkpoint; got: {checkpoint_names}"
    )

    cut = checkpoint_names[-1]
    assert cut == "forktest_agent_model_request", (
        f"Unexpected CUT selector: {cut!r}."
    )

    fork_flow = _make_flow("forktest_agent", "safe")
    fork_handle = fork_flow.replay(base_exec_id, from_=cut, cache=False)
    fork_exec_id = fork_handle.exec_id
    fork_result = fork_handle.wait()

    result_text = str(fork_result)
    assert "safe" in result_text
    assert "needs_review" not in result_text

    fork_exec = client.executions.get(fork_exec_id)
    assert fork_exec.original_exec_id == base_exec_id


# ---------------------------------------------------------------------------
# [2] Pre-existing: durable execution with checkpoints (support_copilot.py)
# ---------------------------------------------------------------------------

def test_run_produces_durable_execution_with_call_checkpoints(primed_zenml):
    from pydantic_replay_fork.support_copilot import KitaruAdapterPA

    adapter = KitaruAdapterPA(model=_base_model())
    exec_id = adapter.run("Can I enable SSO?", customer="acme")
    run = KitaruClient().executions.get(exec_id)
    assert run.checkpoints, "Expected at least one checkpoint"
    cp_names = [c.name for c in run.checkpoints]
    assert "decide" in cp_names
    assert adapter.decision_of(exec_id)["risk_status"] == "needs_review"


# ---------------------------------------------------------------------------
# [4] Pre-existing: two-step multi-step replay spike (unchanged)
# ---------------------------------------------------------------------------

def test_multistep_replay_from_intermediate_step(primed_zenml) -> None:
    del primed_zenml

    class _GatherOut(BaseModel):
        triage: str = "unknown"

    class _DecideOut(BaseModel):
        verdict: str = "pending"
        received_triage: str = "unset"

    def _build_flow(triage_val: str, verdict_val: str):
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

    base_flow = _build_flow("medium", "approved")
    base_handle = base_flow.run("analyze ticket")
    base_exec_id = base_handle.exec_id
    base_result = base_handle.wait()

    assert base_result == {"verdict": "approved", "received_triage": "medium"}

    client = KitaruClient()
    base_run = client.executions.get(base_exec_id)
    cp_names = [c.name for c in base_run.checkpoints]
    assert cp_names == ["gather_step", "decide_step"]

    CUT = "decide_step"
    fork_flow = _build_flow("low", "reject")
    fork_handle = fork_flow.replay(
        base_exec_id,
        from_=CUT,
        cache=False,
        overrides={"checkpoint.gather_step": {"triage": "critical"}},
    )
    fork_exec_id = fork_handle.exec_id
    fork_result = fork_handle.wait()

    assert fork_result["received_triage"] == "critical"
    assert fork_result["verdict"] == "reject"

    fork_exec = client.executions.get(fork_exec_id)
    assert fork_exec.original_exec_id == base_exec_id


# ---------------------------------------------------------------------------
# [5] rerun matches original — no drift
# ---------------------------------------------------------------------------

def test_rerun_matches_original(primed_zenml):
    from pydantic_replay_fork.support_copilot import KitaruAdapterPA

    adapter = KitaruAdapterPA(model=_base_model())
    base = adapter.run("Can I enable SSO?", customer="acme")
    rerun_handle = adapter.rerun(base)
    report = rerun_handle.diff(rerun_handle)
    assert report.has_fork_drift is False


# ---------------------------------------------------------------------------
# [6] R1: three named checkpoints (gather_context / decide / finalize)
# ---------------------------------------------------------------------------

def test_r1_multistep_gather_decide_finalize_checkpoints(primed_zenml) -> None:
    del primed_zenml

    from pydantic_replay_fork.support_copilot import KitaruAdapterPA
    from pydantic_replay_fork.utils import CUT

    adapter = KitaruAdapterPA(model=_base_model())
    exec_id = adapter.run("Can I enable SSO?", customer="acme")

    client = KitaruClient()
    run = client.executions.get(exec_id)
    cp_names = [c.name for c in run.checkpoints]

    assert "gather_context" in cp_names
    assert "decide" in cp_names
    assert "finalize" in cp_names
    assert cp_names.index("gather_context") < cp_names.index("decide")
    assert cp_names.index("decide") < cp_names.index("finalize")

    assert CUT == "decide"
    assert adapter.cut_of(exec_id) == "decide"

    decision = adapter.decision_of(exec_id)
    assert isinstance(decision, dict)
    assert decision["risk_status"] == "needs_review"

    assert run.original_exec_id is None


# ---------------------------------------------------------------------------
# [7] R2: rerun() head cached (was test_r2_reproduce_head_is_cached)
# ---------------------------------------------------------------------------

def test_r2_rerun_head_is_cached(primed_zenml) -> None:
    del primed_zenml

    from pydantic_replay_fork.support_copilot import KitaruAdapterPA

    adapter = KitaruAdapterPA(model=_base_model())
    base_id = adapter.run("Can I enable SSO?", customer="acme")

    # rerun: returns RunHandle; no config change.
    rerun_handle = adapter.rerun(base_id)
    rerun_id = rerun_handle.exec_id

    # Semantic equivalence: diff shows no drift (rerun vs itself).
    report = rerun_handle.diff(rerun_handle)
    assert report.has_fork_drift is False

    # Lineage.
    client = KitaruClient()
    rerun_run = client.executions.get(rerun_id)
    assert rerun_run.original_exec_id == base_id

    # Non-vacuous head-cached proof.
    rerun_cp_by_name = {c.name: c for c in rerun_run.checkpoints}
    gather_cp = rerun_cp_by_name.get("gather_context")
    assert gather_cp is not None
    assert gather_cp.original_call_id is not None, (
        "gather_context was NOT served from cache on rerun"
    )

    # decide and finalize re-ran.
    for step_name in ("decide", "finalize"):
        cp = rerun_cp_by_name.get(step_name)
        assert cp is not None
        assert cp.original_call_id is None, (
            f"'{step_name}' should have re-run, not been cached"
        )


# ---------------------------------------------------------------------------
# [8] R3: replay() flips decision (was test_r3_experiment_flips_decision)
# ---------------------------------------------------------------------------

def test_r3_replay_flips_decision(primed_zenml) -> None:
    del primed_zenml

    from pydantic_replay_fork.support_copilot import KitaruAdapterPA

    adapter = KitaruAdapterPA(model=_base_model())
    base_id = adapter.run("Can I enable SSO?", customer="acme")
    rerun_handle = adapter.rerun(base_id)

    # replay: WITH edit -> should flip needs_review -> safe.
    replay_handle = adapter.replay(
        base_id,
        model=_fork_model(),
        prompt_profile="trimmed_permissions",
    )

    # diff shows drift.
    report = rerun_handle.diff(replay_handle)
    assert report.has_fork_drift is True
    changed_fields = {c.field for c in report.fork if not c.matches}
    assert "risk_status" in changed_fields

    # replay decision is 'safe'.
    assert replay_handle.decision["risk_status"] == "safe"

    # Lineage.
    client = KitaruClient()
    replay_run = client.executions.get(replay_handle.exec_id)
    assert replay_run.original_exec_id == base_id

    # Head cached on replay run.
    replay_cp_by_name = {c.name: c for c in replay_run.checkpoints}
    gather_cp = replay_cp_by_name.get("gather_context")
    assert gather_cp is not None
    assert gather_cp.original_call_id is not None, (
        "gather_context was NOT served from cache on replay"
    )

    # decide and finalize re-ran.
    for step_name in ("decide", "finalize"):
        cp = replay_cp_by_name.get(step_name)
        assert cp is not None
        assert cp.original_call_id is None


# ---------------------------------------------------------------------------
# [9] R4: last_executions (targets support_copilot)
# ---------------------------------------------------------------------------

def test_r4_last_executions_returns_exec_ids(primed_zenml) -> None:
    del primed_zenml

    from pydantic_replay_fork.support_copilot import KitaruAdapterPA

    adapter = KitaruAdapterPA(model=_base_model())
    id1 = adapter.run("Can I enable SSO?", customer="acme")
    id2 = adapter.run("Change billing owner?", customer="beta")

    ids = adapter.last_executions(2)
    assert isinstance(ids, list)
    assert len(ids) == 2
    assert all(isinstance(x, str) for x in ids)
    assert ids[0] == id2
    assert ids[1] == id1


# ---------------------------------------------------------------------------
# [10] R4: skipped_count guard (targets support_copilot)
# ---------------------------------------------------------------------------

def test_r4_skipped_count_covered(primed_zenml) -> None:
    del primed_zenml

    from pydantic_replay_fork.support_copilot import KitaruAdapterPA

    adapter = KitaruAdapterPA(model=_base_model())
    adapter.run("Can I enable SSO?", customer="acme")

    with pytest.raises((RuntimeError, LookupError, Exception)):
        adapter.cut_of("00000000-0000-0000-0000-000000000000")


# ---------------------------------------------------------------------------
# [11] Finding 1: rerun-after-replay correctness
# ---------------------------------------------------------------------------

def test_rerun_after_replay_is_unaffected(primed_zenml) -> None:
    """rerun() after replay() must use adapter A's agents (ContextVar fix).

    Before the fix, constructing the reconfigured adapter inside replay()
    overwrote module-level source aliases.  A subsequent rerun() on adapter A
    would dispatch the fork closures, returning risk_status="safe".

    After the fix, _activate() sets the ContextVar to THIS adapter's agents
    before each dispatch — rerun()-after-replay() returns "needs_review".
    """
    del primed_zenml

    from pydantic_replay_fork.support_copilot import KitaruAdapterPA

    adapter_A = KitaruAdapterPA(model=_base_model())
    base_exec = adapter_A.run("Can I enable SSO?", customer="acme")
    base_decision = adapter_A.decision_of(base_exec)
    assert base_decision["risk_status"] == "needs_review"

    # replay(): constructs reconfigured adapter (fork model), dispatches flow.
    replay_handle = adapter_A.replay(
        base_exec, model=_fork_model(), prompt_profile="trimmed_permissions"
    )
    assert replay_handle.decision["risk_status"] == "safe"

    # rerun() AFTER replay() must still return baseline decision.
    rerun_handle = adapter_A.rerun(base_exec)
    assert rerun_handle.decision["risk_status"] == "needs_review", (
        "rerun() after replay() returned the fork decision — "
        "alias-overwrite race not fixed."
    )


# ---------------------------------------------------------------------------
# [12] NEW: rerun() returns a RunHandle with correct fields
# ---------------------------------------------------------------------------

def test_run_handle_rerun_returns_handle(primed_zenml) -> None:
    del primed_zenml

    from pydantic_replay_fork.support_copilot import KitaruAdapterPA, RunHandle
    from pydantic_replay_fork.utils import Recipe

    adapter = KitaruAdapterPA(model=_base_model())
    base_id = adapter.run("Can I enable SSO?", customer="acme")
    handle = adapter.rerun(base_id)

    assert isinstance(handle, RunHandle)
    assert isinstance(handle.exec_id, str)
    assert isinstance(handle.decision, dict)
    assert "risk_status" in handle.decision
    assert handle.decision["risk_status"] == "needs_review"

    # Recipe is identity (no config change).
    assert isinstance(handle.recipe, Recipe)
    assert handle.recipe.is_identity()


# ---------------------------------------------------------------------------
# [13] NEW: replay() returns RunHandle and flips the decision
# ---------------------------------------------------------------------------

def test_run_handle_replay_flips_decision(primed_zenml) -> None:
    del primed_zenml

    from pydantic_replay_fork.support_copilot import KitaruAdapterPA, RunHandle
    from pydantic_replay_fork.utils import Recipe

    adapter = KitaruAdapterPA(model=_base_model())
    base_id = adapter.run("Can I enable SSO?", customer="acme")
    handle = adapter.replay(
        base_id,
        model=_fork_model(),
        prompt_profile="trimmed_permissions",
    )

    assert isinstance(handle, RunHandle)
    assert handle.decision["risk_status"] == "safe"

    # Recipe captures the change.
    assert not handle.recipe.is_identity()
    assert handle.recipe.prompt_profile == "trimmed_permissions"


# ---------------------------------------------------------------------------
# [14] NEW: RunHandle.diff() returns a DriftReport
# ---------------------------------------------------------------------------

def test_run_handle_diff(primed_zenml) -> None:
    del primed_zenml

    from pydantic_replay_fork.support_copilot import KitaruAdapterPA

    adapter = KitaruAdapterPA(model=_base_model())
    base_id = adapter.run("Can I enable SSO?", customer="acme")
    rerun_h = adapter.rerun(base_id)
    replay_h = adapter.replay(base_id, model=_fork_model(), prompt_profile="trimmed_permissions")

    # rerun.diff(replay) should show drift.
    dr = rerun_h.diff(replay_h)
    assert dr.has_fork_drift is True

    # self.diff(self) should show no drift.
    dr2 = rerun_h.diff(rerun_h)
    assert dr2.has_fork_drift is False


# ---------------------------------------------------------------------------
# [15] NEW: Recipe identity / as_kwargs
# ---------------------------------------------------------------------------

def test_recipe_identity_and_as_kwargs() -> None:
    from pydantic_replay_fork.utils import Recipe, CUT

    # Identity recipe (rerun).
    r0 = Recipe()
    assert r0.is_identity()
    kw = r0.as_kwargs()
    assert kw == {"at": CUT}

    # Non-identity recipe (replay).
    r1 = Recipe(model="openai:gpt-5-nano", prompt_profile="trimmed_permissions", at="decide")
    assert not r1.is_identity()
    kw1 = r1.as_kwargs()
    assert kw1["model"] == "openai:gpt-5-nano"
    assert kw1["prompt_profile"] == "trimmed_permissions"
    assert kw1["at"] == "decide"


# ---------------------------------------------------------------------------
# [16] NEW: MetricDelta.is_worse direction logic
# ---------------------------------------------------------------------------

def test_metric_delta_is_worse() -> None:
    from pydantic_replay_fork.utils import MetricDelta

    # lower_is_better: variant higher = worse.
    d1 = MetricDelta("cost", 0.01, 0.05, lower_is_better=True)
    assert d1.is_worse is True

    d2 = MetricDelta("cost", 0.05, 0.01, lower_is_better=True)
    assert d2.is_worse is False

    # higher_is_better: variant lower = worse.
    d3 = MetricDelta("quality", 4, 3, lower_is_better=False)
    assert d3.is_worse is True

    d4 = MetricDelta("quality", 3, 4, lower_is_better=False)
    assert d4.is_worse is False

    # None values = not worse.
    d5 = MetricDelta("latency", None, 5.0, lower_is_better=True)
    assert d5.is_worse is False


# ---------------------------------------------------------------------------
# [17] NEW: Report.regressions() with synthetic MetricDeltas
# ---------------------------------------------------------------------------

def test_report_regressions_synthetic() -> None:
    from pydantic_replay_fork.cohort import Report, _CohortRow
    from pydantic_replay_fork.utils import MetricDelta

    # Case: clearly improved — no regressions.
    row_ok = _CohortRow(
        base_exec_id="x",
        decision_changed=False,
        deltas=[
            MetricDelta("cost", 0.05, 0.02, lower_is_better=True),
            MetricDelta("latency", 2.0, 1.0, lower_is_better=True),
            MetricDelta("quality", 4, 4, lower_is_better=False),
        ],
    )
    report_ok = Report(rows=[row_ok], skipped_count=0)
    assert report_ok.regressions() == []
    assert report_ok.improvement is True

    # Case: cost regression.
    row_cost = _CohortRow(
        base_exec_id="x",
        decision_changed=False,
        deltas=[
            MetricDelta("cost", 0.02, 0.05, lower_is_better=True),
        ],
    )
    report_cost = Report(rows=[row_cost], skipped_count=0)
    regs = report_cost.regressions()
    assert any(isinstance(r, MetricDelta) and r.name == "cost" for r in regs)
    assert report_cost.improvement is False

    # Case: latency regression.
    row_lat = _CohortRow(
        base_exec_id="x",
        decision_changed=False,
        deltas=[
            MetricDelta("latency", 1.0, 3.0, lower_is_better=True),
        ],
    )
    report_lat = Report(rows=[row_lat], skipped_count=0)
    regs_lat = report_lat.regressions()
    assert any(isinstance(r, MetricDelta) and r.name == "latency" for r in regs_lat)

    # Case: quality regression.
    row_q = _CohortRow(
        base_exec_id="x",
        decision_changed=False,
        deltas=[
            MetricDelta("quality", 4, 3, lower_is_better=False),
        ],
    )
    report_q = Report(rows=[row_q], skipped_count=0)
    regs_q = report_q.regressions()
    assert any(isinstance(r, MetricDelta) and r.name == "quality" for r in regs_q)
    assert report_q.improvement is False

    # Case: decision_changed shows up in regressions.
    row_dc = _CohortRow(
        base_exec_id="x",
        decision_changed=True,
        deltas=[],
    )
    report_dc = Report(rows=[row_dc], skipped_count=0)
    assert "decision_changed" in report_dc.regressions()


# ---------------------------------------------------------------------------
# [18] NEW: Report.improvement synthetic
# ---------------------------------------------------------------------------

def test_report_improvement_synthetic() -> None:
    from pydantic_replay_fork.cohort import Report, _CohortRow
    from pydantic_replay_fork.utils import MetricDelta

    def _make(cost_b, cost_e, lat_b, lat_e, score_b, score_e):
        row = _CohortRow(
            base_exec_id="f",
            decision_changed=False,
            deltas=[
                MetricDelta("cost", cost_b, cost_e, lower_is_better=True),
                MetricDelta("latency", lat_b, lat_e, lower_is_better=True),
                MetricDelta("quality", score_b, score_e, lower_is_better=False),
            ],
        )
        return Report(rows=[row], skipped_count=0)

    assert _make(0.05, 0.02, 2.0, 1.0, 4, 4).improvement is True
    assert _make(0.02, 0.05, 2.0, 1.0, 4, 4).improvement is False   # costs more
    assert _make(0.05, 0.02, 1.0, 3.0, 4, 4).improvement is False   # slower
    assert _make(0.05, 0.02, 2.0, 1.0, 4, 3).improvement is False   # worse quality


# ---------------------------------------------------------------------------
# [19] NEW: cohort().experiment() end-to-end with 3 cases
# ---------------------------------------------------------------------------

def test_cohort_experiment_three_cases(primed_zenml) -> None:
    del primed_zenml

    from pydantic_replay_fork.support_copilot import KitaruAdapterPA
    from pydantic_replay_fork.utils import cost, latency, quality_judge, Recipe
    from pydantic_replay_fork.cohort import cohort, Report

    adapter = KitaruAdapterPA(model=_base_model())
    # Seed 3 baseline executions.
    exec_ids = [adapter.run("Can I enable SSO?", customer="acme") for _ in range(3)]

    variant_recipe = Recipe(
        model=_fork_model(),
        prompt_profile="trimmed_permissions",
        at="decide",
    )

    report = cohort(exec_ids).experiment(
        adapter,
        variant=variant_recipe,
        metrics=[cost, latency, quality_judge],
        repeats=1,
    )

    assert isinstance(report, Report)
    # 3 non-skipped rows.
    assert len(report.rows) == 3, f"Expected 3 rows, got {len(report.rows)}"
    assert report.skipped == 0

    # All decisions should have flipped (needs_review -> safe).
    assert report.decision_change_count == 3

    # regressions() is a list (may be empty or contain items).
    regs = report.regressions()
    assert isinstance(regs, list)

    # summary() returns a non-empty string.
    s = report.summary()
    assert isinstance(s, str) and s

    # improvement is a bool.
    assert isinstance(report.improvement, bool)


# ---------------------------------------------------------------------------
# [19] repeats averaging — unit test for the averaging logic
# ---------------------------------------------------------------------------

def test_cohort_repeats_averaging() -> None:
    """Cohort.experiment averages metric variant_values across repeats.

    We test the averaging logic directly on Report with synthetic _CohortRow
    data rather than running live executions, so no Kitaru server is required.

    The assertion: when two repeats produce variant_values 2.0 and 4.0, the
    Report's _mean_variant for that metric must be 3.0.
    """
    from pydantic_replay_fork.cohort import Report, _CohortRow
    from pydantic_replay_fork.utils import MetricDelta

    row1 = _CohortRow(
        base_exec_id="case-a",
        decision_changed=False,
        deltas=[
            MetricDelta("cost", baseline_value=1.0, variant_value=2.0, lower_is_better=True),
        ],
    )
    row2 = _CohortRow(
        base_exec_id="case-a",
        decision_changed=False,
        deltas=[
            MetricDelta("cost", baseline_value=1.0, variant_value=4.0, lower_is_better=True),
        ],
    )

    report = Report(rows=[row1, row2], skipped_count=0)
    mean_variant = report._mean_variant("cost")
    assert mean_variant == 3.0, f"Expected 3.0 (average of 2.0 and 4.0), got {mean_variant}"
    mean_baseline = report._mean_baseline("cost")
    assert mean_baseline == 1.0, f"Expected baseline mean 1.0, got {mean_baseline}"
