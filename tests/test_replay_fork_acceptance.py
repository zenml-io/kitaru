"""Persisted acceptance coverage for the shipped replay-fork journey."""

import json
from typing import Any

import pytest
from pydantic_ai import messages as pydantic_messages
from pydantic_ai.models.function import FunctionModel

from kitaru import ExperimentSpec, KitaruClient, RegressionLimits
from kitaru.errors import KitaruStateError
from kitaru.imports import (
    ImportedReplayBoundaryKind,
    ImportedReplayMode,
    ImportOutcomeStatus,
    ReplayReadinessStatus,
)
from kitaru.scoring import (
    ExperimentVerdict,
    ImportedReplayComparability,
)
from tests.replay_fork_support import (
    _final_result,
    _tool_returns,
    bootstrap_account_setting_comparable_suite,
)

STATUS_TRACE_ID = "support-service-status"
BASELINE_REPRODUCTION_VERSION = "recorded-path-reproduction-v1"
FIXED_CANDIDATE_VERSION = "permissions-fix-v1"


def _structured_argument_mismatch_model() -> FunctionModel:
    def respond(
        messages: list[pydantic_messages.ModelMessage],
        _info: Any,
    ) -> pydantic_messages.ModelResponse:
        returned_names = [part.tool_name for part in _tool_returns(messages)]
        if "escalate_to_human" not in returned_names:
            return pydantic_messages.ModelResponse(
                parts=[
                    pydantic_messages.ToolCallPart(
                        tool_name="escalate_to_human",
                        args={
                            "customer_id": "cust_acme",
                            "policy_label": "billing_policy",
                        },
                        tool_call_id="candidate-mismatched-escalation",
                    )
                ]
            )
        return _final_result(
            {
                "policy_label": "permissions_policy",
                "risk_status": "blocked",
                "required_action": "refuse_write",
                "summary": "The mismatched escalation was blocked.",
                "evidence_ids": [],
                "tool_names": ["escalate_to_human"],
            }
        )

    return FunctionModel(respond)


def _divergent_model() -> FunctionModel:
    def respond(
        messages: list[pydantic_messages.ModelMessage],
        _info: Any,
    ) -> pydantic_messages.ModelResponse:
        returned_names = [part.tool_name for part in _tool_returns(messages)]
        if "update_customer_setting" not in returned_names:
            return pydantic_messages.ModelResponse(
                parts=[
                    pydantic_messages.ToolCallPart(
                        tool_name="update_customer_setting",
                        args={
                            "customer_id": "cust_acme",
                            "setting": "beta_exports_fast_path",
                            "value": "true",
                        },
                        tool_call_id="candidate-unrecorded-write",
                    )
                ]
            )
        return _final_result(
            {
                "policy_label": "permissions_policy",
                "risk_status": "blocked",
                "required_action": "refuse_write",
                "summary": "The unrecorded write was blocked.",
                "evidence_ids": [],
                "tool_names": ["update_customer_setting"],
            }
        )

    return FunctionModel(respond)


def test_imported_replay_journey_persists_contract_faithful_evidence(
    primed_zenml: None,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    del primed_zenml
    bootstrap = bootstrap_account_setting_comparable_suite(monkeypatch, request)
    repository_root = bootstrap.repository_root
    demo = bootstrap.demo
    client = bootstrap.client
    source_state = bootstrap.source_state
    repeated_source_state = bootstrap.repeated_source_state
    preview = bootstrap.preview
    imported = bootstrap.imported
    repeated_import = bootstrap.repeated_import
    account_execution_id = bootstrap.account_execution_id
    boundary = bootstrap.tool_boundary
    model_boundary = bootstrap.model_boundary
    model_resumed = bootstrap.model_resumed
    recorded_model = bootstrap.recorded_model
    assert (repository_root / ".kitaru").is_dir()
    assert (
        repeated_source_state.binding.manifest.agent_version_id
        == source_state.binding.manifest.agent_version_id
    )
    assert source_state.binding.manifest.entrypoint == "evals.register:baseline_agent"
    assert preview.dry_run is True
    assert preview.selected_trace_count == 1
    assert [item.status for item in imported.outcomes] == [ImportOutcomeStatus.CREATED]
    assert [item.status for item in repeated_import.outcomes] == [
        ImportOutcomeStatus.UNCHANGED
    ]
    assert repeated_import.outcomes[0].execution_id == imported.outcomes[0].execution_id

    account_execution = client.executions.get(account_execution_id)
    assert account_execution.import_info is not None
    assert account_execution.import_info.source_agent_version_id == (
        source_state.binding.manifest.agent_version_id
    )
    assert (
        account_execution.import_info.source_agent_version_label == demo.SOURCE_VERSION
    )
    assert account_execution.import_info.attribution.status.value == "source_verified"
    assert account_execution.import_info.replay_readiness is not None
    assert (
        account_execution.import_info.replay_readiness.root_input_candidate_rerun.status
        is ReplayReadinessStatus.READY
    )
    with pytest.raises(KitaruStateError, match="Imported"):
        account_execution.replay(at="support_agent_model_request")

    assert boundary.kind is ImportedReplayBoundaryKind.TOOL_RESULT
    assert boundary.observation_id
    assert boundary.sequence is not None
    assert boundary.occurrence is not None
    assert boundary.call_id

    def forbid_live_tool(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("a persisted replay invoked the original tool callable")

    monkeypatch.setattr(
        "reference_agent.agent.SupportAgentDeps.execute",
        forbid_live_tool,
    )
    resumed = demo._resume_case(
        account_execution_id,
        boundary_kind="tool-result",
        boundary_index=1,
        name="account-setting-resumption",
        idempotency_key="account-setting-resumption-v1",
        candidate_variant="baseline",
        candidate_version=BASELINE_REPRODUCTION_VERSION,
        model=recorded_model,
    )
    resumed_member = resumed.record.imported_replay_members[0]
    assert resumed.spec.planning_rows[0].replay_plan.boundary == boundary
    assert resumed_member.mode is ImportedReplayMode.MESSAGE_HISTORY
    assert resumed_member.boundary == boundary
    resumed_child = KitaruClient().executions.get(resumed_member.child_execution_id)
    assert resumed_child.status == "completed", resumed_child.failure
    assert resumed_member.recorded_response_hits == 1, (
        resumed_member.model_dump_json(indent=2),
        resumed_child.failure,
    )
    assert resumed_member.recorded_response_misses == 0
    assert resumed_member.blocked_calls == 0
    assert resumed_member.path_diverged is False
    assert (
        resumed_member.comparability
        is ImportedReplayComparability.RECORDED_PATH_COMPARABLE
    )
    assert resumed.verdict is not None
    assert resumed.verdict.verdict is ExperimentVerdict.PASS, (
        resumed.verdict.model_dump_json(indent=2)
    )

    mismatched = demo._resume_case(
        account_execution_id,
        boundary_kind="tool-result",
        boundary_index=1,
        name="account-setting-structured-mismatch",
        idempotency_key="account-setting-structured-mismatch-v1",
        candidate_variant="baseline",
        candidate_version="v2.3-structured-mismatch",
        model=_structured_argument_mismatch_model(),
    )
    mismatch_member = mismatched.record.imported_replay_members[0]
    mismatch_child = KitaruClient().executions.get(mismatch_member.child_execution_id)
    assert mismatch_child.status == "completed", mismatch_child.failure
    assert mismatch_member.recorded_response_hits == 0
    assert mismatch_member.recorded_response_misses == 1
    assert mismatch_member.blocked_calls == 1
    assert mismatch_member.path_diverged is True
    assert mismatch_member.comparability is ImportedReplayComparability.DEGRADED
    assert len(mismatch_member.decisions) == 1
    assert mismatch_member.decisions[0].decision == "blocked"
    assert mismatch_member.decisions[0].block_reason == "argument_mismatch"
    assert mismatched.verdict is not None
    assert mismatched.verdict.verdict is ExperimentVerdict.HOLD

    assert model_boundary.kind is ImportedReplayBoundaryKind.MODEL_MESSAGE
    assert model_boundary.call_id is None
    assert model_resumed.record.imported_replay_members, (
        model_resumed.record.model_dump_json(indent=2)
    )
    model_member = model_resumed.record.imported_replay_members[0]
    assert model_member.boundary == model_boundary
    assert model_member.recorded_response_hits >= 1
    assert model_member.recorded_response_misses == 0
    assert model_member.blocked_calls == 0
    assert model_member.path_diverged is False
    assert (
        model_member.comparability
        is ImportedReplayComparability.RECORDED_PATH_COMPARABLE
    )
    assert model_resumed.verdict is not None
    assert model_resumed.verdict.verdict is ExperimentVerdict.PASS

    comparable_limits = RegressionLimits(
        max_trials=2,
        max_incurred_tokens=100_000,
        max_duration_seconds=60,
    )
    first_comparable_rerun = demo._rerun_suite(
        "account-setting-model-message",
        idempotency_key="account-setting-model-message-rerun-v1",
        limits=comparable_limits,
        candidate_variant="mini_tool_budget_2",
        candidate_version=FIXED_CANDIDATE_VERSION,
        model=recorded_model,
    )
    assert first_comparable_rerun.verdict is not None
    assert first_comparable_rerun.verdict.verdict is ExperimentVerdict.PASS
    assert (
        first_comparable_rerun.spec.source_experiment_id
        == model_resumed.spec.experiment_id
    )
    assert first_comparable_rerun.spec.regression_limits == comparable_limits

    retried_comparable_rerun = demo._rerun_suite(
        "account-setting-model-message",
        idempotency_key="account-setting-model-message-rerun-v1",
        limits=comparable_limits,
        candidate_variant="mini_tool_budget_2",
        candidate_version=FIXED_CANDIDATE_VERSION,
        model=recorded_model,
    )
    assert (
        retried_comparable_rerun.spec.experiment_id
        == first_comparable_rerun.spec.experiment_id
    )
    assert retried_comparable_rerun.record == first_comparable_rerun.record

    status_import = client.imports.langfuse(
        str(repository_root / "trace_fixtures" / "imported-support-cases.jsonl"),
        source_project_id="langfuse-replay-example",
        agent=demo.AGENT_NAME,
        version=demo.SOURCE_VERSION,
        trace_ids=[STATUS_TRACE_ID],
        dry_run=False,
        confirm_data_storage=True,
    )
    assert [item.status for item in status_import.outcomes] == [
        ImportOutcomeStatus.CREATED
    ]
    status_execution_id = str(status_import.outcomes[0].execution_id)

    suite = demo._replay_cases(
        [account_execution_id, status_execution_id],
        name="support-imported-regression",
        idempotency_key="support-imported-regression-v1",
        repeats=1,
        candidate_variant="mini_tool_budget_2",
        candidate_version=FIXED_CANDIDATE_VERSION,
        model=recorded_model,
    )
    assert suite.spec.suite_key == "support-imported-regression"
    assert suite.spec.executable.entrypoint == (
        "evals.register:mini_tool_budget_2_agent"
    )
    assert suite.spec.target_membership.execution_ids == [
        account_execution_id,
        status_execution_id,
    ]
    assert suite.record.counts.verified == 2
    assert suite.record.imported_replay_evidence is not None
    assert suite.record.imported_replay_evidence.recorded_response_hits == 5
    assert suite.record.imported_replay_evidence.recorded_response_misses == 0
    assert suite.record.imported_replay_evidence.blocked_calls == 0
    assert suite.verdict is not None
    assert suite.verdict.verdict is ExperimentVerdict.HOLD
    assert suite.verdict.message == "HOLD: imported_replay_not_comparable"
    for member in suite.record.imported_replay_members:
        assert member.comparability is ImportedReplayComparability.COUNTERFACTUAL
        assert member.source_agent_version_id != member.candidate_agent_version_id
        assert member.parent_execution_id == member.target_execution_id
        assert member.root_execution_id == member.target_execution_id

    child_id = suite.record.imported_replay_members[0].child_execution_id
    persisted_suite = client.agents.experiments.resolve_source(
        suite.spec.experiment_id,
        agent=demo.AGENT_NAME,
    )
    score_aggregate = persisted_suite.score_aggregate
    assert score_aggregate is not None
    assert len(score_aggregate.observation_ids) == 6
    assert {row.scorer_name for row in score_aggregate.scorer_aggregates} == {
        "support-resolution",
        "completed-execution",
        "no-unapproved-setting-writes",
    }
    assert all(row.scored == 2 for row in score_aggregate.scorer_aggregates)

    child_execution = client.executions.get(child_id)
    assert child_execution.original_exec_id == account_execution_id
    assert child_execution.root_exec_id == account_execution_id
    inspected_execution = demo._inspect_execution(child_id)
    assert inspected_execution["immediate_parent_id"] == account_execution_id
    assert inspected_execution["root_execution_id"] == account_execution_id
    assert "cost" in inspected_execution
    inspected_suite = demo._inspect_experiment(suite.spec.experiment_id)
    assert inspected_suite["attempt"]["suite_key"] == "support-imported-regression"
    assert inspected_suite["attempt"]["score_aggregate_data"] == score_aggregate
    assert len(inspected_suite["members"]) == 2
    serialized_inspection = json.loads(demo._json(inspected_suite))
    assert serialized_inspection["attempt"]["score_aggregate_data"][
        "observation_ids"
    ] == list(score_aggregate.observation_ids)

    limits = RegressionLimits(
        max_trials=2,
        max_cost_usd=0.01,
        max_incurred_tokens=100,
        max_duration_seconds=60,
    )
    with pytest.raises(AssertionError, match="Regression suite did not pass"):
        demo._rerun_suite(
            "support-imported-regression",
            idempotency_key="support-imported-regression-rerun-v1",
            limits=limits,
            candidate_variant="mini_tool_budget_2",
            candidate_version=FIXED_CANDIDATE_VERSION,
            model=recorded_model,
        )
    first_rerun = client.agents.experiments.resolve_source(
        "support-imported-regression",
        agent=demo.AGENT_NAME,
    )
    assert isinstance(first_rerun.spec, ExperimentSpec)
    assert first_rerun.spec.source_experiment_id == suite.spec.experiment_id
    assert first_rerun.spec.regression_limits is not None
    assert first_rerun.spec.regression_limits.max_trials == 2
    assert first_rerun.record.operational_limit is not None
    assert first_rerun.verdict is not None
    assert first_rerun.verdict.verdict is ExperimentVerdict.HOLD

    with pytest.raises(AssertionError, match="Regression suite did not pass"):
        demo._rerun_suite(
            "support-imported-regression",
            idempotency_key="support-imported-regression-rerun-v1",
            limits=limits,
            candidate_variant="mini_tool_budget_2",
            candidate_version=FIXED_CANDIDATE_VERSION,
            model=recorded_model,
        )
    retried_rerun = client.agents.experiments.resolve_source(
        "support-imported-regression",
        agent=demo.AGENT_NAME,
    )
    assert retried_rerun.experiment_id == first_rerun.experiment_id
    assert retried_rerun.record == first_rerun.record

    divergent_model = _divergent_model()
    counterfactual = demo._replay_cases(
        [account_execution_id],
        name="support-permissions-counterfactual",
        idempotency_key="support-permissions-counterfactual-v1",
        repeats=1,
        candidate_variant="nano_trimmed_permissions",
        candidate_version="v2.3-counterfactual",
        model=divergent_model,
    )
    assert counterfactual.spec.executable.entrypoint == (
        "evals.register:nano_trimmed_permissions_agent"
    )
    counterfactual_member = counterfactual.record.imported_replay_members[0]
    assert counterfactual_member.recorded_response_hits == 0
    assert counterfactual_member.recorded_response_misses == 3
    assert counterfactual_member.blocked_calls == 1
    assert counterfactual_member.path_diverged is True
    assert counterfactual_member.comparability is ImportedReplayComparability.DEGRADED
    assert counterfactual.verdict is not None
    assert counterfactual.verdict.verdict is ExperimentVerdict.FAIL
    assert {
        protection.protection_id
        for protection in counterfactual.verdict.protections
        if protection.passed is False
    } == {"no-unapproved-setting-writes"}
    assert "protection_below_passing_score" in {
        reason.value for reason in counterfactual.verdict.reason_codes
    }
    assert {
        "imported_recorded_responses_incomplete",
        "imported_replay_not_comparable",
    } <= {reason.value for reason in counterfactual.verdict.reason_codes}
