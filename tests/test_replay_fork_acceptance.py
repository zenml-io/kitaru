"""Persisted acceptance coverage for the shipped replay-fork journey."""

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
from pydantic_ai import messages as pydantic_messages
from pydantic_ai.models.function import FunctionModel
from zenml.client import Client

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
from tests.test_replay_fork_demo import DEMO_ROOT, _load_demo_module

FIXTURE = DEMO_ROOT / "trace_fixtures" / "imported-support-cases.jsonl"
ACCOUNT_TRACE_ID = "support-account-setting"
STATUS_TRACE_ID = "support-service-status"
ESCALATION_REASON = (
    "Account owner requested enabling account-wide feature flag "
    "'beta_exports_fast_path'. Account-wide feature-flag changes are restricted "
    "admin actions per permissions policy; escalate for human review and approval."
)


def _initialize_repository(repository_root: Path) -> None:
    (repository_root / ".gitignore").write_text(".kitaru/\n", encoding="utf-8")
    (repository_root / "acceptance-entrypoint.txt").write_text(
        "replay acceptance\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "init", "-q", str(repository_root)], check=True)
    subprocess.run(
        ["git", "-C", str(repository_root), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repository_root), "config", "user.name", "Test"],
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(repository_root),
            "add",
            ".gitignore",
            "acceptance-entrypoint.txt",
        ],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repository_root), "commit", "-qm", "acceptance entrypoint"],
        check=True,
    )


def _tool_returns(
    messages: list[pydantic_messages.ModelMessage],
) -> list[pydantic_messages.ToolReturnPart]:
    return [
        part
        for message in messages
        if isinstance(message, pydantic_messages.ModelRequest)
        for part in message.parts
        if isinstance(part, pydantic_messages.ToolReturnPart)
    ]


def _root_prompt(messages: list[pydantic_messages.ModelMessage]) -> str:
    for message in messages:
        if not isinstance(message, pydantic_messages.ModelRequest):
            continue
        for part in message.parts:
            if isinstance(part, pydantic_messages.UserPromptPart):
                return str(part.content)
    raise AssertionError("deterministic model received no root prompt")


def _final_result(arguments: dict[str, Any]) -> pydantic_messages.ModelResponse:
    return pydantic_messages.ModelResponse(
        parts=[
            pydantic_messages.TextPart(content=json.dumps(arguments, sort_keys=True))
        ]
    )


def _recorded_path_model() -> FunctionModel:
    def respond(
        messages: list[pydantic_messages.ModelMessage],
        _info: Any,
    ) -> pydantic_messages.ModelResponse:
        prompt = _root_prompt(messages)
        returns = _tool_returns(messages)
        returned_names = [part.tool_name for part in returns]

        if "beta_exports_fast_path" in prompt:
            if "lookup_customer" not in returned_names:
                return pydantic_messages.ModelResponse(
                    parts=[
                        pydantic_messages.ToolCallPart(
                            tool_name="lookup_customer",
                            args={"email_or_id": "Acme"},
                            tool_call_id="candidate-lookup",
                        )
                    ]
                )
            if "search_kb" not in returned_names:
                return pydantic_messages.ModelResponse(
                    parts=[
                        pydantic_messages.ToolCallPart(
                            tool_name="search_kb",
                            args={
                                "query": (
                                    "beta_exports_fast_path enable account setting "
                                    "policy admin change SSO beta feature enablement "
                                    "policy"
                                )
                            },
                            tool_call_id="candidate-kb",
                        )
                    ]
                )
            if "escalate_to_human" not in returned_names:
                return pydantic_messages.ModelResponse(
                    parts=[
                        pydantic_messages.ToolCallPart(
                            tool_name="escalate_to_human",
                            args={
                                "customer_id": "cust_acme",
                                "reason": ESCALATION_REASON,
                            },
                            tool_call_id="candidate-escalation",
                        )
                    ]
                )
            return _final_result(
                {
                    "policy_label": "permissions_policy",
                    "risk_status": "needs_review",
                    "required_action": "escalate_to_human",
                    "summary": "Restricted setting change sent for human approval.",
                    "evidence_ids": ["db:audit:escalation:cust_acme"],
                    "tool_names": ["escalate_to_human"],
                }
            )

        if "currently timing out" in prompt:
            if "get_service_status" not in returned_names:
                return pydantic_messages.ModelResponse(
                    parts=[
                        pydantic_messages.ToolCallPart(
                            tool_name="get_service_status",
                            args={"service": "export API"},
                            tool_call_id="candidate-status",
                        )
                    ]
                )
            if "search_kb" not in returned_names:
                return pydantic_messages.ModelResponse(
                    parts=[
                        pydantic_messages.ToolCallPart(
                            tool_name="search_kb",
                            args={"query": "export API timeout outage"},
                            tool_call_id="candidate-status-kb",
                        )
                    ]
                )
            return _final_result(
                {
                    "policy_label": "incident_policy",
                    "risk_status": "safe",
                    "required_action": "answer_directly",
                    "summary": (
                        "The recorded status and incident evidence were reproduced."
                    ),
                    "evidence_ids": ["api:status:export API"],
                    "tool_names": ["get_service_status", "search_kb"],
                }
            )

        raise AssertionError(f"unexpected deterministic prompt: {prompt}")

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
) -> None:
    del primed_zenml
    repository_root = Path(Client.find_repository())
    _initialize_repository(repository_root)
    monkeypatch.setattr(
        "kitaru._agent_registration._module_path_within_repository",
        lambda *_args, **_kwargs: True,
    )

    monkeypatch.setenv("OPENAI_API_KEY", "deterministic-test-key")
    demo = _load_demo_module()
    source_agent, _objective = demo._registered_agent(
        variant=demo.SOURCE_VARIANT,
        version=demo.SOURCE_VERSION,
    )
    source_state = source_agent._registered_state
    assert source_state is not None
    assert source_state.binding.manifest.entrypoint == "evals.register:baseline_agent"

    imported = demo._import_traces(
        str(FIXTURE.resolve()),
        source_project_id="langfuse-replay-example",
        trace_ids=[ACCOUNT_TRACE_ID, STATUS_TRACE_ID],
        limit=None,
        dry_run=False,
    )
    repeated_import = demo._import_traces(
        str(FIXTURE.resolve()),
        source_project_id="langfuse-replay-example",
        trace_ids=[ACCOUNT_TRACE_ID, STATUS_TRACE_ID],
        limit=None,
        dry_run=False,
    )
    assert [item.status for item in imported.outcomes] == [
        ImportOutcomeStatus.CREATED,
        ImportOutcomeStatus.CREATED,
    ]
    assert [item.status for item in repeated_import.outcomes] == [
        ImportOutcomeStatus.UNCHANGED,
        ImportOutcomeStatus.UNCHANGED,
    ]
    execution_ids = [item.execution_id for item in imported.outcomes]
    assert all(execution_ids)
    account_execution_id, status_execution_id = [
        str(execution_id) for execution_id in execution_ids
    ]
    assert [item.execution_id for item in repeated_import.outcomes] == execution_ids

    client = KitaruClient()
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

    boundary = demo._message_history_boundary(
        account_execution_id,
        kind="tool-result",
        index=1,
    )
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
    recorded_model = _recorded_path_model()

    resumed = demo._resume_case(
        account_execution_id,
        boundary_kind="tool-result",
        boundary_index=1,
        name="account-setting-resumption",
        idempotency_key="account-setting-resumption-v1",
        candidate_variant="baseline",
        candidate_version="v2.2-recorded-fix",
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

    model_boundary = demo._message_history_boundary(
        account_execution_id,
        kind="model-message",
        index=0,
    )
    assert model_boundary.kind is ImportedReplayBoundaryKind.MODEL_MESSAGE
    assert model_boundary.call_id is None
    model_resumed = demo._resume_case(
        account_execution_id,
        boundary_kind="model-message",
        boundary_index=0,
        name="account-setting-model-message",
        idempotency_key="account-setting-model-message-v1",
        candidate_variant="baseline",
        candidate_version="v2.2-recorded-fix",
        model=recorded_model,
    )
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
        candidate_variant="baseline",
        candidate_version="v2.2-recorded-fix",
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
        candidate_variant="baseline",
        candidate_version="v2.2-recorded-fix",
        model=recorded_model,
    )
    assert (
        retried_comparable_rerun.spec.experiment_id
        == first_comparable_rerun.spec.experiment_id
    )
    assert retried_comparable_rerun.record == first_comparable_rerun.record

    suite = demo._replay_cases(
        [account_execution_id, status_execution_id],
        name="support-imported-regression",
        idempotency_key="support-imported-regression-v1",
        repeats=1,
        candidate_variant="baseline",
        candidate_version="v2.2-recorded-fix",
        model=recorded_model,
    )
    assert suite.spec.suite_key == "support-imported-regression"
    assert suite.spec.executable.entrypoint == "evals.register:baseline_agent"
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
    assert len(score_aggregate.observation_ids) == 4
    assert {row.scorer_name for row in score_aggregate.scorer_aggregates} == {
        "support-resolution",
        "completed-execution",
    }
    assert all(row.scored == 2 for row in score_aggregate.scorer_aggregates)

    inspected_execution = demo._inspect_execution(child_id)
    assert inspected_execution["immediate_parent_id"] == account_execution_id
    assert inspected_execution["root_execution_id"] == account_execution_id
    assert "cost" in inspected_execution
    inspected_suite = demo._inspect_experiment("support-imported-regression")
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
            candidate_variant="baseline",
            candidate_version="v2.2-recorded-fix",
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
            candidate_variant="baseline",
            candidate_version="v2.2-recorded-fix",
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
        candidate_version="v2.2-counterfactual",
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
    assert counterfactual.verdict.verdict is ExperimentVerdict.HOLD
