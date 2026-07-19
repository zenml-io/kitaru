"""CLI contract tests for the imported replay example."""

import importlib
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
from click.testing import CliRunner
from pydantic_ai.models.test import TestModel

DEMO_ROOT = Path("examples/end_to_end/replay_fork_demo")


def _replay_result_stub(*, verdict: str = "hold") -> SimpleNamespace:
    verdict_value = SimpleNamespace(value=verdict)
    verdict_record = SimpleNamespace(
        verdict=verdict_value,
        objective=SimpleNamespace(
            scorer=SimpleNamespace(name="support-resolution"),
            mean=1.0,
            minimum_mean=1.0,
            passed=True,
        ),
        protections=[SimpleNamespace(protection_id="completed-execution", passed=True)],
        message="Imported replay did not remain on a comparable recorded path.",
        reason_codes=[SimpleNamespace(value="imported_replay_not_comparable")],
    )
    record = SimpleNamespace(
        status="completed",
        spec=SimpleNamespace(
            experiment_id="experiment-one",
            suite_key="suite-one",
            at="root",
            planning_rows=[
                SimpleNamespace(
                    disposition="imported",
                    replay_plan=SimpleNamespace(
                        mode=SimpleNamespace(value="root_input"),
                        boundary=SimpleNamespace(
                            kind=SimpleNamespace(value="root_input")
                        ),
                    ),
                )
            ],
        ),
        counts=SimpleNamespace(verified=1, intended=1),
        imported_replay_evidence=SimpleNamespace(
            comparability=SimpleNamespace(value="counterfactual"),
            recorded_response_hits=0,
            eligible_recorded_responses=1,
            recorded_response_misses=1,
            blocked_calls=1,
            path_divergences=1,
        ),
        imported_replay_members=[SimpleNamespace(child_execution_id="candidate-run")],
        verdict=verdict_record,
        operational_limit=None,
    )

    def assert_pass() -> None:
        if verdict != "pass":
            raise AssertionError("not a passing result")

    return SimpleNamespace(
        record=record,
        submission=SimpleNamespace(compare_url="https://example.com/compare"),
        assert_pass=assert_pass,
    )


def _load_demo_module() -> ModuleType:
    demo_root = str(DEMO_ROOT.resolve())
    if demo_root not in sys.path:
        sys.path.insert(0, demo_root)
    spec = importlib.util.spec_from_file_location(
        "pydantic_replay_fork_demo_under_test",
        DEMO_ROOT / "demo.py",
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_import_help_names_jsonl_and_trace_uri_without_loading_agent() -> None:
    sys.modules.pop("evals.register", None)
    demo = _load_demo_module()

    result = CliRunner().invoke(demo.cli, ["import-traces", "--help"])

    assert result.exit_code == 0, result.output
    assert "JSONL export or langfuse://trace/<id> URI" in result.output
    assert "evals.register" not in sys.modules


class _FakeImports:
    def __init__(self, calls: list[dict[str, Any]]) -> None:
        self._calls = calls

    def langfuse(self, source: str, **kwargs: Any) -> dict[str, Any]:
        self._calls.append({"source": source, **kwargs})
        return {"selected_trace_count": 1, "dry_run": kwargs["dry_run"]}


def test_resume_command_rejects_negative_boundary_index() -> None:
    demo = _load_demo_module()

    result = CliRunner().invoke(
        demo.cli,
        [
            "resume",
            "imported-run",
            "--boundary-index",
            "-1",
            "--idempotency-key",
            "resume-negative-boundary",
        ],
    )

    assert result.exit_code == 2
    assert "is not in the range" in result.output


def test_import_command_uses_declared_source_version_without_loading_agent(
    monkeypatch: Any,
) -> None:
    sys.modules.pop("evals.register", None)
    demo = _load_demo_module()
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        demo,
        "KitaruClient",
        lambda: SimpleNamespace(imports=_FakeImports(calls)),
    )

    result = CliRunner().invoke(
        demo.cli,
        [
            "import-traces",
            "trace_fixtures/support-traces.jsonl",
            "--source-project-id",
            "langfuse-project",
            "--trace-id",
            "trace-48211",
            "--limit",
            "1",
            "--commit",
            "--output",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "evals.register" not in sys.modules
    assert calls == [
        {
            "source": "trace_fixtures/support-traces.jsonl",
            "source_project_id": "langfuse-project",
            "agent": demo.AGENT_NAME,
            "version": demo.SOURCE_VERSION,
            "trace_ids": ["trace-48211"],
            "limit": 1,
            "dry_run": False,
            "confirm_data_storage": True,
        }
    ]
    assert '"dry_run": false' in result.output


def test_import_command_accepts_uri_and_defaults_to_read_only(monkeypatch: Any) -> None:
    demo = _load_demo_module()
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        demo,
        "KitaruClient",
        lambda: SimpleNamespace(imports=_FakeImports(calls)),
    )

    result = CliRunner().invoke(
        demo.cli,
        [
            "import-traces",
            "langfuse://trace/trace-48211",
            "--output",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls[0]["source_project_id"] is None
    assert calls[0]["trace_ids"] is None
    assert calls[0]["limit"] is None
    assert calls[0]["dry_run"] is True
    assert calls[0]["confirm_data_storage"] is False


def test_register_command_selects_explicit_source_variant(monkeypatch: Any) -> None:
    demo = _load_demo_module()
    calls: list[dict[str, str]] = []

    def register(*, variant: str, version: str) -> tuple[object, object]:
        calls.append({"variant": variant, "version": version})
        return object(), object()

    monkeypatch.setattr(demo, "_registered_agent", register)
    result = CliRunner().invoke(demo.cli, ["register", "--role", "source"])

    assert result.exit_code == 0, result.output
    assert calls == [{"variant": demo.SOURCE_VARIANT, "version": demo.SOURCE_VERSION}]
    assert "Registered AgentVersion" in result.output
    assert "Role     source" in result.output
    assert f"Version  {demo.SOURCE_VERSION}" in result.output


def test_source_registration_rejects_fixture_mismatches(monkeypatch: Any) -> None:
    demo = _load_demo_module()
    calls: list[dict[str, str]] = []

    def register(*, variant: str, version: str) -> tuple[object, object]:
        calls.append({"variant": variant, "version": version})
        return object(), object()

    monkeypatch.setattr(demo, "_registered_agent", register)

    wrong_variant = CliRunner().invoke(
        demo.cli,
        [
            "register",
            "--role",
            "source",
            "--variant",
            "mini_tool_budget_2",
        ],
    )
    wrong_version = CliRunner().invoke(
        demo.cli,
        [
            "register",
            "--role",
            "source",
            "--version",
            "unrelated-version",
        ],
    )

    assert wrong_variant.exit_code == 2
    assert "source fixture is immutable" in wrong_variant.output
    assert wrong_version.exit_code == 2
    assert "source fixture is immutable" in wrong_version.output
    assert calls == []


def test_source_version_cannot_bind_a_different_variant(monkeypatch: Any) -> None:
    demo = _load_demo_module()
    monkeypatch.setattr(
        demo,
        "_registration_module",
        lambda: (_ for _ in ()).throw(AssertionError("must reject before loading")),
    )

    with pytest.raises(demo.KitaruUsageError, match="is frozen to variant"):
        demo._registered_agent(
            variant="mini_tool_budget_2",
            version=demo.SOURCE_VERSION,
        )


def test_source_fixture_matches_json_text_agent_contract() -> None:
    demo = _load_demo_module()
    agent_module = importlib.import_module("reference_agent.agent")
    config_module = importlib.import_module("reference_agent.config")
    tools_module = importlib.import_module("reference_agent.tools")

    wrapped = agent_module.build_support_agent(
        config_module.load_variant(demo.SOURCE_VARIANT),
        model=TestModel(),
    ).wrapped
    assert wrapped.output_type is str
    expected_tools = [
        {
            "type": "function",
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.function_schema.json_schema,
        }
        for tool in wrapped._function_toolset.tools.values()
    ]

    fixture = DEMO_ROOT / "trace_fixtures" / "imported-support-cases.jsonl"
    rows = [
        json.loads(line) for line in fixture.read_text(encoding="utf-8").splitlines()
    ]
    for row in rows:
        assert row["traceVersion"] == demo.SOURCE_VERSION
        assert row["metadata"]["agent_version"] == demo.SOURCE_VERSION
        assert row["metadata"]["variant"] == demo.SOURCE_VARIANT
        assert row["input"]["tools"] == expected_tools
        final_text = row["input"]["messages"][-1]["content"]
        assert isinstance(final_text, str)
        config_module.SupportDecision.model_validate_json(final_text)
        assert json.loads(final_text) == row["output"]
        assert row["metadata"]["fixture_generation_id"] == (
            "kitaru-replay-example-json-text-v1"
        )
        assert row["metadata"]["fixture_contract_revision"] == (
            "structured-escalation-derived-v1"
        )

    account_row = next(
        row for row in rows if row["traceId"] == "support-account-setting"
    )
    escalation_call = next(
        tool_call["function"]
        for message in account_row["input"]["messages"]
        for tool_call in message.get("tool_calls", [])
        if tool_call["function"]["name"] == "escalate_to_human"
    )
    escalation_result = next(
        message["content"]
        for message in account_row["input"]["messages"]
        if message.get("name") == "escalate_to_human"
    )
    assert escalation_call["arguments"] == {
        "customer_id": "cust_acme",
        "policy_label": "permissions_policy",
    }
    assert escalation_result["args"] == escalation_call["arguments"]
    assert (
        escalation_result["result"]["reason"]
        == tools_module.ESCALATION_AUDIT_REASONS["permissions_policy"]
    )
    assert escalation_result["blocked"] is False
    assert escalation_result["wrote_state"] is True


def test_experiment_replays_explicit_imported_set_with_objective(
    monkeypatch: Any,
) -> None:
    demo = _load_demo_module()
    calls: list[dict[str, Any]] = []
    objective = object()

    class FakeAgent:
        def replay(self, selected: list[str], **kwargs: Any) -> dict[str, Any]:
            calls.append({"execution_ids": selected, **kwargs})
            return {"target_count": len(selected)}

    monkeypatch.setattr(
        demo,
        "_registered_agent",
        lambda **_kwargs: (FakeAgent(), objective),
    )
    result = CliRunner().invoke(
        demo.cli,
        [
            "experiment",
            "run-a",
            "run-b",
            "run-c",
            "--idempotency-key",
            "permissions-v2-attempt-1",
            "--repeats",
            "2",
            "--output",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        {
            "execution_ids": ["run-a", "run-b", "run-c"],
            "imported_mode": demo.ImportedReplayMode.ROOT_INPUT,
            "on_error": "collect",
            "idempotency_key": "permissions-v2-attempt-1",
            "repeats": 2,
            "wait": True,
            "name": demo.DEFAULT_EXPERIMENT,
            "suite_key": demo.DEFAULT_EXPERIMENT,
            "scorers": [objective],
            "objective_minimum_mean": 1.0,
        }
    ]
    assert '"target_count": 3' in result.output


def test_resume_command_forwards_explicit_boundary_selection(monkeypatch: Any) -> None:
    demo = _load_demo_module()
    calls: list[dict[str, Any]] = []

    def resume(execution_id: str, **kwargs: Any) -> dict[str, Any]:
        calls.append({"execution_id": execution_id, **kwargs})
        return {"resumed": execution_id}

    monkeypatch.setattr(demo, "_resume_case", resume)
    result = CliRunner().invoke(
        demo.cli,
        [
            "resume",
            "imported-run",
            "--boundary-kind",
            "tool-result",
            "--boundary-index",
            "2",
            "--idempotency-key",
            "resume-1",
            "--output",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls[0]["boundary_kind"] == "tool-result"
    assert calls[0]["boundary_index"] == 2
    assert calls[0]["candidate_variant"] == demo.DEFAULT_CANDIDATE_VARIANT
    assert calls[0]["candidate_version"] == demo.DEFAULT_CANDIDATE_VERSION


def test_resume_command_defaults_to_readable_summary(monkeypatch: Any) -> None:
    demo = _load_demo_module()
    monkeypatch.setattr(
        demo, "_resume_case", lambda *_args, **_kwargs: _replay_result_stub()
    )

    result = CliRunner().invoke(
        demo.cli,
        [
            "resume",
            "imported-run",
            "--idempotency-key",
            "resume-readable",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "HOLD  suite-one" in result.output
    assert "Trials" in result.output
    assert "1/1 verified" in result.output
    assert "Recorded replies" in result.output
    assert "0/1 served, 1 missed" in result.output
    assert "Blocked calls" in result.output
    assert "imported_replay_not_comparable" in result.output
    assert '"planning_rows"' not in result.output


def test_replay_command_uses_real_helper_signature(monkeypatch: Any) -> None:
    demo = _load_demo_module()

    class FakeAgent:
        def replay(self, _selected: list[str], **_kwargs: Any) -> SimpleNamespace:
            return _replay_result_stub()

    monkeypatch.setattr(
        demo,
        "_registered_agent",
        lambda **_kwargs: (FakeAgent(), object()),
    )

    result = CliRunner().invoke(
        demo.cli,
        [
            "replay",
            "imported-run",
            "--idempotency-key",
            "replay-readable",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "HOLD  suite-one" in result.output


def test_rerun_command_prints_summary_before_nonzero_exit(monkeypatch: Any) -> None:
    demo = _load_demo_module()
    calls: list[dict[str, Any]] = []

    def rerun(*_args: Any, **kwargs: Any) -> SimpleNamespace:
        calls.append(kwargs)
        return _replay_result_stub()

    monkeypatch.setattr(demo, "_rerun_suite", rerun)

    result = CliRunner().invoke(
        demo.cli,
        [
            "rerun",
            "suite-one",
            "--idempotency-key",
            "rerun-readable",
        ],
    )

    assert result.exit_code == 1
    assert "HOLD  suite-one" in result.output
    assert "Why" in result.output
    assert "Traceback" not in result.output
    assert calls[0]["assert_pass"] is False


def test_rerun_command_uses_limits_objective_and_asserts_pass(monkeypatch: Any) -> None:
    demo = _load_demo_module()
    calls: list[dict[str, Any]] = []
    objective = object()

    class FakeResult:
        def assert_pass(self) -> None:
            calls.append({"operation": "assert_pass"})

        def to_json(self) -> dict[str, Any]:
            return {"verdict": "pass"}

    class FakeAgent:
        def replay(self, **kwargs: Any) -> FakeResult:
            calls.append({"operation": "replay", **kwargs})
            return FakeResult()

    monkeypatch.setattr(
        demo,
        "_registered_agent",
        lambda **_kwargs: (FakeAgent(), objective),
    )
    result = CliRunner().invoke(
        demo.cli,
        [
            "rerun",
            "support-agent-permissions-v2",
            "--idempotency-key",
            "permissions-v2-attempt-2",
            "--max-trials",
            "2",
            "--max-cost-usd",
            "0.5",
            "--max-incurred-tokens",
            "5000",
            "--max-duration-seconds",
            "60",
            "--output",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls[0]["operation"] == "replay"
    assert calls[0]["experiment"] == "support-agent-permissions-v2"
    assert calls[0]["idempotency_key"] == "permissions-v2-attempt-2"
    assert calls[0]["repeats"] == 1
    assert calls[0]["scorers"] == [objective]
    assert calls[0]["limits"] == demo.RegressionLimits(
        max_trials=2,
        max_cost_usd=0.5,
        max_incurred_tokens=5000,
        max_duration_seconds=60.0,
    )
    assert calls[1] == {"operation": "assert_pass"}
    assert '"verdict": "pass"' in result.output


def test_inspect_execution_uses_compact_attempt_reference(
    monkeypatch: Any,
) -> None:
    demo = _load_demo_module()
    attempt = SimpleNamespace(
        experiment_id="experiment-one",
        spec=SimpleNamespace(suite_key="suite-one"),
        verdict=None,
        record=SimpleNamespace(imported_replay_members=[]),
        to_json=lambda: (_ for _ in ()).throw(
            AssertionError("execution inspection must not serialize full attempts")
        ),
    )
    scores = list(range(demo._INSPECTION_MAX_SCORES + 1))
    execution = SimpleNamespace(
        exec_id="run-one",
        status="completed",
        project_id="project-one",
        original_exec_id=None,
        root_exec_id=None,
        metadata={demo.EXPERIMENT_ID_METADATA_KEY: "experiment-one"},
        import_info=None,
        llm_usage_summary=None,
        scores=SimpleNamespace(list=lambda: scores),
    )
    client = SimpleNamespace(
        executions=SimpleNamespace(get=lambda _execution_id: execution),
        agents=SimpleNamespace(
            experiments=SimpleNamespace(
                get_attempt=lambda experiment_id, *, agent: attempt,
            )
        ),
    )

    result = demo._inspect_execution("run-one", client=client)

    assert result["experiments"] == [
        {
            "experiment_id": "experiment-one",
            "suite_key": "suite-one",
            "verdict": None,
        }
    ]
    assert len(result["scores"]) == demo._INSPECTION_MAX_SCORES
    assert result["scores_truncated"] is True


def test_inspect_execution_text_skips_score_history(monkeypatch: Any) -> None:
    demo = _load_demo_module()
    calls: list[dict[str, Any]] = []

    def inspect(execution_id: str, **kwargs: Any) -> dict[str, Any]:
        calls.append({"execution_id": execution_id, **kwargs})
        return {
            "execution_id": execution_id,
            "status": "completed",
            "immediate_parent_id": "imported-run",
            "root_execution_id": "imported-run",
            "import": None,
            "cost": None,
            "scores": [],
            "scores_truncated": False,
            "scores_omitted": True,
            "available_boundaries": [],
        }

    monkeypatch.setattr(demo, "_inspect_execution", inspect)

    result = CliRunner().invoke(demo.cli, ["inspect-execution", "candidate-run"])

    assert result.exit_code == 0, result.output
    assert calls == [
        {
            "execution_id": "candidate-run",
            "include_scores": False,
            "boundary_limit": 10,
        }
    ]
    assert "use --output json for score records" in result.output


def test_inspect_experiment_reuses_attempt_context_and_page_metadata(
    monkeypatch: Any,
) -> None:
    demo = _load_demo_module()
    page_calls: list[tuple[int, int]] = []
    inspected: list[dict[str, Any]] = []
    members = [SimpleNamespace(id=f"run-{index}") for index in range(100)]

    class Runs:
        def list(self, *, page: int, size: int) -> SimpleNamespace:
            page_calls.append((page, size))
            if page != 1:
                raise AssertionError("page metadata should prevent an empty request")
            return SimpleNamespace(items=members, total=200, total_pages=2)

    serializations = 0

    def serialize_attempt() -> dict[str, str]:
        nonlocal serializations
        serializations += 1
        return {"experiment_id": "experiment-one"}

    attempt = SimpleNamespace(
        experiment_id="experiment-one",
        runs=Runs(),
        score_aggregate={"scored": 100},
        to_json=serialize_attempt,
    )
    experiments = SimpleNamespace(
        resolve_source=lambda source, *, agent: attempt,
    )
    client = SimpleNamespace(agents=SimpleNamespace(experiments=experiments))
    monkeypatch.setattr(demo, "KitaruClient", lambda: client)

    def inspect(execution_id: str, **kwargs: Any) -> dict[str, str]:
        inspected.append({"execution_id": execution_id, **kwargs})
        return {"execution_id": execution_id}

    monkeypatch.setattr(demo, "_inspect_execution", inspect)

    result = demo._inspect_experiment(
        "experiment-one",
        page=1,
        page_size=100,
    )

    assert page_calls == [(1, 100)]
    assert serializations == 1
    assert result["member_page"] == {
        "page": 1,
        "page_size": 100,
        "returned": 100,
        "total": 200,
        "total_pages": 2,
        "has_more": True,
    }
    assert len(result["members"]) == 100
    assert all(item["client"] is client for item in inspected)
    assert all(item["attempts"] == (attempt,) for item in inspected)


def test_inspect_experiment_text_skips_deep_member_loading(monkeypatch: Any) -> None:
    demo = _load_demo_module()
    members = [SimpleNamespace(id="run-one"), SimpleNamespace(id="run-two")]
    member_page = SimpleNamespace(items=members, total=2, total_pages=1)
    record = SimpleNamespace(
        spec=SimpleNamespace(experiment_id="experiment-one", suite_key="suite-one"),
        status="completed",
        counts=SimpleNamespace(
            model_dump=lambda **_kwargs: {"intended": 2, "verified": 2}
        ),
        imported_replay_evidence=SimpleNamespace(
            model_dump=lambda **_kwargs: {
                "comparability": "recorded_path_comparable",
                "recorded_response_hits": 2,
                "eligible_recorded_responses": 2,
                "recorded_response_misses": 0,
                "blocked_calls": 0,
                "path_divergences": 0,
            }
        ),
        verdict=None,
    )
    attempt = SimpleNamespace(
        record=record,
        runs=SimpleNamespace(list=lambda **_kwargs: member_page),
        to_json=lambda: (_ for _ in ()).throw(
            AssertionError("text output must not serialize the full attempt")
        ),
    )
    experiments = SimpleNamespace(
        resolve_source=lambda _source, *, agent: attempt,
    )
    client = SimpleNamespace(agents=SimpleNamespace(experiments=experiments))
    monkeypatch.setattr(demo, "KitaruClient", lambda: client)
    monkeypatch.setattr(
        demo,
        "_inspect_execution",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("text output must not inspect every member")
        ),
    )

    result = CliRunner().invoke(demo.cli, ["inspect-experiment", "suite-one"])

    assert result.exit_code == 0, result.output
    assert "NOT GRADED  suite-one" in result.output
    assert "2/2 verified" in result.output
    assert "2/2 served, 0 missed" in result.output
    assert "2 shown · 2 total · page 1/1" in result.output
