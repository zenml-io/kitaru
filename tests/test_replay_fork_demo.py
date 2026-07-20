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


def _load_demo_module(root: Path = DEMO_ROOT) -> ModuleType:
    demo_root = str(root.resolve())
    if demo_root not in sys.path:
        sys.path.insert(0, demo_root)
    spec = importlib.util.spec_from_file_location(
        "pydantic_replay_fork_demo_under_test",
        root / "demo.py",
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_help_exposes_only_replay_and_inspection_commands() -> None:
    demo = _load_demo_module()

    result = CliRunner().invoke(demo.cli, ["--help"])

    assert result.exit_code == 0, result.output
    assert "register" not in result.output
    assert "import-traces" not in result.output
    for command in (
        "experiment",
        "inspect-execution",
        "inspect-experiment",
        "replay",
        "rerun",
        "resume",
    ):
        assert command in result.output


def test_demo_accepts_an_explicit_agent_name(monkeypatch: Any) -> None:
    monkeypatch.setenv("KITARU_AGENT_NAME", "support-agent-3")

    demo = _load_demo_module()

    assert demo.AGENT_NAME == "support-agent-3"


def test_resume_command_rejects_negative_boundary_index() -> None:
    demo = _load_demo_module()

    result = CliRunner().invoke(
        demo.cli,
        ["resume", "imported-run", "--boundary-index", "-1"],
    )

    assert result.exit_code == 2
    assert "is not in the range" in result.output


def test_canonical_idempotency_is_stable_and_covers_behavior_inputs() -> None:
    demo = _load_demo_module()
    request = {
        "execution_ids": ["run-a", "run-b"],
        "boundary": {"kind": "tool-result", "sequence": 2},
        "candidate_variant": "baseline",
        "candidate_version": "candidate-v1",
        "name": "suite-one",
        "repeats": 1,
    }

    first = demo._canonical_idempotency_key("resume", **request)
    repeated = demo._canonical_idempotency_key("resume", **request)

    assert first == repeated
    for field, changed in (
        ("execution_ids", ["run-b", "run-a"]),
        ("boundary", {"kind": "tool-result", "sequence": 3}),
        ("candidate_variant", "mini_tool_budget_2"),
        ("candidate_version", "candidate-v2"),
        ("name", "suite-two"),
        ("repeats", 2),
    ):
        assert (
            demo._canonical_idempotency_key("resume", **{**request, field: changed})
            != first
        )


def test_exploratory_replay_derives_key_from_inputs_and_honors_override(
    monkeypatch: Any,
) -> None:
    demo = _load_demo_module()
    calls: list[dict[str, Any]] = []

    def replay(execution_ids: list[str], **kwargs: Any) -> dict[str, Any]:
        calls.append({"execution_ids": execution_ids, **kwargs})
        return {"target_count": len(execution_ids)}

    monkeypatch.setattr(demo, "_replay_cases", replay)
    runner = CliRunner()
    invocations = [
        ["replay", "run-a", "--output", "json"],
        ["replay", "run-a", "--output", "json"],
        ["replay", "run-b", "--output", "json"],
        ["replay", "run-a", "--name", "named-case", "--output", "json"],
        ["replay", "run-a", "--repeats", "2", "--output", "json"],
        [
            "replay",
            "run-a",
            "--candidate-version",
            "counterfactual-v2",
            "--output",
            "json",
        ],
        [
            "replay",
            "run-a",
            "--idempotency-key",
            "manual-replay-key",
            "--output",
            "json",
        ],
    ]
    results = [runner.invoke(demo.cli, args) for args in invocations]

    assert all(result.exit_code == 0 for result in results)
    keys = [json.loads(result.stdout)["idempotency_key"] for result in results]
    assert keys[0] == keys[1]
    assert all(changed_key != keys[0] for changed_key in keys[2:6])
    assert keys[6] == "manual-replay-key"
    assert calls[0]["candidate_variant"] == demo.DEFAULT_REPLAY_VARIANT
    assert calls[0]["candidate_version"] == demo.DEFAULT_REPLAY_VERSION
    assert calls[0]["idempotency_key"] == keys[0]
    assert calls[6]["idempotency_key"] == "manual-replay-key"


def test_json_execution_output_redirects_runtime_logs_to_stderr(
    monkeypatch: Any,
) -> None:
    demo = _load_demo_module()

    class Result:
        def to_json(self) -> dict[str, int]:
            print("serialization preamble")
            return {"target_count": 1}

    def replay(_execution_ids: list[str], **_kwargs: Any) -> Result:
        print("runtime preamble")
        return Result()

    monkeypatch.setattr(demo, "_replay_cases", replay)

    result = CliRunner().invoke(
        demo.cli,
        ["replay", "run-a", "--output", "json"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["target_count"] == 1
    assert "runtime preamble" not in result.stdout
    assert "runtime preamble" in result.stderr
    assert "serialization preamble" in result.stderr


def test_resume_json_redirects_boundary_and_runtime_logs(
    monkeypatch: Any,
) -> None:
    demo = _load_demo_module()

    def boundary(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        print("boundary preamble")
        return {"kind": "tool-result", "sequence": 2}

    def resume(*_args: Any, **_kwargs: Any) -> dict[str, bool]:
        print("resume preamble")
        return {"resumed": True}

    monkeypatch.setattr(demo, "_message_history_boundary", boundary)
    monkeypatch.setattr(demo, "_resume_case", resume)

    result = CliRunner().invoke(
        demo.cli,
        ["resume", "run-a", "--output", "json"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["resumed"] is True
    assert "boundary preamble" not in result.stdout
    assert "resume preamble" not in result.stdout
    assert "boundary preamble" in result.stderr
    assert "resume preamble" in result.stderr


@pytest.mark.parametrize(
    ("command", "helper"),
    (
        ("inspect-execution", "_inspect_execution"),
        ("inspect-experiment", "_inspect_experiment"),
    ),
)
def test_inspection_json_redirects_read_logs(
    monkeypatch: Any,
    command: str,
    helper: str,
) -> None:
    demo = _load_demo_module()

    def inspect(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        print("inspection preamble")
        return {"status": "completed"}

    monkeypatch.setattr(demo, helper, inspect)

    result = CliRunner().invoke(
        demo.cli,
        [command, "item-one", "--output", "json"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {"status": "completed"}
    assert "inspection preamble" not in result.stdout
    assert "inspection preamble" in result.stderr


def test_experiment_and_rerun_require_explicit_candidate_identity() -> None:
    demo = _load_demo_module()

    experiment = CliRunner().invoke(demo.cli, ["experiment", "run-a"])
    rerun = CliRunner().invoke(
        demo.cli,
        ["rerun", "suite-one", "--idempotency-key", "rerun-one"],
    )

    assert experiment.exit_code == 2
    assert "Missing option '--candidate-variant'" in experiment.output
    assert rerun.exit_code == 2
    assert "Missing option '--candidate-variant'" in rerun.output


def test_experiment_and_rerun_require_candidate_version_independently() -> None:
    demo = _load_demo_module()

    experiment = CliRunner().invoke(
        demo.cli,
        ["experiment", "run-a", "--candidate-variant", "baseline"],
    )
    rerun = CliRunner().invoke(
        demo.cli,
        [
            "rerun",
            "suite-one",
            "--idempotency-key",
            "rerun-one",
            "--candidate-variant",
            "baseline",
        ],
    )

    assert experiment.exit_code == 2
    assert "Missing option '--candidate-version'" in experiment.output
    assert rerun.exit_code == 2
    assert "Missing option '--candidate-version'" in rerun.output


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
            "kitaru-replay-example-20260720-regenerated"
        )
        assert row["metadata"]["fixture_contract_revision"] == (
            "pydantic-ai-final-generation-v1"
        )

    account_row = next(
        row for row in rows if row["traceId"] == "390972667ef147cbbbd6db2b30e8ad1b"
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


def test_raw_source_fixture_preserves_regenerated_langfuse_observations() -> None:
    fixture = DEMO_ROOT / "trace_fixtures" / "raw-imported-support-cases.jsonl"
    rows = [
        json.loads(line) for line in fixture.read_text(encoding="utf-8").splitlines()
    ]

    account_trace_id = "390972667ef147cbbbd6db2b30e8ad1b"
    status_trace_id = "00cbb102c7844e00aeb0149e8deea83b"
    assert len(rows) == 14
    assert {row["traceId"] for row in rows} == {account_trace_id, status_trace_id}
    assert {row["type"] for row in rows if row["traceId"] == account_trace_id} == {
        "AGENT",
        "GENERATION",
        "SPAN",
        "TOOL",
    }
    assert [
        row["name"]
        for row in rows
        if row["traceId"] == account_trace_id and row["type"] == "TOOL"
    ] == ["lookup_customer", "search_kb", "escalate_to_human"]
    roots = [row for row in rows if row["name"] == "support-agent"]
    assert {row["metadata"]["fixture_generation_id"] for row in roots} == {
        "kitaru-replay-example-20260720-regenerated"
    }
    assert all(
        "public_key" not in json.dumps(row.get("metadata", {})).lower() for row in rows
    )


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
            "--candidate-variant",
            "mini_tool_budget_2",
            "--candidate-version",
            "candidate-v1",
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

    monkeypatch.setattr(
        demo,
        "_message_history_boundary",
        lambda *_args, **_kwargs: {"kind": "tool-result", "sequence": 2},
    )
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
    assert calls[0]["candidate_variant"] == demo.DEFAULT_RESUME_VARIANT
    assert calls[0]["candidate_version"] == demo.DEFAULT_RESUME_VERSION
    assert calls[0]["boundary"] == {"kind": "tool-result", "sequence": 2}


def test_resume_command_defaults_to_readable_summary(monkeypatch: Any) -> None:
    demo = _load_demo_module()
    monkeypatch.setattr(
        demo,
        "_message_history_boundary",
        lambda *_args, **_kwargs: {"kind": "tool-result", "sequence": 2},
    )
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
    assert "kitaru agents experiments support-agent experiment-one" in result.output
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
            "--candidate-variant",
            "baseline",
            "--candidate-version",
            "candidate-v1",
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
            "--candidate-variant",
            "baseline",
            "--candidate-version",
            "candidate-v1",
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
