"""Contract tests for the replay overrides demo."""

from __future__ import annotations

import ast
from pathlib import Path

DEMO_ROOT = Path("examples/end_to_end/replay_overrides_demo")
README = DEMO_ROOT / "README.md"
DEMO = DEMO_ROOT / "demo.py"
SCENARIO_DIR = DEMO_ROOT / "replay_scenarios"
SEED_SCRIPT = DEMO_ROOT / "seed_prod_runs.py"

NEW_FLAGS = (
    "--flow-overrides",
    "--checkpoint-overrides",
    "--invocation-overrides",
    "--skip",
    "--tag",
    "--on-error",
    "diff-matrix",
)
OLD_TERMS = (
    "replay_many",
    "replay-many",
    "--args",
    "--mock-output",
    "--tool",
    "--llm-model",
    "inject-output",
    "record_replay_observation",
)
REPLAY_SCENARIO_FILES = (
    "flow_override.py",
    "publish_input.py",
    "checkpoint_code_swap.py",
    "invocation_model_override.py",
    "explicit_skip.py",
    "tagged_batch.py",
)


def test_support_agent_declares_required_graph_names() -> None:
    source = (DEMO_ROOT / "support_agent.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    assignments: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Constant):
            continue
        if not isinstance(node.value.value, str):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                assignments[target.id] = node.value.value

    assert assignments["FLOW_NAME"] == "support_copilot_flow"
    assert assignments["REPLAY_POINT"] == "lookup_policy_tool"
    assert assignments["FINAL_DECISION_CHECKPOINT"] == "publish_support_decision"


def test_support_agent_has_no_synthetic_replay_tail() -> None:
    source = (DEMO_ROOT / "support_agent.py").read_text(encoding="utf-8")

    assert "record_replay_observation" not in source
    assert "REPORTING_CHECKPOINT" not in source


def test_readme_documents_new_replay_surface_only() -> None:
    combined = README.read_text(encoding="utf-8")

    assert ".env.example" in combined
    assert "demo.py flow-override" in combined
    assert "demo.py publish-input" in combined
    assert "replay_scenarios/" in combined
    for flag in NEW_FLAGS:
        assert flag in combined
    for old_term in OLD_TERMS:
        assert old_term not in combined


def test_replay_scenarios_use_unified_client_replay_api() -> None:
    combined = "\n".join(
        (SCENARIO_DIR / name).read_text(encoding="utf-8") for name in REPLAY_SCENARIO_FILES
    )

    assert "client.executions.replay(" in combined
    assert "flow_overrides=" in combined
    assert "checkpoint_overrides=" in combined
    assert "invocation_overrides=" in combined
    assert "tag=REPLAY_TAG" in combined
    assert "skip=[FINAL_DECISION_CHECKPOINT]" in combined
    assert 'FINAL_DECISION_CHECKPOINT: {"input": INJECTED_DECISION}' in combined
    assert "at=FINAL_DECISION_CHECKPOINT" in combined
    assert "wait=True" in combined
    assert "sys.path.insert" not in combined
    assert ".replay_many(" not in combined
    assert "replay_many" not in combined


def test_demo_dispatches_replay_scenarios() -> None:
    source = DEMO.read_text(encoding="utf-8")

    assert "from replay_scenarios import" in source
    assert "def resolve_prod_id(" in source
    assert "def resolve_prod_ids(" in source
    assert "def resolve_replay_id(" in source
    assert 'command == "flow-override"' in source
    assert 'command == "publish-input"' in source
    assert "publish_input.replay_with_publish_input_override(resolve_prod_id())" in source
    assert "flow_override.replay_with_flow_overrides(resolve_prod_id())" in source
    assert "seed_prod_runs(" in source
    assert "tagged_batch.replay_tagged_batch(resolve_prod_ids(" in source
    assert "diff_report.report_execution_diff(" in source
    assert "resolve_replay_id(" in source
    assert "inject-output" not in source


def test_replay_scenarios_use_descriptive_entrypoints() -> None:
    combined = "\n".join(
        (SCENARIO_DIR / name).read_text(encoding="utf-8") for name in REPLAY_SCENARIO_FILES
    )

    assert "def resolve_prod_id(" not in combined
    assert "def main(" not in combined
    assert "def replay_with_flow_overrides(prod_id: str)" in combined
    assert "def replay_with_publish_input_override(prod_id: str)" in combined
    assert "def replay_tagged_batch(prod_ids: list[str])" in combined


def test_seed_script_writes_prod_exec_ids_fixture() -> None:
    source = SEED_SCRIPT.read_text(encoding="utf-8")

    assert "support_copilot_flow.run(" in source
    assert "prod_exec_ids" in source
    assert "DEFAULT_COUNT = 1" in source
    assert "wait_for_execution(handle)" in source


def test_scenarios_fixture_has_fifteen_entries() -> None:
    import json

    payload = json.loads((DEMO_ROOT / "fixtures" / "scenarios.json").read_text())
    assert len(payload) >= 15
    for item in payload:
        assert {"label", "customer", "prompt"} <= set(item)


def test_example_coverage_lists_manual_live_caveats() -> None:
    manifest = Path("examples/example-coverage.yaml").read_text(encoding="utf-8")

    assert "id: replay-overrides-demo" in manifest
    assert "path: examples/end_to_end/replay_overrides_demo/demo.py" in manifest
    assert "test_file: tests/test_replay_overrides_demo.py" in manifest
    assert "status: manual_only" in manifest
    assert "provider: openai" in manifest
    assert "OPENAI_API_KEY" in manifest
    assert "gpt-5-mini" in manifest
    assert "gpt-5-nano" in manifest


def test_env_example_documents_openai_key() -> None:
    env_example = (DEMO_ROOT / ".env.example").read_text(encoding="utf-8")
    assert "OPENAI_API_KEY=" in env_example
    assert "cp .env.example .env" in env_example


def test_load_support_decision_prefers_flow_result_ref() -> None:
    import sys
    from types import SimpleNamespace

    sys.path.insert(0, str(DEMO_ROOT.resolve()))
    try:
        from utils.load_decision import load_support_decision
    finally:
        sys.path.pop(0)

    decision = {
        "policy_label": "restricted_account_change",
        "risk_status": "needs_review",
        "required_action": "escalate_to_human",
        "summary": "Escalate",
    }
    client = SimpleNamespace(
        executions=SimpleNamespace(
            get=lambda _exec_id: SimpleNamespace(
                metadata={"kitaru_flow_result_ref_v1": "artifact-123"},
                checkpoints=[],
                list_artifacts=lambda: [],
            )
        ),
        artifacts=SimpleNamespace(
            get=lambda artifact_id: SimpleNamespace(load=lambda: decision)
        ),
    )

    loaded = load_support_decision(client, "exec-id")  # type: ignore[arg-type]
    assert loaded == decision
