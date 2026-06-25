"""Contract tests for the replay overrides demo."""

from __future__ import annotations

import ast
from pathlib import Path

DEMO_ROOT = Path("examples/end_to_end/replay_overrides_demo")
README = DEMO_ROOT / "README.md"
PLAYBOOK = DEMO_ROOT / "PLAYBOOK.md"
DEMO = DEMO_ROOT / "demo.py"

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


def test_readme_and_playbook_document_new_replay_surface_only() -> None:
    combined = "\n".join(
        [
            README.read_text(encoding="utf-8"),
            PLAYBOOK.read_text(encoding="utf-8"),
        ]
    )

    for flag in NEW_FLAGS:
        assert flag in combined
    for old_term in OLD_TERMS:
        assert old_term not in combined


def test_demo_uses_unified_client_replay_api_only() -> None:
    source = DEMO.read_text(encoding="utf-8")

    assert "client.executions.replay(" in source
    assert "at=REPLAY_POINT" in source
    assert "flow_overrides=" in source
    assert "checkpoint_overrides=" in source
    assert "invocation_overrides=" in source
    assert "tag=REPLAY_TAG" in source
    assert "skip=[FINAL_DECISION_CHECKPOINT]" in source
    assert ".replay_many(" not in source
    assert "replay_many" not in source


def test_scenarios_fixture_is_batch_ready() -> None:
    import json

    payload = json.loads((DEMO_ROOT / "fixtures" / "scenarios.json").read_text())
    assert len(payload) >= 4
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
