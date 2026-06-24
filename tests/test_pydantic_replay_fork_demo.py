"""Contract tests for the PydanticAI replay-fork demo."""

from __future__ import annotations

import ast
import json
from pathlib import Path

DEMO_ROOT = Path("examples/end_to_end/pydantic_replay_fork")


def test_support_agent_declares_replay_point() -> None:
    source = (DEMO_ROOT / "support_agent.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    replay_points = [
        node.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
        and target.id == "REPLAY_POINT"
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    ]
    assert replay_points == ["lookup_policy_tool"]


def test_demo_uses_new_replay_api() -> None:
    source = (DEMO_ROOT / "demo.py").read_text(encoding="utf-8")
    assert "at=REPLAY_POINT" in source
    assert "kitaru.cohort" in source or "kitaru import" in source
    assert "seed" in source
    assert "seed-cohort" in source
    assert 'tool={"lookup_policy": "mocks.lookup_policy"}' in source


def test_cohort_scenarios_file_has_ten_entries() -> None:
    payload = json.loads(
        (DEMO_ROOT / "fixtures" / "cohort_scenarios.json").read_text(encoding="utf-8")
    )
    assert len(payload) >= 10
    for item in payload[:10]:
        assert {"label", "customer", "prompt"} <= set(item)


def test_cohort_uses_replay_many() -> None:
    source = (DEMO_ROOT / "utils" / "cohort.py").read_text(encoding="utf-8")
    assert "replay_many" in source
    assert "at=REPLAY_POINT" in source
    assert "reproduce" not in source


def test_readme_documents_at_not_from() -> None:
    readme = (DEMO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "--at lookup_policy_tool" in readme
    assert "--from lookup_policy_tool" not in readme
