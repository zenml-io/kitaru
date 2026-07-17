"""CLI wiring tests for the case-first PydanticAI replay example."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

from click.testing import CliRunner

DEMO_ROOT = Path("examples/end_to_end/replay_fork_demo")


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


def test_import_command_starts_from_langfuse_trace(monkeypatch: Any) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    demo = _load_demo_module()
    calls: list[dict[str, Any]] = []

    def fake_import(source: str, *, trace_format: str | None, name: str | None) -> Any:
        calls.append({"source": source, "format": trace_format, "name": name})
        return {"imported": 1, "execution_ids": ["exec-imported"]}

    monkeypatch.setattr(demo, "_import_traces", fake_import)
    result = CliRunner().invoke(
        demo.cli,
        ["import-traces", "langfuse://trace/trace-48211", "--name", "ticket-48211"],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        {
            "source": "langfuse://trace/trace-48211",
            "format": None,
            "name": "ticket-48211",
        }
    ]
    assert '"exec-imported"' in result.output


def test_experiment_replays_complete_filtered_set(monkeypatch: Any) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    demo = _load_demo_module()
    executions = [object(), object(), object()]
    calls: list[dict[str, Any]] = []

    monkeypatch.setattr(demo, "_find_cases", lambda where: executions)

    def fake_replay(selected: list[Any], *, name: str) -> Any:
        calls.append({"executions": selected, "name": name})
        return {"target_count": len(selected)}

    monkeypatch.setattr(demo, "_replay_cases", fake_replay)
    result = CliRunner().invoke(
        demo.cli,
        ["experiment", "--where", "metadata.intent == 'permissions'"],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        {
            "executions": executions,
            "name": demo.DEFAULT_EXPERIMENT,
        }
    ]
    assert '"target_count": 3' in result.output


def test_score_sweep_reads_complete_filtered_set(monkeypatch: Any) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    demo = _load_demo_module()
    executions = [object(), object()]
    calls: list[dict[str, Any]] = []

    monkeypatch.setattr(demo, "_find_cases", lambda where: executions)

    def fake_score(selected: list[Any], *, name: str) -> Any:
        calls.append({"executions": selected, "name": name})
        return {"target_count": len(selected), "kind": "score"}

    monkeypatch.setattr(demo, "_score_cases", fake_score)
    result = CliRunner().invoke(demo.cli, ["score"])

    assert result.exit_code == 0, result.output
    assert calls == [
        {
            "executions": executions,
            "name": demo.DEFAULT_SCORE_SWEEP,
        }
    ]
    assert '"kind": "score"' in result.output
