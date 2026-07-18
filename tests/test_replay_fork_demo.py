"""CLI contract tests for the PydanticAI replay example."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
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


class _FakeImports:
    def __init__(self, calls: list[dict[str, Any]]) -> None:
        self._calls = calls

    def langfuse(self, path: str, **kwargs: Any) -> dict[str, Any]:
        self._calls.append({"path": path, **kwargs})
        return {"selected_trace_count": 1, "dry_run": kwargs["dry_run"]}


def test_import_command_uses_langfuse_jsonl_api(monkeypatch: Any) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
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
            "--commit",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        {
            "path": "trace_fixtures/support-traces.jsonl",
            "source_project_id": "langfuse-project",
            "agent_name": demo.AGENT_NAME,
            "trace_ids": ["trace-48211"],
            "dry_run": False,
            "confirm_data_storage": True,
        }
    ]
    assert '"dry_run": false' in result.output


def test_import_command_defaults_to_read_only_dry_run(monkeypatch: Any) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
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
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls[0]["trace_ids"] is None
    assert calls[0]["dry_run"] is True
    assert calls[0]["confirm_data_storage"] is False


def test_register_command_uses_agent_version_as_label(monkeypatch: Any) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    demo = _load_demo_module()
    calls: list[dict[str, Any]] = []

    class FakeAgent:
        def register(self, **kwargs: Any) -> dict[str, Any]:
            calls.append(kwargs)
            return {"created": True}

    monkeypatch.setattr(demo, "kagent", FakeAgent())
    result = CliRunner().invoke(demo.cli, ["register"])

    assert result.exit_code == 0, result.output
    assert calls == [{"label": demo.AGENT_VERSION}]


def test_experiment_replays_explicit_native_set(monkeypatch: Any) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    demo = _load_demo_module()
    calls: list[dict[str, Any]] = []

    class FakeAgent:
        def register(self, **kwargs: Any) -> dict[str, Any]:
            calls.append({"operation": "register", **kwargs})
            return {"created": False}

        def replay(self, selected: list[str], **kwargs: Any) -> dict[str, Any]:
            calls.append({"operation": "replay", "execution_ids": selected, **kwargs})
            return {"target_count": len(selected)}

    monkeypatch.setattr(demo, "kagent", FakeAgent())
    result = CliRunner().invoke(
        demo.cli,
        [
            "experiment",
            "run-a",
            "run-b",
            "run-c",
            "--idempotency-key",
            "permissions-v2-attempt-1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == [
        {"operation": "register", "label": demo.AGENT_VERSION},
        {
            "operation": "replay",
            "execution_ids": ["run-a", "run-b", "run-c"],
            "at": demo.DEFAULT_AT,
            "on_error": "collect",
            "uncovered_policy": "fail",
            "idempotency_key": "permissions-v2-attempt-1",
            "repeats": 3,
            "wait": False,
            "name": demo.DEFAULT_EXPERIMENT,
        },
    ]
    assert '"target_count": 3' in result.output
