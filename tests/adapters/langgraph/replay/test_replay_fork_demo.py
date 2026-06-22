"""Regression tests for the replay/fork demo import path."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import click
import pytest
from click.testing import CliRunner

from kitaru.adapters.langgraph.replay import import_langgraph_trace

DEMO_ROOT = Path("examples/end_to_end/replay_fork_demo")
FIXTURES_DIR = DEMO_ROOT / "reference_agent" / "fixtures"
RICH_FIXTURE = FIXTURES_DIR / "langfuse_rich_observations.jsonl"
SHALLOW_FIXTURE = FIXTURES_DIR / "langfuse_export.jsonl"
RICH_TRACE_ID = "trace-replay-fork-rich-baseline"
SHALLOW_TRACE_ID = "0dd856f91d31445fa3ce3bb9e3b2d400"


def _load_demo_module() -> ModuleType:
    """Load demo.py the same way a user runs it from the demo directory."""
    demo_root = str(DEMO_ROOT.resolve())
    if demo_root not in sys.path:
        sys.path.insert(0, demo_root)
    sys.modules.pop("utils", None)
    spec = importlib.util.spec_from_file_location(
        "replay_fork_demo_cli_under_test",
        DEMO_ROOT / "demo.py",
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def test_demo_jsonl_loader_requires_trace_id_for_multiple_traces() -> None:
    demo = _load_demo_module()

    with pytest.raises(click.ClickException) as error:
        demo._load(str(SHALLOW_FIXTURE))

    message = str(error.value)
    assert "contains 18 traces" in message
    assert "Select one with --trace-id" in message
    assert "0dd856f91d31445fa3ce3bb9e3b2d400" in message


def test_demo_jsonl_loader_rejects_rows_without_trace_ids(tmp_path: Path) -> None:
    demo = _load_demo_module()
    path = tmp_path / "no_trace_ids.jsonl"
    path.write_text('{"id": "obs-without-trace"}\n', encoding="utf-8")

    with pytest.raises(click.ClickException) as error:
        demo._load(str(path))

    message = str(error.value)
    assert "do not contain Langfuse observation trace IDs" in message
    assert "not top-level case summaries" in message


def test_demo_preflight_rejects_shallow_fixture_after_trace_selection() -> None:
    demo = _load_demo_module()

    with pytest.raises(click.ClickException) as error:
        demo._load_demo_case(str(SHALLOW_FIXTURE), trace_id=SHALLOW_TRACE_ID)

    assert "Imported trace has 0 recorded calls" in str(error.value)


def test_rich_demo_fixture_imports_and_summarizes(
    capsys: pytest.CaptureFixture[str],
) -> None:
    demo = _load_demo_module()

    case = demo._load_demo_case(str(RICH_FIXTURE), trace_id=RICH_TRACE_ID)
    captured = capsys.readouterr()

    assert case.source_ref.source_id == RICH_TRACE_ID
    assert len(case.recorded_calls) > 0
    assert "Trace import summary" in captured.out
    assert "recorded calls:" in captured.out
    assert "collect_evidence_with_tools" in captured.out
    assert "side-effect note:" in captured.out


def test_rich_demo_fixture_has_node_outputs_needed_for_cut() -> None:
    rows = _read_jsonl(RICH_FIXTURE)
    case = import_langgraph_trace(rows=rows, trace_id=RICH_TRACE_ID)
    demo = _load_demo_module()

    summary = demo.utils.summarize_case(case)

    assert summary.missing_upstream_node_outputs == []
    assert summary.node_output_names == [
        "receive_request",
        "collect_evidence_with_tools",
        "summarize_evidence",
        "decide_action",
        "final_response",
    ]


def test_import_trace_command_prints_practical_summary() -> None:
    demo = _load_demo_module()
    runner = CliRunner()

    result = runner.invoke(
        demo.cli,
        ["import-trace", str(RICH_FIXTURE), "--trace-id", RICH_TRACE_ID],
    )

    assert result.exit_code == 0, result.output
    assert "Trace import summary" in result.output
    assert "recorded calls:" in result.output
    assert "case-enterprise-permission-001" in result.output
