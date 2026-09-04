#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Tests for the local insight-generation harness."""

import importlib.util
import json
import uuid
from pathlib import Path
from types import ModuleType

import pytest
from pydantic import ValidationError

from kitaru.insights.models import InsightGenerationResult

REPO_ROOT = Path(__file__).parents[2]
SCRIPT = REPO_ROOT / "scripts" / "generate_insights.py"


def _load_harness() -> ModuleType:
    spec = importlib.util.spec_from_file_location("generate_insights", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _context() -> dict[str, object]:
    return {
        "agent_id": str(uuid.UUID("01990000-0000-7000-8000-000000000002")),
        "agent_name": "returns-agent",
        "source_import": {
            "task_id": str(uuid.UUID("01990000-0000-7000-8000-000000000003")),
            "provider": "synthetic",
        },
    }


async def test_harness_runs_deterministically_without_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _load_harness()
    sessions_path = tmp_path / "sessions.json"
    context_path = tmp_path / "context.json"
    output_path = tmp_path / "result.json"
    sessions_path.write_text("[]")
    context_path.write_text(json.dumps(_context()))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("KITARU_INSIGHTS_LANGFUSE_SECRET_KEY", raising=False)

    exit_code = await harness.run(
        [
            "--sessions",
            str(sessions_path),
            "--context",
            str(context_path),
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    result = InsightGenerationResult.model_validate_json(output_path.read_text())
    assert result.empty_reason == "no_eligible_candidates"


async def test_harness_rejects_unpaired_surrogate_before_empty_serialization(
    tmp_path: Path,
) -> None:
    harness = _load_harness()
    sessions_path = tmp_path / "sessions.json"
    context_path = tmp_path / "context.json"
    output_path = tmp_path / "result.json"
    sessions_path.write_text("[]")
    context = _context()
    context["agent_name"] = "broken-\ud800-name"
    context_path.write_text(json.dumps(context))

    with pytest.raises(ValidationError, match="valid UTF-8 text"):
        await harness.run(
            [
                "--sessions",
                str(sessions_path),
                "--context",
                str(context_path),
                "--output",
                str(output_path),
            ]
        )

    assert not output_path.exists()


def test_production_output_must_be_outside_or_ignored(tmp_path: Path) -> None:
    harness = _load_harness()

    with pytest.raises(ValueError, match="gitignored"):
        harness.validate_output_path(
            REPO_ROOT / "insight-output.json", production_derived=True
        )

    harness.validate_output_path(
        REPO_ROOT / "devtools" / ".run" / "insight-output.json",
        production_derived=True,
    )
    harness.validate_output_path(
        tmp_path / "insight-output.json", production_derived=True
    )


def test_harness_rejects_raw_jsonl(tmp_path: Path) -> None:
    harness = _load_harness()
    raw_export = tmp_path / "langfuse.jsonl"
    raw_export.write_text("{}\n")

    with pytest.raises(ValueError, match="not JSONL"):
        harness._load_sessions(raw_export)


def test_invalid_observation_configuration_is_best_effort(monkeypatch) -> None:
    harness = _load_harness()
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "generic-public")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "generic-secret")
    monkeypatch.delenv("KITARU_INSIGHTS_LANGFUSE_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("KITARU_INSIGHTS_LANGFUSE_SECRET_KEY", raising=False)

    assert harness._build_observer(True) is None


def test_broken_observation_client_is_best_effort(monkeypatch) -> None:
    harness = _load_harness()
    monkeypatch.setenv("KITARU_INSIGHTS_LANGFUSE_PUBLIC_KEY", "insight-public")
    monkeypatch.setenv("KITARU_INSIGHTS_LANGFUSE_SECRET_KEY", "insight-secret")

    def fail_import(name: str):
        raise RuntimeError("invalid observer configuration")

    monkeypatch.setattr(
        "kitaru.insights.observability.importlib.import_module", fail_import
    )

    assert harness._build_observer(True) is None


def test_atomic_output_preserves_previous_file_on_replace_failure(
    tmp_path: Path, monkeypatch
) -> None:
    harness = _load_harness()
    output = tmp_path / "result.json"
    output.write_text("previous\n")

    def fail_replace(source: Path, destination: Path) -> None:
        raise OSError("simulated replace failure")

    monkeypatch.setattr(harness.os, "replace", fail_replace)
    with pytest.raises(OSError, match="replace failure"):
        harness._write_result_atomic(output, "new\n")

    assert output.read_text() == "previous\n"
    assert list(tmp_path.iterdir()) == [output]
