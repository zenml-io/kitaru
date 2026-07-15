"""Tests for external trace import CLI commands."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from kitaru.cli import app
from kitaru.imports import (
    ImportOutcomeStatus,
    LangfuseImportResult,
    TraceImportOutcome,
    TraceIntegrity,
)


def _result(
    *,
    dry_run: bool = True,
    status: ImportOutcomeStatus = ImportOutcomeStatus.WOULD_CREATE,
    existing_execution_id: str | None = None,
    reason: str | None = None,
    resolution: str | None = None,
) -> LangfuseImportResult:
    return LangfuseImportResult(
        dry_run=dry_run,
        source_project_id="source-project",
        agent_name="support-agent",
        total_trace_count=3,
        selected_trace_count=1,
        outcomes=(
            TraceImportOutcome(
                trace_id="trace-one",
                integrity=TraceIntegrity.COMPLETE,
                observation_count=4,
                status=status,
                execution_id=("execution-one" if not dry_run else None),
                existing_execution_id=existing_execution_id,
                reason=reason,
                resolution=resolution,
            ),
        ),
    )


def test_langfuse_import_defaults_to_read_only_preview(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()
    fake_client.imports.langfuse.return_value = _result()

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent-name",
                "support-agent",
                "--trace-id",
                "trace-two",
                "--trace-id",
                "trace-one",
                "--limit",
                "1",
                "--allow-fragmented",
                "--max-workers",
                "2",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.imports.langfuse.assert_called_once_with(
        Path("export.jsonl"),
        source_project_id="source-project",
        agent_name="support-agent",
        trace_ids=["trace-two", "trace-one"],
        limit=1,
        dry_run=True,
        confirm_data_storage=False,
        allow_fragmented=True,
        max_workers=2,
    )
    output = capsys.readouterr().out
    assert "Langfuse trace import" in output
    assert "Preview" in output
    assert "imported_support-agent__langfuse_v4_" in output
    assert "trace-one" in output
    assert "would_create" in output


def test_langfuse_import_write_requires_storage_confirmation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent-name",
                "support-agent",
                "--write",
            ]
        )

    assert exc_info.value.code == 1
    fake_client.imports.langfuse.assert_not_called()
    assert "--confirm-data-storage" in capsys.readouterr().err


def test_langfuse_import_confirmation_requires_write(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent-name",
                "support-agent",
                "--confirm-data-storage",
            ]
        )

    assert exc_info.value.code == 1
    fake_client.imports.langfuse.assert_not_called()
    assert "requires `--write`" in capsys.readouterr().err


def test_langfuse_import_write_forwards_explicit_consent() -> None:
    fake_client = Mock()
    fake_client.imports.langfuse.return_value = _result(
        dry_run=False,
        status=ImportOutcomeStatus.CREATED,
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent-name",
                "support-agent",
                "--write",
                "--confirm-data-storage",
            ]
        )

    assert exc_info.value.code == 0
    fake_client.imports.langfuse.assert_called_once_with(
        Path("export.jsonl"),
        source_project_id="source-project",
        agent_name="support-agent",
        trace_ids=None,
        limit=None,
        dry_run=False,
        confirm_data_storage=True,
        allow_fragmented=False,
        max_workers=1,
    )


def test_langfuse_import_json_emits_one_structured_result(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()
    fake_client.imports.langfuse.return_value = _result()

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent-name",
                "support-agent",
                "--output",
                "json",
            ]
        )

    assert exc_info.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "import.langfuse"
    assert payload["item"]["dry_run"] is True
    assert payload["item"]["flow_name"].startswith(
        "imported_support-agent__langfuse_v4_"
    )
    assert payload["item"]["counts"] == {"would_create": 1}
    assert payload["item"]["outcomes"] == [
        {
            "trace_id": "trace-one",
            "integrity": "complete",
            "observation_count": 4,
            "status": "would_create",
            "execution_id": None,
            "existing_execution_id": None,
            "reason": None,
            "resolution": None,
        }
    ]


def test_langfuse_import_partial_failure_exits_nonzero_after_rendering(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()
    fake_client.imports.langfuse.return_value = _result(
        status=ImportOutcomeStatus.REJECTED
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent-name",
                "support-agent",
            ]
        )

    assert exc_info.value.code == 1
    assert "rejected" in capsys.readouterr().out


def test_langfuse_import_conflict_prints_existing_execution_and_next_action(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake_client = Mock()
    fake_client.imports.langfuse.return_value = _result(
        status=ImportOutcomeStatus.CONFLICT,
        existing_execution_id="execution-existing",
        reason="Already imported with agent_name='original-agent'.",
        resolution="Retry with agent_name='original-agent'.",
    )

    with (
        patch("kitaru.cli.KitaruClient", return_value=fake_client),
        pytest.raises(SystemExit) as exc_info,
    ):
        app(
            [
                "import",
                "langfuse",
                "export.jsonl",
                "--source-project-id",
                "source-project",
                "--agent-name",
                "support-agent",
            ]
        )

    assert exc_info.value.code == 1
    output = capsys.readouterr().out
    assert "execution-existing" in output
    assert "Retry with agent_name='original-agent'." in output
