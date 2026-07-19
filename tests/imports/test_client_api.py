"""Tests for the imported-trace SDK namespace."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

from kitaru._client._imports import ImportsAPI


def test_langfuse_import_forwards_explicit_stack(monkeypatch) -> None:
    backend = MagicMock()
    client_ref = SimpleNamespace(_client=lambda: backend)
    import_langfuse = MagicMock(return_value=object())
    monkeypatch.setattr("kitaru._client._imports.import_langfuse", import_langfuse)

    ImportsAPI(cast(Any, client_ref)).langfuse(
        path=Path("export.jsonl"),
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        stack="cloud-stack",
        cohort_tag="customer-a",
    )

    import_langfuse.assert_called_once_with(
        Path("export.jsonl"),
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        stack="cloud-stack",
        trace_ids=None,
        limit=None,
        dry_run=True,
        confirm_data_storage=False,
        allow_fragmented=False,
        max_workers=1,
        cohort_tag="customer-a",
        client=backend,
    )


def test_langfuse_uri_forwards_without_a_declared_project(monkeypatch) -> None:
    backend = MagicMock()
    client_ref = SimpleNamespace(_client=lambda: backend)
    import_langfuse = MagicMock(return_value=object())
    monkeypatch.setattr("kitaru._client._imports.import_langfuse", import_langfuse)

    ImportsAPI(cast(Any, client_ref)).langfuse(
        "langfuse://trace/trace-one",
        agent="support-agent",
        version="prod",
        dry_run=False,
        confirm_data_storage=True,
    )

    import_langfuse.assert_called_once_with(
        "langfuse://trace/trace-one",
        source_project_id=None,
        agent="support-agent",
        version="prod",
        stack=None,
        trace_ids=None,
        limit=None,
        dry_run=False,
        confirm_data_storage=True,
        allow_fragmented=False,
        max_workers=1,
        cohort_tag=None,
        client=backend,
    )
