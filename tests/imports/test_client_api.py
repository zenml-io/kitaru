"""Tests for the imported-trace SDK namespace."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

from kitaru._client._imports import ImportsAPI


def test_langfuse_import_forwards_explicit_stack(monkeypatch) -> None:
    backend = MagicMock()
    client_ref = SimpleNamespace(_client=lambda: backend)
    import_langfuse_jsonl = MagicMock(return_value=object())
    monkeypatch.setattr(
        "kitaru._client._imports.import_langfuse_jsonl", import_langfuse_jsonl
    )

    ImportsAPI(cast(Any, client_ref)).langfuse(
        Path("export.jsonl"),
        source_project_id="source-project",
        agent="support-agent",
        version="prod",
        stack="cloud-stack",
        cohort_tag="customer-a",
    )

    import_langfuse_jsonl.assert_called_once_with(
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
