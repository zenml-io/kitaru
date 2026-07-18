"""Imported-trace API namespace for :class:`kitaru.KitaruClient`."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

from kitaru.imports import LangfuseImportResult, import_langfuse_jsonl

if TYPE_CHECKING:
    from kitaru.client import KitaruClient


class ImportsAPI:
    """Operations for importing external traces as observed executions."""

    def __init__(self, client_ref: KitaruClient) -> None:
        self._client_ref = client_ref

    def langfuse(
        self,
        path: str | Path,
        *,
        source_project_id: str,
        agent: str,
        version: str,
        trace_ids: Sequence[str] | None = None,
        stack: str | None = None,
        limit: int | None = None,
        dry_run: bool = True,
        confirm_data_storage: bool = False,
        allow_fragmented: bool = False,
        max_workers: int = 1,
        cohort_tag: str | None = None,
    ) -> LangfuseImportResult:
        """Preview or import a Langfuse observations JSONL export.

        The default dry run is read-only. An actual import persists the selected
        raw rows and normalized replay evidence, including observation inputs
        and outputs, and therefore requires explicit data-storage confirmation.
        """
        return import_langfuse_jsonl(
            path,
            source_project_id=source_project_id,
            agent=agent,
            version=version,
            stack=stack,
            trace_ids=trace_ids,
            limit=limit,
            dry_run=dry_run,
            confirm_data_storage=confirm_data_storage,
            allow_fragmented=allow_fragmented,
            max_workers=max_workers,
            cohort_tag=cohort_tag,
            client=self._client_ref._client(),
        )
