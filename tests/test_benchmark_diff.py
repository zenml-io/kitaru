"""Tests for the cohort-diff benchmark instrumentation."""

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from scripts.benchmark_diff import (
    Counters,
    Measurement,
    _count_backend_pages,
    _measure_local,
)

from kitaru.client import KitaruClient


def test_count_backend_pages_reports_unrelated_rows() -> None:
    """Local measurements should account for pre-existing same-flow runs."""
    page = SimpleNamespace(
        items=[
            SimpleNamespace(id="original-a"),
            SimpleNamespace(id="replay-a"),
            SimpleNamespace(id="pre-existing"),
        ]
    )
    client = SimpleNamespace()
    zenml_client = SimpleNamespace(list_pipeline_runs=lambda *args, **kwargs: page)
    client._client = lambda: zenml_client
    counters = Counters()
    unrelated_candidate_rows = [0]

    with _count_backend_pages(
        cast(KitaruClient, client),
        counters,
        relevant_exec_ids={"original-a", "replay-a"},
        unrelated_candidate_rows=unrelated_candidate_rows,
    ):
        result = zenml_client.list_pipeline_runs()

    assert result is page
    assert counters.backend_pages_read == 1
    assert counters.candidate_rows_read == 3
    assert unrelated_candidate_rows == [1]


def test_measure_local_reports_scanned_unrelated_rows() -> None:
    """The local measurement should expose unrelated rows in its result."""

    @contextmanager
    def fake_page_counter(
        client: object,
        counters: Counters,
        *,
        relevant_exec_ids: set[str],
        unrelated_candidate_rows: list[int],
    ) -> Any:
        assert relevant_exec_ids == {"original-a", "replay-a"}
        unrelated_candidate_rows[0] = 7
        yield

    def fake_measure(
        *, operation: Any, counters: Counters, **kwargs: Any
    ) -> Measurement:
        operation()
        return Measurement(
            scenario=kwargs["scenario"],
            mode=kwargs["mode"],
            originals=kwargs["originals"],
            replays=kwargs["replays"],
            unrelated=kwargs["unrelated"],
            artifact_bytes=kwargs["artifact_bytes"],
            explicit_ids=kwargs["explicit_ids"],
            elapsed_seconds=0.0,
            peak_memory_bytes=0,
            counters=counters,
            warning_count=0,
        )

    fake_client = SimpleNamespace()
    with (
        patch("scripts.benchmark_diff.KitaruClient", return_value=fake_client),
        patch("scripts.benchmark_diff._CountingClient", return_value=fake_client),
        patch("scripts.benchmark_diff._count_backend_pages", fake_page_counter),
        patch("scripts.benchmark_diff.diff_cohort", return_value=object()),
        patch("scripts.benchmark_diff._measure", side_effect=fake_measure),
    ):
        measurement = _measure_local(
            "local scenario",
            original_ids=["original-a"],
            replay_ids={"original-a": ["replay-a"]},
            artifact_bytes=32,
        )

    assert measurement.unrelated == 7
