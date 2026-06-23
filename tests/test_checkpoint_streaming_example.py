"""Example-level integration test for checkpoint live-event streaming."""

from __future__ import annotations

from typing import Any

from examples.features.checkpoint_streaming import checkpoint_streaming as example

from kitaru import events
from kitaru._client._models import ExecutionStatus
from kitaru.client import KitaruClient


class FakeZenMLStreaming:
    """Capture live events that the example publishes through ZenML streaming."""

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []
        self.flushes: list[float] = []

    def publish(
        self,
        payload: dict[str, Any],
        *,
        kind: str,
        correlation_id: str | None,
        index: int | None,
    ) -> None:
        self.published.append(
            {
                "kind": kind,
                "payload": payload,
                "correlation_id": correlation_id,
                "index": index,
            }
        )

    def flush(self, timeout: float = 2.0) -> bool:
        self.flushes.append(timeout)
        return True


def test_checkpoint_streaming_example_runs_and_publishes_events(
    primed_zenml,
    monkeypatch,
) -> None:
    """Run the public example entrypoint and inspect the durable execution."""
    fake_streaming = FakeZenMLStreaming()
    monkeypatch.setattr(events, "_load_zenml_streaming", lambda: fake_streaming)

    result = example.run_workflow("release confidence")

    assert result == "release confidence: context; why it matters; next step"

    client = KitaruClient()
    execution_summary = client.executions.latest(flow="streaming_demo")
    execution = client.executions.get(execution_summary.exec_id)
    assert execution.status == ExecutionStatus.COMPLETED
    assert {checkpoint.name for checkpoint in execution.checkpoints} >= {
        "prepare_outline",
        "write_summary",
    }

    published_kinds = {event["kind"] for event in fake_streaming.published}
    assert events.CHECKPOINT_PROGRESS_KIND in published_kinds
    assert "report.outline.ready" in published_kinds

    outline_event = next(
        event
        for event in fake_streaming.published
        if event["kind"] == "report.outline.ready"
    )
    assert outline_event["payload"]["data"] == {"section_count": 3}
    assert outline_event["payload"]["message"] == "Outline ready"
