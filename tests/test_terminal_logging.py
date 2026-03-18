"""Tests for the Kitaru terminal log intercept."""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterator
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from kitaru._env import ZENML_DEBUG_ENV
from kitaru._terminal_logging import (
    CheckpointState,
    CheckpointTracker,
    _build_tree_rail_snapshot,
    _classify,
    _decide,
    _ExcInfo,
    _FlowLiveSession,
    _is_kitaru_terminal_handler_instance,
    _KitaruTerminalHandler,
    _PriorCheckpointHints,
    _render,
    _terminal_debug_enabled,
    _TerminalDecision,
    _TreeRailConcurrentGroup,
    install_terminal_log_intercept,
    register_checkpoint_submission,
    register_flow_execution,
)
from kitaru.config import KITARU_DEBUG_ENV, KITARU_MACHINE_MODE_ENV


def _make_record(
    name: str,
    msg: str,
    level: int = logging.INFO,
    *,
    created: float | None = None,
    exc_info: _ExcInfo = None,
) -> logging.LogRecord:
    """Create a LogRecord with a pre-formatted message."""
    record = logging.LogRecord(
        name=name,
        level=level,
        pathname="",
        lineno=0,
        msg=msg,
        args=(),
        exc_info=exc_info,
    )
    if created is not None:
        record.created = created
    return record


@pytest.fixture
def _snapshot_root_handlers() -> Iterator[None]:
    """Save and restore root logger handlers around a test."""
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    try:
        yield
    finally:
        root.handlers = original_handlers
        root.level = original_level


@pytest.fixture(autouse=True)
def _clear_terminal_debug_cache() -> Iterator[None]:
    """Keep debug-env tests independent despite the terminal debug cache."""
    _terminal_debug_enabled.cache_clear()
    try:
        yield
    finally:
        _terminal_debug_enabled.cache_clear()


# ---------------------------------------------------------------------------
# Decision rule tests
# ---------------------------------------------------------------------------


class TestDecideStepLifecycle:
    """Step lifecycle messages should be rewritten to checkpoint vocabulary."""

    def test_step_started(self) -> None:
        record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Step `fetch_data` has started.",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` started."
        assert decision.kind == "info"

    def test_step_finished_with_duration(self) -> None:
        record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Step `fetch_data` has finished in `1.23s`.",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` finished in 1.23s."
        assert decision.kind == "success"

    def test_step_finished_successfully_with_duration(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Step `fetch_data` finished successfully in 2.5s.",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` finished in 2.5s."
        assert decision.kind == "success"

    def test_step_finished_successfully_bare(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Step `fetch_data` finished successfully.",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` finished."
        assert decision.kind == "success"

    def test_step_failed_with_duration(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Step `fetch_data` failed after 3.1s.",
            level=logging.ERROR,
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` failed after 3.1s."
        assert decision.kind == "error"

    def test_step_failed_bare(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Step `fetch_data` failed.",
            level=logging.ERROR,
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` failed."
        assert decision.kind == "error"

    def test_step_cached(self) -> None:
        record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Using cached version of step `fetch_data`.",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` cached."
        assert decision.kind == "detail"

    def test_step_skipped(self) -> None:
        record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Skipping step `fetch_data`.",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Skipping checkpoint `fetch_data`."

    def test_failed_to_run_step(self) -> None:
        record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Failed to run step `fetch_data`: some error",
            level=logging.ERROR,
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` failed: some error"
        assert decision.kind == "error"


class TestDecidePipelineLifecycle:
    """Pipeline lifecycle messages should be rewritten to flow vocabulary."""

    def test_pipeline_initiating(self) -> None:
        record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Initiating a new run for the pipeline: `my_flow`.",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Starting flow `my_flow`."
        assert decision.kind == "info"

    def test_pipeline_completed(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Pipeline completed successfully.",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Flow completed."
        assert decision.kind == "success"

    def test_pausing_pipeline_run(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Pausing pipeline run `abc123`.",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Pausing execution `abc123`."
        assert decision.kind == "warning"

    def test_resuming_run(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Resuming run `abc123`.",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Resuming execution `abc123`."

    def test_dashboard_url_rewritten(self) -> None:
        record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Dashboard URL for Pipeline Run: https://example.com/runs/abc",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Execution URL: https://example.com/runs/abc"
        assert decision.kind == "detail"

    def test_pipeline_run_finished_with_duration(self) -> None:
        record = _make_record(
            "zenml.orchestrators.local.local_orchestrator",
            "Pipeline run has finished in `1.368s`.",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Execution finished in 1.368s."
        assert decision.kind == "success"


class TestDecideWaitLifecycle:
    """Wait condition messages should be rewritten to Kitaru vocabulary."""

    def test_waiting_on_wait_condition(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Waiting on wait condition `approval` "
            "(type=external_input, timeout=60s, poll=5s).",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == (
            "Waiting on `approval` (type=external_input, timeout=60s, poll=5s)."
        )
        assert decision.kind == "info"

    def test_waiting_on_auto_named_wait_condition(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Waiting on wait condition `wait_condition:0` "
            "(type=external_input, timeout=600s, poll=5s).",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == (
            "Waiting on `wait_condition:0` "
            "(type=external_input, timeout=600s, poll=5s)."
        )


class TestDecideAliasCleanup:
    """Alias names should be stripped from matched capture groups."""

    def test_alias_in_pipeline_init(self) -> None:
        record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Initiating a new run for the pipeline: "
            "`__kitaru_pipeline_source_my_flow`.",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Starting flow `my_flow`."
        assert "__kitaru" not in decision.text

    def test_alias_in_step_start(self) -> None:
        record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Step `__kitaru_checkpoint_source_fetch_data` has started.",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` started."
        assert "__kitaru" not in decision.text


class TestDecideDropRules:
    """ZenML-specific noise should be dropped."""

    def test_dashboard_promo_dropped(self) -> None:
        record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "You can visualize your pipeline runs in the `ZenML Dashboard`...",
        )
        assert _decide(record) is None

    def test_using_user_dropped(self) -> None:
        record = _make_record("zenml.pipelines", "Using user: `admin`")
        assert _decide(record) is None

    def test_using_build_dropped(self) -> None:
        record = _make_record("zenml.pipelines", "Using a build:")
        assert _decide(record) is None

    def test_registered_new_pipeline_dropped(self) -> None:
        record = _make_record("zenml.pipelines", "Registered new pipeline: `foo`")
        assert _decide(record) is None

    def test_component_listing_dropped(self) -> None:
        record = _make_record(
            "zenml.pipelines",
            "  orchestrator: `default`",
        )
        assert _decide(record) is None

    def test_zenml_warning_code_dropped(self) -> None:
        record = _make_record(
            "zenml.utils.warnings.controller",
            "[ZML002](USAGE) - You are specifying docker settings but no "
            "component in your stack makes use of them.",
        )
        assert _decide(record) is None

    def test_uploading_external_artifact_dropped(self) -> None:
        record = _make_record(
            "zenml.artifacts.external_artifact",
            "Uploading external artifact to 'external_artifacts/external_abc'.",
        )
        assert _decide(record) is None

    def test_finished_uploading_external_artifact_dropped(self) -> None:
        record = _make_record(
            "zenml.artifacts.external_artifact",
            "Finished uploading external artifact 502ea0bd-2a2b-4a86.",
        )
        assert _decide(record) is None


class TestDecideLiteLLMNoise:
    """LiteLLM chatter should stay out of the terminal by default."""

    def test_litellm_info_is_dropped_by_default(self) -> None:
        record = _make_record(
            "LiteLLM",
            "LiteLLM completion() model= gpt-4o-mini; provider = openai",
        )
        assert _decide(record) is None

    def test_litellm_info_is_kept_in_debug_mode(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(KITARU_DEBUG_ENV, "true")
        record = _make_record(
            "LiteLLM",
            "LiteLLM completion() model= gpt-4o-mini; provider = openai",
        )

        decision = _decide(record)

        assert decision is not None
        assert (
            decision.text
            == "LiteLLM completion() model= gpt-4o-mini; provider = openai"
        )

    def test_litellm_info_stays_hidden_with_zenml_debug_only(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(ZENML_DEBUG_ENV, "true")
        record = _make_record(
            "LiteLLM",
            "\nLiteLLM completion() model= gpt-4o-mini; provider = openai",
        )

        assert _decide(record) is None

    def test_litellm_warning_still_survives_default_filtering(self) -> None:
        record = _make_record(
            "LiteLLM",
            "Rate limit warning",
            level=logging.WARNING,
        )

        decision = _decide(record)

        assert decision is not None
        assert decision.text == "Rate limit warning"
        assert decision.kind == "warning"


class TestDecidePassthrough:
    """Non-ZenML records and unmatched ZenML messages pass through."""

    def test_user_record_passes_through(self) -> None:
        record = _make_record("my_app.module", "Processing 42 items")
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Processing 42 items"

    def test_unmatched_zenml_passes_through_with_alias_cleanup(self) -> None:
        record = _make_record(
            "zenml.something",
            "Unknown message about __kitaru_pipeline_source_foo",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Unknown message about foo"
        assert "__kitaru" not in decision.text

    def test_kitaru_record_passes_through(self) -> None:
        record = _make_record("kitaru.flow", "Flow started")
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Flow started"


# ---------------------------------------------------------------------------
# Tracker tests
# ---------------------------------------------------------------------------


class TestTrackerClassification:
    def test_classify_includes_tracker_event(self) -> None:
        record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Step `fetch_data` has started.",
        )

        resolved = _classify(record)

        assert resolved.decision is not None
        assert resolved.decision.text == "Checkpoint `fetch_data` started."
        assert resolved.tracker_event is not None
        assert resolved.tracker_event.kind == "checkpoint_started"
        assert resolved.tracker_event.checkpoint_name == "fetch_data"


class TestCheckpointTracker:
    def test_flow_start_seeds_prior_checkpoint_hints(self) -> None:
        tracker = CheckpointTracker()
        record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Initiating a new run for the pipeline: `my_flow`.",
            created=10.0,
        )
        resolved = _classify(record)

        with patch.object(
            CheckpointTracker,
            "_lookup_prior_checkpoint_hints",
            return_value=_PriorCheckpointHints(
                matched_prior_run=True,
                checkpoint_names=["fetch_data", "process_data"],
            ),
        ):
            tracker.ingest(event=resolved.tracker_event, record=record)

        assert tracker.flow_name == "my_flow"
        assert tracker.flow_started_at == 10.0
        assert tracker.matched_prior_run is True
        assert list(tracker.checkpoints) == ["fetch_data", "process_data"]
        assert tracker.checkpoints["fetch_data"].status == "pending"
        assert tracker.checkpoints["process_data"].status == "pending"

    def test_register_execution_before_flow_start_applies_on_flow_start(self) -> None:
        tracker = CheckpointTracker()
        tracker.register_execution("exec-123")
        record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Initiating a new run for the pipeline: `my_flow`.",
            created=10.0,
        )

        with patch.object(
            CheckpointTracker,
            "_lookup_prior_checkpoint_hints",
            return_value=_PriorCheckpointHints(
                matched_prior_run=False,
                checkpoint_names=[],
            ),
        ):
            tracker.ingest(event=_classify(record).tracker_event, record=record)

        assert tracker.execution_id == "exec-123"
        assert tracker._pending_execution_id is None
        assert tracker.matched_prior_run is False

    def test_stack_selected_before_flow_start_carries_into_started_flow(self) -> None:
        tracker = CheckpointTracker()
        stack_record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Using stack: `prod`",
            created=1.0,
        )
        flow_record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Initiating a new run for the pipeline: `my_flow`.",
            created=2.0,
        )

        tracker.ingest(event=_classify(stack_record).tracker_event, record=stack_record)
        with patch.object(
            CheckpointTracker,
            "_lookup_prior_checkpoint_hints",
            return_value=_PriorCheckpointHints(
                matched_prior_run=False,
                checkpoint_names=[],
            ),
        ):
            tracker.ingest(
                event=_classify(flow_record).tracker_event, record=flow_record
            )

        assert tracker.stack_name == "prod"

    def test_submit_group_is_consumed_by_launch_event(self) -> None:
        tracker = CheckpointTracker()
        start_record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Initiating a new run for the pipeline: `my_flow`.",
            created=1.0,
        )
        launch_record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Step `fetch_data` launched.",
            created=2.0,
        )

        with patch.object(
            CheckpointTracker,
            "_lookup_prior_checkpoint_hints",
            return_value=_PriorCheckpointHints(
                matched_prior_run=False,
                checkpoint_names=[],
            ),
        ):
            tracker.ingest(
                event=_classify(start_record).tracker_event, record=start_record
            )

        tracker.register_submission("fetch_data")
        tracker.ingest(
            event=_classify(launch_record).tracker_event, record=launch_record
        )

        assert tracker.checkpoints["fetch_data"].status == "pending"
        assert tracker.checkpoints["fetch_data"].submit_group == 1

    def test_failure_then_retry_clears_error_and_finishes_passed(self) -> None:
        tracker = CheckpointTracker()
        start_record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Step `fetch_data` has started.",
            created=5.0,
        )

        try:
            raise RuntimeError("boom")
        except RuntimeError:
            failure_record = _make_record(
                "zenml.execution.pipeline.dynamic.runner",
                "Step `fetch_data` failed.",
                level=logging.ERROR,
                created=6.0,
                exc_info=sys.exc_info(),
            )

        retry_start_record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Step `fetch_data` has started.",
            created=7.0,
        )
        finish_record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Step `fetch_data` finished successfully in 2.0s.",
            created=9.0,
        )

        tracker.ingest(event=_classify(start_record).tracker_event, record=start_record)
        tracker.ingest(
            event=_classify(failure_record).tracker_event, record=failure_record
        )

        failed_state = tracker.checkpoints["fetch_data"]
        assert failed_state.status == "failed"
        assert failed_state.started_at == 5.0
        assert failed_state.finished_at == 6.0
        assert failed_state.error == "RuntimeError: boom"
        assert failed_state.traceback_frames is not None

        tracker.ingest(
            event=_classify(retry_start_record).tracker_event,
            record=retry_start_record,
        )
        tracker.ingest(
            event=_classify(finish_record).tracker_event, record=finish_record
        )

        final_state = tracker.checkpoints["fetch_data"]
        assert final_state.status == "passed"
        assert final_state.started_at == 5.0
        assert final_state.finished_at == 9.0
        assert final_state.error is None
        assert final_state.traceback_frames is None

    def test_failure_without_exc_info_still_marks_terminal_failure(self) -> None:
        tracker = CheckpointTracker()
        start_record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Step `fetch_data` has started.",
            created=5.0,
        )
        failure_record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Failed to run step `fetch_data`: some error",
            level=logging.ERROR,
            created=6.0,
        )

        tracker.ingest(event=_classify(start_record).tracker_event, record=start_record)
        tracker.ingest(
            event=_classify(failure_record).tracker_event,
            record=failure_record,
        )

        assert tracker.terminal_status == "failed"
        assert tracker.terminal_checkpoint_name == "fetch_data"
        assert tracker.checkpoints["fetch_data"].error == (
            "Failed to run step `fetch_data`: some error"
        )

    def test_wait_condition_updates_tracker_level_metadata_only(self) -> None:
        tracker = CheckpointTracker()
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Waiting on wait condition `approval` "
            "(type=external_input, timeout=60s, poll=5s).",
            created=12.0,
        )

        tracker.ingest(event=_classify(record).tracker_event, record=record)

        assert tracker.active_wait_condition == "approval"
        assert tracker.active_wait_type == "external_input"
        assert tracker.active_wait_timeout_seconds == "60"
        assert tracker.active_wait_poll_seconds == "5"
        assert tracker.checkpoints == {}

    def test_new_flow_start_resets_prior_state(self) -> None:
        tracker = CheckpointTracker()
        first_flow = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Initiating a new run for the pipeline: `first_flow`.",
            created=1.0,
        )
        second_flow = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Initiating a new run for the pipeline: `second_flow`.",
            created=20.0,
        )
        started = _make_record(
            "zenml.orchestrators.step_launcher",
            "Step `fetch_data` has started.",
            created=2.0,
        )

        with patch.object(
            CheckpointTracker,
            "_lookup_prior_checkpoint_hints",
            side_effect=[
                _PriorCheckpointHints(
                    matched_prior_run=True,
                    checkpoint_names=["fetch_data"],
                ),
                _PriorCheckpointHints(
                    matched_prior_run=False,
                    checkpoint_names=[],
                ),
            ],
        ):
            tracker.ingest(event=_classify(first_flow).tracker_event, record=first_flow)
            tracker.register_submission("fetch_data")
            tracker.ingest(event=_classify(started).tracker_event, record=started)
            tracker.ingest(
                event=_classify(second_flow).tracker_event, record=second_flow
            )

        assert tracker.flow_name == "second_flow"
        assert tracker.flow_started_at == 20.0
        assert tracker.checkpoints == {}
        assert tracker._pending_submit_groups == {}
        assert tracker._next_submit_group_id == 1

    def test_lookup_failures_are_swallowed(self) -> None:
        tracker = CheckpointTracker()
        record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Initiating a new run for the pipeline: `my_flow`.",
        )

        with patch.object(
            CheckpointTracker,
            "_lookup_prior_checkpoint_hints",
            side_effect=RuntimeError("boom"),
        ):
            tracker.ingest(event=_classify(record).tracker_event, record=record)

        assert tracker.flow_name == "my_flow"
        assert tracker.checkpoints == {}
        assert tracker.matched_prior_run is None


class TestTreeRailSnapshot:
    def test_execution_url_populates_execution_id_from_dashboard_url(self) -> None:
        tracker = CheckpointTracker(flow_name="my_flow")
        record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Dashboard URL for Pipeline Run: https://example.com/runs/exec-123",
            created=3.0,
        )

        tracker.ingest(event=_classify(record).tracker_event, record=record)

        assert tracker.execution_url == "https://example.com/runs/exec-123"
        assert tracker.execution_id == "exec-123"

    def test_completed_snapshot_hides_unstarted_pending_rows_and_adds_first_run_hint(
        self,
    ) -> None:
        tracker = CheckpointTracker(
            flow_name="my_flow",
            execution_id="exec-123",
            flow_started_at=0.0,
            flow_finished_at=5.0,
            matched_prior_run=False,
            terminal_status="completed",
            checkpoints={
                "fetch_data": CheckpointState(
                    name="fetch_data",
                    status="passed",
                    started_at=1.0,
                    finished_at=4.0,
                ),
                "process_data": CheckpointState(
                    name="process_data",
                    status="pending",
                ),
            },
        )

        snapshot = _build_tree_rail_snapshot(tracker, now=5.0)

        assert len(snapshot.rows) == 1
        assert getattr(snapshot.rows[0], "name", None) == "fetch_data"
        assert snapshot.terminal_status == "completed"
        assert "kitaru executions logs exec-123" in snapshot.hint_lines

        tracker.matched_prior_run = True
        later_snapshot = _build_tree_rail_snapshot(tracker, now=5.0)
        assert "kitaru executions logs exec-123" not in later_snapshot.hint_lines

    def test_wait_snapshot_adds_input_hint(self) -> None:
        tracker = CheckpointTracker(
            flow_name="my_flow",
            execution_id="exec-123",
            flow_started_at=0.0,
            active_wait_condition="approval",
            active_wait_type="external_input",
            active_wait_timeout_seconds="60",
            active_wait_poll_seconds="5",
        )

        snapshot = _build_tree_rail_snapshot(tracker, now=12.0)

        assert snapshot.terminal_status == "waiting"
        assert any("approval" in line for line in snapshot.wait_lines)
        assert "kitaru executions input exec-123" in snapshot.hint_lines

    def test_snapshot_compacts_completed_rows_and_groups_concurrent_fanout(
        self,
    ) -> None:
        tracker = CheckpointTracker(flow_name="my_flow", flow_started_at=0.0)
        for index in range(11):
            tracker.checkpoints[f"done_{index}"] = CheckpointState(
                name=f"done_{index}",
                status="passed" if index % 2 == 0 else "cached",
                started_at=float(index),
                finished_at=float(index) + 0.5,
            )
        tracker.checkpoints["fanout_a"] = CheckpointState(
            name="fanout_a",
            status="pending",
            submit_group=7,
        )
        tracker.checkpoints["fanout_b"] = CheckpointState(
            name="fanout_b",
            status="pending",
            submit_group=7,
        )

        snapshot = _build_tree_rail_snapshot(tracker, now=12.0)

        assert any(
            type(row).__name__ == "_TreeRailCompactionRow" for row in snapshot.rows
        )
        concurrent_groups = [
            row for row in snapshot.rows if isinstance(row, _TreeRailConcurrentGroup)
        ]
        assert len(concurrent_groups) == 1
        assert [child.name for child in concurrent_groups[0].children] == [
            "fanout_a",
            "fanout_b",
        ]


class TestHandlerTrackerIntegration:
    def test_emit_updates_tracker_without_changing_output(self) -> None:
        tracker = CheckpointTracker()
        record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Step `fetch_data` has started.",
        )
        rendered: list[str] = []

        handler = _KitaruTerminalHandler(tracker=tracker)
        handler._interactive = False
        handler._machine_mode = False
        handler._write = rendered.append

        handler.emit(record)

        assert "".join(rendered) == "Kitaru: Checkpoint `fetch_data` started.\n"
        assert tracker.checkpoints["fetch_data"].status == "running"

    def test_active_tracker_submission_bridge_assigns_submit_group(self) -> None:
        tracker = CheckpointTracker()
        rendered: list[str] = []
        handler = _KitaruTerminalHandler(tracker=tracker)
        handler._interactive = False
        handler._machine_mode = False
        handler._write = rendered.append

        flow_record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Initiating a new run for the pipeline: `my_flow`.",
        )
        launch_record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Step `fetch_data` launched.",
        )

        with patch.object(
            CheckpointTracker,
            "_lookup_prior_checkpoint_hints",
            return_value=_PriorCheckpointHints(
                matched_prior_run=False,
                checkpoint_names=[],
            ),
        ):
            handler.emit(flow_record)

        register_checkpoint_submission("fetch_data")
        handler.emit(launch_record)

        assert tracker.checkpoints["fetch_data"].submit_group == 1

    def test_active_flow_execution_bridge_updates_tracker_and_refreshes_handler(
        self,
    ) -> None:
        tracker = CheckpointTracker()
        handler = _KitaruTerminalHandler(tracker=tracker)
        handler._interactive = False
        handler._machine_mode = False
        flow_record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Initiating a new run for the pipeline: `my_flow`.",
        )

        with patch.object(
            CheckpointTracker,
            "_lookup_prior_checkpoint_hints",
            return_value=_PriorCheckpointHints(
                matched_prior_run=False,
                checkpoint_names=[],
            ),
        ):
            handler.emit(flow_record)

        with patch.object(handler, "refresh_live_session") as refresh_mock:
            register_flow_execution("exec-123")

        assert tracker.execution_id == "exec-123"
        refresh_mock.assert_called_once_with()

    def test_execution_bridge_finalizes_terminal_live_session_once_id_is_known(
        self,
    ) -> None:
        tracker = CheckpointTracker(flow_name="my_flow", terminal_status="completed")
        handler = _KitaruTerminalHandler(tracker=tracker)
        handler._interactive = True
        handler._machine_mode = False
        live_session = cast(_FlowLiveSession, MagicMock())
        handler._live_session = live_session

        register_flow_execution("exec-123")

        assert tracker.execution_id == "exec-123"
        assert handler._live_session is None
        assert cast(MagicMock, live_session).finalize.call_count == 1
        assert cast(MagicMock, live_session).refresh.call_count == 0

    def test_flow_terminal_finalize_with_execution_id_stops_without_extra_refresh(
        self,
    ) -> None:
        tracker = CheckpointTracker(
            flow_name="my_flow",
            terminal_status="completed",
            execution_id="exec-123",
        )
        handler = _KitaruTerminalHandler(tracker=tracker)
        handler._interactive = True
        handler._machine_mode = False
        live_session = cast(_FlowLiveSession, MagicMock())
        handler._live_session = live_session
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Pipeline completed successfully.",
        )

        handler.emit(record)

        assert handler._live_session is None
        assert cast(MagicMock, live_session).refresh.call_count == 0
        assert cast(MagicMock, live_session).finalize.call_count == 1

    def test_terminal_live_managed_record_after_finalize_does_not_spawn_new_session(
        self,
    ) -> None:
        tracker = CheckpointTracker(
            flow_name="my_flow",
            terminal_status="completed",
            execution_id="exec-123",
        )
        handler = _KitaruTerminalHandler(tracker=tracker)
        handler._interactive = True
        handler._machine_mode = False
        record = _make_record(
            "zenml.orchestrators.local.local_orchestrator",
            "Pipeline run has finished in `1.0s`.",
        )

        with patch.object(handler, "_ensure_live_session") as ensure_live_session:
            handler.emit(record)

        ensure_live_session.assert_not_called()
        assert handler._live_session is None

    def test_flow_terminal_finalize_waits_for_execution_id(self) -> None:
        tracker = CheckpointTracker(flow_name="my_flow", terminal_status="completed")
        handler = _KitaruTerminalHandler(tracker=tracker)
        handler._interactive = True
        handler._machine_mode = False
        live_session = cast(_FlowLiveSession, MagicMock())
        handler._live_session = live_session
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Pipeline completed successfully.",
        )

        handler.emit(record)

        assert handler._live_session is live_session
        assert cast(MagicMock, live_session).refresh.call_count == 1
        assert cast(MagicMock, live_session).finalize.call_count == 0


class TestFlowLiveSession:
    def test_live_session_uses_event_driven_refresh(self) -> None:
        tracker = CheckpointTracker(flow_name="my_flow")

        with patch("kitaru._terminal_logging.Live") as live_cls:
            live = cast(MagicMock, live_cls.return_value)
            session = _FlowLiveSession(
                tracker,
                version="0.1.0",
                write=lambda text: None,
                stream=sys.stdout,
            )

            session.ensure_started()

        assert live_cls.call_count == 1
        assert live_cls.call_args is not None
        assert live_cls.call_args.kwargs["auto_refresh"] is False
        assert live_cls.call_args.kwargs["transient"] is True
        live.start.assert_called_once_with()

    def test_finalize_leaves_last_live_frame_in_place(self) -> None:
        tracker = CheckpointTracker(flow_name="my_flow")
        session = _FlowLiveSession(
            tracker,
            version="0.1.0",
            write=lambda text: None,
            stream=sys.stdout,
        )
        live_mock = MagicMock()
        session._live = live_mock
        session._started = True

        session.finalize()

        assert live_mock.transient is False
        live_mock.stop.assert_called_once_with()


# ---------------------------------------------------------------------------
# Render tests
# ---------------------------------------------------------------------------


class TestRender:
    def test_interactive_has_ansi_and_marker(self) -> None:
        decision = _TerminalDecision(kind="success", text="Flow completed.")
        rendered = _render(decision, interactive=True)
        assert "\x1b[" in rendered
        assert "\u2713" in rendered  # ✓
        assert "Flow completed." in rendered
        assert "Kitaru" in rendered

    def test_non_interactive_is_plain(self) -> None:
        decision = _TerminalDecision(kind="success", text="Flow completed.")
        rendered = _render(decision, interactive=False)
        assert "\x1b[" not in rendered
        assert rendered == "Kitaru: Flow completed."

    def test_error_uses_red_and_cross(self) -> None:
        decision = _TerminalDecision(kind="error", text="Checkpoint failed.")
        rendered = _render(decision, interactive=True)
        assert "\x1b[31m" in rendered  # red
        assert "\u2716" in rendered  # ✖


# ---------------------------------------------------------------------------
# LogRecord immutability test
# ---------------------------------------------------------------------------


class TestLogRecordImmutability:
    """The terminal handler must never mutate the LogRecord."""

    def test_record_fields_unchanged_after_emit(self) -> None:
        record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Step `__kitaru_checkpoint_source_fetch_data` has started.",
        )
        original_msg = record.msg
        original_args = record.args
        original_name = record.name

        handler = _KitaruTerminalHandler()
        # Redirect output to avoid terminal noise
        handler._write = lambda s: None

        handler.emit(record)

        assert record.msg == original_msg
        assert record.args == original_args
        assert record.name == original_name


class TestMachineModeHandler:
    """Machine mode should suppress ANSI formatting and emit tracebacks."""

    def test_machine_mode_resolves_from_environment_on_tty(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(KITARU_MACHINE_MODE_ENV, "true")
        monkeypatch.setattr(sys.stdout, "isatty", lambda: True, raising=False)

        handler = _KitaruTerminalHandler()

        assert handler._interactive is True
        assert handler._machine_mode is True

    def test_machine_mode_renders_plain_text_even_on_tty(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Pipeline completed successfully.",
        )
        rendered: list[str] = []

        handler = _KitaruTerminalHandler()
        handler._interactive = True
        handler._machine_mode = True
        handler._write = rendered.append

        handler.emit(record)

        joined = "".join(rendered)
        assert "\x1b[" not in joined
        assert joined == "Kitaru: Flow completed.\n"

    def test_machine_mode_appends_traceback_text(self) -> None:
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            record = logging.LogRecord(
                name="zenml.execution.pipeline.dynamic.runner",
                level=logging.ERROR,
                pathname="",
                lineno=0,
                msg="Step `fetch_data` failed.",
                args=(),
                exc_info=sys.exc_info(),
            )

        rendered: list[str] = []
        handler = _KitaruTerminalHandler()
        handler._machine_mode = True
        handler._write = rendered.append

        handler.emit(record)

        joined = "".join(rendered)
        assert "Kitaru: Checkpoint `fetch_data` failed.\n" in joined
        assert "Traceback (most recent call last):" in joined
        assert "RuntimeError: boom" in joined


# ---------------------------------------------------------------------------
# Handler swap tests
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_snapshot_root_handlers")
class TestHandlerSwap:
    """Handler installation logic."""

    def test_swap_replaces_console_handler_keeps_storage(self) -> None:
        from zenml.logger import ConsoleFormatter, ZenMLLoggingHandler

        root = logging.getLogger()

        # Set up mock handlers like ZenML's init_logging() would
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(ConsoleFormatter())
        storage_handler = ZenMLLoggingHandler()

        root.handlers = [console_handler, storage_handler]

        install_terminal_log_intercept()

        kitaru_handlers = [
            h for h in root.handlers if isinstance(h, _KitaruTerminalHandler)
        ]
        console_handlers = [
            h
            for h in root.handlers
            if isinstance(getattr(h, "formatter", None), ConsoleFormatter)
        ]
        storage_handlers = [
            h for h in root.handlers if isinstance(h, ZenMLLoggingHandler)
        ]

        assert len(kitaru_handlers) == 1
        assert len(console_handlers) == 0
        assert len(storage_handlers) == 1
        assert storage_handlers[0] is storage_handler

    def test_swap_is_idempotent(self) -> None:
        from zenml.logger import ConsoleFormatter, ZenMLLoggingHandler

        root = logging.getLogger()

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(ConsoleFormatter())
        storage_handler = ZenMLLoggingHandler()
        root.handlers = [console_handler, storage_handler]

        install_terminal_log_intercept()
        install_terminal_log_intercept()

        kitaru_handlers = [
            h for h in root.handlers if isinstance(h, _KitaruTerminalHandler)
        ]
        assert len(kitaru_handlers) == 1

    def test_swap_reuses_legacy_handler_across_reload_identity(self) -> None:
        from zenml.logger import ConsoleFormatter, ZenMLLoggingHandler

        root = logging.getLogger()

        LegacyHandler = type(
            "_KitaruTerminalHandler",
            (logging.Handler,),
            {"__module__": "kitaru._terminal_logging"},
        )
        legacy_handler = LegacyHandler()
        legacy_handler_any = cast(Any, legacy_handler)
        legacy_handler_any._tracker = CheckpointTracker()
        legacy_handler_any._kitaru_terminal_handler = True

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(ConsoleFormatter())
        storage_handler = ZenMLLoggingHandler()
        root.handlers = [legacy_handler, console_handler, storage_handler]

        install_terminal_log_intercept()

        kitaru_handlers = [
            h for h in root.handlers if _is_kitaru_terminal_handler_instance(h)
        ]
        console_handlers = [
            h
            for h in root.handlers
            if isinstance(getattr(h, "formatter", None), ConsoleFormatter)
        ]

        assert kitaru_handlers == [legacy_handler]
        assert console_handlers == []
        assert storage_handler in root.handlers

    def test_zenml_entrypoint_subprocess_suppresses_console_handlers(self) -> None:
        from zenml.logger import ConsoleFormatter, ZenMLLoggingHandler

        root = logging.getLogger()
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(ConsoleFormatter())
        storage_handler = ZenMLLoggingHandler()
        root.handlers = [console_handler, storage_handler]

        with patch.object(
            sys,
            "argv",
            [
                "entrypoint.py",
                "--entrypoint_config_source",
                "zenml.pipelines.dynamic.entrypoint_configuration.DynamicPipelineEntrypointConfiguration",
                "--snapshot_id",
                "snap-123",
                "--run_id",
                "run-123",
            ],
        ):
            install_terminal_log_intercept()

        kitaru_handlers = [
            h for h in root.handlers if isinstance(h, _KitaruTerminalHandler)
        ]
        console_handlers = [
            h
            for h in root.handlers
            if isinstance(getattr(h, "formatter", None), ConsoleFormatter)
        ]
        storage_handlers = [
            h for h in root.handlers if isinstance(h, ZenMLLoggingHandler)
        ]

        assert len(kitaru_handlers) == 0
        assert len(console_handlers) == 0
        assert len(storage_handlers) == 1
        assert storage_handlers[0] is storage_handler

    def test_swap_graceful_when_no_console_handler(self) -> None:
        from zenml.logger import ZenMLLoggingHandler

        root = logging.getLogger()

        storage_handler = ZenMLLoggingHandler()
        root.handlers = [storage_handler]

        install_terminal_log_intercept()

        kitaru_handlers = [
            h for h in root.handlers if isinstance(h, _KitaruTerminalHandler)
        ]
        assert len(kitaru_handlers) == 1
        assert storage_handler in root.handlers


# ---------------------------------------------------------------------------
# Bootstrap integration test
# ---------------------------------------------------------------------------


class TestBootstrapIntegration:
    """After ``import kitaru``, the root logger should have a Kitaru handler."""

    def test_import_kitaru_has_kitaru_handler(self) -> None:
        root = logging.getLogger()
        kitaru_handlers = [
            h for h in root.handlers if isinstance(h, _KitaruTerminalHandler)
        ]
        assert len(kitaru_handlers) >= 1

    def test_zenml_log_record_produces_rewritten_output(self) -> None:
        """A ZenML logger message flows through the Kitaru handler."""
        record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Pipeline completed successfully.",
        )
        decision = _decide(record)
        assert decision is not None
        assert decision.text == "Flow completed."

    def test_reload_patches_both_env_and_intercept(self) -> None:
        """Reloading kitaru should re-run both bootstrap side effects."""
        import importlib

        import kitaru

        with (
            patch("kitaru._env.apply_env_translations") as apply_translations,
            patch(
                "kitaru._terminal_logging.install_terminal_log_intercept"
            ) as install_intercept,
        ):
            importlib.reload(kitaru)

        apply_translations.assert_called_once()
        install_intercept.assert_called_once()
