"""Tests for the Kitaru terminal log intercept."""

from __future__ import annotations

import importlib
import logging
from collections.abc import Iterator
from unittest.mock import patch

import pytest

import kitaru._terminal_logging as terminal_logging


def _make_record(
    name: str,
    msg: str,
    level: int = logging.INFO,
) -> logging.LogRecord:
    """Create a LogRecord with a pre-formatted message."""
    record = logging.LogRecord(
        name=name,
        level=level,
        pathname="",
        lineno=0,
        msg=msg,
        args=(),
        exc_info=None,
    )
    return record


def _kitaru_handlers(root: logging.Logger) -> list[logging.Handler]:
    """Return root handlers recognized as Kitaru terminal handlers."""
    return [
        handler
        for handler in root.handlers
        if terminal_logging._is_kitaru_terminal_handler(handler)
    ]


def _console_handlers(root: logging.Logger) -> list[logging.Handler]:
    """Return root handlers using ZenML's console formatter."""
    from zenml.logger import ConsoleFormatter

    return [
        handler
        for handler in root.handlers
        if isinstance(getattr(handler, "formatter", None), ConsoleFormatter)
    ]


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
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` started."
        assert decision.kind == "info"

    def test_step_finished_with_duration(self) -> None:
        record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Step `fetch_data` has finished in `1.23s`.",
        )
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` finished in 1.23s."
        assert decision.kind == "success"

    def test_step_finished_successfully_with_duration(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Step `fetch_data` finished successfully in 2.5s.",
        )
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` finished in 2.5s."
        assert decision.kind == "success"

    def test_step_finished_successfully_bare(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Step `fetch_data` finished successfully.",
        )
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` finished."
        assert decision.kind == "success"

    def test_step_failed_with_duration(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Step `fetch_data` failed after 3.1s.",
            level=logging.ERROR,
        )
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` failed after 3.1s."
        assert decision.kind == "error"

    def test_step_failed_bare(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Step `fetch_data` failed.",
            level=logging.ERROR,
        )
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` failed."
        assert decision.kind == "error"

    def test_step_cached(self) -> None:
        record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Using cached version of step `fetch_data`.",
        )
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Checkpoint `fetch_data` cached."
        assert decision.kind == "detail"

    def test_step_skipped(self) -> None:
        record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Skipping step `fetch_data`.",
        )
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Skipping checkpoint `fetch_data`."

    def test_failed_to_run_step(self) -> None:
        record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Failed to run step `fetch_data`: some error",
            level=logging.ERROR,
        )
        decision = terminal_logging._decide(record)
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
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Starting flow `my_flow`."
        assert decision.kind == "info"

    def test_pipeline_completed(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Pipeline completed successfully.",
        )
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Flow completed."
        assert decision.kind == "success"

    def test_pausing_pipeline_run(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Pausing pipeline run `abc123`.",
        )
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Pausing execution `abc123`."
        assert decision.kind == "warning"

    def test_resuming_run(self) -> None:
        record = _make_record(
            "zenml.execution.pipeline.dynamic.runner",
            "Resuming run `abc123`.",
        )
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Resuming execution `abc123`."

    def test_dashboard_url_rewritten(self) -> None:
        record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Dashboard URL for Pipeline Run: https://example.com/runs/abc",
        )
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Execution URL: https://example.com/runs/abc"
        assert decision.kind == "detail"

    def test_pipeline_run_finished_with_duration(self) -> None:
        record = _make_record(
            "zenml.orchestrators.local.local_orchestrator",
            "Pipeline run has finished in `1.368s`.",
        )
        decision = terminal_logging._decide(record)
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
        decision = terminal_logging._decide(record)
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
        decision = terminal_logging._decide(record)
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
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Starting flow `my_flow`."
        assert "__kitaru" not in decision.text

    def test_alias_in_step_start(self) -> None:
        record = _make_record(
            "zenml.orchestrators.step_launcher",
            "Step `__kitaru_checkpoint_source_fetch_data` has started.",
        )
        decision = terminal_logging._decide(record)
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
        assert terminal_logging._decide(record) is None

    def test_using_user_dropped(self) -> None:
        record = _make_record("zenml.pipelines", "Using user: `admin`")
        assert terminal_logging._decide(record) is None

    def test_using_build_dropped(self) -> None:
        record = _make_record("zenml.pipelines", "Using a build:")
        assert terminal_logging._decide(record) is None

    def test_registered_new_pipeline_dropped(self) -> None:
        record = _make_record("zenml.pipelines", "Registered new pipeline: `foo`")
        assert terminal_logging._decide(record) is None

    def test_component_listing_dropped(self) -> None:
        record = _make_record(
            "zenml.pipelines",
            "  orchestrator: `default`",
        )
        assert terminal_logging._decide(record) is None

    def test_zenml_warning_code_dropped(self) -> None:
        record = _make_record(
            "zenml.utils.warnings.controller",
            "[ZML002](USAGE) - You are specifying docker settings but no "
            "component in your stack makes use of them.",
        )
        assert terminal_logging._decide(record) is None

    def test_uploading_external_artifact_dropped(self) -> None:
        record = _make_record(
            "zenml.artifacts.external_artifact",
            "Uploading external artifact to 'external_artifacts/external_abc'.",
        )
        assert terminal_logging._decide(record) is None

    def test_finished_uploading_external_artifact_dropped(self) -> None:
        record = _make_record(
            "zenml.artifacts.external_artifact",
            "Finished uploading external artifact 502ea0bd-2a2b-4a86.",
        )
        assert terminal_logging._decide(record) is None

    @pytest.mark.parametrize(
        "msg",
        [
            "Deploying a local daemon ZenML server.",
            "Connecting to the local daemon ZenML server (http://127.0.0.1:8383).",
            "Connected to the local daemon ZenML server (http://127.0.0.1:8383).",
            "Disconnecting from the local (http://127.0.0.1:8383) ZenML server.",
            "Tearing down the local daemon ZenML server.",
            "Shutting down the local daemon ZenML server.",
            "Updated the global store configuration.",
        ],
    )
    def test_server_lifecycle_dropped(self, msg: str) -> None:
        record = _make_record("zenml.zen_server.deploy.deployer", msg)
        assert terminal_logging._decide(record) is None

    @pytest.mark.parametrize(
        "msg",
        [
            "Migrating the ZenML global configuration from version "
            "0.94.0 to version 0.94.1...",
            "Backing up the database before migration to `/tmp/zenml-backup.db`.",
            "Database successfully backed up to `/tmp/zenml-backup.db`. If something "
            "goes wrong with the upgrade, ZenML will attempt to restore the database "
            "from this backup automatically.",
            "Successfully cleaned up database dump file `/tmp/zenml-backup.db`.",
        ],
    )
    def test_migration_noise_dropped(self, msg: str) -> None:
        record = _make_record("zenml.config.global_config", msg)
        assert terminal_logging._decide(record) is None


class TestDecidePassthrough:
    """Non-ZenML records and unmatched ZenML messages pass through."""

    def test_user_record_passes_through(self) -> None:
        record = _make_record("my_app.module", "Processing 42 items")
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Processing 42 items"

    def test_unmatched_zenml_passes_through_with_alias_cleanup(self) -> None:
        record = _make_record(
            "zenml.something",
            "Unknown message about __kitaru_pipeline_source_foo",
        )
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Unknown message about foo"
        assert "__kitaru" not in decision.text

    def test_kitaru_record_passes_through(self) -> None:
        record = _make_record("kitaru.flow", "Flow started")
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Flow started"


# ---------------------------------------------------------------------------
# Render tests
# ---------------------------------------------------------------------------


class TestRender:
    def test_interactive_has_ansi_and_marker(self) -> None:
        decision = terminal_logging._TerminalDecision(
            kind="success", text="Flow completed."
        )
        rendered = terminal_logging._render(decision, interactive=True)
        assert "\x1b[" in rendered
        assert "\u2713" in rendered  # ✓
        assert "Flow completed." in rendered
        assert "Kitaru" in rendered

    def test_non_interactive_is_plain(self) -> None:
        decision = terminal_logging._TerminalDecision(
            kind="success", text="Flow completed."
        )
        rendered = terminal_logging._render(decision, interactive=False)
        assert "\x1b[" not in rendered
        assert rendered == "Kitaru: Flow completed."

    def test_error_uses_red_and_cross(self) -> None:
        decision = terminal_logging._TerminalDecision(
            kind="error", text="Checkpoint failed."
        )
        rendered = terminal_logging._render(decision, interactive=True)
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

        handler = terminal_logging._KitaruTerminalHandler()
        # Redirect output to avoid terminal noise
        handler._write = lambda s: None

        handler.emit(record)

        assert record.msg == original_msg
        assert record.args == original_args
        assert record.name == original_name


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

        terminal_logging.install_terminal_log_intercept()

        kitaru_handlers = _kitaru_handlers(root)
        console_handlers = _console_handlers(root)
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

        terminal_logging.install_terminal_log_intercept()
        terminal_logging.install_terminal_log_intercept()

        kitaru_handlers = _kitaru_handlers(root)
        assert len(kitaru_handlers) == 1

    def test_swap_graceful_when_no_console_handler(self) -> None:
        from zenml.logger import ZenMLLoggingHandler

        root = logging.getLogger()

        storage_handler = ZenMLLoggingHandler()
        root.handlers = [storage_handler]

        terminal_logging.install_terminal_log_intercept()

        kitaru_handlers = _kitaru_handlers(root)
        assert len(kitaru_handlers) == 1
        assert storage_handler in root.handlers

    def test_reload_reuses_existing_kitaru_handler(self) -> None:
        from zenml.logger import ZenMLLoggingHandler

        root = logging.getLogger()

        storage_handler = ZenMLLoggingHandler()
        old_kitaru_handler = terminal_logging._KitaruTerminalHandler()
        old_handler_class = old_kitaru_handler.__class__
        root.handlers = [old_kitaru_handler, storage_handler]

        importlib.reload(terminal_logging)
        terminal_logging.install_terminal_log_intercept()

        assert old_handler_class is not terminal_logging._KitaruTerminalHandler
        assert _kitaru_handlers(root) == [old_kitaru_handler]
        assert storage_handler in root.handlers

    def test_reload_replaces_console_without_duplicating_kitaru(self) -> None:
        from zenml.logger import ConsoleFormatter, ZenMLLoggingHandler

        root = logging.getLogger()

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(ConsoleFormatter())
        storage_handler = ZenMLLoggingHandler()
        old_kitaru_handler = terminal_logging._KitaruTerminalHandler()
        old_handler_class = old_kitaru_handler.__class__
        root.handlers = [console_handler, old_kitaru_handler, storage_handler]

        importlib.reload(terminal_logging)
        terminal_logging.install_terminal_log_intercept()

        assert old_handler_class is not terminal_logging._KitaruTerminalHandler
        assert _kitaru_handlers(root) == [old_kitaru_handler]
        assert len(_console_handlers(root)) == 0
        assert storage_handler in root.handlers

    def test_reinstall_collapses_duplicate_kitaru_handlers(self) -> None:
        from zenml.logger import ZenMLLoggingHandler

        root = logging.getLogger()

        old_kitaru_handler = terminal_logging._KitaruTerminalHandler()
        storage_handler = ZenMLLoggingHandler()

        importlib.reload(terminal_logging)
        duplicate_kitaru_handler = terminal_logging._KitaruTerminalHandler()
        root.handlers = [old_kitaru_handler, storage_handler, duplicate_kitaru_handler]

        terminal_logging.install_terminal_log_intercept()

        assert _kitaru_handlers(root) == [old_kitaru_handler]
        assert storage_handler in root.handlers


# ---------------------------------------------------------------------------
# _is_kitaru_terminal_handler unit tests
# ---------------------------------------------------------------------------


class TestIsKitaruTerminalHandler:
    """Direct tests for the marker-based handler detection function."""

    def test_plain_handler_returns_false(self) -> None:
        handler = logging.StreamHandler()
        assert terminal_logging._is_kitaru_terminal_handler(handler) is False

    def test_kitaru_handler_returns_true(self) -> None:
        handler = terminal_logging._KitaruTerminalHandler()
        assert terminal_logging._is_kitaru_terminal_handler(handler) is True

    def test_handler_with_marker_attribute_returns_true(self) -> None:
        """Simulates a handler from a pre-reload class that still has the marker."""
        handler = logging.StreamHandler()
        setattr(
            handler,
            terminal_logging._KITARU_HANDLER_MARKER_ATTR,
            terminal_logging._KITARU_HANDLER_MARKER_VALUE,
        )
        assert terminal_logging._is_kitaru_terminal_handler(handler) is True

    def test_handler_with_wrong_marker_value_returns_false(self) -> None:
        handler = logging.StreamHandler()
        setattr(
            handler,
            terminal_logging._KITARU_HANDLER_MARKER_ATTR,
            "wrong-value",
        )
        assert terminal_logging._is_kitaru_terminal_handler(handler) is False


# ---------------------------------------------------------------------------
# Bootstrap integration test
# ---------------------------------------------------------------------------


class TestBootstrapIntegration:
    """After ``import kitaru``, the root logger should have a Kitaru handler."""

    def test_import_kitaru_has_kitaru_handler(self) -> None:
        root = logging.getLogger()
        kitaru_handlers = _kitaru_handlers(root)
        assert len(kitaru_handlers) >= 1

    def test_zenml_log_record_produces_rewritten_output(self) -> None:
        """A ZenML logger message flows through the Kitaru handler."""
        record = _make_record(
            "zenml.pipelines.pipeline_definition",
            "Pipeline completed successfully.",
        )
        decision = terminal_logging._decide(record)
        assert decision is not None
        assert decision.text == "Flow completed."

    def test_reload_patches_terminal_intercept(self) -> None:
        """Reloading kitaru should re-run the terminal log intercept."""
        import kitaru

        with patch(
            "kitaru._terminal_logging.install_terminal_log_intercept"
        ) as install_intercept:
            importlib.reload(kitaru)

        install_intercept.assert_called_once()
