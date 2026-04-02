"""Tests for the Kitaru analytics helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from kitaru.analytics import AnalyticsEvent, set_source, track


def test_set_source_prefixes_kitaru_suffix() -> None:
    """Bare suffixes should be normalized to Kitaru source values."""
    with (
        patch("zenml.analytics.source_context") as source_context_mock,
        patch(
            "zenml.enums.SourceContextTypes",
            side_effect=lambda value: f"normalized:{value}",
        ) as source_types_mock,
    ):
        set_source("cli")

    source_types_mock.assert_called_once_with("kitaru-cli")
    source_context_mock.set.assert_called_once_with("normalized:kitaru-cli")


def test_set_source_accepts_full_kitaru_source() -> None:
    """Canonical Kitaru source values should pass through unchanged."""
    with (
        patch("zenml.analytics.source_context") as source_context_mock,
        patch(
            "zenml.enums.SourceContextTypes",
            side_effect=lambda value: f"normalized:{value}",
        ) as source_types_mock,
    ):
        set_source("kitaru-mcp")

    source_types_mock.assert_called_once_with("kitaru-mcp")
    source_context_mock.set.assert_called_once_with("normalized:kitaru-mcp")


def test_track_passes_metadata_through_unchanged() -> None:
    """track() should delegate metadata without injecting interface fields."""
    metadata = {"command": "status"}

    with patch("zenml.analytics.track", return_value=True) as track_mock:
        result = track(AnalyticsEvent.CLI_INVOKED, metadata)

    assert result is True
    track_mock.assert_called_once_with(
        event=AnalyticsEvent.CLI_INVOKED,
        metadata=metadata,
    )


def test_new_event_canonical_strings() -> None:
    """Phase 1 event members should carry the expected canonical strings."""
    assert AnalyticsEvent.FLOW_SUBMITTED == "Kitaru flow submitted"
    assert AnalyticsEvent.REPLAY_REQUESTED == "Kitaru flow replay requested"
    assert AnalyticsEvent.REPLAY_FAILED == "Kitaru flow replay failed"


def test_phase2_event_canonical_strings() -> None:
    """Phase 2 core-funnel events should carry the expected canonical strings."""
    assert AnalyticsEvent.PROJECT_INITIALIZED == "Kitaru project initialized"
    assert AnalyticsEvent.LOGIN_COMPLETED == "Kitaru login completed"
    assert AnalyticsEvent.LOCAL_SERVER_STARTED == "Kitaru local server started"
    assert AnalyticsEvent.LOCAL_SERVER_STOPPED == "Kitaru local server stopped"
    assert AnalyticsEvent.FLOW_TERMINAL == "Kitaru flow terminal"
    assert AnalyticsEvent.WAIT_CREATED == "Kitaru wait created"
    assert AnalyticsEvent.WAIT_RESOLVED == "Kitaru wait resolved"
    assert AnalyticsEvent.EXECUTION_RETRIED == "Kitaru execution retried"
    assert AnalyticsEvent.EXECUTION_RESUMED == "Kitaru execution resumed"
    assert AnalyticsEvent.EXECUTION_CANCELLED == "Kitaru execution cancelled"


def test_phase3_event_canonical_strings() -> None:
    """Phase 3 feature-adoption events should carry expected canonical strings."""
    assert AnalyticsEvent.LLM_CALLED == "Kitaru LLM called"
    assert AnalyticsEvent.ARTIFACT_SAVED == "Kitaru artifact saved"
    assert AnalyticsEvent.ARTIFACT_LOADED == "Kitaru artifact loaded"
    assert AnalyticsEvent.SECRET_UPSERTED == "Kitaru secret upserted"
    assert AnalyticsEvent.STACK_CREATED == "Kitaru stack created"
    assert AnalyticsEvent.STACK_ACTIVATED == "Kitaru stack activated"
    assert AnalyticsEvent.MODEL_ALIAS_REGISTERED == "Kitaru model alias registered"
    assert AnalyticsEvent.LOG_STORE_CONFIGURED == "Kitaru log store configured"


def test_phase4_event_canonical_strings() -> None:
    """Phase 4 adapter events should carry expected canonical strings."""
    assert AnalyticsEvent.PYDANTIC_AI_WRAPPED == "Kitaru PydanticAI wrapped"
    assert AnalyticsEvent.PYDANTIC_AI_RUN_COMPLETED == "Kitaru PydanticAI run completed"


def test_cli_entrypoint_tracks_two_token_command_granularity() -> None:
    """CLI entrypoint should capture command + subcommand from argv."""
    with (
        patch("kitaru.analytics.set_source") as set_source_mock,
        patch("kitaru.analytics.track", return_value=True) as track_mock,
        patch("kitaru.cli.GlobalConfiguration") as gc_mock,
        patch("kitaru.cli._apply_runtime_version"),
        patch("kitaru.cli.app"),
        patch("sys.argv", ["kitaru", "executions", "logs", "kr-123"]),
    ):
        gc_mock.return_value = MagicMock()
        from kitaru.cli import cli

        cli()

    set_source_mock.assert_called_once_with("cli")
    track_mock.assert_called_once_with(
        AnalyticsEvent.CLI_INVOKED,
        {"command": "executions logs"},
    )


def test_cli_entrypoint_tracks_single_command() -> None:
    """Single-token commands should still work correctly."""
    with (
        patch("kitaru.analytics.set_source"),
        patch("kitaru.analytics.track", return_value=True) as track_mock,
        patch("kitaru.cli.GlobalConfiguration") as gc_mock,
        patch("kitaru.cli._apply_runtime_version"),
        patch("kitaru.cli.app"),
        patch("sys.argv", ["kitaru", "status"]),
    ):
        gc_mock.return_value = MagicMock()
        from kitaru.cli import cli

        cli()

    track_mock.assert_called_once_with(
        AnalyticsEvent.CLI_INVOKED,
        {"command": "status"},
    )


def test_cli_entrypoint_tracks_help_for_bare_invocation() -> None:
    """Bare `kitaru` invocation should track 'help'."""
    with (
        patch("kitaru.analytics.set_source"),
        patch("kitaru.analytics.track", return_value=True) as track_mock,
        patch("kitaru.cli.GlobalConfiguration") as gc_mock,
        patch("kitaru.cli._apply_runtime_version"),
        patch("kitaru.cli.app"),
        patch("sys.argv", ["kitaru"]),
    ):
        gc_mock.return_value = MagicMock()
        from kitaru.cli import cli

        cli()

    track_mock.assert_called_once_with(
        AnalyticsEvent.CLI_INVOKED,
        {"command": "help"},
    )
