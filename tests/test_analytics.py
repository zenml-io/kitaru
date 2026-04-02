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
