"""Tests for the Kitaru analytics helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from kitaru import analytics
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


def test_track_enriches_metadata_with_authoritative_versions() -> None:
    """track() should copy caller metadata and add central version fields."""
    metadata = {
        "command": "status",
        "kitaru_version": "caller-value",
        "zenml_version": "caller-value",
    }

    with (
        patch("kitaru.analytics.resolve_installed_version", return_value="1.2.3"),
        patch("kitaru.analytics.resolve_zenml_version", return_value="4.5.6"),
        patch("zenml.analytics.track", return_value=True) as track_mock,
    ):
        result = track(AnalyticsEvent.CLI_INVOKED, metadata)

    assert result is True
    track_mock.assert_called_once()
    sent_metadata = track_mock.call_args.kwargs["metadata"]
    assert sent_metadata == {
        "command": "status",
        "kitaru_version": "1.2.3",
        "zenml_version": "4.5.6",
    }
    assert sent_metadata is not metadata
    assert metadata == {
        "command": "status",
        "kitaru_version": "caller-value",
        "zenml_version": "caller-value",
    }


def test_track_supports_metadata_none() -> None:
    """track() should still work when callers have no metadata to add."""
    with (
        patch("kitaru.analytics.resolve_installed_version", return_value="1.2.3"),
        patch("kitaru.analytics.resolve_zenml_version", return_value="4.5.6"),
        patch("zenml.analytics.track", return_value=True) as track_mock,
    ):
        result = track(AnalyticsEvent.STATUS_VIEWED, None)

    assert result is True
    track_mock.assert_called_once_with(
        event=AnalyticsEvent.STATUS_VIEWED,
        metadata={"kitaru_version": "1.2.3", "zenml_version": "4.5.6"},
    )


def test_resolve_zenml_version_falls_back_to_unknown() -> None:
    """Missing ZenML package metadata should produce the analytics fallback value."""
    analytics.resolve_zenml_version.cache_clear()
    with patch(
        "kitaru.analytics.importlib.metadata.version",
        side_effect=analytics.importlib.metadata.PackageNotFoundError,
    ):
        assert analytics.resolve_zenml_version() == "unknown"
    analytics.resolve_zenml_version.cache_clear()


def test_track_returns_false_when_zenml_tracking_fails() -> None:
    """Analytics failures should never raise into user-facing code."""
    with (
        patch("kitaru.analytics.resolve_installed_version", return_value="1.2.3"),
        patch("kitaru.analytics.resolve_zenml_version", return_value="4.5.6"),
        patch("zenml.analytics.track", side_effect=RuntimeError("boom")),
    ):
        assert track(AnalyticsEvent.CLI_INVOKED, {"command": "status"}) is False


def test_track_returns_false_when_version_enrichment_raises() -> None:
    """Version resolution failures must not propagate to callers."""
    with (
        patch(
            "kitaru.analytics.resolve_installed_version",
            side_effect=RuntimeError("broken metadata"),
        ),
        patch("kitaru.analytics.resolve_zenml_version", return_value="4.5.6"),
        patch("zenml.analytics.track", return_value=True) as track_mock,
    ):
        result = track(AnalyticsEvent.CLI_INVOKED, {"command": "status"})

    assert result is True
    sent_metadata = track_mock.call_args.kwargs["metadata"]
    assert sent_metadata["kitaru_version"] == "unknown"
    assert sent_metadata["zenml_version"] == "4.5.6"


def test_track_returns_false_when_both_version_helpers_raise() -> None:
    """Both version helpers failing should still track with unknown versions."""
    with (
        patch(
            "kitaru.analytics.resolve_installed_version",
            side_effect=RuntimeError("bad"),
        ),
        patch(
            "kitaru.analytics.resolve_zenml_version",
            side_effect=RuntimeError("bad"),
        ),
        patch("zenml.analytics.track", return_value=True) as track_mock,
    ):
        result = track(AnalyticsEvent.CLI_INVOKED, {"command": "status"})

    assert result is True
    sent_metadata = track_mock.call_args.kwargs["metadata"]
    assert sent_metadata["kitaru_version"] == "unknown"
    assert sent_metadata["zenml_version"] == "unknown"


def test_flow_lifecycle_event_canonical_strings() -> None:
    """Flow-lifecycle events should carry the expected canonical strings."""
    assert AnalyticsEvent.FLOW_SUBMITTED == "Kitaru flow submitted"
    assert AnalyticsEvent.REPLAY_REQUESTED == "Kitaru flow replay requested"
    assert AnalyticsEvent.REPLAY_FAILED == "Kitaru flow replay failed"


def test_core_funnel_event_canonical_strings() -> None:
    """Core-funnel events should carry the expected canonical strings."""
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


def test_feature_adoption_event_canonical_strings() -> None:
    """Feature-adoption events should carry the expected canonical strings."""
    assert AnalyticsEvent.LLM_CALLED == "Kitaru LLM called"
    assert AnalyticsEvent.ARTIFACT_SAVED == "Kitaru artifact saved"
    assert AnalyticsEvent.ARTIFACT_LOADED == "Kitaru artifact loaded"
    assert AnalyticsEvent.MEMORY_WRITTEN == "Kitaru memory written"
    assert AnalyticsEvent.MEMORY_DELETED == "Kitaru memory deleted"
    assert AnalyticsEvent.MEMORY_PURGED == "Kitaru memory purged"
    assert AnalyticsEvent.MEMORY_COMPACTED == "Kitaru memory compacted"
    assert AnalyticsEvent.MEMORY_REINDEX_RUN == "Kitaru memory reindex run"
    assert AnalyticsEvent.SECRET_UPSERTED == "Kitaru secret upserted"
    assert AnalyticsEvent.STACK_CREATED == "Kitaru stack created"
    assert AnalyticsEvent.STACK_ACTIVATED == "Kitaru stack activated"
    assert AnalyticsEvent.MODEL_ALIAS_REGISTERED == "Kitaru model alias registered"
    assert AnalyticsEvent.LOG_STORE_CONFIGURED == "Kitaru log store configured"


def test_adapter_event_canonical_strings() -> None:
    """Adapter events should carry the expected canonical strings."""
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


def test_cli_entrypoint_tracks_memory_subcommand_granularity() -> None:
    """Memory commands should track command + subcommand from argv."""
    with (
        patch("kitaru.analytics.set_source"),
        patch("kitaru.analytics.track", return_value=True) as track_mock,
        patch("kitaru.cli.GlobalConfiguration") as gc_mock,
        patch("kitaru.cli._apply_runtime_version"),
        patch("kitaru.cli.app"),
        patch("sys.argv", ["kitaru", "memory", "compact", "--scope", "demo"]),
    ):
        gc_mock.return_value = MagicMock()
        from kitaru.cli import cli

        cli()

    track_mock.assert_called_once_with(
        AnalyticsEvent.CLI_INVOKED,
        {"command": "memory compact"},
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


def test_cli_entrypoint_excludes_positional_arg_from_command() -> None:
    """Single-command verbs with positional args should not leak user data."""
    with (
        patch("kitaru.analytics.set_source"),
        patch("kitaru.analytics.track", return_value=True) as track_mock,
        patch("kitaru.cli.GlobalConfiguration") as gc_mock,
        patch("kitaru.cli._apply_runtime_version"),
        patch("kitaru.cli.app"),
        patch("sys.argv", ["kitaru", "login", "https://my-server.example.com"]),
    ):
        gc_mock.return_value = MagicMock()
        from kitaru.cli import cli

        cli()

    track_mock.assert_called_once_with(
        AnalyticsEvent.CLI_INVOKED,
        {"command": "login"},
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
