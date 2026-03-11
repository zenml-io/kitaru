"""Tests for Kitaru sandbox sessions."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from kitaru.config import (
    ResolvedMontySandboxSettings,
    ResolvedSandboxConfig,
    SandboxProviderKind,
)
from kitaru.errors import (
    KitaruContextError,
    KitaruSandboxCapabilityError,
    KitaruSandboxNotConfiguredError,
)
from kitaru.runtime import _checkpoint_scope, _flow_scope
from kitaru.sandbox import SandboxExecutionResult, run_sandbox_smoke_test, sandbox


@pytest.fixture
def resolved_sandbox() -> ResolvedSandboxConfig:
    """Return a standard resolved Monty config for sandbox tests."""
    return ResolvedSandboxConfig(
        provider=SandboxProviderKind.MONTY,
        monty=ResolvedMontySandboxSettings(
            max_duration_secs=1.0,
            max_memory_mb=64,
            type_check=True,
        ),
    )


def test_sandbox_requires_flow_context() -> None:
    """sandbox() should fail outside a flow."""
    with pytest.raises(KitaruContextError, match="inside a @flow"):
        sandbox()


def test_sandbox_requires_configured_provider() -> None:
    """sandbox() should fail clearly when no provider is configured."""
    with (
        _flow_scope(name="demo"),
        pytest.raises(
            KitaruSandboxNotConfiguredError,
            match="No sandbox provider",
        ),
    ):
        sandbox()


def test_sandbox_returns_execution_scoped_session(
    resolved_sandbox: ResolvedSandboxConfig,
) -> None:
    """sandbox() should return an execution-scoped session in the flow body."""
    with _flow_scope(name="demo", sandbox_config=resolved_sandbox):
        session = sandbox(name="coding-loop")
        same_name = sandbox(name="coding-loop")

    assert session.scope == "execution"
    assert session.name == "coding_loop"
    assert same_name.name == "coding_loop"
    assert session.capabilities.supports_stateful_python is True


def test_sandbox_returns_checkpoint_scoped_session_inside_checkpoint(
    resolved_sandbox: ResolvedSandboxConfig,
) -> None:
    """sandbox() should switch to checkpoint scope inside a checkpoint."""
    with (
        _flow_scope(name="demo", sandbox_config=resolved_sandbox),
        _checkpoint_scope(name="step", checkpoint_type=None),
    ):
        session = sandbox(name="step-sandbox")

    assert session.scope == "checkpoint"
    assert session.name == "step_sandbox"


def test_execution_scoped_run_code_uses_synthetic_checkpoint_dispatch(
    resolved_sandbox: ResolvedSandboxConfig,
) -> None:
    """Flow-scope sandbox calls should route through the synthetic checkpoint helper."""
    result = SandboxExecutionResult(
        call_name="sandbox_1",
        provider=SandboxProviderKind.MONTY,
        session_name="sandbox",
        value=2,
        duration_ms=1.0,
    )
    with (
        _flow_scope(name="demo", sandbox_config=resolved_sandbox),
        patch(
            "kitaru.sandbox._sandbox_run_code_checkpoint_call",
            return_value=result,
        ) as mock_call,
    ):
        session = sandbox()
        resolved = session.run_code("1 + 1")

    assert resolved.value == 2
    mock_call.assert_called_once()


def test_pause_and_resume_preserve_state(
    resolved_sandbox: ResolvedSandboxConfig,
) -> None:
    """Monty-backed sessions should preserve REPL state across pause/resume."""
    with (
        _flow_scope(name="demo", sandbox_config=resolved_sandbox),
        patch("kitaru.sandbox._save_with_fallback", return_value="artifact"),
        patch("kitaru.sandbox.log"),
        patch("kitaru.sandbox._sandbox_run_code_checkpoint_call") as mock_call,
    ):
        session = sandbox(name="math")

        def _dispatch(request):
            return session._manager.run_code(
                scope=request.scope,
                scope_id=request.scope_id,
                session_name=request.session_name,
                code=request.code,
                inputs=request.inputs,
                call_name=request.call_name,
            )

        mock_call.side_effect = _dispatch

        session.run_code("x = 41")
        session.pause()
        session.resume()
        result = session.run_code("x + 1")

    assert result.value == 42


def test_run_command_is_not_supported_for_monty(
    resolved_sandbox: ResolvedSandboxConfig,
) -> None:
    """Monty should reject shell-style command execution."""
    with _flow_scope(name="demo", sandbox_config=resolved_sandbox):
        session = sandbox()
        with pytest.raises(
            KitaruSandboxCapabilityError,
            match="cannot run shell commands",
        ):
            session.run_command("ls -la")


def test_run_sandbox_smoke_test_works_with_monty(
    resolved_sandbox: ResolvedSandboxConfig,
) -> None:
    """The standalone smoke test should verify pause/resume behavior."""
    run_sandbox_smoke_test(resolved_sandbox)
