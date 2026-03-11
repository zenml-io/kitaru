"""Tests for internal runtime scope helpers."""

from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

from kitaru.config import (
    _KITARU_RESOLVED_SANDBOX_ENV,
    ResolvedMontySandboxSettings,
    ResolvedSandboxConfig,
    SandboxProviderKind,
)
from kitaru.runtime import (
    _checkpoint_scope,
    _flow_scope,
    _get_current_sandbox_config,
    _is_inside_checkpoint,
    _is_inside_flow,
    _sandbox_after_wait,
    _sandbox_before_wait,
    _set_current_sandbox_manager,
    _submission_sandbox_config,
    _suspend_checkpoint_scope,
)
from kitaru.wait import wait


def _scope_ids() -> tuple[str, str]:
    """Return valid UUID strings for flow/checkpoint scope setup."""
    return str(uuid4()), str(uuid4())


def test_suspend_checkpoint_scope_temporarily_clears_checkpoint_scope() -> None:
    """Checkpoint scope should be disabled only inside suspension context."""
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        assert _is_inside_flow()
        assert _is_inside_checkpoint()

        with _suspend_checkpoint_scope():
            assert _is_inside_flow()
            assert not _is_inside_checkpoint()

        assert _is_inside_flow()
        assert _is_inside_checkpoint()


def test_suspend_checkpoint_scope_restores_state_after_exception() -> None:
    """Checkpoint scope should be restored even when the body raises."""
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        try:
            with _suspend_checkpoint_scope():
                raise RuntimeError("boom")
        except RuntimeError:
            pass

        assert _is_inside_checkpoint()


def test_wait_runs_when_checkpoint_scope_is_suspended() -> None:
    """wait() should succeed once checkpoint scope is temporarily suspended."""
    execution_id, checkpoint_id = _scope_ids()

    def mock_zenml_wait(**_: object) -> tuple[bool, object]:
        return True, object()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch("kitaru.wait._resolve_zenml_wait", return_value=mock_zenml_wait),
        _suspend_checkpoint_scope(),
    ):
        assert wait(name="approve") is True


def test_flow_scope_reads_submission_sandbox_config() -> None:
    """Flow scope should capture resolved sandbox config from submission context."""
    sandbox_config = ResolvedSandboxConfig(
        provider=SandboxProviderKind.MONTY,
        monty=ResolvedMontySandboxSettings(
            max_duration_secs=1.0,
            max_memory_mb=64,
            type_check=True,
        ),
    )

    with _submission_sandbox_config(sandbox_config), _flow_scope(name="demo"):
        assert _get_current_sandbox_config() == sandbox_config


def test_flow_scope_reads_sandbox_config_from_env(monkeypatch) -> None:
    """Flow scope should fall back to the propagated sandbox env var."""
    sandbox_config = ResolvedSandboxConfig(
        provider=SandboxProviderKind.MONTY,
        monty=ResolvedMontySandboxSettings(
            max_duration_secs=2.0,
            max_memory_mb=128,
            type_check=False,
        ),
    )
    monkeypatch.setenv(
        _KITARU_RESOLVED_SANDBOX_ENV,
        sandbox_config.model_dump_json(exclude_none=True),
    )

    with _flow_scope(name="demo"):
        assert _get_current_sandbox_config() == sandbox_config


def test_wait_hooks_delegate_to_active_sandbox_manager() -> None:
    """Runtime wait helpers should call the active sandbox manager when present."""

    class _Manager:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str | None]] = []

        def close_checkpoint_scope(self, checkpoint_id: str | None) -> None:
            self.calls.append(("close_checkpoint_scope", checkpoint_id))

        def close_execution_scope(self, execution_id: str | None) -> None:
            self.calls.append(("close_execution_scope", execution_id))

        def before_wait(self, execution_id: str | None) -> None:
            self.calls.append(("before_wait", execution_id))

        def after_wait(self, execution_id: str | None) -> None:
            self.calls.append(("after_wait", execution_id))

    sandbox_manager = _Manager()
    _set_current_sandbox_manager(sandbox_manager)
    execution_id, _ = _scope_ids()
    try:
        with _flow_scope(name="demo", execution_id=execution_id):
            _sandbox_before_wait()
            _sandbox_after_wait()
    finally:
        _set_current_sandbox_manager(None)

    assert sandbox_manager.calls[:2] == [
        ("before_wait", execution_id),
        ("after_wait", execution_id),
    ]
