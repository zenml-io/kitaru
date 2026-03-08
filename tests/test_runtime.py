"""Tests for internal runtime scope helpers."""

from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

from kitaru.runtime import (
    _checkpoint_scope,
    _flow_scope,
    _is_inside_checkpoint,
    _is_inside_flow,
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
