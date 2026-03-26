"""Tests for internal runtime scope helpers."""

from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

from kitaru.engines._protocols import RuntimeSession
from kitaru.runtime import (
    _checkpoint_scope,
    _flow_scope,
    _get_current_runtime_session,
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

    def mock_zenml_wait(**_: object) -> None:
        return None

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
        assert wait(name="approve") is None


# -- Runtime session lifecycle -------------------------------------------------


def test_flow_scope_installs_runtime_session() -> None:
    """_flow_scope() should install a runtime session when none is active."""
    assert _get_current_runtime_session() is None

    with _flow_scope(name="my_flow", execution_id=str(uuid4())):
        session = _get_current_runtime_session()
        assert session is not None
        assert isinstance(session, RuntimeSession)

    assert _get_current_runtime_session() is None


def test_flow_scope_removes_session_on_exit() -> None:
    """Session should be cleaned up after flow scope exits."""
    with _flow_scope(name="my_flow", execution_id=str(uuid4())):
        assert _get_current_runtime_session() is not None

    assert _get_current_runtime_session() is None


def test_nested_flow_scope_reuses_existing_session() -> None:
    """Nested _flow_scope() should reuse the outer session."""
    with _flow_scope(name="outer_flow", execution_id=str(uuid4())):
        outer_session = _get_current_runtime_session()
        assert outer_session is not None

        with _flow_scope(name="inner_flow", execution_id=str(uuid4())):
            inner_session = _get_current_runtime_session()
            assert inner_session is outer_session

        assert _get_current_runtime_session() is outer_session


def test_flow_scope_removes_session_even_on_exception() -> None:
    """Session should be cleaned up even when the flow body raises."""
    try:
        with _flow_scope(name="my_flow", execution_id=str(uuid4())):
            assert _get_current_runtime_session() is not None
            raise RuntimeError("boom")
    except RuntimeError:
        pass

    assert _get_current_runtime_session() is None


def test_suspend_checkpoint_scope_preserves_session() -> None:
    """Suspending checkpoint scope should not affect the runtime session."""
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="my_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="my_checkpoint",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        session_before = _get_current_runtime_session()
        assert session_before is not None

        with _suspend_checkpoint_scope():
            assert _get_current_runtime_session() is session_before

        assert _get_current_runtime_session() is session_before
