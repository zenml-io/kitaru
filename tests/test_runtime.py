"""Tests for internal runtime scope helpers."""

from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

from kitaru.runtime import (
    _checkpoint_scope,
    _flow_scope,
    _get_current_flow,
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
    flow_id = str(uuid4())
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="demo_flow", flow_id=flow_id, execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        assert _is_inside_flow()
        assert _is_inside_checkpoint()
        current = _get_current_flow()
        assert current is not None
        assert current.flow_id == flow_id

        with _suspend_checkpoint_scope():
            assert _is_inside_flow()
            assert not _is_inside_checkpoint()

        assert _is_inside_flow()
        assert _is_inside_checkpoint()


def test_suspend_checkpoint_scope_restores_state_after_exception() -> None:
    """Checkpoint scope should be restored even when the body raises."""
    flow_id = str(uuid4())
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="demo_flow", flow_id=flow_id, execution_id=execution_id),
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
    flow_id = str(uuid4())
    execution_id, checkpoint_id = _scope_ids()

    def mock_zenml_wait(**_: object) -> None:
        return None

    with (
        _flow_scope(name="demo_flow", flow_id=flow_id, execution_id=execution_id),
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


def test_flow_scope_records_explicit_flow_id() -> None:
    """Flow scope should preserve durable flow identity alongside display name."""
    flow_id = str(uuid4())

    with _flow_scope(name="demo_flow", flow_id=flow_id):
        current_flow = _get_current_flow()

        assert current_flow is not None
        assert current_flow.name == "demo_flow"
        assert current_flow.flow_id == flow_id


def test_flow_scope_resolves_flow_id_from_step_context() -> None:
    """Flow scope should resolve IDs from the active ZenML step context."""
    flow_id = str(uuid4())
    step_context = type(
        "StepContextStub",
        (),
        {
            "pipeline_run": type(
                "PipelineRunStub",
                (),
                {
                    "pipeline": type(
                        "PipelineStub",
                        (),
                        {"id": flow_id, "name": "demo_flow"},
                    )()
                },
            )()
        },
    )()

    with (
        patch("kitaru.runtime.StepContext.get", return_value=step_context),
        patch("kitaru.runtime.DynamicPipelineRunContext.get", return_value=None),
        _flow_scope(name="demo_flow"),
    ):
        current_flow = _get_current_flow()

        assert current_flow is not None
        assert current_flow.flow_id == flow_id


def test_flow_scope_resolves_flow_id_from_dynamic_run_context() -> None:
    """Flow scope should resolve IDs from the active ZenML run context."""
    flow_id = str(uuid4())
    run_context = type(
        "DynamicRunContextStub",
        (),
        {
            "run": type(
                "RunStub",
                (),
                {
                    "pipeline": type(
                        "PipelineStub",
                        (),
                        {"id": flow_id, "name": "demo_flow"},
                    )()
                },
            )(),
            "pipeline": type(
                "LegacyPipelineStub",
                (),
                {"id": None, "name": None},
            )(),
        },
    )()

    with (
        patch("kitaru.runtime.StepContext.get", return_value=None),
        patch("kitaru.runtime.DynamicPipelineRunContext.get", return_value=run_context),
        _flow_scope(name="demo_flow"),
    ):
        current_flow = _get_current_flow()

        assert current_flow is not None
        assert current_flow.flow_id == flow_id


def test_flow_scope_resolves_flow_id_from_legacy_pipeline_attr() -> None:
    """Fall back to run_context.pipeline.id when run.pipeline.id is unavailable."""
    flow_id = str(uuid4())
    run_context = type(
        "DynamicRunContextStub",
        (),
        {
            "run": type(
                "RunStub",
                (),
                {
                    "pipeline": type(
                        "PipelineStub",
                        (),
                        {"id": None, "name": None},
                    )()
                },
            )(),
            "pipeline": type(
                "LegacyPipelineStub",
                (),
                {"id": flow_id, "name": "demo_flow"},
            )(),
        },
    )()

    with (
        patch("kitaru.runtime.StepContext.get", return_value=None),
        patch("kitaru.runtime.DynamicPipelineRunContext.get", return_value=run_context),
        _flow_scope(name="demo_flow"),
    ):
        current_flow = _get_current_flow()

        assert current_flow is not None
        assert current_flow.flow_id == flow_id
