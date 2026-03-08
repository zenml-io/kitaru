"""Tests for `kitaru.log()` structured metadata behavior."""

from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from zenml.enums import MetadataResourceTypes
from zenml.models.v2.misc.run_metadata import RunMetadataResource

from kitaru.logging import log
from kitaru.runtime import _checkpoint_scope, _flow_scope


def _get_logged_resource(client_mock: MagicMock) -> RunMetadataResource:
    """Extract the single resource passed to `create_run_metadata`."""
    resources = client_mock.create_run_metadata.call_args.kwargs["resources"]
    assert len(resources) == 1
    return resources[0]


def test_log_raises_outside_flow() -> None:
    with pytest.raises(RuntimeError, match=r"inside a @kitaru\.flow"):
        log(cost=0.01)


def test_log_attaches_to_execution_inside_flow() -> None:
    execution_id = str(uuid4())

    with (
        _flow_scope(name="my_flow", execution_id=execution_id),
        patch("kitaru.logging.Client") as client_cls,
    ):
        log(cost=0.01, tokens=128)

    client = client_cls.return_value
    client.create_run_metadata.assert_called_once()

    resource = _get_logged_resource(client)
    assert resource.type == MetadataResourceTypes.PIPELINE_RUN
    assert resource.id == UUID(execution_id)

    assert client.create_run_metadata.call_args.kwargs["metadata"] == {
        "cost": 0.01,
        "tokens": 128,
    }
    assert client.create_run_metadata.call_args.kwargs["publisher_step_id"] is None


def test_log_attaches_to_checkpoint_inside_checkpoint() -> None:
    execution_id = str(uuid4())
    checkpoint_id = str(uuid4())

    with (
        _flow_scope(name="my_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="my_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch("kitaru.logging.Client") as client_cls,
    ):
        log(quality_score=0.92)

    client = client_cls.return_value
    client.create_run_metadata.assert_called_once()

    resource = _get_logged_resource(client)
    assert resource.type == MetadataResourceTypes.STEP_RUN
    assert resource.id == UUID(checkpoint_id)
    assert client.create_run_metadata.call_args.kwargs["publisher_step_id"] == UUID(
        checkpoint_id
    )


def test_log_prefers_checkpoint_target_when_both_scopes_are_active() -> None:
    execution_id = str(uuid4())
    checkpoint_id = str(uuid4())

    with (
        _flow_scope(name="my_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="my_checkpoint",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch("kitaru.logging.Client") as client_cls,
    ):
        log(stage="checkpoint")

    resource = _get_logged_resource(client_cls.return_value)
    assert resource.type == MetadataResourceTypes.STEP_RUN
    assert resource.id == UUID(checkpoint_id)


def test_log_requires_execution_id_inside_flow() -> None:
    with (
        _flow_scope(name="my_flow", execution_id=None),
        patch("kitaru.logging.Client") as client_cls,
        pytest.raises(RuntimeError, match="active execution ID"),
    ):
        log(cost=0.01)

    client_cls.assert_not_called()


def test_log_requires_checkpoint_id_inside_checkpoint() -> None:
    execution_id = str(uuid4())

    with (
        _flow_scope(name="my_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="my_checkpoint",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=None,
        ),
        patch("kitaru.logging.Client") as client_cls,
        pytest.raises(RuntimeError, match="active checkpoint ID"),
    ):
        log(cost=0.01)

    client_cls.assert_not_called()


def test_log_rejects_invalid_execution_uuid() -> None:
    with (
        _flow_scope(name="my_flow", execution_id="exec-not-a-uuid"),
        patch("kitaru.logging.Client") as client_cls,
        pytest.raises(RuntimeError, match="invalid execution ID"),
    ):
        log(cost=0.01)

    client_cls.assert_not_called()


def test_log_rejects_invalid_checkpoint_uuid() -> None:
    execution_id = str(uuid4())

    with (
        _flow_scope(name="my_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="my_checkpoint",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id="checkpoint-not-a-uuid",
        ),
        patch("kitaru.logging.Client") as client_cls,
        pytest.raises(RuntimeError, match="invalid checkpoint ID"),
    ):
        log(cost=0.01)

    client_cls.assert_not_called()
