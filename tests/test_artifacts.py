"""Tests for `kitaru.save()` and `kitaru.load()` artifact behavior."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from zenml.enums import ArtifactSaveType, ArtifactType

from kitaru.artifacts import load, save
from kitaru.runtime import _checkpoint_scope, _flow_scope


def _artifact(
    *,
    name: str,
    save_type: ArtifactSaveType,
    artifact_id: UUID | None = None,
) -> SimpleNamespace:
    """Create a lightweight artifact-like object for tests."""
    return SimpleNamespace(
        id=artifact_id or uuid4(),
        name=name,
        save_type=save_type,
    )


def _hydrated_run(
    *,
    step_outputs: dict[str, dict[str, list[SimpleNamespace]]],
) -> SimpleNamespace:
    """Create a lightweight hydrated run-like object for tests."""
    return SimpleNamespace(
        id=uuid4(),
        steps={
            step_name: SimpleNamespace(outputs=outputs)
            for step_name, outputs in step_outputs.items()
        },
    )


def _scope_ids() -> tuple[str, str]:
    """Return valid execution and checkpoint IDs for runtime scopes."""
    return str(uuid4()), str(uuid4())


def test_save_raises_outside_checkpoint() -> None:
    with pytest.raises(RuntimeError, match=r"inside a @checkpoint"):
        save("artifact", 123)


def test_save_requires_execution_id_inside_checkpoint() -> None:
    _, checkpoint_id = _scope_ids()

    with (
        _checkpoint_scope(
            name="research",
            checkpoint_type=None,
            execution_id=None,
            checkpoint_id=checkpoint_id,
        ),
        pytest.raises(RuntimeError, match="active execution ID"),
    ):
        save("artifact", 123)


def test_save_requires_checkpoint_id_inside_checkpoint() -> None:
    execution_id, _ = _scope_ids()

    with (
        _checkpoint_scope(
            name="research",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=None,
        ),
        pytest.raises(RuntimeError, match="active checkpoint ID"),
    ):
        save("artifact", 123)


def test_save_rejects_invalid_execution_uuid_in_scope() -> None:
    _, checkpoint_id = _scope_ids()

    with (
        _checkpoint_scope(
            name="research",
            checkpoint_type=None,
            execution_id="bad-execution-id",
            checkpoint_id=checkpoint_id,
        ),
        pytest.raises(RuntimeError, match="invalid execution ID"),
    ):
        save("artifact", 123)


def test_save_rejects_invalid_checkpoint_uuid_in_scope() -> None:
    execution_id, _ = _scope_ids()

    with (
        _checkpoint_scope(
            name="research",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id="bad-checkpoint-id",
        ),
        pytest.raises(RuntimeError, match="invalid checkpoint ID"),
    ):
        save("artifact", 123)


def test_save_rejects_unsupported_artifact_type() -> None:
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="flow", execution_id=execution_id),
        _checkpoint_scope(
            name="research",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        pytest.raises(ValueError, match="Unsupported Kitaru artifact type"),
    ):
        save("artifact", 123, type="weird")


def test_save_delegates_to_zenml_manual_artifact_publisher() -> None:
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="flow", execution_id=execution_id),
        _checkpoint_scope(
            name="research",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch("kitaru.artifacts.save_artifact") as save_artifact_mock,
    ):
        payload = {"notes": ["a", "b"]}
        save("research_context", payload, type="context", tags=["debug"])

    save_artifact_mock.assert_called_once_with(
        data={"notes": ["a", "b"]},
        name="research_context",
        artifact_type=ArtifactType.DATA,
        tags=["debug"],
        user_metadata={"kitaru_artifact_type": "context"},
    )


def test_load_raises_outside_checkpoint() -> None:
    with pytest.raises(RuntimeError, match=r"inside a @checkpoint"):
        load(str(uuid4()), "research")


def test_load_rejects_invalid_target_execution_id() -> None:
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="flow", execution_id=execution_id),
        _checkpoint_scope(
            name="reader",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        pytest.raises(ValueError, match="expected `exec_id` to be a UUID"),
    ):
        load("not-a-uuid", "research")


def test_load_resolves_manual_saved_artifact_by_name() -> None:
    execution_id, checkpoint_id = _scope_ids()
    target_execution_id = str(uuid4())

    manual_artifact = _artifact(
        name="research_context",
        save_type=ArtifactSaveType.MANUAL,
    )
    hydrated_run = _hydrated_run(
        step_outputs={
            "research": {
                "research_context": [manual_artifact],
            }
        }
    )

    run_response = MagicMock()
    run_response.get_hydrated_version.return_value = hydrated_run

    loaded_artifact = MagicMock()
    loaded_artifact.load.return_value = {"topic": "kitaru"}

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = run_response
    client_mock.get_artifact_version.return_value = loaded_artifact

    with (
        _flow_scope(name="flow", execution_id=execution_id),
        _checkpoint_scope(
            name="reader",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch("kitaru.artifacts.Client", return_value=client_mock),
    ):
        value = load(target_execution_id, "research_context")

    assert value == {"topic": "kitaru"}
    client_mock.get_pipeline_run.assert_called_once_with(
        UUID(target_execution_id),
        allow_name_prefix_match=False,
    )
    client_mock.get_artifact_version.assert_called_once_with(
        manual_artifact.id,
        hydrate=True,
    )


def test_load_resolves_checkpoint_output_by_checkpoint_name() -> None:
    execution_id, checkpoint_id = _scope_ids()

    step_output_artifact = _artifact(
        name="output",
        save_type=ArtifactSaveType.STEP_OUTPUT,
    )
    hydrated_run = _hydrated_run(
        step_outputs={
            "research": {
                "output": [step_output_artifact],
            }
        }
    )

    run_response = MagicMock()
    run_response.get_hydrated_version.return_value = hydrated_run

    loaded_artifact = MagicMock()
    loaded_artifact.load.return_value = "notes"

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = run_response
    client_mock.get_artifact_version.return_value = loaded_artifact

    with (
        _flow_scope(name="flow", execution_id=execution_id),
        _checkpoint_scope(
            name="reader",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch("kitaru.artifacts.Client", return_value=client_mock),
    ):
        value = load(str(uuid4()), "research")

    assert value == "notes"
    client_mock.get_artifact_version.assert_called_once_with(
        step_output_artifact.id,
        hydrate=True,
    )


def test_load_resolves_checkpoint_output_with_source_alias_step_name() -> None:
    execution_id, checkpoint_id = _scope_ids()

    step_output_artifact = _artifact(
        name="output",
        save_type=ArtifactSaveType.STEP_OUTPUT,
    )
    hydrated_run = _hydrated_run(
        step_outputs={
            "__kitaru_checkpoint_source_research": {
                "output": [step_output_artifact],
            }
        }
    )

    run_response = MagicMock()
    run_response.get_hydrated_version.return_value = hydrated_run

    loaded_artifact = MagicMock()
    loaded_artifact.load.return_value = "notes"

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = run_response
    client_mock.get_artifact_version.return_value = loaded_artifact

    with (
        _flow_scope(name="flow", execution_id=execution_id),
        _checkpoint_scope(
            name="reader",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch("kitaru.artifacts.Client", return_value=client_mock),
    ):
        value = load(str(uuid4()), "research")

    assert value == "notes"


def test_load_raises_when_name_is_not_found() -> None:
    execution_id, checkpoint_id = _scope_ids()

    hydrated_run = _hydrated_run(
        step_outputs={
            "research": {
                "output": [
                    _artifact(
                        name="output",
                        save_type=ArtifactSaveType.STEP_OUTPUT,
                    )
                ]
            }
        }
    )

    run_response = MagicMock()
    run_response.get_hydrated_version.return_value = hydrated_run

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = run_response

    with (
        _flow_scope(name="flow", execution_id=execution_id),
        _checkpoint_scope(
            name="reader",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch("kitaru.artifacts.Client", return_value=client_mock),
        pytest.raises(RuntimeError, match="No artifact named"),
    ):
        load(str(uuid4()), "research_context")


def test_load_raises_on_ambiguous_matches() -> None:
    execution_id, checkpoint_id = _scope_ids()

    duplicate_name = "shared"
    hydrated_run = _hydrated_run(
        step_outputs={
            "research": {
                "shared": [
                    _artifact(
                        name=duplicate_name,
                        save_type=ArtifactSaveType.MANUAL,
                    )
                ]
            },
            "review": {
                "shared": [
                    _artifact(
                        name=duplicate_name,
                        save_type=ArtifactSaveType.MANUAL,
                    )
                ]
            },
        }
    )

    run_response = MagicMock()
    run_response.get_hydrated_version.return_value = hydrated_run

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = run_response

    with (
        _flow_scope(name="flow", execution_id=execution_id),
        _checkpoint_scope(
            name="reader",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch("kitaru.artifacts.Client", return_value=client_mock),
        pytest.raises(RuntimeError, match="Multiple artifacts named"),
    ):
        load(str(uuid4()), duplicate_name)
