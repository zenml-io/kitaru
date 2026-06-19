"""Tests for `kitaru.save()` and `kitaru.load()` artifact behavior."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from zenml.enums import ArtifactSaveType, ArtifactType

from kitaru._client._mappers import _map_artifact_ref
from kitaru.artifacts import _parse_scope_uuid, load, save
from kitaru.errors import (
    KitaruContextError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
)
from kitaru.runtime import _checkpoint_scope, _flow_scope

if TYPE_CHECKING:
    from zenml.models.v2.core.artifact_version import ArtifactVersionResponse

    from kitaru.client import KitaruClient


@dataclass(slots=True)
class _StrictArtifactVersion:
    """Minimal ZenML artifact-version shape read by artifact contracts."""

    id: UUID
    name: str
    save_type: ArtifactSaveType
    run_metadata: dict[str, Any] = field(default_factory=dict)
    producer_step_run_id: UUID | None = None


@dataclass(slots=True)
class _StrictHydratedArtifactVersion:
    """Hydrated artifact-version shape returned before materialization."""

    value: Any
    load_call_count: int = 0

    def load(self) -> Any:
        """Materialize the stored artifact value."""
        self.load_call_count += 1
        return self.value


@dataclass(slots=True)
class _StrictStepRun:
    """Minimal hydrated step-run shape used by `kitaru.load()`."""

    outputs: dict[str, list[_StrictArtifactVersion]]


@dataclass(slots=True)
class _StrictHydratedRun:
    """Minimal hydrated run shape used by `kitaru.load()`."""

    id: UUID
    steps: dict[str, _StrictStepRun]


@dataclass(slots=True)
class _StrictRunResponse:
    """Pipeline-run response that proves hydration is explicitly requested."""

    hydrated_run: _StrictHydratedRun
    hydration_count: int = 0

    def get_hydrated_version(self) -> _StrictHydratedRun:
        """Return the hydrated run and record that Kitaru required it."""
        self.hydration_count += 1
        return self.hydrated_run


@dataclass(slots=True)
class _StrictKitaruClient:
    """Minimal Kitaru client shape used by `ArtifactRef.load()`."""

    hydrated_artifact: _StrictHydratedArtifactVersion
    artifact_version_calls: list[tuple[str, bool]] = field(default_factory=list)

    def _get_artifact_version(
        self,
        artifact_id: str,
        *,
        hydrate: bool,
    ) -> _StrictHydratedArtifactVersion:
        """Record artifact-version fetches and return a hydrated artifact."""
        self.artifact_version_calls.append((artifact_id, hydrate))
        return self.hydrated_artifact


def _artifact(
    *,
    name: str,
    save_type: ArtifactSaveType,
    artifact_id: UUID | None = None,
) -> _StrictArtifactVersion:
    """Create a strict artifact-like object for tests."""
    return _StrictArtifactVersion(
        id=artifact_id or uuid4(),
        name=name,
        save_type=save_type,
    )


def _hydrated_run(
    *,
    step_outputs: dict[str, dict[str, list[_StrictArtifactVersion]]],
) -> _StrictHydratedRun:
    """Create a strict hydrated run-like object for tests."""
    return _StrictHydratedRun(
        id=uuid4(),
        steps={
            step_name: _StrictStepRun(outputs=outputs)
            for step_name, outputs in step_outputs.items()
        },
    )


def _scope_ids() -> tuple[str, str]:
    """Return valid execution and checkpoint IDs for runtime scopes."""
    return str(uuid4()), str(uuid4())


@pytest.mark.parametrize(
    ("api_name", "scope_name"),
    [
        ("save", "execution"),
        ("save", "checkpoint"),
        ("load", "execution"),
    ],
)
def test_parse_scope_uuid_returns_uuid(api_name: str, scope_name: str) -> None:
    execution_id, _ = _scope_ids()

    assert _parse_scope_uuid(
        execution_id,
        scope_name=scope_name,
        api_name=api_name,
    ) == UUID(execution_id)


@pytest.mark.parametrize(
    ("api_name", "scope_name"),
    [
        ("save", "execution"),
        ("save", "checkpoint"),
        ("load", "execution"),
    ],
)
def test_parse_scope_uuid_rejects_invalid_uuid(
    api_name: str,
    scope_name: str,
) -> None:
    with pytest.raises(
        KitaruStateError,
        match=rf"kitaru\.{api_name}\(\) found an invalid {scope_name} ID",
    ):
        _parse_scope_uuid(
            "bad-execution-id",
            scope_name=scope_name,
            api_name=api_name,
        )


def test_save_raises_outside_checkpoint() -> None:
    with pytest.raises(KitaruContextError, match=r"inside a @checkpoint"):
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
        pytest.raises(KitaruStateError, match="active execution ID"),
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
        pytest.raises(KitaruStateError, match="active checkpoint ID"),
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
        pytest.raises(KitaruStateError, match="invalid execution ID"),
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
        pytest.raises(KitaruStateError, match="invalid checkpoint ID"),
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
        pytest.raises(KitaruUsageError, match="Unsupported Kitaru artifact type"),
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
    with pytest.raises(KitaruContextError, match=r"inside a @checkpoint"):
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
        pytest.raises(KitaruUsageError, match="expected `exec_id` to be a UUID"),
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

    run_response = _StrictRunResponse(hydrated_run=hydrated_run)
    loaded_artifact = _StrictHydratedArtifactVersion(value={"topic": "kitaru"})

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
    assert run_response.hydration_count == 1
    assert loaded_artifact.load_call_count == 1


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

    run_response = _StrictRunResponse(hydrated_run=hydrated_run)
    loaded_artifact = _StrictHydratedArtifactVersion(value="notes")

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
    assert run_response.hydration_count == 1
    assert loaded_artifact.load_call_count == 1


def test_load_resolves_checkpoint_output_by_artifact_name() -> None:
    execution_id, checkpoint_id = _scope_ids()

    step_output_artifact = _artifact(
        name="research_summary",
        save_type=ArtifactSaveType.STEP_OUTPUT,
    )
    hydrated_run = _hydrated_run(
        step_outputs={
            "research": {
                "output": [step_output_artifact],
            }
        }
    )

    run_response = _StrictRunResponse(hydrated_run=hydrated_run)
    loaded_artifact = _StrictHydratedArtifactVersion(value="named notes")

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
        value = load(str(uuid4()), "research_summary")

    assert value == "named notes"
    client_mock.get_artifact_version.assert_called_once_with(
        step_output_artifact.id,
        hydrate=True,
    )
    assert run_response.hydration_count == 1
    assert loaded_artifact.load_call_count == 1


def test_load_resolves_checkpoint_output_by_raw_step_name() -> None:
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

    run_response = _StrictRunResponse(hydrated_run=hydrated_run)
    loaded_artifact = _StrictHydratedArtifactVersion(value="notes")

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
    assert run_response.hydration_count == 1
    assert loaded_artifact.load_call_count == 1


def test_load_resolves_checkpoint_output_by_normalized_checkpoint_name() -> None:
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

    run_response = _StrictRunResponse(hydrated_run=hydrated_run)
    loaded_artifact = _StrictHydratedArtifactVersion(value="notes")

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
    assert run_response.hydration_count == 1
    assert loaded_artifact.load_call_count == 1


def test_load_deduplicates_repeated_artifact_ids() -> None:
    execution_id, checkpoint_id = _scope_ids()
    repeated_artifact_id = uuid4()

    first_reference = _artifact(
        name="shared",
        save_type=ArtifactSaveType.MANUAL,
        artifact_id=repeated_artifact_id,
    )
    second_reference = _artifact(
        name="shared",
        save_type=ArtifactSaveType.MANUAL,
        artifact_id=repeated_artifact_id,
    )
    hydrated_run = _hydrated_run(
        step_outputs={
            "research": {
                "first": [first_reference],
                "second": [second_reference],
            },
        }
    )

    run_response = _StrictRunResponse(hydrated_run=hydrated_run)
    loaded_artifact = _StrictHydratedArtifactVersion(value={"shared": True})

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
        value = load(str(uuid4()), "shared")

    assert value == {"shared": True}
    client_mock.get_artifact_version.assert_called_once_with(
        repeated_artifact_id,
        hydrate=True,
    )
    assert run_response.hydration_count == 1
    assert loaded_artifact.load_call_count == 1


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

    run_response = _StrictRunResponse(hydrated_run=hydrated_run)

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
        pytest.raises(KitaruRuntimeError, match="No artifact named"),
    ):
        load(str(uuid4()), "research_context")

    assert run_response.hydration_count == 1
    client_mock.get_artifact_version.assert_not_called()


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

    run_response = _StrictRunResponse(hydrated_run=hydrated_run)

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
        pytest.raises(KitaruRuntimeError, match="Multiple artifacts named"),
    ):
        load(str(uuid4()), duplicate_name)

    assert run_response.hydration_count == 1
    client_mock.get_artifact_version.assert_not_called()


def test_map_artifact_ref_and_ref_load_use_strict_artifact_contract() -> None:
    artifact_id = uuid4()
    producer_step_run_id = uuid4()
    artifact = _StrictArtifactVersion(
        id=artifact_id,
        name="research_context",
        save_type=ArtifactSaveType.MANUAL,
        run_metadata={
            "kitaru_artifact_type": "context",
            "reviewed": True,
        },
        producer_step_run_id=producer_step_run_id,
    )
    hydrated_artifact = _StrictHydratedArtifactVersion(
        value={"topic": "kitaru"},
    )
    client = _StrictKitaruClient(hydrated_artifact=hydrated_artifact)

    artifact_ref = _map_artifact_ref(
        artifact=cast("ArtifactVersionResponse", artifact),
        client=cast("KitaruClient", client),
        producing_call="research",
    )

    assert artifact_ref.artifact_id == str(artifact_id)
    assert artifact_ref.name == "research_context"
    assert artifact_ref.kind == "context"
    assert artifact_ref.save_type == ArtifactSaveType.MANUAL.value
    assert artifact_ref.producing_call == "research"
    assert artifact_ref.metadata == {
        "kitaru_artifact_type": "context",
        "reviewed": True,
    }

    assert artifact_ref.load() == {"topic": "kitaru"}
    assert client.artifact_version_calls == [(str(artifact_id), True)]
    assert hydrated_artifact.load_call_count == 1


def test_flow_with_artifacts_example_checkpoint_bodies_are_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from examples.features.basic_flow import flow_with_artifacts

    save_calls: list[dict[str, Any]] = []
    load_calls: list[tuple[str, str]] = []

    def fake_save(
        name: str,
        value: Any,
        *,
        type: str = "output",
        tags: list[str] | None = None,
    ) -> None:
        save_calls.append(
            {
                "name": name,
                "value": value,
                "type": type,
                "tags": tags,
            }
        )

    def fake_load(exec_id: str, name: str) -> Any:
        load_calls.append((exec_id, name))
        if name == "research":
            return "Research notes about kitaru."
        if name == "research_context":
            return {"topic": "kitaru"}
        raise AssertionError(f"Unexpected artifact load: {name}")

    monkeypatch.setattr(flow_with_artifacts.kitaru, "save", fake_save)
    monkeypatch.setattr(flow_with_artifacts.kitaru, "load", fake_load)

    research_body = cast(
        Callable[[str], str],
        cast(Any, flow_with_artifacts.research).__wrapped__,
    )
    follow_up_body = cast(
        Callable[[str], str],
        cast(Any, flow_with_artifacts.follow_up_from_previous).__wrapped__,
    )

    assert research_body("kitaru") == "Research notes about kitaru."
    assert save_calls == [
        {
            "name": "research_context",
            "value": {
                "topic": "kitaru",
                "notes": "Research notes about kitaru.",
            },
            "type": "context",
            "tags": None,
        }
    ]

    assert follow_up_body("previous-exec-id") == (
        "Research notes about kitaru. [topic=kitaru]"
    )
    assert load_calls == [
        ("previous-exec-id", "research"),
        ("previous-exec-id", "research_context"),
    ]
