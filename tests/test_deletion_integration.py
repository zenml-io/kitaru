"""Local-store integration tests for flow and execution deletion."""

from typing import Any
from uuid import UUID

import pytest
from zenml import ExternalArtifact, pipeline, step
from zenml.client import Client as ZenMLClient

from kitaru import KitaruClient


@step(enable_cache=False)
def _consume_shared_deletion_artifact(value: dict[str, Any]) -> bool:
    """Consume one shared artifact so ZenML records its run relationships."""
    return bool(value["shared"])


def _external_artifact_reference(artifact_id: str) -> ExternalArtifact:
    """Build the ID-backed state that ZenML creates after uploading by value."""
    reference = ExternalArtifact(value=None)
    reference.id = UUID(artifact_id)
    return reference


@pipeline(enable_cache=False)
def _deletion_cascade_flow(shared_artifact_id: str) -> None:
    """Flow deleted by the cascade integration test."""
    _consume_shared_deletion_artifact(_external_artifact_reference(shared_artifact_id))


@pipeline(enable_cache=False)
def _deletion_preserved_flow(shared_artifact_id: str) -> None:
    """Flow that keeps using the shared artifact after another flow is deleted."""
    _consume_shared_deletion_artifact(_external_artifact_reference(shared_artifact_id))


@pipeline(enable_cache=False)
def _execution_isolation_flow(shared_artifact_id: str) -> None:
    """Flow run twice to verify that one execution can be deleted in isolation."""
    _consume_shared_deletion_artifact(_external_artifact_reference(shared_artifact_id))


def _upload_shared_artifact(zenml_client: ZenMLClient) -> Any:
    """Upload and return one artifact version for multiple runs to consume."""
    artifact_id = ExternalArtifact(value={"shared": True}).upload_by_value()
    return zenml_client.get_artifact_version(artifact_id)


def _assert_run_consumes_artifact(run: Any, artifact_id: Any) -> None:
    """Assert that the run's only step references the uploaded artifact as input."""
    step_runs = run.steps
    assert len(step_runs) == 1
    step_run = next(iter(step_runs.values()))
    assert list(step_run.inputs) == ["value"]
    assert [artifact.id for artifact in step_run.inputs["value"]] == [artifact_id]


@pytest.mark.usefixtures("primed_zenml")
def test_flow_delete_cascades_and_preserves_shared_artifact() -> None:
    """Whole-flow deletion should use the local store's real cascade relationships."""
    zenml_client = ZenMLClient()
    shared_artifact = _upload_shared_artifact(zenml_client)

    deleted_run = _deletion_cascade_flow(str(shared_artifact.id))
    preserved_run = _deletion_preserved_flow(str(shared_artifact.id))
    assert deleted_run is not None
    assert preserved_run is not None
    assert deleted_run.pipeline is not None
    assert deleted_run.snapshot is not None
    _assert_run_consumes_artifact(deleted_run, shared_artifact.id)
    _assert_run_consumes_artifact(preserved_run, shared_artifact.id)

    kitaru_client = KitaruClient()
    flow_name = deleted_run.pipeline.name
    deployment = kitaru_client.deployments.create(
        flow=flow_name,
        source_snapshot=deleted_run.snapshot,
    )
    pipeline_id = deleted_run.pipeline.id
    deployment_snapshot_id = deployment.deployment_id

    assert (
        zenml_client.get_snapshot(deployment_snapshot_id).id == deleted_run.snapshot.id
    )
    assert zenml_client.get_pipeline_run(deleted_run.id).id == deleted_run.id
    assert zenml_client.get_pipeline_run(preserved_run.id).id == preserved_run.id

    kitaru_client.flows.delete(flow_name)

    with pytest.raises(KeyError):
        zenml_client.get_pipeline(pipeline_id)
    with pytest.raises(KeyError):
        zenml_client.get_snapshot(deployment_snapshot_id)
    with pytest.raises(KeyError):
        zenml_client.get_pipeline_run(deleted_run.id)

    assert zenml_client.get_pipeline_run(preserved_run.id).id == preserved_run.id
    preserved_artifact = zenml_client.get_artifact_version(shared_artifact.id)
    assert preserved_artifact.load() == {"shared": True}


@pytest.mark.usefixtures("primed_zenml")
def test_execution_delete_leaves_other_execution_and_shared_artifact_intact() -> None:
    """Deleting one execution should not affect another run of the same flow."""
    zenml_client = ZenMLClient()
    shared_artifact = _upload_shared_artifact(zenml_client)
    first_run = _execution_isolation_flow(str(shared_artifact.id))
    second_run = _execution_isolation_flow(str(shared_artifact.id))
    assert first_run is not None
    assert second_run is not None
    assert first_run.pipeline is not None
    assert first_run.snapshot is not None
    _assert_run_consumes_artifact(first_run, shared_artifact.id)
    _assert_run_consumes_artifact(second_run, shared_artifact.id)

    kitaru_client = KitaruClient()
    flow_name = first_run.pipeline.name
    pipeline_id = first_run.pipeline.id
    deployment = kitaru_client.deployments.create(
        flow=flow_name,
        source_snapshot=first_run.snapshot,
    )

    kitaru_client.executions.delete(str(first_run.id))

    with pytest.raises(KeyError):
        zenml_client.get_pipeline_run(first_run.id)

    assert zenml_client.get_pipeline_run(second_run.id).id == second_run.id
    assert zenml_client.get_pipeline(pipeline_id).name == flow_name
    assert (
        zenml_client.get_snapshot(deployment.deployment_id).id == first_run.snapshot.id
    )
    preserved_artifact = zenml_client.get_artifact_version(shared_artifact.id)
    assert preserved_artifact.load() == {"shared": True}
