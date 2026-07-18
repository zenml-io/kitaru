"""Execution evidence freezing tests."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

from kitaru.errors import KitaruMetadataConflictError, KitaruStateError
from kitaru.scoring import (
    ArtifactEvidenceManifestReference,
    ExecutionEvidence,
    InlineEvidenceManifestReference,
    freeze_execution_evidence_manifest,
    load_evidence_manifest,
    persist_evidence_manifest,
)


def _artifact(artifact_id: str, name: str = "output") -> Any:
    return SimpleNamespace(
        id=artifact_id,
        name=name,
        content_hash=f"sha256:{'1' * 64}",
        materializer="json",
        data_type="dict",
    )


def _step(name: str, *artifacts: Any) -> Any:
    return SimpleNamespace(
        id=uuid4(),
        name=name,
        outputs={"output": list(artifacts)},
    )


def _run(
    run_id: str,
    *,
    project_id: str = "project-id",
    original_run: Any | None = None,
) -> Any:
    return SimpleNamespace(
        id=run_id,
        project_id=project_id,
        status=SimpleNamespace(value="completed"),
        original_run=original_run,
        steps={"answer": _step("answer", _artifact(f"artifact-{run_id}"))},
        config=SimpleNamespace(parameters={"topic": "support"}),
        run_metadata={
            "usage_tokens": SimpleNamespace(value=12),
            "cost_usd": SimpleNamespace(value=0.01),
        },
    )


class _Client:
    def __init__(self, runs: dict[str, Any] | None = None) -> None:
        self.runs = runs or {}
        self.active_project = SimpleNamespace(id="project-id")
        self.artifacts: list[Any] = []

    def get_pipeline_run(self, *, name_id_or_prefix: str, **_: Any) -> Any:
        return self.runs[name_id_or_prefix]

    def get_artifact_version(self, *, name_id_or_prefix: str, **_: Any) -> Any:
        return next(
            artifact for artifact in self.artifacts if artifact.id == name_id_or_prefix
        )

    def list_artifact_versions(self, *, name: str, **_: Any) -> Any:
        expected = name.removeprefix("equals:")
        return SimpleNamespace(
            items=[
                artifact
                for artifact in self.artifacts
                if getattr(artifact, "name", None) == expected
            ]
        )


def test_execution_evidence_is_hash_verified_and_adapter_neutral() -> None:
    evidence = ExecutionEvidence.from_run(_run("run-1"))

    assert evidence.execution_id == "run-1"
    assert evidence.project_id == "project-id"
    assert evidence.status == "completed"
    assert evidence.inputs == {"topic": "support"}
    assert evidence.usage == {"usage_tokens": 12}
    assert evidence.cost == {"cost_usd": 0.01}
    assert evidence.artifact_references[0].artifact_version_id == "artifact-run-1"

    payload = evidence.model_dump(mode="json")
    payload["status"] = "tampered"
    with pytest.raises(ValueError, match="content_hash"):
        ExecutionEvidence.model_validate(payload)


def test_comparative_manifest_uses_immediate_original_and_marks_missing() -> None:
    original = _run("original")
    candidate = _run("candidate", original_run=SimpleNamespace(id="original"))
    missing = _run("missing")
    client = _Client({"original": original})

    manifest = freeze_execution_evidence_manifest(
        [candidate, missing],
        project_id="project-id",
        comparative=True,
        client=client,
        created_at="2026-07-18T10:00:00Z",
    )

    assert [entry.target_execution_id for entry in manifest.entries] == [
        "candidate",
        "missing",
    ]
    assert manifest.entries[0].comparison is not None
    assert manifest.entries[0].comparison.original.execution_id == "original"
    assert manifest.entries[1].availability == "missing_original"
    assert manifest.entries[1].comparison is None


def test_evidence_manifest_persistence_inlines_small_and_verifies_large() -> None:
    runs = [_run(f"run-{index}") for index in range(2)]
    manifest = freeze_execution_evidence_manifest(
        runs,
        project_id="project-id",
        created_at="2026-07-18T10:00:00Z",
    )
    reference = persist_evidence_manifest(manifest, project_id="project-id")

    assert isinstance(reference, InlineEvidenceManifestReference)
    assert load_evidence_manifest(reference, project_id="project-id") == manifest

    with pytest.raises(KitaruStateError, match="Agent Project"):
        persist_evidence_manifest(
            manifest,
            project_id="other-project",
            client=_Client(),
        )


def test_artifact_backed_manifest_round_trips_and_hash_mismatch_fails() -> None:
    runs = [_run(f"run-{index}") for index in range(101)]
    manifest = freeze_execution_evidence_manifest(
        runs,
        project_id="project-id",
        created_at="2026-07-18T10:00:00Z",
    )
    client = _Client()

    def save(**kwargs: Any) -> Any:
        artifact = SimpleNamespace(
            id="manifest-artifact-id",
            name=kwargs["name"],
            load=lambda: kwargs["data"],
        )
        client.artifacts.append(artifact)
        return artifact

    reference = persist_evidence_manifest(
        manifest,
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    )
    retry_reference = persist_evidence_manifest(
        manifest,
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    )

    assert isinstance(reference, ArtifactEvidenceManifestReference)
    assert retry_reference == reference
    assert len(client.artifacts) == 1
    assert (
        load_evidence_manifest(reference, project_id="project-id", client=client)
        == manifest
    )

    tampered = reference.model_copy(update={"sha256": f"sha256:{'2' * 64}"})
    with pytest.raises(KitaruMetadataConflictError, match="hash"):
        load_evidence_manifest(tampered, project_id="project-id", client=client)


def test_small_manifest_uses_artifact_when_serialized_evidence_is_large() -> None:
    run = _run("large-run")
    run.config.parameters = {"prompt": "x" * (70 * 1024)}
    manifest = freeze_execution_evidence_manifest(
        [run],
        project_id="project-id",
        created_at="2026-07-18T10:00:00Z",
    )
    client = _Client()

    def save(**kwargs: Any) -> Any:
        artifact = SimpleNamespace(
            id="large-manifest-artifact-id",
            name=kwargs["name"],
            load=lambda: kwargs["data"],
        )
        client.artifacts.append(artifact)
        return artifact

    reference = persist_evidence_manifest(
        manifest,
        project_id="project-id",
        client=client,
        save_artifact_fn=save,
    )

    assert isinstance(reference, ArtifactEvidenceManifestReference)
    assert reference.count == 1
