"""Experiment spec compatibility tests for scoring foundation."""

from __future__ import annotations

from typing import Any, cast

import pytest
from pydantic import ValidationError

from kitaru._experiments import (
    ExperimentRecord,
    InlineTargetMembership,
    ScoreExperimentSpec,
    ScoreRequestInputs,
)
from kitaru._experiments._models import experiment_request_hash
from kitaru.scoring import (
    EvidenceManifest,
    EvidenceManifestEntry,
    ExecutionEvidence,
    InlineEvidenceManifestReference,
    Score,
    ScorerSnapshot,
)
from tests.experiments._helpers import _plan


def _snapshot(_: object) -> Score:
    return Score(value=1.0)


def _evidence(run_id: str) -> ExecutionEvidence:
    payload = {
        "schema_version": 1,
        "execution_id": run_id,
        "project_id": "project-id",
        "source": "zenml_pipeline_run",
        "status": "completed",
        "checkpoint_ids": [],
        "inputs": {},
        "outputs": {},
        "messages": [],
        "tool_calls": [],
        "artifact_references": [],
        "usage": {},
        "cost": {},
    }
    from kitaru.scoring._contracts import sha256_json

    return ExecutionEvidence(**payload, content_hash=sha256_json(payload))


def _manifest() -> InlineEvidenceManifestReference:
    entry = EvidenceManifestEntry(
        target_execution_id="run-1",
        evidence=_evidence("run-1"),
    )
    manifest = EvidenceManifest.create(
        project_id="project-id",
        entries=[entry],
        created_at="2026-07-18T10:00:00Z",
        manifest_id="manifest-1",
    )
    return InlineEvidenceManifestReference(
        manifest=manifest,
        count=1,
        sha256=manifest.content_hash,
    )


def _score_spec(**overrides: Any) -> ScoreExperimentSpec:
    snapshot = ScorerSnapshot.from_callable(_snapshot, capability="pure")
    payload: dict[str, Any] = {
        "experiment_id": "exp-score",
        "kind": "score",
        "name": "score attempt",
        "display_name": "Score attempt",
        "suite_key": "suite-score",
        "idempotency_key": "score-request",
        "created_at": "2026-07-18T10:00:00Z",
        "candidate_project_id": "project-id",
        "target_membership": InlineTargetMembership(execution_ids=["run-1"], count=1),
        "scorers": [snapshot],
        "evidence_manifest": _manifest(),
        "request_inputs": ScoreRequestInputs(comparative=False),
    }
    payload.update(overrides)
    provisional = cast(Any, ScoreExperimentSpec).model_construct(
        schema_version=1,
        request_hash=f"sha256:{'0' * 64}",
        **payload,
    )
    payload["request_hash"] = experiment_request_hash(provisional)
    return ScoreExperimentSpec.model_validate(payload)


def test_score_experiment_spec_round_trips_under_existing_record_lifecycle() -> None:
    spec = _score_spec()
    record = ExperimentRecord.pending(spec)

    assert spec.kind == "score"
    assert record.counts.target_count == 1
    assert record.counts.intended == 1
    assert ExperimentRecord.model_validate(record.model_dump(mode="json")) == record


def test_score_only_specs_reject_replay_fields_and_count_mismatches() -> None:
    with pytest.raises(ValidationError):
        _score_spec(executable={"kind": "entrypoint", "entrypoint": "x:y"})

    bad_manifest = _manifest().model_copy(update={"count": 2})
    with pytest.raises(ValidationError, match="reference count"):
        _score_spec(evidence_manifest=bad_manifest)


def test_replay_specs_deserialize_unchanged_and_scorer_free_hash_is_stable() -> None:
    spec = _plan().spec
    old_json = spec.model_dump(mode="json", exclude={"scorers", "evidence_manifest"})
    reloaded = type(spec).model_validate(old_json)

    assert reloaded == spec
    assert reloaded.scorers == []
    assert reloaded.evidence_manifest is None
    assert experiment_request_hash(reloaded) == spec.request_hash
