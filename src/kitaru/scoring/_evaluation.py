"""Shared score evaluation service for stored executions."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, cast
from uuid import NAMESPACE_URL, uuid5

from pydantic import JsonValue
from zenml.client import Client

from kitaru._experiments._catalog import (
    attach_experiment_score_aggregate,
    finalize_experiment_outcomes,
    get_experiment_by_idempotency_key,
    reserve_experiment,
    transition_experiment_to_running,
)
from kitaru._experiments._membership import (
    load_target_membership,
    persist_target_membership,
)
from kitaru._experiments._models import (
    _TERMINAL_STATUSES,
    ExperimentCounts,
    ExperimentIssue,
    ExperimentRecord,
    ScoreExperimentSpec,
    ScoreRequestInputs,
    experiment_request_hash,
)
from kitaru.errors import KitaruMetadataConflictError, KitaruUsageError
from kitaru.scoring._aggregates import (
    ScoreAggregateReference,
    ScoreAttemptAggregate,
    load_score_aggregate,
    persist_score_aggregate,
)
from kitaru.scoring._contracts import (
    GroundedPolicySnapshot,
    Score,
    ScoreObservation,
    ScoreObservationOutcome,
    ScoreObservationStatus,
    ScorerCapability,
    ScorerSnapshot,
    canonical_json,
    require_string,
    scorer_snapshot,
)
from kitaru.scoring._evidence import (
    EvidenceManifest,
    EvidenceManifestEntry,
    freeze_execution_evidence_manifest,
    load_evidence_manifest,
    persist_evidence_manifest,
)
from kitaru.scoring._grounded import (
    GroundedCapability,
    GroundedCapabilityBlocked,
    GroundedWorld,
)
from kitaru.scoring._repository import ObservationQuery, ScoreObservationRepository

ScorerInput = Callable[..., Any]


@dataclass(frozen=True)
class ScoreAttemptResult:
    """Durable result returned by score evaluation entry points."""

    record: Any
    observations: list[ScoreObservation]
    aggregate: ScoreAttemptAggregate | None = None
    aggregate_reference: ScoreAggregateReference | None = None

    @property
    def experiment_id(self) -> str:
        """Return the durable attempt ID."""
        return self.record.spec.experiment_id

    def to_json(self) -> dict[str, Any]:
        """Serialize Kitaru score models without exposing raw ZenML responses."""
        return {
            "record": self.record.model_dump(mode="json"),
            "observations": [
                item.model_dump(mode="json") for item in self.observations
            ],
            "aggregate": None
            if self.aggregate is None
            else self.aggregate.model_dump(mode="json"),
            "aggregate_reference": (
                None
                if self.aggregate_reference is None
                else self.aggregate_reference.model_dump(mode="json")
            ),
        }


class ScoreEvaluationService:
    """Evaluate stored executions with caller-provided scorer declarations."""

    def __init__(
        self,
        *,
        project_id: str,
        client: Any | None = None,
        save_artifact_fn: Callable[..., Any] | None = None,
        run_loader: Callable[[str], Any] | None = None,
    ) -> None:
        self.project_id = require_string(project_id, field_name="Project ID")
        self.client = client or Client()
        self._save_artifact_fn = save_artifact_fn
        self._run_loader = run_loader

    def evaluate(
        self,
        executions: Sequence[Any],
        scorers: Sequence[ScorerInput],
        *,
        name: str | None = None,
        suite_key: str | None = None,
        idempotency_key: str | None = None,
        comparative: bool | None = None,
        metadata: Mapping[str, JsonValue] | None = None,
        grounded_policy: GroundedPolicySnapshot | None = None,
        grounded_capabilities: Mapping[str, GroundedCapability] | None = None,
    ) -> ScoreAttemptResult:
        """Reserve and run one score-only attempt against stored executions."""
        execution_items = list(executions)
        if not execution_items:
            raise KitaruUsageError("At least one execution is required for evaluation.")
        declarations = [_scorer_callable(item) for item in scorers]
        if not declarations:
            raise KitaruUsageError("At least one scorer is required for evaluation.")
        snapshots = [scorer_snapshot(item) for item in declarations]
        target_ids = [self._execution_id(item) for item in execution_items]
        normalized_name = name.strip() if name is not None else None
        if normalized_name == "":
            raise KitaruUsageError("Score attempt name cannot be empty.")
        is_comparative = (
            any(snapshot.comparative for snapshot in snapshots)
            if comparative is None
            else comparative
        )
        key = idempotency_key or _request_key(
            target_ids,
            snapshots,
            name=normalized_name,
            suite_key=suite_key,
            comparative=is_comparative,
            metadata=metadata,
            grounded_policy=grounded_policy,
        )
        experiment_id = _score_experiment_id(self.project_id, key)
        resolved_suite = suite_key or f"suite-{experiment_id.removeprefix('exp-')}"
        existing = get_experiment_by_idempotency_key(
            self.project_id,
            key,
            client_factory=lambda: self.client,
        )
        if existing is not None:
            self._validate_existing_request(
                existing,
                target_ids=target_ids,
                scorers=snapshots,
                name=normalized_name,
                suite_key=resolved_suite,
                comparative=is_comparative,
                metadata=metadata,
                grounded_policy=grounded_policy,
            )
            return self._resume_score_attempt(
                existing,
                scorers=declarations,
                grounded_policy=grounded_policy,
                grounded_capabilities=grounded_capabilities or {},
            )

        runs = [self._resolve_run(item) for item in execution_items]
        created_at = datetime.now(UTC).isoformat()
        manifest = freeze_execution_evidence_manifest(
            runs,
            project_id=self.project_id,
            comparative=is_comparative,
            client=self.client,
            run_loader=self._load_run_by_id,
            created_at="1970-01-01T00:00:00+00:00",
        )
        manifest_reference = persist_evidence_manifest(
            manifest,
            project_id=self.project_id,
            client=self.client,
            save_artifact_fn=self._save_artifact_fn or _default_save_artifact,
        )
        target_ids = [entry.target_execution_id for entry in manifest.entries]
        membership = persist_target_membership(
            experiment_id=experiment_id,
            project_id=self.project_id,
            execution_ids=target_ids,
            client=self.client,
            save_artifact_fn=self._save_artifact_fn or _default_save_artifact,
        )
        spec = _score_spec(
            experiment_id=experiment_id,
            name=normalized_name,
            suite_key=suite_key,
            idempotency_key=key,
            created_at=created_at,
            project_id=self.project_id,
            membership=membership,
            scorers=snapshots,
            manifest_reference=manifest_reference,
            comparative=is_comparative,
            metadata=metadata,
            grounded_policy=grounded_policy,
        )
        reservation = reserve_experiment(
            self.project_id,
            spec,
            client_factory=lambda: self.client,
        )
        if not reservation.created:
            return self._resume_score_attempt(
                reservation.record,
                scorers=declarations,
                grounded_policy=grounded_policy,
                grounded_capabilities=grounded_capabilities or {},
            )

        transition_experiment_to_running(
            self.project_id,
            experiment_id,
            client_factory=lambda: self.client,
        )
        observations = self._evaluate_matrix(
            manifest=manifest,
            experiment_id=experiment_id,
            scorers=declarations,
            grounded_policy=grounded_policy,
            grounded_capabilities=grounded_capabilities or {},
        )
        return self._finalize_score_attempt(
            record=reservation.record,
            observations=observations,
        )

    def _validate_existing_request(
        self,
        record: ExperimentRecord,
        *,
        target_ids: Sequence[str],
        scorers: Sequence[ScorerSnapshot],
        name: str | None,
        suite_key: str,
        comparative: bool,
        metadata: Mapping[str, JsonValue] | None,
        grounded_policy: GroundedPolicySnapshot | None,
    ) -> None:
        spec = record.spec
        if not isinstance(spec, ScoreExperimentSpec):
            raise KitaruMetadataConflictError(
                "The idempotency key already identifies a non-scoring request."
            )
        stored_targets = load_target_membership(
            spec.target_membership,
            project_id=self.project_id,
            client=self.client,
        )
        requested_inputs = ScoreRequestInputs(
            comparative=comparative,
            metadata=dict(metadata or {}),
            grounded_policy=grounded_policy,
        )
        if (
            stored_targets != list(target_ids)
            or spec.scorers != list(scorers)
            or spec.name != name
            or spec.suite_key != suite_key
            or spec.request_inputs != requested_inputs
        ):
            raise KitaruMetadataConflictError(
                "The idempotency key already identifies a different score request."
            )

    def _resume_score_attempt(
        self,
        record: ExperimentRecord,
        *,
        scorers: Sequence[ScorerInput],
        grounded_policy: GroundedPolicySnapshot | None,
        grounded_capabilities: Mapping[str, GroundedCapability],
    ) -> ScoreAttemptResult:
        spec = record.spec
        if not isinstance(spec, ScoreExperimentSpec):
            raise KitaruMetadataConflictError(
                "The existing experiment is not a score attempt."
            )
        observations = self._load_attempt_observations(record)
        if record.status in _TERMINAL_STATUSES:
            if record.score_aggregate is None:
                if record.status == "cancelled":
                    return ScoreAttemptResult(record=record, observations=observations)
                raise KitaruMetadataConflictError(
                    "The terminal score attempt has no aggregate reference."
                )
            aggregate = load_score_aggregate(
                record.score_aggregate,
                project_id=self.project_id,
                client=self.client,
            )
            return ScoreAttemptResult(
                record=record,
                observations=observations,
                aggregate=aggregate,
                aggregate_reference=record.score_aggregate,
            )

        if record.status == "pending":
            record = transition_experiment_to_running(
                self.project_id,
                spec.experiment_id,
                client_factory=lambda: self.client,
            )
        manifest = load_evidence_manifest(
            spec.evidence_manifest,
            project_id=self.project_id,
            client=self.client,
        )
        observations = self._evaluate_matrix(
            manifest=manifest,
            experiment_id=spec.experiment_id,
            scorers=scorers,
            grounded_policy=grounded_policy,
            grounded_capabilities=grounded_capabilities,
            existing=observations,
        )
        return self._finalize_score_attempt(record=record, observations=observations)

    def _load_attempt_observations(
        self, record: ExperimentRecord
    ) -> list[ScoreObservation]:
        repo = ScoreObservationRepository(
            project_id=self.project_id,
            client=self.client,
            save_artifact_fn=self._save_artifact_fn or _default_save_artifact,
        )
        expected = record.counts.intended
        page_size = min(100, expected + 1)
        observations: list[ScoreObservation] = []
        page = 1
        while True:
            items = repo.list(
                ObservationQuery(experiment_id=record.spec.experiment_id),
                page=page,
                size=page_size,
            )
            observations.extend(items)
            if len(observations) > expected:
                raise KitaruMetadataConflictError(
                    "The score attempt contains more observations than planned."
                )
            if len(items) < page_size:
                return observations
            page += 1

    def _finalize_score_attempt(
        self,
        *,
        record: ExperimentRecord,
        observations: Sequence[ScoreObservation],
    ) -> ScoreAttemptResult:
        spec = cast(ScoreExperimentSpec, record.spec)
        selected = list(observations)
        aggregate = ScoreAttemptAggregate.create(
            experiment_id=spec.experiment_id,
            project_id=self.project_id,
            observations=selected,
            planned=record.counts.intended,
        )
        aggregate_reference = persist_score_aggregate(
            aggregate,
            project_id=self.project_id,
            client=self.client,
            save_artifact_fn=self._save_artifact_fn or _default_save_artifact,
        )
        counts = _counts_for_observations(
            target_count=spec.target_membership.count,
            scorer_count=len(spec.scorers),
            observations=selected,
        )
        final_record = finalize_experiment_outcomes(
            self.project_id,
            spec.experiment_id,
            status=_terminal_status(counts),
            counts=counts,
            errors=_issues_for_status(selected, ScoreObservationStatus.ERROR),
            skips=[
                *_issues_for_status(selected, ScoreObservationStatus.ABSTAINED),
                *_issues_for_status(selected, ScoreObservationStatus.BLOCKED),
            ],
            aggregate_reference=aggregate_reference,
            client_factory=lambda: self.client,
        )
        return ScoreAttemptResult(
            record=final_record,
            observations=selected,
            aggregate=aggregate,
            aggregate_reference=aggregate_reference,
        )

    def evaluate_existing_attempt(
        self,
        *,
        experiment_id: str,
        executions: Sequence[Any],
        scorers: Sequence[ScorerInput],
        grounded_policy: GroundedPolicySnapshot | None = None,
        grounded_capabilities: Mapping[str, GroundedCapability] | None = None,
    ) -> ScoreAttemptResult:
        """Score verified replay children and attach an immutable aggregate."""
        runs = [self._resolve_run(item) for item in executions]
        declarations = [_scorer_callable(item) for item in scorers]
        if not runs:
            raise KitaruUsageError(
                "Replay scoring requires at least one verified replay child."
            )
        if not declarations:
            raise KitaruUsageError("Replay scoring requires at least one scorer.")
        snapshots = [scorer_snapshot(item) for item in declarations]
        manifest = freeze_execution_evidence_manifest(
            runs,
            project_id=self.project_id,
            comparative=any(snapshot.comparative for snapshot in snapshots),
            client=self.client,
            run_loader=self._load_run_by_id,
        )
        observations = self._evaluate_matrix(
            manifest=manifest,
            experiment_id=experiment_id,
            scorers=declarations,
            grounded_policy=grounded_policy,
            grounded_capabilities=grounded_capabilities or {},
        )
        aggregate = ScoreAttemptAggregate.create(
            experiment_id=experiment_id,
            project_id=self.project_id,
            observations=observations,
            planned=len(manifest.entries) * len(snapshots),
        )
        aggregate_reference = persist_score_aggregate(
            aggregate,
            project_id=self.project_id,
            client=self.client,
            save_artifact_fn=self._save_artifact_fn or _default_save_artifact,
        )
        record = attach_experiment_score_aggregate(
            self.project_id,
            experiment_id,
            aggregate_reference=aggregate_reference,
            client_factory=lambda: self.client,
        )
        return ScoreAttemptResult(
            record=record,
            observations=observations,
            aggregate=aggregate,
            aggregate_reference=aggregate_reference,
        )

    def _evaluate_matrix(
        self,
        *,
        manifest: EvidenceManifest,
        experiment_id: str,
        scorers: Sequence[ScorerInput],
        grounded_policy: GroundedPolicySnapshot | None,
        grounded_capabilities: Mapping[str, GroundedCapability],
        existing: Sequence[ScoreObservation] = (),
    ) -> list[ScoreObservation]:
        repo = ScoreObservationRepository(
            project_id=self.project_id,
            client=self.client,
            save_artifact_fn=self._save_artifact_fn or _default_save_artifact,
        )
        observations_by_key: dict[tuple[str, str, str, str], ScoreObservation] = {}
        for observation in existing:
            key = _observation_key(observation.execution_id, observation.scorer)
            if (
                key in observations_by_key
                or observation.experiment_id != experiment_id
                or observation.evidence_manifest_sha256 != manifest.content_hash
            ):
                raise KitaruMetadataConflictError(
                    "Existing score observations conflict with the frozen matrix."
                )
            observations_by_key[key] = observation
        observations: list[ScoreObservation] = []
        for entry in manifest.entries:
            for scorer in scorers:
                snapshot = scorer_snapshot(scorer)
                key = _observation_key(entry.target_execution_id, snapshot)
                existing_observation = observations_by_key.pop(key, None)
                if existing_observation is not None:
                    observations.append(existing_observation)
                    continue
                observation = self._invoke_one(
                    entry=entry,
                    scorer=scorer,
                    experiment_id=experiment_id,
                    evidence_manifest_sha256=manifest.content_hash,
                    grounded_policy=grounded_policy,
                    grounded_capabilities=grounded_capabilities,
                )
                observations.append(
                    repo.append_once(
                        observation,
                        idempotency_key=canonical_json(
                            {
                                "experiment_id": experiment_id,
                                "execution_id": key[0],
                                "scorer_name": key[1],
                                "scorer_revision": key[2],
                                "scorer_configuration_hash": key[3],
                            }
                        ),
                    )
                )
        if observations_by_key:
            raise KitaruMetadataConflictError(
                "Existing score observations fall outside the frozen matrix."
            )
        return observations

    def _invoke_one(
        self,
        *,
        entry: EvidenceManifestEntry,
        scorer: ScorerInput,
        experiment_id: str,
        evidence_manifest_sha256: str,
        grounded_policy: GroundedPolicySnapshot | None,
        grounded_capabilities: Mapping[str, GroundedCapability],
    ) -> ScoreObservation:
        snapshot = scorer_snapshot(scorer)
        started = datetime.now(UTC).isoformat()
        provenance = None
        try:
            outcome, provenance = _invoke_scorer(
                scorer=scorer,
                snapshot=snapshot,
                entry=entry,
                grounded_policy=grounded_policy,
                grounded_capabilities=grounded_capabilities,
            )
        except GroundedCapabilityBlocked as exc:
            outcome = ScoreObservationOutcome(
                status=ScoreObservationStatus.BLOCKED, reason=str(exc)
            )
        except TimeoutError as exc:
            outcome = ScoreObservationOutcome(
                status=ScoreObservationStatus.BLOCKED, reason=str(exc)
            )
        except Exception as exc:
            outcome = ScoreObservationOutcome(
                status=ScoreObservationStatus.ERROR,
                reason=str(exc) or type(exc).__name__,
            )
        return ScoreObservation(
            project_id=self.project_id,
            execution_id=entry.target_execution_id,
            experiment_id=experiment_id,
            scorer=snapshot,
            outcome=outcome,
            completed_at=started,
            evidence_manifest_sha256=evidence_manifest_sha256,
            comparative_original_execution_id=(
                entry.original_evidence.execution_id
                if entry.original_evidence is not None
                else None
            ),
            grounded_provenance=provenance,
            explanation=outcome.score.explanation
            if outcome.score is not None
            else outcome.reason,
        )

    def _resolve_run(self, value: Any) -> Any:
        if isinstance(value, str) or hasattr(value, "exec_id"):
            return self._load_run_by_id(self._execution_id(value))
        return value

    def _load_run_by_id(self, execution_id: str) -> Any:
        if self._run_loader is not None:
            return self._run_loader(execution_id)
        try:
            return self.client.get_pipeline_run(
                name_id_or_prefix=execution_id,
                allow_name_prefix_match=False,
                project=self.project_id,
                hydrate=True,
            )
        except TypeError:
            return self.client.get_pipeline_run(
                name_id_or_prefix=execution_id,
                allow_name_prefix_match=False,
                hydrate=True,
            )

    def _execution_id(self, value: Any) -> str:
        if isinstance(value, str):
            return require_string(value, field_name="Execution ID")
        public_id = getattr(value, "exec_id", None)
        if public_id is not None:
            return require_string(str(public_id), field_name="Execution ID")
        native_id = getattr(value, "id", None)
        if native_id is not None:
            return require_string(str(native_id), field_name="Execution ID")
        raise KitaruUsageError("Executions must be execution IDs or execution objects.")


def _invoke_scorer(
    *,
    scorer: ScorerInput,
    snapshot: ScorerSnapshot,
    entry: EvidenceManifestEntry,
    grounded_policy: GroundedPolicySnapshot | None,
    grounded_capabilities: Mapping[str, GroundedCapability],
) -> tuple[ScoreObservationOutcome, Any | None]:
    if snapshot.comparative and entry.original_evidence is None:
        return (
            ScoreObservationOutcome(
                status=ScoreObservationStatus.ABSTAINED,
                reason=entry.reason or "Execution has no immediate original.",
            ),
            None,
        )

    args: list[Any] = [entry.evidence]
    if snapshot.comparative:
        args.append(entry.original_evidence)

    world: GroundedWorld | None = None
    if snapshot.capability == ScorerCapability.GROUNDED:
        if grounded_policy is None:
            return (
                ScoreObservationOutcome(
                    status=ScoreObservationStatus.BLOCKED,
                    reason="Grounded scoring requires an explicit policy.",
                ),
                None,
            )
        world = GroundedWorld(
            policy=grounded_policy,
            capabilities=grounded_capabilities,
        )
        args.append(world)

    outcome = _normalize_outcome(scorer(*args))
    return outcome, None if world is None else world.provenance


def _scorer_callable(value: ScorerInput) -> ScorerInput:
    if not callable(value):
        raise KitaruUsageError("Evaluation requires callable @scorer declarations.")
    scorer_snapshot(value)
    return value


def _normalize_outcome(value: Any) -> ScoreObservationOutcome:
    if isinstance(value, ScoreObservationOutcome):
        return value
    if value is None:
        return ScoreObservationOutcome(
            status=ScoreObservationStatus.ABSTAINED, reason="Scorer abstained."
        )
    if isinstance(value, Score):
        return ScoreObservationOutcome(
            status=ScoreObservationStatus.SCORED, score=value
        )
    return ScoreObservationOutcome(
        status=ScoreObservationStatus.SCORED, score=Score(value=value)
    )


def _counts_for_observations(
    *,
    target_count: int,
    scorer_count: int,
    observations: Sequence[ScoreObservation],
) -> ExperimentCounts:
    scored = sum(item.status is ScoreObservationStatus.SCORED for item in observations)
    abstained_or_blocked = sum(
        item.status
        in {ScoreObservationStatus.ABSTAINED, ScoreObservationStatus.BLOCKED}
        for item in observations
    )
    errors = sum(item.status is ScoreObservationStatus.ERROR for item in observations)
    return ExperimentCounts(
        target_count=target_count,
        intended=target_count * scorer_count,
        submitted=scored,
        verified=scored,
        skipped=abstained_or_blocked,
        failed=errors,
        unverified=0,
    )


def _terminal_status(
    counts: ExperimentCounts,
) -> Literal["completed", "partial", "failed"]:
    if counts.verified == counts.intended:
        return "completed"
    if counts.verified > 0:
        return "partial"
    return "failed"


def _issues_for_status(
    observations: Sequence[ScoreObservation],
    status: ScoreObservationStatus,
) -> list[ExperimentIssue]:
    return [
        ExperimentIssue(
            target_execution_id=item.execution_id,
            reason=f"{item.scorer.name}: {item.outcome.reason or status}",
        )
        for item in observations
        if item.status is status
    ]


def _score_spec(
    *,
    experiment_id: str,
    name: str | None,
    suite_key: str | None,
    idempotency_key: str,
    created_at: str,
    project_id: str,
    membership: Any,
    scorers: Sequence[ScorerSnapshot],
    manifest_reference: Any,
    comparative: bool,
    metadata: Mapping[str, JsonValue] | None,
    grounded_policy: GroundedPolicySnapshot | None,
) -> ScoreExperimentSpec:
    resolved_suite = suite_key or f"suite-{experiment_id.removeprefix('exp-')}"
    payload = {
        "experiment_id": experiment_id,
        "kind": "score",
        "name": name,
        "display_name": name or f"Score {experiment_id[-8:]}",
        "suite_key": resolved_suite,
        "idempotency_key": idempotency_key,
        "created_at": created_at,
        "candidate_project_id": project_id,
        "target_membership": membership,
        "scorers": list(scorers),
        "evidence_manifest": manifest_reference,
        "request_inputs": ScoreRequestInputs(
            comparative=comparative,
            metadata=dict(metadata or {}),
            grounded_policy=grounded_policy,
        ),
    }
    provisional = cast(Any, ScoreExperimentSpec).model_construct(
        schema_version=1,
        request_hash="sha256:" + "0" * 64,
        **payload,
    )
    payload["request_hash"] = experiment_request_hash(provisional)
    return ScoreExperimentSpec.model_validate(payload)


def _score_experiment_id(project_id: str, idempotency_key: str) -> str:
    value = uuid5(
        NAMESPACE_URL, f"kitaru-score-experiment:{project_id}:{idempotency_key}"
    )
    return f"exp-{value.hex}"


def _request_key(
    target_ids: Sequence[str],
    snapshots: Sequence[ScorerSnapshot],
    *,
    name: str | None,
    suite_key: str | None,
    comparative: bool,
    metadata: Mapping[str, JsonValue] | None,
    grounded_policy: GroundedPolicySnapshot | None,
) -> str:
    return _score_experiment_id(
        "request",
        canonical_json(
            {
                "target_ids": list(target_ids),
                "scorers": [
                    {
                        "name": item.name,
                        "revision": item.revision,
                        "configuration_hash": item.configuration_hash,
                    }
                    for item in snapshots
                ],
                "name": name,
                "suite_key": suite_key,
                "comparative": comparative,
                "metadata": dict(metadata or {}),
                "grounded_policy": (
                    None
                    if grounded_policy is None
                    else grounded_policy.model_dump(mode="json")
                ),
            }
        ),
    )


def _observation_key(
    execution_id: str, snapshot: ScorerSnapshot
) -> tuple[str, str, str, str]:
    return (
        execution_id,
        snapshot.name,
        snapshot.revision,
        snapshot.configuration_hash,
    )


def _default_save_artifact(**kwargs: Any) -> Any:
    from zenml.artifacts.utils import save_artifact

    return save_artifact(**kwargs)
