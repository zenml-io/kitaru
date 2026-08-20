#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Shared export operation contracts."""

import asyncio
import threading
import uuid
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from kitaru.exports.config import ExportRequest
from kitaru.exports.models import (
    CONTENT_CATEGORIES,
    V1_EXPORT_BUDGETS,
    ArtifactProvenance,
    BoundedPathSummary,
    ContentCategory,
    ContentPolicy,
    EnvironmentPolicy,
    ExportError,
    ExportManifest,
    RuntimeRequirements,
    SourcePolicy,
    TaskProvenance,
    ValidationReceipt,
)
from kitaru.exports.operation import (
    ExportOperationRevoked,
    ExportOperationState,
    ExportOperationStateMachine,
    export_experiment,
)


def _request(tmp_path: Path, **changes: Any) -> ExportRequest:
    values: dict[str, Any] = {
        "experiment_id": uuid.uuid4(),
        "cohort_version_id": uuid.uuid4(),
        "agent_version_id": uuid.uuid4(),
        "format": "verifiers-v1",
        "source_root": tmp_path,
        "destination": tmp_path / "out",
        "primary_reward": "quality:correctness:score",
    }
    values.update(changes)
    return ExportRequest(**values)


def test_request_defaults_to_complete_content_and_included_environment(
    tmp_path: Path,
) -> None:
    request = _request(tmp_path)

    assert request.content_policy == ContentPolicy()
    assert request.environment_policy == EnvironmentPolicy()
    assert request.source_policy == SourcePolicy()
    assert request.policy_warnings == ()
    assert "components" not in ExportRequest.model_fields
    serialized = request.model_dump(mode="json")
    assert serialized == ExportRequest.model_validate(serialized).model_dump(
        mode="json"
    )


def test_request_accepts_runtime_only_environment_handling(tmp_path: Path) -> None:
    request = _request(
        tmp_path,
        environment_policy=EnvironmentPolicy(mode="runtime_only"),
    )

    assert request.environment_policy.mode == "runtime_only"

    with pytest.raises(ValidationError):
        EnvironmentPolicy.model_validate({"mode": "automatic_redaction"})


@pytest.mark.parametrize("category", CONTENT_CATEGORIES)
def test_content_policy_accepts_each_optional_omission(
    category: ContentCategory,
) -> None:
    policy = ContentPolicy(omit=(category,))

    assert policy.omit == (category,)
    assert policy.is_included(category) is False


def test_content_policy_canonicalizes_combined_omissions() -> None:
    policy = ContentPolicy(omit=tuple(reversed(CONTENT_CATEGORIES)))

    assert policy.omit == CONTENT_CATEGORIES
    assert len(policy.warnings) == 1
    assert policy.warnings[0].code == "content_policy_changes_evaluation"


def test_policy_rejects_required_content_and_contradictory_source_rules() -> None:
    with pytest.raises(ValidationError):
        ContentPolicy.model_validate({"omit": ["task_inputs"]})

    with pytest.raises(ValidationError, match="both included and excluded"):
        SourcePolicy(include=("dist/main.js",), exclude=("dist/main.js",))


def test_source_policy_enforces_the_utf8_path_budget() -> None:
    SourcePolicy(include=("x" * V1_EXPORT_BUDGETS.max_relative_path_bytes,))

    with pytest.raises(ValidationError, match="1,024 UTF-8 bytes"):
        SourcePolicy(include=("x" * (V1_EXPORT_BUDGETS.max_relative_path_bytes + 1),))


def test_v1_budgets_and_receipt_path_summary_boundaries() -> None:
    assert V1_EXPORT_BUDGETS.max_sessions == 1_000
    assert V1_EXPORT_BUDGETS.max_session_bytes == 16 * 1024 * 1024
    assert V1_EXPORT_BUDGETS.max_total_session_bytes == 256 * 1024 * 1024
    assert V1_EXPORT_BUDGETS.max_receipt_path_samples == 100
    assert V1_EXPORT_BUDGETS.max_receipt_path_characters == 512
    assert V1_EXPORT_BUDGETS.max_artifact_bytes == 2 * 1024 * 1024 * 1024

    paths = tuple(f"excluded/{index:03d}/{'x' * 600}" for index in range(101))
    summary = BoundedPathSummary.from_paths(reversed(paths))

    assert summary.total_count == 101
    assert len(summary.samples) == 100
    assert all(len(path) <= 512 for path in summary.samples)
    assert summary.truncated is True
    assert summary == BoundedPathSummary.from_paths(paths)


def test_manifest_serializes_assurance_and_provenance_deterministically() -> None:
    experiment_id = uuid.uuid4()
    cohort_version_id = uuid.uuid4()
    agent_version_id = uuid.uuid4()
    evaluator_version_id = uuid.uuid4()
    session_id = uuid.uuid4()
    provenance = ArtifactProvenance(
        artifact_digest="a" * 64,
        benchmark_digest="b" * 64,
        default_harness_digest="c" * 64,
        runtime_bundle_digest="d" * 64,
        plugin_id="kitaru-export-aabbccdd",
        distribution_name="kitaru-export-aabbccdd",
        module_name="kitaru_export_aabbccdd",
    )
    manifest = ExportManifest(
        format="verifiers-v1",
        target_version="0.3.0",
        experiment_id=experiment_id,
        cohort_version_id=cohort_version_id,
        agent_version_id=agent_version_id,
        evaluator_version_ids=(evaluator_version_id,),
        primary_reward="quality:correctness:score",
        source_digest="0" * 64,
        content_policy=ContentPolicy(omit=("visible_reasoning",)),
        provenance=provenance,
        runtime_requirements=RuntimeRequirements(
            task_private=("SCORING_TOKEN",),
            bundled_harness=("AGENT_TOKEN",),
        ),
        task_provenance=(
            TaskProvenance(session_id=session_id, content_digest="e" * 64),
        ),
        validation=ValidationReceipt(
            level="structural", status="passed", target_version="0.3.0"
        ),
    )

    serialized = manifest.model_dump(mode="json")
    assert serialized == ExportManifest.model_validate(serialized).model_dump(
        mode="json"
    )
    assert serialized["assurance"]["preflight"]["status"] == "passed"
    assert serialized["assurance"]["structural_validation"]["status"] == "passed"
    assert serialized["assurance"]["release_compatibility"]["status"] == "not_performed"
    assert serialized["warnings"][0]["code"] == "content_policy_changes_evaluation"
    assert serialized["runtime_requirements"]["all"] == [
        "AGENT_TOKEN",
        "SCORING_TOKEN",
    ]


def test_operation_state_machine_accepts_only_valid_transitions() -> None:
    operation = ExportOperationStateMachine()

    assert operation.try_start_commit() is True
    assert operation.state is ExportOperationState.COMMITTING
    assert operation.request_revocation() is False
    operation.mark_completed()
    assert operation.state is ExportOperationState.COMPLETED

    with pytest.raises(ExportError, match="completed -> failed"):
        operation.mark_failed()


def test_operation_state_machine_acknowledges_revocation_before_cancellation() -> None:
    operation = ExportOperationStateMachine()

    assert operation.request_revocation() is True
    assert operation.try_start_commit() is False
    operation.mark_cancelled()
    assert operation.state is ExportOperationState.CANCELLED


def test_operation_checkpoint_rejects_work_after_revocation() -> None:
    operation = ExportOperationStateMachine()

    assert operation.request_revocation() is True
    with pytest.raises(ExportOperationRevoked):
        operation.checkpoint()


def test_harbor_requires_an_explicit_trace_contract(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="trace_format and trace_path"):
        ExportRequest(
            experiment_id=uuid.uuid4(),
            cohort_version_id=uuid.uuid4(),
            agent_version_id=uuid.uuid4(),
            format="harbor",
            source_root=tmp_path,
            destination=tmp_path / "out",
            primary_reward="quality:correctness:score",
        )


async def test_dry_run_resolves_without_calling_a_renderer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    resolved = type(
        "Resolved",
        (),
        {
            "experiment": type("Item", (), {"id": uuid.uuid4()})(),
            "cohort_version": type("Item", (), {"id": uuid.uuid4()})(),
            "agent_version": type("Item", (), {"id": uuid.uuid4()})(),
            "sessions": (object(),),
            "evaluators": (object(),),
        },
    )()

    remote = object()

    async def fake_resolve_remote(*_args: Any, **_kwargs: Any) -> Any:
        return remote

    def fake_finalize(actual_remote: Any, **_kwargs: Any) -> Any:
        assert actual_remote is remote
        return resolved

    monkeypatch.setattr(
        "kitaru.exports.operation.resolve_remote_export", fake_resolve_remote
    )
    monkeypatch.setattr(
        "kitaru.exports.operation.finalize_remote_export", fake_finalize
    )
    destination = tmp_path / "out"
    receipt = await export_experiment(
        object(),
        ExportRequest(
            experiment_id=uuid.uuid4(),
            cohort_version_id=uuid.uuid4(),
            agent_version_id=uuid.uuid4(),
            format="verifiers-v1",
            source_root=tmp_path,
            destination=destination,
            primary_reward="quality:correctness:score",
            content_policy=ContentPolicy(omit=("visible_reasoning",)),
            environment_policy=EnvironmentPolicy(mode="runtime_only"),
            dry_run=True,
        ),
    )
    assert receipt.dry_run is True
    assert receipt.session_count == 1
    assert receipt.environment_policy.mode == "runtime_only"
    assert receipt.warnings[0].code == "content_policy_changes_evaluation"
    assert receipt.assurance is not None
    assert receipt.assurance.preflight.status == "passed"
    assert receipt.assurance.structural_validation.status == "not_performed"
    assert receipt.assurance.release_compatibility.status == "not_performed"
    assert not destination.exists()


async def test_remote_denial_happens_before_local_source_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def deny_remote(*_args: Any, **_kwargs: Any) -> Any:
        raise ExportError("secret_authorization_failed", "Export is not authorized.")

    def fail_if_source_is_read(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("source acquisition ran before remote authorization")

    monkeypatch.setattr("kitaru.exports.operation.resolve_remote_export", deny_remote)
    monkeypatch.setattr(
        "kitaru.exports.operation.inventory_source", fail_if_source_is_read
    )

    with pytest.raises(ExportError, match="secret_authorization_failed"):
        await export_experiment(
            object(),
            _request(tmp_path, source_root=tmp_path / "unreadable-source"),
        )


async def test_dry_run_rejects_an_existing_destination(tmp_path: Path) -> None:
    destination = tmp_path / "out"
    destination.mkdir()
    with pytest.raises(ExportError, match="Destination exists"):
        await export_experiment(
            object(),
            ExportRequest(
                experiment_id=uuid.uuid4(),
                cohort_version_id=uuid.uuid4(),
                agent_version_id=uuid.uuid4(),
                format="verifiers-v1",
                source_root=tmp_path,
                destination=destination,
                primary_reward="quality:correctness:score",
                dry_run=True,
            ),
        )


async def test_operation_publishes_renderer_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    experiment_id = uuid.uuid4()
    cohort_version_id = uuid.uuid4()
    agent_version_id = uuid.uuid4()
    evaluator_version_id = uuid.uuid4()
    resolved = type(
        "Resolved",
        (),
        {
            "experiment": type("Item", (), {"id": experiment_id})(),
            "cohort_version": type("Item", (), {"id": cohort_version_id})(),
            "agent_version": type("Item", (), {"id": agent_version_id})(),
            "sessions": (object(),),
            "evaluators": (
                type(
                    "Evaluator",
                    (),
                    {"version": type("Version", (), {"id": evaluator_version_id})()},
                )(),
            ),
        },
    )()

    remote = object()

    async def fake_resolve_remote(*_args: Any, **_kwargs: Any) -> Any:
        return remote

    def fake_finalize(actual_remote: Any, **_kwargs: Any) -> Any:
        assert actual_remote is remote
        return resolved

    def fake_render(_resolved: Any, root: Path, **_kwargs: Any) -> ExportManifest:
        (root / "README.md").write_text("ready\n")
        return ExportManifest(
            format="verifiers-v1",
            target_version="0.3.0",
            experiment_id=experiment_id,
            cohort_version_id=cohort_version_id,
            agent_version_id=agent_version_id,
            evaluator_version_ids=(evaluator_version_id,),
            primary_reward="quality:correctness:score",
            source_digest="0" * 64,
            validation=ValidationReceipt(
                level="structural", status="passed", target_version="0.3.0"
            ),
        )

    monkeypatch.setattr(
        "kitaru.exports.operation.resolve_remote_export", fake_resolve_remote
    )
    monkeypatch.setattr(
        "kitaru.exports.operation.finalize_remote_export", fake_finalize
    )
    monkeypatch.setattr("kitaru.exports.operation.render_verifiers_v1", fake_render)
    destination = tmp_path / "bundle"
    receipt = await export_experiment(
        object(),
        ExportRequest(
            experiment_id=experiment_id,
            cohort_version_id=cohort_version_id,
            agent_version_id=agent_version_id,
            format="verifiers-v1",
            source_root=tmp_path,
            destination=destination,
            primary_reward="quality:correctness:score",
        ),
    )
    assert (destination / "README.md").read_text() == "ready\n"
    assert receipt.validation_level == "structural"


async def test_operation_revocation_during_source_inventory_joins_without_publishing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    operation = ExportOperationStateMachine()
    inventory_started = threading.Event()
    release_inventory = threading.Event()
    remote = object()

    async def fake_resolve_remote(*_args: Any, **_kwargs: Any) -> Any:
        return remote

    def blocked_inventory(
        *_args: Any, cancellation_checkpoint: Any, **_kwargs: Any
    ) -> Any:
        inventory_started.set()
        assert release_inventory.wait(timeout=5)
        cancellation_checkpoint()
        raise AssertionError("revoked inventory continued")

    monkeypatch.setattr(
        "kitaru.exports.operation.resolve_remote_export", fake_resolve_remote
    )
    monkeypatch.setattr("kitaru.exports.operation.inventory_source", blocked_inventory)
    destination = tmp_path / "bundle"
    task = asyncio.create_task(
        export_experiment(
            object(),
            _request(tmp_path, destination=destination),
            operation=operation,
        )
    )
    assert await asyncio.to_thread(inventory_started.wait, 5)
    assert operation.request_revocation() is True
    release_inventory.set()

    with pytest.raises(ExportOperationRevoked):
        await task
    assert task.done()
    assert operation.state is ExportOperationState.CANCELLED
    assert not destination.exists()
