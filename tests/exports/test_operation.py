#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Shared export operation contracts."""

import asyncio
import threading
import uuid
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from kitaru.exports.config import ExportFormat, ExportRequest
from kitaru.exports.models import (
    CONTENT_CATEGORIES,
    V1_EXPORT_BUDGETS,
    ArtifactProvenance,
    BoundedPathSummary,
    ContentCategory,
    ContentPolicy,
    EnvironmentPolicy,
    ExportAssurance,
    ExporterProvenance,
    ExportError,
    ExportManifest,
    RewardSelector,
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
from kitaru.exports.plugin import (
    ExporterContext,
    ExporterMetadata,
    ExporterOptions,
    LoadedExporter,
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


class FakeExporter:
    """Record operation calls through the public exporter contract."""

    def __init__(self, metadata: ExporterMetadata) -> None:
        self.metadata = metadata
        self.preflight_calls: list[tuple[Any, ExporterOptions, ExporterContext]] = []
        self.render_calls: list[tuple[Any, Path, ExporterOptions, ExporterContext]] = []
        self.manifest: ExportManifest | None = None
        self.embedded_manifest: ExportManifest | None = None

    def preflight(
        self,
        resolved: Any,
        *,
        options: ExporterOptions,
        context: ExporterContext,
    ) -> None:
        self.preflight_calls.append((resolved, options, context))
        context.checkpoint()

    def render(
        self,
        resolved: Any,
        staging_root: Path,
        *,
        options: ExporterOptions,
        context: ExporterContext,
    ) -> ExportManifest:
        self.render_calls.append((resolved, staging_root, options, context))
        context.checkpoint()
        assert self.manifest is not None
        (staging_root / "README.md").write_text("ready\n")
        (staging_root / "kitaru-export.json").write_text(
            (self.embedded_manifest or self.manifest).model_dump_json()
        )
        return self.manifest


def _loaded_exporter(
    *, format: ExportFormat = "verifiers-v1", target_version: str = "0.3.0"
) -> tuple[LoadedExporter, FakeExporter]:
    distribution_name = (
        "kitaru-harbor-exporter" if format == "harbor" else "kitaru-verifiers-exporter"
    )
    metadata = ExporterMetadata(
        contract_version=1,
        distribution_name=distribution_name,
        distribution_version="1.2.3",
        format=format,
        target_version=target_version,
    )
    implementation = FakeExporter(metadata)
    return LoadedExporter(
        implementation=implementation, metadata=metadata
    ), implementation


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
        exporter=ExporterProvenance(
            contract_version=1,
            distribution_name="kitaru-verifiers-exporter",
            distribution_version="1.2.3",
            format="verifiers-v1",
            target_version="0.3.0",
        ),
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


def test_harbor_trace_path_matches_exporter_option_budget(tmp_path: Path) -> None:
    maximum_trace_path = "/" + "x" * 1_023
    request = _request(
        tmp_path,
        format="harbor",
        trace_format="atif",
        trace_path=maximum_trace_path,
    )

    assert request.trace_path == maximum_trace_path

    with pytest.raises(ValidationError, match="at most 1024 characters"):
        _request(
            tmp_path,
            format="harbor",
            trace_format="atif",
            trace_path=maximum_trace_path + "x",
        )


async def test_dry_run_calls_plugin_preflight_without_rendering(
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
    loaded, exporter = _loaded_exporter()
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
        exporter=loaded,
    )
    assert receipt.dry_run is True
    assert receipt.session_count == 1
    assert receipt.environment_policy.mode == "runtime_only"
    assert receipt.warnings[0].code == "content_policy_changes_evaluation"
    assert receipt.assurance is not None
    assert receipt.assurance.preflight.status == "passed"
    assert receipt.assurance.structural_validation.status == "not_performed"
    assert receipt.assurance.release_compatibility.status == "not_performed"
    assert [call[0] for call in exporter.preflight_calls] == [resolved]
    assert exporter.render_calls == []
    assert receipt.exporter == loaded.provenance
    assert receipt.target_version == loaded.metadata.target_version
    assert not destination.exists()


async def test_missing_exporter_fails_before_remote_or_source_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    touched: list[str] = []

    async def fail_remote(*_args: Any, **_kwargs: Any) -> Any:
        touched.append("remote")
        raise AssertionError("remote resolution ran")

    def fail_source(*_args: Any, **_kwargs: Any) -> Any:
        touched.append("source")
        raise AssertionError("source inventory ran")

    monkeypatch.setattr("kitaru.exports.operation.resolve_remote_export", fail_remote)
    monkeypatch.setattr("kitaru.exports.operation.inventory_source", fail_source)
    monkeypatch.setattr(
        "kitaru.exports.operation.resolve_exporter",
        lambda _format: (_ for _ in ()).throw(
            ExportError("exporter_not_installed", "Install the exporter.")
        ),
    )

    with pytest.raises(ExportError) as raised:
        await export_experiment(object(), _request(tmp_path))

    assert raised.value.code == "exporter_not_installed"
    assert touched == []


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
            exporter=_loaded_exporter()[0],
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
            exporter=_loaded_exporter()[0],
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
            "reward": type(
                "Reward",
                (),
                {
                    "evaluator": "quality",
                    "result": "correctness",
                    "field": "score",
                },
            )(),
            "source": type("Source", (), {"digest": "0" * 64, "excluded": ()})(),
            "sessions": (object(),),
            "evaluators": (
                type(
                    "Evaluator",
                    (),
                    {
                        "name": "quality",
                        "version": type("Version", (), {"id": evaluator_version_id})(),
                    },
                )(),
            ),
            "required_environment_names": (),
            "content_policy": ContentPolicy(),
            "environment_policy": EnvironmentPolicy(),
            "source_policy": SourcePolicy(),
        },
    )()

    remote = object()

    async def fake_resolve_remote(*_args: Any, **_kwargs: Any) -> Any:
        return remote

    def fake_finalize(actual_remote: Any, **_kwargs: Any) -> Any:
        assert actual_remote is remote
        return resolved

    loaded, exporter = _loaded_exporter()
    exporter.manifest = ExportManifest(
        format="verifiers-v1",
        target_version="0.3.0",
        exporter=loaded.provenance,
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
        exporter=loaded,
    )
    assert (destination / "README.md").read_text() == "ready\n"
    assert receipt.validation_level == "structural"
    assert receipt.exporter == loaded.provenance
    assert len(exporter.preflight_calls) == 1
    assert len(exporter.render_calls) == 1


async def test_operation_rejects_manifest_identity_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = _request(tmp_path, destination=tmp_path / "bundle")
    resolved = type(
        "Resolved",
        (),
        {
            "experiment": type("Item", (), {"id": request.experiment_id})(),
            "cohort_version": type("Item", (), {"id": request.cohort_version_id})(),
            "agent_version": type("Item", (), {"id": request.agent_version_id})(),
            "reward": RewardSelector.parse(request.primary_reward),
            "source": type("Source", (), {"digest": "0" * 64, "excluded": ()})(),
            "sessions": (),
            "evaluators": (),
            "required_environment_names": (),
            "content_policy": ContentPolicy(),
            "environment_policy": EnvironmentPolicy(),
            "source_policy": SourcePolicy(),
        },
    )()

    async def fake_resolve_remote(*_args: Any, **_kwargs: Any) -> object:
        return object()

    monkeypatch.setattr(
        "kitaru.exports.operation.resolve_remote_export", fake_resolve_remote
    )
    monkeypatch.setattr(
        "kitaru.exports.operation.finalize_remote_export",
        lambda *_args, **_kwargs: resolved,
    )
    loaded, exporter = _loaded_exporter()
    exporter.manifest = ExportManifest(
        format="verifiers-v1",
        target_version="0.3.0",
        exporter=ExporterProvenance(
            contract_version=1,
            distribution_name="counterfeit-exporter",
            distribution_version="1.2.3",
            format="verifiers-v1",
            target_version="0.3.0",
        ),
        experiment_id=request.experiment_id,
        cohort_version_id=request.cohort_version_id,
        agent_version_id=request.agent_version_id,
        evaluator_version_ids=(),
        primary_reward=request.primary_reward,
        source_digest="0" * 64,
        validation=ValidationReceipt(
            level="structural", status="passed", target_version="0.3.0"
        ),
    )

    with pytest.raises(ExportError) as raised:
        await export_experiment(object(), request, exporter=loaded)

    assert raised.value.code == "exporter_invalid_result"
    assert not request.destination.exists()


@pytest.mark.parametrize(
    "mutation",
    [
        "evaluator_order",
        "content_policy",
        "environment_policy",
        "source_policy",
        "source_exclusions",
        "required_environment_names",
        "validation_level",
        "validation_status",
        "assurance",
    ],
)
async def test_operation_rejects_manifest_claims_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "agent.py").write_text("print('ready')\n")
    (tmp_path / "build").mkdir()
    (tmp_path / "build" / "artifact.txt").write_text("ignored\n")
    request = _request(
        tmp_path,
        destination=tmp_path / "bundle",
        required_environment_names=("REQUESTED_TOKEN",),
        content_policy=ContentPolicy(omit=("visible_reasoning",)),
        environment_policy=EnvironmentPolicy(mode="runtime_only"),
        source_policy=SourcePolicy(include=("src/agent.py",), exclude=("build",)),
    )
    alpha_evaluator_id = uuid.uuid4()
    beta_evaluator_id = uuid.uuid4()
    resolved = type(
        "Resolved",
        (),
        {
            "experiment": type("Item", (), {"id": request.experiment_id})(),
            "cohort_version": type("Item", (), {"id": request.cohort_version_id})(),
            "agent_version": type("Item", (), {"id": request.agent_version_id})(),
            "reward": RewardSelector.parse(request.primary_reward),
            "source": type(
                "Source",
                (),
                {"digest": "0" * 64, "excluded": ("ignored.log",)},
            )(),
            "sessions": (),
            "evaluators": (
                type(
                    "Evaluator",
                    (),
                    {
                        "name": "beta",
                        "version": type("Version", (), {"id": beta_evaluator_id})(),
                    },
                )(),
                type(
                    "Evaluator",
                    (),
                    {
                        "name": "alpha",
                        "version": type("Version", (), {"id": alpha_evaluator_id})(),
                    },
                )(),
            ),
            "required_environment_names": ("ATTACHED_TOKEN",),
            "content_policy": request.content_policy,
            "environment_policy": request.environment_policy,
            "source_policy": request.source_policy,
        },
    )()

    async def fake_resolve_remote(*_args: Any, **_kwargs: Any) -> object:
        return object()

    monkeypatch.setattr(
        "kitaru.exports.operation.resolve_remote_export", fake_resolve_remote
    )
    monkeypatch.setattr(
        "kitaru.exports.operation.finalize_remote_export",
        lambda *_args, **_kwargs: resolved,
    )
    loaded, exporter = _loaded_exporter()
    manifest = ExportManifest(
        format="verifiers-v1",
        target_version="0.3.0",
        exporter=loaded.provenance,
        experiment_id=request.experiment_id,
        cohort_version_id=request.cohort_version_id,
        agent_version_id=request.agent_version_id,
        evaluator_version_ids=(alpha_evaluator_id, beta_evaluator_id),
        primary_reward=request.primary_reward,
        source_digest="0" * 64,
        required_environment_names=("ATTACHED_TOKEN", "REQUESTED_TOKEN"),
        exclusions=("ignored.log",),
        content_policy=request.content_policy,
        environment_policy=request.environment_policy,
        source_policy=request.source_policy,
        validation=ValidationReceipt(
            level="structural", status="passed", target_version="0.3.0"
        ),
    )
    if mutation == "evaluator_order":
        manifest = manifest.model_copy(
            update={"evaluator_version_ids": (beta_evaluator_id, alpha_evaluator_id)}
        )
    elif mutation == "content_policy":
        manifest = manifest.model_copy(update={"content_policy": ContentPolicy()})
    elif mutation == "environment_policy":
        manifest = manifest.model_copy(
            update={"environment_policy": EnvironmentPolicy()}
        )
    elif mutation == "source_policy":
        manifest = manifest.model_copy(update={"source_policy": SourcePolicy()})
    elif mutation == "source_exclusions":
        manifest = manifest.model_copy(update={"exclusions": ()})
    elif mutation == "required_environment_names":
        manifest = manifest.model_copy(
            update={"required_environment_names": ("REQUESTED_TOKEN",)}
        )
    elif mutation == "validation_level":
        manifest = manifest.model_copy(
            update={
                "validation": ValidationReceipt(
                    level="preflight", status="passed", target_version="0.3.0"
                ),
                "assurance": ExportAssurance.preflight_only("0.3.0"),
            }
        )
    elif mutation == "validation_status":
        validation = ValidationReceipt(
            level="structural", status="failed", target_version="0.3.0"
        )
        manifest = manifest.model_copy(
            update={
                "validation": validation,
                "assurance": ExportAssurance.for_artifact(validation),
            }
        )
    else:
        assert mutation == "assurance"
        manifest = manifest.model_copy(
            update={"assurance": ExportAssurance.preflight_only("0.3.0")}
        )
    exporter.manifest = manifest
    exporter.embedded_manifest = manifest

    with pytest.raises(ExportError) as raised:
        await export_experiment(object(), request, exporter=loaded)

    assert raised.value.code == "exporter_invalid_result"
    assert not request.destination.exists()


async def test_operation_revocation_during_plugin_preflight_prevents_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    operation = ExportOperationStateMachine()
    preflight_started = threading.Event()
    release_preflight = threading.Event()
    loaded, exporter = _loaded_exporter()

    def blocked_preflight(
        _resolved: Any,
        *,
        options: ExporterOptions,
        context: ExporterContext,
    ) -> None:
        assert options.required_environment_names == ()
        preflight_started.set()
        assert release_preflight.wait(timeout=5)
        context.checkpoint()
        raise AssertionError("revoked preflight continued")

    exporter.preflight = blocked_preflight  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]

    async def fake_resolve_remote(*_args: Any, **_kwargs: Any) -> object:
        return object()

    monkeypatch.setattr(
        "kitaru.exports.operation.resolve_remote_export", fake_resolve_remote
    )
    monkeypatch.setattr(
        "kitaru.exports.operation.finalize_remote_export",
        lambda *_args, **_kwargs: object(),
    )
    destination = tmp_path / "bundle"
    task = asyncio.create_task(
        export_experiment(
            object(),
            _request(tmp_path, destination=destination),
            operation=operation,
            exporter=loaded,
        )
    )
    assert await asyncio.to_thread(preflight_started.wait, 5)
    assert operation.request_revocation() is True
    release_preflight.set()

    with pytest.raises(ExportOperationRevoked):
        await task
    assert operation.state is ExportOperationState.CANCELLED
    assert not destination.exists()


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
            exporter=_loaded_exporter()[0],
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
