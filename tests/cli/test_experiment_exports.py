#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""CLI experiment export contracts."""

import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from kitaru.cli import experiment_exports
from kitaru.cli.output import CLIError
from kitaru.exports.models import (
    ArtifactProvenance,
    ContentPolicy,
    DependencyReceipt,
    EnvironmentPolicy,
    ExportAssurance,
    ExportError,
    RuntimeRequirements,
    SourcePolicy,
)
from kitaru.exports.operation import ExportReceipt
from kitaru.exports.plugin import ExporterMetadata, LoadedExporter


def _loaded_exporter() -> LoadedExporter:
    metadata = ExporterMetadata(
        contract_version=1,
        distribution_name="kitaru-verifiers-exporter",
        distribution_version="1.2.3",
        format="verifiers-v1",
        target_version="0.3.0",
    )
    return LoadedExporter(implementation=cast(Any, object()), metadata=metadata)


async def test_cli_resolves_references_before_shared_operation(
    tmp_path: Path, monkeypatch: Any
) -> None:
    experiment_id = uuid.uuid4()
    cohort_version_id = uuid.uuid4()
    agent_version_id = uuid.uuid4()
    seen = []
    events: list[str] = []
    loaded = _loaded_exporter()

    def resolve_exporter(_format: str) -> LoadedExporter:
        events.append("exporter")
        return loaded

    async def resolve_asset(*_args: Any) -> Any:
        events.append("experiment")
        return SimpleNamespace(id=experiment_id)

    async def get_cohort_version(*_args: Any) -> Any:
        return object(), SimpleNamespace(id=cohort_version_id)

    async def get_agent_version(*_args: Any) -> Any:
        return object(), SimpleNamespace(id=agent_version_id)

    async def export(
        _client: Any, request: Any, *, exporter: LoadedExporter
    ) -> ExportReceipt:
        assert exporter is loaded
        seen.append(request)
        return ExportReceipt(
            format=request.format,
            dry_run=True,
            experiment_id=str(request.experiment_id),
            cohort_version_id=str(request.cohort_version_id),
            agent_version_id=str(request.agent_version_id),
            session_count=2,
            task_count=2,
            evaluator_count=1,
            source_digest="0" * 64,
            destination=str(request.destination),
            exporter=loaded.provenance,
        )

    monkeypatch.setattr(experiment_exports, "resolve_exporter", resolve_exporter)
    monkeypatch.setattr(experiment_exports, "resolve_asset", resolve_asset)
    monkeypatch.setattr(experiment_exports, "get_cohort_version", get_cohort_version)
    monkeypatch.setattr(experiment_exports, "get_agent_version", get_agent_version)
    monkeypatch.setattr(experiment_exports, "export_experiment", export)
    result = await experiment_exports.export_experiment_command(
        SimpleNamespace(experiments=object()),
        "experiment",
        cohort_version="cohort@1",
        agent="agent@1",
        format="verifiers-v1",
        source_root=tmp_path,
        destination=tmp_path / "bundle",
        primary_reward="quality:correctness:score",
        required_env=["TOKEN"],
        omit_content=["visible_reasoning", "usage_and_cost"],
        environment_mode="runtime_only",
        include_source=["dist/main.js"],
        exclude_source=["build/cache"],
        trace_format=None,
        trace_path=None,
        archive=False,
        dry_run=True,
    )
    assert seen[0].cohort_version_id == cohort_version_id
    assert events[:2] == ["exporter", "experiment"]
    assert seen[0].agent_version_id == agent_version_id
    assert seen[0].content_policy == ContentPolicy(
        omit=("visible_reasoning", "usage_and_cost")
    )
    assert seen[0].environment_policy == EnvironmentPolicy(mode="runtime_only")
    assert seen[0].source_policy == SourcePolicy(
        include=("dist/main.js",), exclude=("build/cache",)
    )
    assert result.item["dry_run"] is True


async def test_cli_returns_complete_rich_receipt(
    tmp_path: Path, monkeypatch: Any
) -> None:
    identifier = uuid.uuid4()
    loaded = _loaded_exporter()

    async def resolve_asset(*_args: Any) -> Any:
        return SimpleNamespace(id=identifier)

    async def resolve_version(*_args: Any) -> Any:
        return object(), SimpleNamespace(id=identifier)

    async def export(
        _client: Any, request: Any, *, exporter: LoadedExporter
    ) -> ExportReceipt:
        assert exporter is loaded
        return ExportReceipt(
            format=request.format,
            dry_run=False,
            experiment_id=identifier,
            cohort_version_id=identifier,
            agent_version_id=identifier,
            session_count=3,
            task_count=3,
            evaluator_count=1,
            source_digest="0" * 64,
            destination=str(request.destination),
            bundle_digest="1" * 64,
            target_version="0.3.0",
            exporter=loaded.provenance,
            content_policy=request.content_policy,
            environment_policy=request.environment_policy,
            source_policy=request.source_policy,
            assurance=ExportAssurance.preflight_only("0.3.0"),
            dependencies=DependencyReceipt(
                status="locked", requirement_digest="2" * 64
            ),
            provenance=ArtifactProvenance(
                artifact_digest="3" * 64,
                benchmark_digest="4" * 64,
                default_harness_digest="5" * 64,
                runtime_bundle_digest="6" * 64,
                plugin_id="kitaru-benchmark",
                distribution_name="kitaru-benchmark",
                module_name="kitaru_benchmark",
            ),
            runtime_requirements=RuntimeRequirements(
                task_private=("EVALUATOR_TOKEN",),
                bundled_harness=("MODEL_TOKEN",),
            ),
        )

    monkeypatch.setattr(experiment_exports, "resolve_asset", resolve_asset)
    monkeypatch.setattr(experiment_exports, "resolve_exporter", lambda _format: loaded)
    monkeypatch.setattr(experiment_exports, "get_cohort_version", resolve_version)
    monkeypatch.setattr(experiment_exports, "get_agent_version", resolve_version)
    monkeypatch.setattr(experiment_exports, "export_experiment", export)

    result = await experiment_exports.export_experiment_command(
        SimpleNamespace(experiments=object()),
        "experiment",
        cohort_version="cohort@1",
        agent="agent@1",
        format="verifiers-v1",
        source_root=tmp_path,
        destination=tmp_path / "bundle",
        primary_reward="quality:correctness:score",
        required_env=None,
        omit_content=None,
        environment_mode="include",
        include_source=None,
        exclude_source=None,
        trace_format=None,
        trace_path=None,
        archive=False,
        dry_run=False,
    )

    assert result.item["task_count"] == 3
    assert result.item["dependencies"]["status"] == "locked"
    assert result.item["provenance"] == {
        "artifact_digest": "3" * 64,
        "benchmark_digest": "4" * 64,
        "default_harness_digest": "5" * 64,
        "runtime_bundle_digest": "6" * 64,
        "plugin_id": "kitaru-benchmark",
        "distribution_name": "kitaru-benchmark",
        "module_name": "kitaru_benchmark",
    }
    assert result.item["runtime_requirements"]["all"] == [
        "EVALUATOR_TOKEN",
        "MODEL_TOKEN",
    ]


async def test_cli_treats_malformed_export_receipt_as_internal_error(
    tmp_path: Path, monkeypatch: Any
) -> None:
    identifier = uuid.uuid4()
    loaded = _loaded_exporter()

    async def resolve_asset(*_args: Any) -> Any:
        return SimpleNamespace(id=identifier)

    async def resolve_version(*_args: Any) -> Any:
        return object(), SimpleNamespace(id=identifier)

    async def export(_client: Any, _request: Any, *, exporter: LoadedExporter) -> Any:
        assert exporter is loaded
        return {"format": "verifiers-v1"}

    monkeypatch.setattr(experiment_exports, "resolve_asset", resolve_asset)
    monkeypatch.setattr(experiment_exports, "resolve_exporter", lambda _format: loaded)
    monkeypatch.setattr(experiment_exports, "get_cohort_version", resolve_version)
    monkeypatch.setattr(experiment_exports, "get_agent_version", resolve_version)
    monkeypatch.setattr(experiment_exports, "export_experiment", export)

    with pytest.raises(CLIError) as raised:
        await experiment_exports.export_experiment_command(
            SimpleNamespace(experiments=object()),
            "experiment",
            cohort_version="cohort@1",
            agent="agent@1",
            format="verifiers-v1",
            source_root=tmp_path,
            destination=tmp_path / "bundle",
            primary_reward="quality:correctness:score",
            required_env=None,
            omit_content=None,
            environment_mode="include",
            include_source=None,
            exclude_source=None,
            trace_format=None,
            trace_path=None,
            archive=False,
            dry_run=True,
        )

    assert raised.value.kind == "internal_error"
    assert "validation" not in raised.value.message.lower()


async def test_cli_maps_missing_exporter_before_reference_resolution(
    tmp_path: Path, monkeypatch: Any
) -> None:
    references_touched = False

    async def resolve_asset(*_args: Any) -> Any:
        nonlocal references_touched
        references_touched = True
        raise AssertionError("reference resolution ran")

    def reject(_format: str) -> LoadedExporter:
        raise ExportError(
            "exporter_not_installed",
            "Export format verifiers-v1 requires kitaru-verifiers-exporter. "
            "Install it with `uv add kitaru-verifiers-exporter`.",
        )

    monkeypatch.setattr(experiment_exports, "resolve_asset", resolve_asset)
    monkeypatch.setattr(experiment_exports, "resolve_exporter", reject)

    with pytest.raises(CLIError) as raised:
        await experiment_exports.export_experiment_command(
            SimpleNamespace(experiments=object()),
            "experiment",
            cohort_version="cohort@1",
            agent="agent@1",
            format="verifiers-v1",
            source_root=tmp_path,
            destination=tmp_path / "bundle",
            primary_reward="quality:correctness:score",
            required_env=None,
            omit_content=None,
            environment_mode="include",
            include_source=None,
            exclude_source=None,
            trace_format=None,
            trace_path=None,
            archive=False,
            dry_run=True,
        )

    assert raised.value.kind == "invalid_configuration"
    assert raised.value.details == {"export_code": "exporter_not_installed"}
    assert raised.value.hint is not None
    assert "Python environment running this command" in raised.value.hint
    assert references_touched is False
