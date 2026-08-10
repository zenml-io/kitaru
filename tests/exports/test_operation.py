#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Shared export operation contracts."""

import uuid
from pathlib import Path
from typing import Any

import pytest

from kitaru.exports.config import ExportRequest
from kitaru.exports.models import ExportError, ExportManifest, ValidationReceipt
from kitaru.exports.operation import export_experiment


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

    async def fake_resolve(*_args: Any, **_kwargs: Any) -> Any:
        return resolved

    monkeypatch.setattr("kitaru.exports.operation.resolve_export", fake_resolve)
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
            dry_run=True,
        ),
    )
    assert receipt.dry_run is True
    assert receipt.session_count == 1
    assert not destination.exists()


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

    async def fake_resolve(*_args: Any, **_kwargs: Any) -> Any:
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

    monkeypatch.setattr("kitaru.exports.operation.resolve_export", fake_resolve)
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
