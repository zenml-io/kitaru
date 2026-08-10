"""Tests for the Verifiers 0.3 v1 export renderer."""

import hashlib
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

from kitaru._exports.formats.verifiers_v1 import (
    render_verifiers_v1,
    validate_verifiers_v1,
)
from kitaru._exports.models import (
    ExportError,
    MaterializedEvaluator,
    ResolvedExport,
    RewardSelector,
)
from kitaru._exports.source import inventory_source
from kitaru.api_models.v1.agent_version import AgentVersionResponse, RunSpec
from kitaru.api_models.v1.cohort_version import CohortVersionResponse
from kitaru.api_models.v1.evaluator import EvaluatorVersionResponse
from kitaru.api_models.v1.experiment import ExperimentResponse
from kitaru.api_models.v1.plugin import ScriptPluginSource
from kitaru.api_models.v1.session import SessionOrigin, SessionResponse, SessionStatus
from kitaru.api_models.v1.session_node import SessionWithNodesResponse


def _resolved(tmp_path: Path) -> ResolvedExport:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    experiment_id = uuid.UUID(int=10)
    cohort_version_id = uuid.UUID(int=11)
    agent_version_id = uuid.UUID(int=12)
    session = SessionWithNodesResponse(
        session=SessionResponse(
            id=uuid.UUID(int=20),
            owner_id=uuid.UUID(int=21),
            agent_id=uuid.UUID(int=22),
            agent_version_id=agent_version_id,
            number=1,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
            inputs={"question": "6 * 7"},
            outputs="42",
            metadata={},
            llm_call_count=0,
            tool_call_count=0,
            created=now,
            updated=now,
        ),
        nodes=[],
    )
    script = (
        b"def evaluate(session):\n    return {'name': 'correctness', 'score': 1.0}\n"
    )
    version = EvaluatorVersionResponse(
        id=uuid.UUID(int=30),
        evaluator_id=uuid.UUID(int=31),
        version=2,
        display_version="2",
        source=ScriptPluginSource(blob_id=uuid.UUID(int=32), entrypoint="evaluate"),
        created=now,
        updated=now,
    )
    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "agent.py").write_text("print('agent')\n")
    return ResolvedExport(
        experiment=ExperimentResponse.model_construct(id=experiment_id),
        cohort_version=CohortVersionResponse.model_construct(id=cohort_version_id),
        agent_version=AgentVersionResponse.model_construct(
            id=agent_version_id,
            run_spec=RunSpec(
                command="python agent.py --mode solve",
                working_dir=None,
                env={"MODE": "eval"},
                timeout_seconds=90,
            ),
        ),
        sessions=(session,),
        evaluators=(
            MaterializedEvaluator(
                name="quality",
                version=version,
                params={"strict": True},
                script=script,
                source_sha256=hashlib.sha256(script).hexdigest(),
            ),
        ),
        reward=RewardSelector.parse("quality:correctness:score"),
        source=inventory_source(source_root),
    )


def test_render_writes_installable_v1_environment(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    resolved = _resolved(tmp_path)

    manifest = render_verifiers_v1(
        resolved,
        root,
        required_environment_names=("MODEL_API_KEY",),
    )

    assert manifest.format == "verifiers-v1"
    assert manifest.target_version == "0.3.0"
    pyproject = (root / "pyproject.toml").read_text()
    assert '"verifiers==0.3.0"' in pyproject
    assert '"kitaru==0.21.0"' in pyproject
    assert (root / "agent_source" / "agent.py").read_text() == "print('agent')\n"
    task = json.loads((root / "data" / "tasks.jsonl").read_text())
    assert task["inputs"] == {"question": "6 * 7"}
    assert task["context"]["session"]["id"] == str(uuid.UUID(int=20))

    taskset = (root / "kitaru_verifiers_v1" / "taskset.py").read_text()
    assert "class KitaruData(vf.TaskData)" in taskset
    assert "@vf.reward(weight=1.0)" in taskset
    bridge = (root / "kitaru_verifiers_v1" / "bridge.py").read_text()
    assert 'format="verifiers-v1"' in bridge
    assert "evaluate_session" in bridge

    harness = (root / "kitaru_verifiers_v1" / "harness.py").read_text()
    assert "class KitaruHarness(vf.Harness[KitaruHarnessConfig])" in harness
    assert "runtime.run_program" in harness
    assert '["sh", "-lc", command]' in harness
    assert '"KITARU_TASK_INPUTS"' in harness
    assert '"OPENAI_BASE_URL": endpoint' in harness
    assert "mcp_urls" in harness
    assert "timeout_seconds" not in harness


def test_structural_validation_does_not_import_exported_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "bundle"
    render_verifiers_v1(_resolved(tmp_path), root)

    def reject_import(*args: object, **kwargs: object) -> object:
        raise AssertionError("structural validation imported generated code")

    monkeypatch.setattr("builtins.__import__", reject_import)
    receipt = validate_verifiers_v1(root)

    assert receipt.status == "passed"
    assert receipt.level == "structural"
    assert receipt.target_version == "0.3.0"


def test_structural_validation_rejects_tampered_task_count(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    render_verifiers_v1(_resolved(tmp_path), root)
    with (root / "data" / "tasks.jsonl").open("a") as tasks:
        tasks.write("{}\n")

    with pytest.raises(ExportError) as raised:
        validate_verifiers_v1(root)

    assert raised.value.code == "invalid_verifiers_bundle"


def test_render_accepts_only_an_empty_staging_directory(tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()

    render_verifiers_v1(_resolved(tmp_path), staging)

    occupied = tmp_path / "occupied"
    occupied.mkdir()
    (occupied / "user.txt").write_text("keep")
    other = tmp_path / "other"
    other.mkdir()
    with pytest.raises(ExportError, match="invalid_destination"):
        render_verifiers_v1(_resolved(other), occupied)
    assert (occupied / "user.txt").read_text() == "keep"


@pytest.mark.parametrize("names", [("TOKEN", "TOKEN"), ("BAD-NAME",), ("1TOKEN",)])
def test_render_rejects_invalid_environment_names(
    tmp_path: Path, names: tuple[str, ...]
) -> None:
    destination = tmp_path / "bundle"

    with pytest.raises(ExportError, match="invalid_environment_name"):
        render_verifiers_v1(
            _resolved(tmp_path),
            destination,
            required_environment_names=names,
        )

    assert not destination.exists()


def test_render_is_deterministic_and_records_required_names(tmp_path: Path) -> None:
    resolved = _resolved(tmp_path)
    first = tmp_path / "first"
    second = tmp_path / "second"

    render_verifiers_v1(resolved, first, required_environment_names=("TOKEN",))
    render_verifiers_v1(resolved, second, required_environment_names=("TOKEN",))

    first_files = {
        path.relative_to(first): path.read_bytes()
        for path in first.rglob("*")
        if path.is_file()
    }
    second_files = {
        path.relative_to(second): path.read_bytes()
        for path in second.rglob("*")
        if path.is_file()
    }
    assert first_files == second_files
    assert b"TOKEN" in (first / "kitaru-export.json").read_bytes()
