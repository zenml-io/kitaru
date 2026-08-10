"""Tests for the Harbor 0.20 experiment export renderer."""

import hashlib
import json
import tomllib
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from kitaru.api_models.v1.agent_version import AgentVersionResponse, RunSpec
from kitaru.api_models.v1.cohort_version import CohortVersionResponse
from kitaru.api_models.v1.evaluator import EvaluatorVersionResponse
from kitaru.api_models.v1.experiment import ExperimentResponse
from kitaru.api_models.v1.plugin import ScriptPluginSource
from kitaru.api_models.v1.session import (
    SessionOrigin,
    SessionResponse,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import SessionWithNodesResponse
from kitaru.exports.formats.harbor import (
    HARBOR_VERSION,
    harbor_task_digest,
    render_harbor,
    validate_harbor,
)
from kitaru.exports.models import (
    ExportError,
    MaterializedEvaluator,
    ResolvedExport,
    RewardSelector,
)
from kitaru.exports.source import inventory_source


def _resolved(source_root: Path, *, session_count: int = 2) -> ResolvedExport:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    agent_id = uuid.UUID(int=10)
    evaluator_script = (
        Path(__file__).parents[1] / "fixtures" / "exports" / "evaluator.py"
    ).read_bytes()
    evaluator = MaterializedEvaluator(
        name="quality",
        version=EvaluatorVersionResponse.model_construct(
            id=uuid.UUID(int=20),
            evaluator_id=uuid.UUID(int=21),
            version=3,
            display_version="3",
            source=ScriptPluginSource(blob_id=uuid.UUID(int=22), entrypoint="evaluate"),
            created=now,
            updated=now,
        ),
        params={"expected": "42", "weight": 0.5},
        script=evaluator_script,
        source_sha256=hashlib.sha256(evaluator_script).hexdigest(),
    )
    sessions = tuple(
        SessionWithNodesResponse(
            session=SessionResponse(
                id=uuid.UUID(int=100 + index),
                owner_id=uuid.UUID(int=2),
                agent_id=agent_id,
                agent_version_id=uuid.UUID(int=11),
                number=index + 1,
                origin=SessionOrigin.RECORDED,
                status=SessionStatus.COMPLETED,
                inputs={"question": f"case {index}"},
                outputs={"answer": "old"},
                metadata={},
                llm_call_count=0,
                tool_call_count=0,
                created=now,
                updated=now,
            ),
            nodes=[],
        )
        for index in range(session_count)
    )
    return ResolvedExport(
        experiment=ExperimentResponse.model_construct(
            id=uuid.UUID(int=1), name="Canonical", agent_id=agent_id
        ),
        cohort_version=CohortVersionResponse.model_construct(
            id=uuid.UUID(int=3), cohort_id=uuid.UUID(int=2), version=1
        ),
        agent_version=AgentVersionResponse.model_construct(
            id=uuid.UUID(int=11),
            agent_id=agent_id,
            version=4,
            run_spec=RunSpec(
                command="python agent.py",
                working_dir=None,
                env={"MODEL": "fixture"},
                timeout_seconds=90,
            ),
        ),
        sessions=sessions,
        evaluators=(evaluator,),
        reward=RewardSelector.parse("quality:correctness:score"),
        source=inventory_source(source_root),
    )


def test_render_harbor_emits_native_dataset_and_shared_image(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "agent.py").write_text("print('agent')\n")
    output = tmp_path / "bundle"

    render_harbor(
        _resolved(source),
        output,
        trace_format="atif",
        trace_path="/workspace/trajectory.json",
        required_environment_names=("MODEL_API_KEY",),
    )

    assert (output / "agent_image/agent_source/agent.py").is_file()
    assert (output / "agent_image/bridge/trace.py").is_file()
    assert (output / "agent_image/evaluators/evaluator-0.py").is_file()
    assert (output / "agent/kitaru_agent.py").is_file()
    tasks = sorted((output / "dataset").glob("task-*"))
    assert len(tasks) == 2
    for task in tasks:
        config = tomllib.loads((task / "task.toml").read_text())
        assert config["schema_version"] == "1.3"
        assert config["environment"]["docker_image"].startswith("kitaru-export:")
        assert (task / "tests/test.sh").stat().st_mode & 0o111
        task_data = json.loads((task / "inputs/task.json").read_text())
        assert task_data["primary_reward"] == {
            "evaluator": "quality",
            "field": "score",
            "result": "correctness",
        }

    dataset = tomllib.loads((output / "dataset/dataset.toml").read_text())
    assert dataset["dataset"]["name"].startswith("kitaru/")
    assert len(dataset["tasks"]) == 2
    for task, reference in zip(tasks, dataset["tasks"], strict=True):
        assert reference["digest"] == f"sha256:{harbor_task_digest(task)}"

    manifest = json.loads((output / "kitaru-export.json").read_text())
    assert manifest["target_version"] == HARBOR_VERSION
    assert manifest["source_digest"] == _resolved(source).source.digest
    assert manifest["required_environment_names"] == ["MODEL_API_KEY"]
    launcher = (output / "agent/kitaru_agent.py").read_text()
    assert "python agent.py" in launcher
    compile(launcher, "kitaru_agent.py", "exec")
    evaluator_metadata = json.loads(
        (output / "agent_image/evaluators.json").read_text()
    )
    assert (
        evaluator_metadata[0]["source_sha256"]
        == _resolved(source).evaluators[0].source_sha256
    )
    readme = (output / "README.md").read_text()
    assert "docker build" in readme
    assert "harbor run -p dataset" in readme
    assert "--agent agent.kitaru_agent:KitaruAgent" in readme

    validate_harbor(output)


def test_render_harbor_accepts_empty_staging_directory(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "agent.py").write_text("print('agent')\n")
    output = tmp_path / "staging"
    output.mkdir()

    render_harbor(
        _resolved(source, session_count=1),
        output,
        trace_format="atif",
        trace_path="/workspace/trajectory.json",
    )

    assert (output / "kitaru-export.json").is_file()


def test_harbor_digest_matches_official_algorithm(tmp_path: Path) -> None:
    (tmp_path / "task.toml").write_text("task")
    (tmp_path / "instruction.md").write_text("instruction")
    expected = hashlib.sha256()
    for name in ("instruction.md", "task.toml"):
        file_hash = hashlib.sha256((tmp_path / name).read_bytes()).hexdigest()
        expected.update(f"{name}\0{file_hash}\n".encode())

    assert harbor_task_digest(tmp_path) == expected.hexdigest()


@pytest.mark.parametrize(
    ("trace_format", "trace_path", "code"),
    [
        ("verifiers-v1", "/workspace/trace.json", "unsupported_trace_format"),
        ("atif", "relative.json", "invalid_trace_path"),
        ("atif", "", "missing_trace_path"),
    ],
)
def test_render_harbor_rejects_unsupported_trace_declarations(
    tmp_path: Path, trace_format: str, trace_path: str, code: str
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "agent.py").write_text("print('agent')\n")

    with pytest.raises(ExportError) as raised:
        render_harbor(
            _resolved(source),
            tmp_path / "bundle",
            trace_format=trace_format,
            trace_path=trace_path,
        )

    assert raised.value.code == code
    assert not (tmp_path / "bundle").exists()


def test_validate_harbor_does_not_import_agent_or_evaluator(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "agent.py").write_text("raise RuntimeError('must not import')\n")
    output = tmp_path / "bundle"
    resolved = _resolved(source, session_count=1)
    evaluator = resolved.evaluators[0]
    script = b"raise RuntimeError('must not import')\n"
    resolved = replace(
        resolved,
        evaluators=(
            replace(
                evaluator,
                script=script,
                source_sha256=hashlib.sha256(script).hexdigest(),
            ),
        ),
    )
    render_harbor(
        resolved,
        output,
        trace_format="kitaru",
        trace_path="/workspace/session.json",
    )

    validate_harbor(output)
