"""Tests for the Harbor 0.20 experiment export renderer."""

import hashlib
import json
import sys
import tomllib
import types
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

import kitaru_harbor_exporter
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
from kitaru.exports import ExporterContext, ExporterOptions, ExporterProvenance
from kitaru.exports._dependencies import classify_dependencies
from kitaru.exports.models import (
    ContentPolicy,
    EnvironmentPolicy,
    ExportError,
    MaterializedEvaluator,
    ResolvedExport,
    RewardSelector,
    SourcePolicy,
)
from kitaru.exports.source import inventory_source
from kitaru.exports.writer import file_digests
from kitaru_harbor_exporter import create_exporter
from kitaru_harbor_exporter.exporter import (
    HARBOR_VERSION,
    SCORING_TIMEOUT_SECONDS,
    HarborExporter,
    harbor_task_digest,
    validate_harbor,
)

_EXPORTER = HarborExporter()


def _get_context(
    checkpoint: Any = lambda: None,
) -> ExporterContext:
    return ExporterContext(
        exporter=ExporterProvenance.model_validate(_EXPORTER.metadata.model_dump()),
        cancellation_checkpoint=checkpoint,
    )


def render_harbor(
    resolved: ResolvedExport,
    destination: Path,
    *,
    trace_format: str,
    trace_path: str,
    required_environment_names: tuple[str, ...] = (),
    context: ExporterContext | None = None,
) -> Any:
    """Invoke the Harbor renderer through the installed exporter contract."""
    return _EXPORTER.render(
        resolved,
        destination,
        options=ExporterOptions.model_construct(
            trace_format=trace_format,
            trace_path=trace_path,
            required_environment_names=required_environment_names,
        ),
        context=context or _get_context(),
    )


def _resolved(source_root: Path, *, session_count: int = 2) -> ResolvedExport:
    if not any(
        (source_root / name).exists() for name in ("pyproject.toml", "requirements.txt")
    ):
        (source_root / "pyproject.toml").write_text(
            '[project]\nname = "fixture-agent"\nversion = "1.0.0"\n'
        )
    now = datetime(2026, 1, 1, tzinfo=UTC)
    agent_id = uuid.UUID(int=10)
    evaluator_script = (
        Path(__file__).parents[3] / "tests" / "fixtures" / "exports" / "evaluator.py"
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
        source=(source := inventory_source(source_root)),
        command_argv=("python", "agent.py"),
        dependency_plan=classify_dependencies(source),
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
        assert (task / "environment/.gitkeep").is_file()
        assert (task / "tests/test.sh").stat().st_mode & 0o111
        task_data = json.loads((task / "inputs/task.json").read_text())
        assert task_data["primary_reward"] == {
            "evaluator": "quality",
            "field": "score",
            "result": "correctness",
        }
        assert config["agent"]["timeout_sec"] == 90.0
        assert config["verifier"]["timeout_sec"] > SCORING_TIMEOUT_SECONDS
        assert task_data["evaluator_timeout_seconds"] == SCORING_TIMEOUT_SECONDS

    dataset = tomllib.loads((output / "dataset/dataset.toml").read_text())
    assert dataset["dataset"]["name"].startswith("kitaru/")
    assert len(dataset["tasks"]) == 2
    for task, reference in zip(tasks, dataset["tasks"], strict=True):
        assert reference["digest"] == f"sha256:{harbor_task_digest(task)}"

    manifest = json.loads((output / "kitaru-export.json").read_text())
    assert manifest["target_version"] == HARBOR_VERSION
    assert manifest["exporter"] == _EXPORTER.metadata.model_dump(mode="json")
    assert manifest["source_digest"] == _resolved(source).source.digest
    assert manifest["required_environment_names"] == ["MODEL_API_KEY"]
    assert manifest["dependencies"]["status"] == "declared"
    assert manifest["runtime_bridge"]["schema_version"] == 1
    assert len(manifest["runtime_bridge"]["sha256"]) == 64
    launcher = (output / "agent/kitaru_agent.py").read_text()
    runtime = json.loads((output / "agent_image/agent-runtime.json").read_text())
    assert runtime["command_argv"] == ["python", "agent.py"]
    assert "python agent.py" not in launcher
    assert "populate_context_post_run" in launcher
    compile(launcher, "kitaru_agent.py", "exec")
    dockerfile = (output / "agent_image/Dockerfile").read_text()
    assert "uv sync --project /workspace --no-dev --no-editable" in dockerfile
    assert "evaluator-requirements.txt" in dockerfile
    assert "kitaru.exports" not in "".join(
        path.read_text() for path in (output / "agent_image/bridge").glob("*.py")
    )
    assert "kitaru_harbor_exporter" not in "".join(
        path.read_text() for path in output.rglob("*.py") if path.is_file()
    )


def test_harbor_exporter_factory_reports_installed_contract() -> None:
    exporter = create_exporter()

    assert kitaru_harbor_exporter.__all__ == ["create_exporter"]
    assert isinstance(exporter, HarborExporter)
    assert exporter.metadata.contract_version == 1
    assert exporter.metadata.distribution_name == "kitaru-harbor-exporter"
    assert exporter.metadata.distribution_version == "0.1.0"
    assert exporter.metadata.format == "harbor"
    assert exporter.metadata.target_version == "0.20.0"


def test_harbor_exporter_honors_cancellation_before_writing(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "agent.py").write_text("print('agent')\n")
    output = tmp_path / "bundle"

    def cancel() -> None:
        raise RuntimeError("cancelled")

    with pytest.raises(RuntimeError, match="cancelled"):
        render_harbor(
            _resolved(source),
            output,
            trace_format="atif",
            trace_path="/workspace/trajectory.json",
            context=_get_context(cancel),
        )

    assert not output.exists()


def test_harbor_exporter_preflight_does_not_write(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "agent.py").write_text("print('agent')\n")

    _EXPORTER.preflight(
        _resolved(source),
        options=ExporterOptions(
            trace_format="atif",
            trace_path="/workspace/trajectory.json",
        ),
        context=_get_context(),
    )

    assert sorted(path.name for path in tmp_path.iterdir()) == ["source"]


def test_harbor_exporter_cancels_inside_bounded_render_loop(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "agent.py").write_text("print('agent')\n")
    staging = tmp_path / "staging"
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel.txt"
    sentinel.write_text("unchanged\n")
    checkpoints = 0

    def cancel_during_render() -> None:
        nonlocal checkpoints
        checkpoints += 1
        if checkpoints == 12:
            raise RuntimeError("cancelled during render")

    with pytest.raises(RuntimeError, match="cancelled during render"):
        render_harbor(
            _resolved(source, session_count=10),
            staging,
            trace_format="atif",
            trace_path="/workspace/trajectory.json",
            context=_get_context(cancel_during_render),
        )

    assert sentinel.read_text() == "unchanged\n"
    assert not (outside / "kitaru-export.json").exists()


def test_harbor_readme_records_effective_policies(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "agent.py").write_text("print('agent')\n")
    resolved = replace(
        _resolved(source, session_count=1),
        content_policy=ContentPolicy(omit=("visible_reasoning",)),
        environment_policy=EnvironmentPolicy(mode="runtime_only"),
        source_policy=SourcePolicy(include=("agent.py",), exclude=("build/cache",)),
    )

    output = tmp_path / "bundle"
    render_harbor(
        resolved,
        output,
        trace_format="atif",
        trace_path="/workspace/trajectory.json",
    )

    readme = (output / "README.md").read_text()
    assert "Content omissions: visible_reasoning." in readme
    assert "Registered environment handling: runtime_only." in readme
    assert "Explicit source includes: agent.py." in readme
    assert "Explicit source exclusions: build/cache." in readme
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
    assert "not the user's agent name or class" in readme

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


def test_generated_agent_imports_without_container_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "agent.py").write_text("print('agent')\n")
    output = tmp_path / "bundle"
    render_harbor(
        _resolved(source, session_count=1),
        output,
        trace_format="atif",
        trace_path="/workspace/trajectory.json",
    )
    harbor = types.ModuleType("harbor")
    agents = types.ModuleType("harbor.agents")
    base = types.ModuleType("harbor.agents.base")
    base_module: Any = base
    base_module.BaseAgent = object
    monkeypatch.setitem(sys.modules, "harbor", harbor)
    monkeypatch.setitem(sys.modules, "harbor.agents", agents)
    monkeypatch.setitem(sys.modules, "harbor.agents.base", base)
    launcher = output / "agent/kitaru_agent.py"
    namespace: dict[str, Any] = {"__file__": str(launcher)}

    exec(compile(launcher.read_text(), str(launcher), "exec"), namespace)

    assert namespace["KitaruAgent"].SUPPORTS_ATIF is True
    assert namespace["_load_runtime"]()["trace_format"] == "atif"


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


@pytest.mark.parametrize(
    ("files", "expected_status", "expected_install"),
    [
        (
            {
                "pyproject.toml": (
                    '[project]\nname = "fixture-agent"\nversion = "1.0.0"\n'
                )
            },
            "declared",
            "uv sync --project /workspace --no-dev --no-editable",
        ),
        (
            {
                "pyproject.toml": (
                    '[project]\nname = "fixture-agent"\nversion = "1.0.0"\n'
                ),
                "uv.lock": "version = 1\n",
            },
            "locked",
            "uv sync --project /workspace --frozen --no-dev --no-editable",
        ),
        (
            {"requirements.txt": "httpx==0.28.1\n"},
            "declared",
            "uv pip install --python /workspace/.venv/bin/python",
        ),
        (
            {"requirements.txt": ("httpx==0.28.1 --hash=sha256:" + "a" * 64 + "\n")},
            "locked",
            "--require-hashes -r /workspace/requirements.txt",
        ),
    ],
)
def test_harbor_consumes_dependency_plan_without_reclassification(
    tmp_path: Path,
    files: dict[str, str],
    expected_status: str,
    expected_install: str,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "agent.py").write_text("print('agent')\n")
    for name, content in files.items():
        (source / name).write_text(content)

    output = tmp_path / "bundle"
    render_harbor(
        _resolved(source, session_count=1),
        output,
        trace_format="atif",
        trace_path="/workspace/trajectory.json",
    )

    dockerfile = (output / "agent_image/Dockerfile").read_text()
    manifest = json.loads((output / "kitaru-export.json").read_text())
    assert expected_install in dockerfile
    assert manifest["dependencies"]["status"] == expected_status


def test_harbor_preserves_workspace_dependency_install(tmp_path: Path) -> None:
    source = tmp_path / "source"
    (source / "packages/child").mkdir(parents=True)
    (source / "agent.py").write_text("print('agent')\n")
    (source / "pyproject.toml").write_text(
        """[project]
name = "fixture-agent"
version = "1.0.0"
dependencies = ["child==1.0.0"]

[tool.uv.sources]
child = { workspace = true }

[tool.uv.workspace]
members = ["packages/*"]
"""
    )
    (source / "packages/child/pyproject.toml").write_text(
        '[project]\nname = "child"\nversion = "1.0.0"\n'
    )

    output = tmp_path / "bundle"
    render_harbor(
        _resolved(source, session_count=1),
        output,
        trace_format="atif",
        trace_path="/workspace/trajectory.json",
    )

    assert (output / "agent_image/agent_source/packages/child/pyproject.toml").is_file()
    assert (
        "uv sync --project /workspace"
        in (output / "agent_image/Dockerfile").read_text()
    )


def test_harbor_keeps_command_argv_and_trace_path_as_inert_json(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "agent.py").write_text("print('agent')\n")
    resolved = replace(
        _resolved(source, session_count=1),
        command_argv=("python", "agent.py", "$(touch /tmp/not-run)", "line\nbreak"),
    )
    trace_path = "/workspace/trace'; touch /tmp/not-run; #.json"
    output = tmp_path / "bundle"

    render_harbor(
        resolved,
        output,
        trace_format="atif",
        trace_path=trace_path,
    )

    runtime = json.loads((output / "agent_image/agent-runtime.json").read_text())
    assert runtime["command_argv"] == list(resolved.command_argv)
    assert runtime["trace_path"] == trace_path
    generated_python = "".join(
        path.read_text()
        for path in (
            output / "agent/kitaru_agent.py",
            output / "agent_image/evaluate.py",
        )
    )
    assert "touch /tmp/not-run" not in generated_python
    assert "touch /tmp/not-run" not in (output / "agent_image/Dockerfile").read_text()


def test_harbor_bridge_and_bundle_bytes_are_deterministic(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "agent.py").write_text("print('agent')\n")
    resolved = _resolved(source, session_count=2)
    first = tmp_path / "first"
    second = tmp_path / "second"

    for output in (first, second):
        render_harbor(
            resolved,
            output,
            trace_format="atif",
            trace_path="/workspace/trajectory.json",
        )

    assert file_digests(first) == file_digests(second)
