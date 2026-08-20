"""Tests for the complete Verifiers 0.3 v1 export renderer."""

import ast
import builtins
import hashlib
import json
import sys
import tomllib
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

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
from kitaru.exports import (
    ExporterContext,
    ExporterOptions,
    ExporterProvenance,
    ExportManifest,
)
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
from kitaru_verifiers_exporter import (
    PRIME_RL_VERSION,
    VERIFIERS_VERSION,
    VerifiersExporter,
    create_exporter,
    validate_verifiers_v1,
)
from kitaru_verifiers_exporter import exporter as verifiers_v1

_EXPORTER = VerifiersExporter()


def _get_context(checkpoint: Any = lambda: None) -> ExporterContext:
    return ExporterContext(
        exporter=ExporterProvenance.model_validate(_EXPORTER.metadata.model_dump()),
        cancellation_checkpoint=checkpoint,
    )


def render_verifiers_v1(
    resolved: ResolvedExport,
    destination: Path,
    *,
    required_environment_names: tuple[str, ...] = (),
    context: ExporterContext | None = None,
) -> ExportManifest:
    """Invoke the Verifiers renderer through the installed exporter contract."""
    return _EXPORTER.render(
        resolved,
        destination,
        options=ExporterOptions(
            required_environment_names=required_environment_names,
        ),
        context=context or _get_context(),
    )


_PUBLIC_TASK_KEYS = {
    "idx",
    "inputs",
    "kitaru_content_digest",
    "kitaru_session_id",
    "name",
    "prompt",
}
_HISTORICAL_SENTINELS = {
    "HISTORICAL_ANSWER",
    "HISTORICAL_REASONING",
    "HISTORICAL_TOOL_RESULT",
    "HISTORICAL_EVALUATOR_RESULT",
}


def _resolved(
    source_root: Path,
    *,
    session_count: int = 1,
    reverse_sessions: bool = False,
    agent_version_id: int = 12,
    question_prefix: str = "case",
    required_environment_names: tuple[str, ...] = (),
) -> ResolvedExport:
    source_root.mkdir()
    (source_root / "agent.py").write_text("print('agent')\n")
    (source_root / "pyproject.toml").write_text(
        '[project]\nname = "fixture-agent"\nversion = "1.0.0"\n'
    )
    now = datetime(2026, 1, 1, tzinfo=UTC)
    sessions = [
        SessionWithNodesResponse(
            session=SessionResponse(
                id=uuid.UUID(int=1000 + index),
                owner_id=uuid.UUID(int=21),
                agent_id=uuid.UUID(int=22),
                agent_version_id=uuid.UUID(int=999),
                number=index + 1,
                origin=SessionOrigin.RECORDED,
                status=SessionStatus.COMPLETED,
                inputs={"question": f"{question_prefix} {index}"},
                outputs={"answer": "HISTORICAL_ANSWER"},
                metadata={
                    "reasoning": "HISTORICAL_REASONING",
                    "tool_result": "HISTORICAL_TOOL_RESULT",
                    "evaluator_result": "HISTORICAL_EVALUATOR_RESULT",
                },
                llm_call_count=0,
                tool_call_count=0,
                created=now,
                updated=now,
            ),
            nodes=[],
        )
        for index in range(session_count)
    ]
    if reverse_sessions:
        sessions.reverse()
    script = (
        b"def evaluate(session):\n    return {'name': 'correctness', 'score': 1.0}\n"
    )
    evaluator = MaterializedEvaluator(
        name="quality",
        version=EvaluatorVersionResponse(
            id=uuid.UUID(int=30),
            evaluator_id=uuid.UUID(int=31),
            version=2,
            display_version="2",
            source=ScriptPluginSource(blob_id=uuid.UUID(int=32), entrypoint="evaluate"),
            created=now,
            updated=now,
        ),
        params={"strict": True},
        script=script,
        source_sha256=hashlib.sha256(script).hexdigest(),
    )
    source = inventory_source(source_root)
    return ResolvedExport(
        experiment=ExperimentResponse.model_construct(id=uuid.UUID(int=10)),
        cohort_version=CohortVersionResponse.model_construct(id=uuid.UUID(int=11)),
        agent_version=AgentVersionResponse.model_construct(
            id=uuid.UUID(int=agent_version_id),
            run_spec=RunSpec(
                command="python agent.py --mode solve",
                working_dir=None,
                env={"MODE": "eval"},
                timeout_seconds=90,
            ),
        ),
        sessions=tuple(sessions),
        evaluators=(evaluator,),
        reward=RewardSelector.parse("quality:correctness:score"),
        source=source,
        command_argv=("python", "agent.py", "--mode", "solve"),
        required_environment_names=required_environment_names,
        dependency_plan=classify_dependencies(source),
    )


def _module(root: Path) -> tuple[dict[str, Any], Path]:
    manifest = json.loads((root / "kitaru-export.json").read_text())
    return manifest, root / manifest["provenance"]["module_name"]


def _tasks(module: Path) -> list[dict[str, object]]:
    return [
        json.loads(line) for line in (module / "tasks.jsonl").read_text().splitlines()
    ]


def _all_files(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def test_render_writes_one_complete_collision_safe_plugin(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    result = render_verifiers_v1(_resolved(tmp_path / "source"), root)
    manifest, module = _module(root)
    provenance = manifest["provenance"]

    assert result.target_version == VERIFIERS_VERSION
    assert manifest["exporter"] == _EXPORTER.metadata.model_dump(mode="json")
    assert provenance["plugin_id"] == provenance["module_name"]
    assert provenance["plugin_id"].startswith("kitaru_verifiers_")
    assert provenance["distribution_name"].startswith("kitaru-verifiers-")
    assert provenance["artifact_digest"].startswith(
        provenance["plugin_id"].removeprefix("kitaru_verifiers_")
    )
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    assert project["name"] == provenance["distribution_name"]
    assert project["requires-python"] == ">=3.12,<3.14"
    assert f"verifiers=={VERIFIERS_VERSION}" in project["dependencies"]
    assert f"kitaru=={version('kitaru')}" in project["dependencies"]
    assert (module / "agent_source/agent.py").read_text() == "print('agent')\n"
    assert (module / "bridge/runtime.py").is_file()
    assert (module / "scoring/evaluators/evaluator-0.py").is_file()


def test_verifiers_exporter_factory_reports_installed_contract() -> None:
    exporter = create_exporter()

    assert isinstance(exporter, VerifiersExporter)
    assert exporter.metadata.contract_version == 1
    assert exporter.metadata.distribution_name == "kitaru-verifiers-exporter"
    assert exporter.metadata.distribution_version == "0.1.0"
    assert exporter.metadata.format == "verifiers-v1"
    assert exporter.metadata.target_version == VERIFIERS_VERSION


def test_verifiers_exporter_honors_cancellation_before_writing(
    tmp_path: Path,
) -> None:
    def cancel() -> None:
        raise RuntimeError("cancelled")

    destination = tmp_path / "bundle"
    with pytest.raises(RuntimeError, match="cancelled"):
        render_verifiers_v1(
            _resolved(tmp_path / "source"),
            destination,
            context=_get_context(cancel),
        )

    assert not destination.exists()


def test_verifiers_exporter_cancels_inside_task_loop(tmp_path: Path) -> None:
    calls = 0

    def cancel_during_render() -> None:
        nonlocal calls
        calls += 1
        if calls == 8:
            raise RuntimeError("cancelled during render")

    with pytest.raises(RuntimeError, match="cancelled during render"):
        render_verifiers_v1(
            _resolved(tmp_path / "source", session_count=20),
            tmp_path / "bundle",
            context=_get_context(cancel_during_render),
        )


def test_verifiers_readme_records_effective_policies(tmp_path: Path) -> None:
    resolved = replace(
        _resolved(tmp_path / "source"),
        content_policy=ContentPolicy(omit=("visible_reasoning",)),
        environment_policy=EnvironmentPolicy(mode="runtime_only"),
        source_policy=SourcePolicy(include=("agent.py",), exclude=("build/cache",)),
    )
    root = tmp_path / "bundle"

    render_verifiers_v1(resolved, root)

    readme = (root / "README.md").read_text()
    assert "Content omissions: visible_reasoning." in readme
    assert "Registered environment handling: runtime_only." in readme
    assert "Explicit source includes: agent.py." in readme
    assert "Explicit source exclusions: build/cache." in readme


@pytest.mark.parametrize("session_count", [1, 7, 1000])
def test_one_taskset_and_harness_cover_every_session(
    tmp_path: Path, session_count: int
) -> None:
    root = tmp_path / "bundle"
    render_verifiers_v1(
        _resolved(tmp_path / "source", session_count=session_count), root
    )
    _, module = _module(root)

    assert len(_tasks(module)) == session_count
    source = (module / "plugin.py").read_text()
    assert source.count("class KitaruTaskset(") == 1
    assert source.count("class KitaruHarness(") == 1
    assert source.count("class KitaruTask(") == 1


def test_taskdata_is_public_and_scoring_is_private(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    render_verifiers_v1(
        _resolved(tmp_path / "source"),
        root,
        required_environment_names=("SCORING_TOKEN",),
    )
    manifest, module = _module(root)
    task = _tasks(module)[0]

    assert set(task) == _PUBLIC_TASK_KEYS
    assert task["inputs"] == {"question": "case 0"}
    public_bytes = (module / "tasks.jsonl").read_text()
    config_bytes = (root / "eval.toml").read_text() + (
        root / "prime-rl.toml"
    ).read_text()
    runtime = json.loads((module / "runtime.json").read_text())
    assert runtime["agent_source"] == "agent_source"
    for sentinel in _HISTORICAL_SENTINELS:
        assert sentinel not in public_bytes
        assert sentinel not in config_bytes
        assert sentinel not in json.dumps(runtime)
    assert "SCORING_TOKEN" not in public_bytes
    private = json.loads(
        (module / "scoring/tasks" / f"{task['kitaru_session_id']}.json").read_text()
    )
    assert private["bridge_task"]["context"]["session"]["outputs"] == {
        "answer": "HISTORICAL_ANSWER"
    }
    assert private["bridge_task"]["primary_reward"]["evaluator"] == "quality"
    assert manifest["runtime_requirements"]["task_private"] == ["SCORING_TOKEN"]


def test_scoring_reads_requirements_from_private_bridge_task(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    render_verifiers_v1(
        _resolved(tmp_path / "source"),
        root,
        required_environment_names=("SCORING_TOKEN",),
    )
    _, module = _module(root)

    scoring_source = (module / "scoring.py").read_text()
    assert 'task["bridge_task"]["required_environment_names"]' in scoring_source
    assert 'task["required_environment_names"]' not in scoring_source


@pytest.mark.parametrize(
    ("payload", "expected_ok"),
    [
        ({"is_completed": True, "ok": False, "errors": []}, True),
        (
            {
                "is_completed": True,
                "ok": False,
                "errors": [{"type": "HarnessError", "message": "failed"}],
            },
            False,
        ),
        ({"is_completed": False, "ok": False, "errors": []}, False),
    ],
)
def test_scoring_normalizes_only_verifiers_close_transient(
    tmp_path: Path, payload: dict[str, Any], expected_ok: bool
) -> None:
    root = tmp_path / "bundle"
    render_verifiers_v1(_resolved(tmp_path / "source"), root)
    _, module = _module(root)
    tree = ast.parse((module / "plugin.py").read_text())
    helper = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_trace_for_scoring"
    )
    namespace: dict[str, Any] = {
        "Any": Any,
        "vf": type("FakeVerifiers", (), {"Trace": object}),
    }
    exec(
        compile(
            ast.fix_missing_locations(ast.Module(body=[helper], type_ignores=[])),
            "generated-plugin.py",
            "exec",
        ),
        namespace,
    )
    trace = type(
        "FakeTrace",
        (),
        {"model_dump": lambda self, **kwargs: dict(payload)},
    )()

    normalized = namespace["_trace_for_scoring"](trace)

    assert normalized["ok"] is expected_ok
    assert normalized["errors"] == payload["errors"]


def test_shuffled_sessions_reproduce_full_taskdata_digests_and_bytes(
    tmp_path: Path,
) -> None:
    forward = _resolved(tmp_path / "source-a", session_count=12)
    reverse = _resolved(tmp_path / "source-b", session_count=12, reverse_sessions=True)
    first = tmp_path / "first"
    second = tmp_path / "second"

    render_verifiers_v1(forward, first)
    render_verifiers_v1(reverse, second)
    first_manifest, first_module = _module(first)
    second_manifest, second_module = _module(second)

    assert (first_module / "tasks.jsonl").read_bytes() == (
        second_module / "tasks.jsonl"
    ).read_bytes()
    assert [task["idx"] for task in _tasks(first_module)] == list(range(12))
    assert first_manifest["task_provenance"] == second_manifest["task_provenance"]
    assert first_manifest["provenance"] == second_manifest["provenance"]
    assert _all_files(first) == _all_files(second)


def test_identity_matrix_separates_benchmark_harness_and_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = _resolved(tmp_path / "source-a")
    new_harness = _resolved(tmp_path / "source-b", agent_version_id=13)
    changed_session = _resolved(tmp_path / "source-c", question_prefix="changed")
    roots = [tmp_path / name for name in ("base", "harness", "benchmark")]
    for resolved, root in zip((base, new_harness, changed_session), roots, strict=True):
        render_verifiers_v1(resolved, root)
    provenances = [_module(root)[0]["provenance"] for root in roots]
    original, harness, benchmark = provenances

    assert harness["benchmark_digest"] == original["benchmark_digest"]
    assert harness["runtime_bundle_digest"] == original["runtime_bundle_digest"]
    assert harness["default_harness_digest"] != original["default_harness_digest"]
    assert benchmark["benchmark_digest"] != original["benchmark_digest"]
    assert benchmark["default_harness_digest"] == original["default_harness_digest"]
    assert benchmark["runtime_bundle_digest"] == original["runtime_bundle_digest"]

    monkeypatch.setattr(
        verifiers_v1,
        "_RUNTIME_TEMPLATE_VERSION",
        verifiers_v1._RUNTIME_TEMPLATE_VERSION + 1,
    )
    changed_runtime_root = tmp_path / "runtime"
    render_verifiers_v1(base, changed_runtime_root)
    changed_runtime = _module(changed_runtime_root)[0]["provenance"]
    assert changed_runtime["benchmark_digest"] == original["benchmark_digest"]
    assert (
        changed_runtime["default_harness_digest"] == original["default_harness_digest"]
    )
    assert changed_runtime["runtime_bundle_digest"] != original["runtime_bundle_digest"]
    assert changed_runtime["artifact_digest"] != original["artifact_digest"]


def test_distinct_artifacts_have_side_by_side_safe_names(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    render_verifiers_v1(_resolved(tmp_path / "source-a"), first)
    render_verifiers_v1(
        _resolved(tmp_path / "source-b", question_prefix="other"), second
    )
    one = _module(first)[0]["provenance"]
    two = _module(second)[0]["provenance"]

    assert one["artifact_digest"] != two["artifact_digest"]
    assert one["plugin_id"] != two["plugin_id"]
    assert one["distribution_name"] != two["distribution_name"]
    assert one["module_name"] != two["module_name"]


def test_task_private_requirements_change_native_identity(tmp_path: Path) -> None:
    resolved = _resolved(tmp_path / "source")
    first = tmp_path / "first"
    second = tmp_path / "second"
    render_verifiers_v1(resolved, first, required_environment_names=("SCORING_TOKEN",))
    render_verifiers_v1(
        resolved, second, required_environment_names=("OTHER_SCORING_TOKEN",)
    )

    one = _module(first)[0]["provenance"]
    two = _module(second)[0]["provenance"]
    assert one["benchmark_digest"] != two["benchmark_digest"]
    assert one["artifact_digest"] != two["artifact_digest"]
    assert one["plugin_id"] != two["plugin_id"]


def test_eval_and_primerl_configs_select_same_docker_composition(
    tmp_path: Path,
) -> None:
    root = tmp_path / "bundle"
    render_verifiers_v1(
        _resolved(tmp_path / "source", required_environment_names=("AGENT_TOKEN",)),
        root,
    )
    manifest, module = _module(root)
    plugin_id = manifest["provenance"]["plugin_id"]
    evaluation = tomllib.loads((root / "eval.toml").read_text())
    training = tomllib.loads((root / "prime-rl.toml").read_text())

    assert evaluation["env"]["taskset"] == {"id": plugin_id}
    assert evaluation["env"]["agent"]["harness"] == {
        "id": plugin_id,
        "forward_env": ["AGENT_TOKEN"],
    }
    assert evaluation["env"]["agent"]["runtime"]["type"] == "docker"
    assert evaluation["env"]["agent"]["timeout"] == {
        "rollout": 90,
        "scoring": 330,
    }
    source = training["orchestrator"]["train"]["source"]
    assert len(source) == 1
    assert source[0]["env"] == evaluation["env"]
    assert PRIME_RL_VERSION in (root / "README.md").read_text()
    assert all(
        forbidden not in training
        for forbidden in ("model", "optimizer", "hardware", "trainer")
    )
    plugin_source = (module / "plugin.py").read_text()
    assert "NEEDS_CONTAINER = True" in plugin_source
    override = json.loads(json.dumps(evaluation))
    override["env"]["agent"]["harness"] = {"id": "independent_harness"}
    assert override["env"]["taskset"] == evaluation["env"]["taskset"]
    assert "without falling back" in (root / "README.md").read_text()


def test_requirement_ownership_is_composition_dependent(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    render_verifiers_v1(
        _resolved(tmp_path / "source", required_environment_names=("AGENT_TOKEN",)),
        root,
        required_environment_names=("SCORING_TOKEN",),
    )
    manifest, module = _module(root)
    requirements = manifest["runtime_requirements"]

    assert requirements == {
        "all": ["AGENT_TOKEN", "SCORING_TOKEN"],
        "bundled_harness": ["AGENT_TOKEN"],
        "task_private": ["SCORING_TOKEN"],
    }
    assert json.loads((module / "scoring/requirements.json").read_text()) == [
        "SCORING_TOKEN"
    ]
    assert tomllib.loads((root / "eval.toml").read_text())["env"]["agent"]["harness"][
        "forward_env"
    ] == ["AGENT_TOKEN"]
    assert all(
        name not in (module / "tasks.jsonl").read_text() for name in requirements["all"]
    )


def test_harness_uses_shell_free_argv_and_container_dependency_setup(
    tmp_path: Path,
) -> None:
    resolved = _resolved(tmp_path / "source")
    resolved = replace(
        resolved,
        command_argv=("python", "agent.py", "quote'", "$(never-executed)"),
    )
    root = tmp_path / "bundle"
    render_verifiers_v1(resolved, root)
    _, module = _module(root)
    runtime = json.loads((module / "runtime.json").read_text())
    plugin = (module / "plugin.py").read_text()

    assert runtime["command_argv"] == list(resolved.command_argv)
    assert runtime["setup_argv"] == [
        [
            "uv",
            "sync",
            "--project",
            "/workspace/kitaru-agent",
            "--no-dev",
            "--no-editable",
        ]
    ]
    assert "runtime.run_program(" in plugin
    assert '"/usr/bin/env"' in plugin
    assert '"-C"' in plugin
    assert "sh -lc" not in plugin
    assert '["sh"' not in plugin
    assert "agent_source" in plugin
    assert ' / "scoring"' not in plugin.split("async def setup", 1)[1]


def test_generated_bridge_is_self_contained_and_all_is_exact(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    render_verifiers_v1(_resolved(tmp_path / "source"), root)
    _, module = _module(root)
    init = (module / "__init__.py").read_text()
    exported = ast.literal_eval(
        next(
            node.value
            for node in ast.parse(init).body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "__all__"
                for target in node.targets
            )
        )
    )

    assert exported == ["KitaruHarness", "KitaruTaskset"]
    assert "Judge" not in init
    assert "Rubric" not in init
    generated_python = "".join(path.read_text() for path in module.rglob("*.py"))
    assert "kitaru.exports" not in generated_python
    assert "from kitaru.api_models" in generated_python
    assert "run_evaluator_process" in (module / "scoring.py").read_text()
    assert '"-m"' in (module / "scoring.py").read_text()


def test_structural_validation_does_not_import_exported_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "bundle"
    render_verifiers_v1(_resolved(tmp_path / "source"), root)
    _, module = _module(root)
    generated_package = module.name
    generated_root = root.resolve()
    original_import = builtins.__import__

    def reject_generated_import(
        name: str,
        globals: dict[str, Any] | None = None,
        locals: dict[str, Any] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> Any:
        package = globals.get("__package__") if globals is not None else None
        module_name = globals.get("__name__") if globals is not None else None
        module_file = globals.get("__file__") if globals is not None else None
        if name == generated_package or name.startswith(f"{generated_package}."):
            raise AssertionError("structural validation imported generated code")
        generated_module = any(
            value == generated_package
            or (isinstance(value, str) and value.startswith(f"{generated_package}."))
            for value in (package, module_name)
        )
        generated_file = isinstance(module_file, str) and Path(
            module_file
        ).resolve().is_relative_to(generated_root)
        if generated_module or generated_file:
            raise AssertionError("structural validation imported generated code")
        return original_import(name, globals, locals, fromlist, level)

    with pytest.raises(AssertionError, match="imported generated code"):
        reject_generated_import(
            "verifiers.v1",
            {
                "__name__": "loaded_under_an_alias",
                "__file__": str(module / "plugin.py"),
            },
        )

    with monkeypatch.context() as import_guard:
        import_guard.setattr(builtins, "__import__", reject_generated_import)
        receipt = validate_verifiers_v1(root)

    assert receipt.status == "passed"
    assert receipt.level == "structural"
    assert generated_package not in sys.modules
    assert not any(name.startswith(f"{generated_package}.") for name in sys.modules)


@pytest.mark.parametrize(
    "tamper",
    [
        "taskdata_evidence",
        "noncontiguous_index",
        "config_runtime",
        "bridge_bytes",
    ],
)
def test_structural_validation_rejects_tampering(tmp_path: Path, tamper: str) -> None:
    root = tmp_path / "bundle"
    render_verifiers_v1(_resolved(tmp_path / "source"), root)
    manifest, module = _module(root)
    if tamper in {"taskdata_evidence", "noncontiguous_index"}:
        task = _tasks(module)[0]
        if tamper == "taskdata_evidence":
            task["historical_output"] = "leak"
        else:
            task["idx"] = 8
        (module / "tasks.jsonl").write_text(json.dumps(task) + "\n")
    elif tamper == "config_runtime":
        (root / "eval.toml").write_text(
            (root / "eval.toml")
            .read_text()
            .replace('type = "docker"', 'type = "subprocess"')
        )
    else:
        bridge_file = next(
            module / "bridge" / path
            for path in manifest["runtime_bridge"]["files"]
            if path != "__init__.py"
        )
        bridge_file.write_text(bridge_file.read_text() + "\n# tampered\n")

    with pytest.raises(ExportError) as raised:
        validate_verifiers_v1(root)

    assert raised.value.code == "invalid_verifiers_bundle"


def test_render_accepts_empty_staging_and_rejects_invalid_inputs(
    tmp_path: Path,
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    resolved = _resolved(tmp_path / "source")
    render_verifiers_v1(resolved, staging)

    occupied = tmp_path / "occupied"
    occupied.mkdir()
    (occupied / "user.txt").write_text("keep")
    with pytest.raises(ExportError) as raised:
        render_verifiers_v1(resolved, occupied)
    assert raised.value.code == "invalid_destination"
    assert (occupied / "user.txt").read_text() == "keep"

    for names in (("TOKEN", "TOKEN"), ("BAD-NAME",), ("1TOKEN",)):
        with pytest.raises(ExportError) as environment_error:
            render_verifiers_v1(
                resolved,
                tmp_path / ("invalid-" + str(len(names)) + names[0][:1]),
                required_environment_names=names,
            )
        assert environment_error.value.code == "invalid_environment_name"

    with pytest.raises(ExportError) as command_error:
        render_verifiers_v1(
            replace(resolved, command_argv=()), tmp_path / "missing-command"
        )
    assert command_error.value.code == "missing_run_command"
    with pytest.raises(ExportError) as dependency_error:
        render_verifiers_v1(
            replace(resolved, dependency_plan=None), tmp_path / "missing-dependencies"
        )
    assert dependency_error.value.code == "missing_dependency_plan"
