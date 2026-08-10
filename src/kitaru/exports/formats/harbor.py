"""Render a Kitaru experiment as a Harbor 0.20 task dataset."""

import hashlib
import importlib.metadata
import json
import shlex
import tomllib
from pathlib import Path, PurePosixPath
from typing import Literal

from kitaru.api_models.v1.agent_version import RunSpec
from kitaru.api_models.v1.plugin import PackagePluginSource, ScriptPluginSource
from kitaru.exports.config import normalize_environment_names
from kitaru.exports.models import (
    ExportError,
    ExportManifest,
    ResolvedExport,
    ValidationReceipt,
)
from kitaru.exports.source import copy_source
from kitaru.exports.writer import (
    directory_digest,
    file_digest,
    file_digests,
    write_canonical_json,
)

HARBOR_VERSION = "0.20.0"
TASK_SCHEMA_VERSION = "1.3"
_IMAGE_PREFIX = "kitaru-export"
_SUPPORTED_TRACES = {"atif", "kitaru"}


def harbor_task_digest(task_dir: Path) -> str:
    """Compute Harbor's task package content digest.

    This mirrors ``harbor.publisher.packager.Packager`` without importing Harbor.
    Harbor packages only its native task files, not exporter metadata under
    ``inputs/``.
    """
    included: list[Path] = []
    for name in ("task.toml", "instruction.md", "README.md"):
        path = task_dir / name
        if path.is_file():
            included.append(path)
    for name in ("environment", "tests", "solution", "steps"):
        directory = task_dir / name
        if directory.is_dir():
            included.extend(path for path in directory.rglob("*") if path.is_file())

    digest = hashlib.sha256()
    for path in sorted(
        included, key=lambda item: item.relative_to(task_dir).as_posix()
    ):
        relative = path.relative_to(task_dir).as_posix()
        file_hash = file_digest(path)
        digest.update(f"{relative}\0{file_hash}\n".encode())
    return digest.hexdigest()


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _validate_trace_declaration(trace_format: str, trace_path: str) -> None:
    if trace_format not in _SUPPORTED_TRACES:
        raise ExportError(
            "unsupported_trace_format",
            "Harbor export requires an ATIF or Kitaru full-session trace.",
        )
    if not trace_path:
        raise ExportError("missing_trace_path", "Harbor export requires a trace path.")
    path = PurePosixPath(trace_path)
    if not path.is_absolute() or ".." in path.parts:
        raise ExportError(
            "invalid_trace_path", "Harbor trace path must be an absolute POSIX path."
        )


def _copy_bridge(destination: Path) -> None:
    source_dir = Path(__file__).parents[1]
    destination.mkdir(parents=True)
    (destination / "__init__.py").write_text("")
    for name in ("models.py", "trace.py", "evaluators.py"):
        (destination / name).write_bytes((source_dir / name).read_bytes())


def _write_evaluators(resolved: ResolvedExport, root: Path) -> list[str]:
    evaluator_dir = root / "evaluators"
    evaluator_dir.mkdir()
    requirements: list[str] = []
    metadata: list[dict[str, object]] = []
    for index, evaluator in enumerate(resolved.evaluators):
        script_path: str | None = None
        source = evaluator.version.source
        if isinstance(source, ScriptPluginSource):
            if evaluator.script is None:
                raise ExportError(
                    "missing_evaluator_source",
                    f"Evaluator {evaluator.name!r} has no materialized script.",
                )
            script_path = f"evaluator-{index}.py"
            (evaluator_dir / script_path).write_bytes(evaluator.script)
        elif isinstance(source, PackagePluginSource):
            requirements.append(source.requirement)
        else:
            raise ExportError(
                "unsupported_evaluator_source",
                f"Evaluator {evaluator.name!r} has an unsupported source.",
            )
        metadata.append(
            {
                "name": evaluator.name,
                "version": evaluator.version.model_dump(mode="json"),
                "params": evaluator.params,
                "script_path": script_path,
                "source_sha256": evaluator.source_sha256,
            }
        )
    write_canonical_json(root / "evaluators.json", metadata)
    return requirements


def _dockerfile(requirements: list[str]) -> str:
    kitaru_version = importlib.metadata.version("kitaru")
    install_requirements = " ".join(
        shlex.quote(requirement)
        for requirement in [f"kitaru=={kitaru_version}", *requirements]
    )
    return f"""FROM python:3.12-slim

WORKDIR /workspace
COPY agent_source/ /workspace/
RUN if [ -f pyproject.toml ] || [ -f setup.py ]; then \
      python -m pip install --no-cache-dir -e .; \
    elif [ -f requirements.txt ]; then \
      python -m pip install --no-cache-dir -r requirements.txt; \
    fi
RUN python -m pip install --no-cache-dir {install_requirements}
COPY bridge/ /opt/kitaru-export/bridge/
COPY evaluators/ /opt/kitaru-export/evaluators/
COPY evaluators.json evaluate.py /opt/kitaru-export/
ENV PYTHONPATH=/opt/kitaru-export
"""


def _agent_source(run_spec: RunSpec, trace_format: str, trace_path: str) -> str:
    command = json.dumps(run_spec.command)
    working_dir = json.dumps(
        f"/workspace/{run_spec.working_dir}" if run_spec.working_dir else "/workspace"
    )
    env = json.dumps(run_spec.env, sort_keys=True)
    emitted_name = (
        "trajectory.json" if trace_format == "atif" else "kitaru-session.json"
    )
    return f'''"""Harbor adapter for the exact Kitaru agent run specification."""

import shlex

from harbor.agents.base import BaseAgent


class KitaruAgent(BaseAgent):
    """Run the exported agent command inside Harbor's task environment."""

    SUPPORTS_ATIF = {trace_format == "atif"!r}

    @staticmethod
    def name() -> str:
        return "kitaru-export"

    def version(self) -> str:
        return "1"

    async def setup(self, environment) -> None:
        return None

    async def run(self, instruction, environment, context) -> None:
        env = {env}
        env.update(self.extra_env)
        env["KITARU_TASK_INPUTS"] = instruction
        env["KITARU_TRACE_PATH"] = {trace_path!r}
        result = await environment.exec(
            {command},
            cwd={working_dir},
            env=env,
            timeout_sec={run_spec.timeout_seconds},
        )
        if result.return_code != 0:
            detail = result.stderr or result.stdout or "no output"
            raise RuntimeError(f"Exported agent failed: {{detail}}")
        copied = await environment.exec(
            "mkdir -p /logs/agent && cp "
            + shlex.quote({trace_path!r})
            + " /logs/agent/{emitted_name}"
        )
        if copied.return_code != 0:
            raise RuntimeError("Exported agent did not produce its declared trace")
'''


_EVALUATE_SOURCE = '''"""Evaluate a Harbor trace with pinned evaluators."""

import json
import os
import sys
from pathlib import Path

from bridge.evaluators import evaluate_session, load_evaluator
from bridge.models import MaterializedEvaluator, RewardSelector
from bridge.trace import convert_trace
from kitaru.api_models.v1.evaluator import EvaluatorVersionResponse
from kitaru.api_models.v1.session_node import SessionWithNodesResponse


def main() -> None:
    task = json.loads(Path(sys.argv[1]).read_text())
    trace = json.loads(Path(sys.argv[2]).read_text())
    metadata = json.loads(Path("/opt/kitaru-export/evaluators.json").read_text())
    loaded = []
    for item in metadata:
        evaluator = MaterializedEvaluator(
            name=item["name"],
            version=EvaluatorVersionResponse.model_validate(item["version"]),
            params=item["params"],
            script=(
                (
                    Path("/opt/kitaru-export/evaluators") / item["script_path"]
                ).read_bytes()
                if item["script_path"]
                else None
            ),
            source_sha256=item["source_sha256"],
        )
        script_path = (
            Path("/opt/kitaru-export/evaluators") / item["script_path"]
            if item["script_path"]
            else None
        )
        loaded.append((evaluator, load_evaluator(evaluator, script_path=script_path)))
    selector = RewardSelector(**task["primary_reward"])
    secrets = [
        os.environ[name]
        for name in task["required_environment_names"]
        if name in os.environ
    ]
    session = convert_trace(
        trace,
        format=task["trace_format"],
        context=SessionWithNodesResponse.model_validate(task["context"]),
        secret_values=secrets,
    )
    outcome = evaluate_session(loaded, selector, session, secret_values=secrets)
    Path("/logs/verifier/reward.txt").write_text(str(outcome.reward))
    Path("/logs/verifier/metrics.json").write_text(
        json.dumps(outcome.metrics, sort_keys=True) + "\\n"
    )


if __name__ == "__main__":
    main()
'''


def _test_script(trace_format: str) -> str:
    trace_name = "trajectory.json" if trace_format == "atif" else "kitaru-session.json"
    return f"""#!/bin/sh
set -eu
trace=/logs/agent/{trace_name}
if [ ! -f \"$trace\" ]; then
  echo \"Missing declared agent trace: $trace\" >&2
  exit 1
fi
mkdir -p /logs/verifier
python /opt/kitaru-export/evaluate.py /tests/task.json \"$trace\"
"""


def _task_toml(
    *,
    task_name: str,
    image: str,
    timeout: int,
    required_environment_names: tuple[str, ...],
) -> str:
    lines = [
        f'schema_version = "{TASK_SCHEMA_VERSION}"',
        "",
        "[task]",
        f"name = {_toml_string(task_name)}",
        'description = "Frozen Kitaru cohort case"',
        "authors = []",
        'keywords = ["kitaru", "evaluation"]',
        "",
        "[agent]",
        f"timeout_sec = {float(timeout)}",
        "",
        "[verifier]",
        f"timeout_sec = {float(timeout)}",
        "",
        "[environment]",
        f"docker_image = {_toml_string(image)}",
        'workdir = "/workspace"',
    ]
    if required_environment_names:
        lines.extend(["", "[environment.env]"])
        lines.extend(
            f"{_toml_string(name)} = {_toml_string('${' + name + '}')}"
            for name in required_environment_names
        )
    return "\n".join(lines) + "\n"


def render_harbor(
    resolved: ResolvedExport,
    destination: Path,
    *,
    trace_format: Literal["atif", "kitaru"] | str,
    trace_path: str,
    required_environment_names: tuple[str, ...] = (),
) -> ExportManifest:
    """Render a complete Harbor dataset into an empty destination directory."""
    _validate_trace_declaration(trace_format, trace_path)
    if destination.exists() and (
        not destination.is_dir() or next(destination.iterdir(), None) is not None
    ):
        raise ExportError(
            "destination_conflict", f"Destination is not empty: {destination}"
        )
    run_spec = resolved.agent_version.run_spec
    if run_spec is None:
        raise ExportError("missing_run_spec", "Agent version has no run specification.")
    try:
        required_environment_names = normalize_environment_names(
            required_environment_names
        )
    except ValueError as error:
        raise ExportError(
            "invalid_environment_name",
            "Required environment names must be unique identifiers.",
        ) from error

    destination.mkdir(parents=True, exist_ok=True)
    agent_image = destination / "agent_image"
    copy_source(resolved.source, agent_image / "agent_source")
    _copy_bridge(agent_image / "bridge")
    requirements = _write_evaluators(resolved, agent_image)
    (agent_image / "evaluate.py").write_text(_EVALUATE_SOURCE)
    (agent_image / "Dockerfile").write_text(_dockerfile(requirements))

    agent_dir = destination / "agent"
    agent_dir.mkdir()
    (agent_dir / "__init__.py").write_text("")
    (agent_dir / "kitaru_agent.py").write_text(
        _agent_source(run_spec, trace_format, trace_path)
    )

    image = f"{_IMAGE_PREFIX}:{directory_digest(agent_image)[:12]}"
    dataset_dir = destination / "dataset"
    dataset_dir.mkdir()
    references: list[tuple[str, str]] = []
    for index, session in enumerate(resolved.sessions, start=1):
        task_id = f"task-{index:05d}-{str(session.session.id)[:8]}"
        task_name = f"kitaru/{task_id}"
        task_dir = dataset_dir / task_id
        (task_dir / "tests").mkdir(parents=True)
        (task_dir / "inputs").mkdir()
        inputs = session.session.inputs
        instruction = json.dumps(
            inputs, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        (task_dir / "instruction.md").write_text(instruction + "\n")
        task_data = {
            "context": session.model_dump(mode="json"),
            "primary_reward": {
                "evaluator": resolved.reward.evaluator,
                "result": resolved.reward.result,
                "field": resolved.reward.field,
            },
            "required_environment_names": list(required_environment_names),
            "trace_format": trace_format,
        }
        write_canonical_json(task_dir / "inputs/task.json", task_data)
        write_canonical_json(task_dir / "tests/task.json", task_data)
        (task_dir / "task.toml").write_text(
            _task_toml(
                task_name=task_name,
                image=image,
                timeout=run_spec.timeout_seconds,
                required_environment_names=required_environment_names,
            )
        )
        test_path = task_dir / "tests/test.sh"
        test_path.write_text(_test_script(trace_format))
        test_path.chmod(0o755)
        references.append((task_name, harbor_task_digest(task_dir)))

    dataset_name = f"kitaru/experiment-{str(resolved.experiment.id)[:8]}"
    dataset_lines = [
        "[dataset]",
        f"name = {_toml_string(dataset_name)}",
        f"description = {_toml_string('Frozen Kitaru cohort export')}",
        "authors = []",
        'keywords = ["kitaru", "evaluation"]',
    ]
    for name, digest in references:
        dataset_lines.extend(
            [
                "",
                "[[tasks]]",
                f"name = {_toml_string(name)}",
                f'digest = "sha256:{digest}"',
            ]
        )
    (dataset_dir / "dataset.toml").write_text("\n".join(dataset_lines) + "\n")

    run_command = "harbor run -p dataset --agent agent.kitaru_agent:KitaruAgent"
    readme = f"""# Kitaru Harbor export

This artifact targets Harbor {HARBOR_VERSION}. Build the shared sandbox image,
then run the frozen dataset from this directory:

```bash
docker build -t {image} agent_image
{run_command}
```

The agent must write its {trace_format} trace to `{trace_path}`. Missing or
malformed traces and evaluator failures fail the task. There is no fallback reward.
"""
    if required_environment_names:
        readme += (
            "\nRequired environment variables: "
            + ", ".join(required_environment_names)
            + ".\n"
        )
    (destination / "README.md").write_text(readme)

    manifest = ExportManifest(
        format="harbor",
        target_version=HARBOR_VERSION,
        experiment_id=resolved.experiment.id,
        cohort_version_id=resolved.cohort_version.id,
        agent_version_id=resolved.agent_version.id,
        evaluator_version_ids=tuple(item.version.id for item in resolved.evaluators),
        primary_reward=(
            f"{resolved.reward.evaluator}:{resolved.reward.result}:{resolved.reward.field}"
        ),
        source_digest=resolved.source.digest,
        generated_files=file_digests(destination),
        required_environment_names=required_environment_names,
        exclusions=resolved.source.excluded,
        validation=ValidationReceipt(
            level="structural", status="passed", target_version=HARBOR_VERSION
        ),
    )
    write_canonical_json(
        destination / "kitaru-export.json", manifest.model_dump(mode="json")
    )
    validate_harbor(destination)
    return manifest


def validate_harbor(root: Path) -> None:
    """Validate the generated Harbor structure without importing executable code."""
    required = [
        root / "README.md",
        root / "kitaru-export.json",
        root / "agent/kitaru_agent.py",
        root / "agent_image/Dockerfile",
        root / "dataset/dataset.toml",
    ]
    missing = [str(path.relative_to(root)) for path in required if not path.is_file()]
    if missing:
        raise ExportError(
            "invalid_harbor_bundle", f"Missing files: {', '.join(missing)}"
        )
    try:
        manifest = json.loads((root / "kitaru-export.json").read_text())
        dataset = tomllib.loads((root / "dataset/dataset.toml").read_text())
    except (json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
        raise ExportError(
            "invalid_harbor_bundle", f"Invalid manifest: {error}"
        ) from error
    if (
        manifest.get("format") != "harbor"
        or manifest.get("target_version") != HARBOR_VERSION
    ):
        raise ExportError(
            "invalid_harbor_bundle", "Manifest target is not Harbor 0.20.0."
        )
    references = dataset.get("tasks")
    if not isinstance(references, list) or not references:
        raise ExportError("invalid_harbor_bundle", "Dataset contains no tasks.")
    tasks = sorted(path for path in (root / "dataset").iterdir() if path.is_dir())
    if len(tasks) != len(references):
        raise ExportError(
            "invalid_harbor_bundle", "Dataset task count does not match its references."
        )
    by_name = {
        reference.get("name"): reference.get("digest") for reference in references
    }
    for task in tasks:
        try:
            config = tomllib.loads((task / "task.toml").read_text())
            task_data = json.loads((task / "tests/task.json").read_text())
        except (OSError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
            raise ExportError(
                "invalid_harbor_bundle", f"Invalid task {task.name}: {error}"
            ) from error
        name = config.get("task", {}).get("name")
        if config.get("schema_version") != TASK_SCHEMA_VERSION:
            raise ExportError(
                "invalid_harbor_bundle", f"Task {task.name} does not use schema 1.3."
            )
        if by_name.get(name) != f"sha256:{harbor_task_digest(task)}":
            raise ExportError(
                "invalid_harbor_bundle", f"Task {task.name} digest does not match."
            )
        if (
            not (task / "instruction.md").is_file()
            or not (task / "tests/test.sh").is_file()
        ):
            raise ExportError(
                "invalid_harbor_bundle", f"Task {task.name} is incomplete."
            )
        if task_data != json.loads((task / "inputs/task.json").read_text()):
            raise ExportError(
                "invalid_harbor_bundle", f"Task {task.name} input copies differ."
            )
