"""Render a Kitaru experiment as a Harbor 0.20 task dataset."""

import hashlib
import json
import tomllib
from pathlib import Path, PurePosixPath
from typing import Literal

from kitaru.api_models.v1.plugin import PackagePluginSource, ScriptPluginSource
from kitaru.exports._bridge import (
    get_runtime_bridge_version,
    materialize_runtime_bridge,
)
from kitaru.exports.config import normalize_environment_names
from kitaru.exports.formats._validation import (
    validate_generated_resources,
    validate_kitaru_requirement,
    validate_runtime_bridge,
)
from kitaru.exports.models import (
    V1_EXPORT_BUDGETS,
    DependencyPlan,
    DependencyReceipt,
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
SCORING_TIMEOUT_SECONDS = 300
_VERIFIER_TIMEOUT_OVERHEAD_SECONDS = 30
_MAX_EVALUATOR_OUTPUT_BYTES = 64 * 1024
_UV_VERSION = "0.12.1"
_IMAGE_PREFIX = "kitaru-export"
_SUPPORTED_TRACES = {"atif", "kitaru"}


def harbor_task_digest(task_dir: Path) -> str:
    """Compute the local structural digest written to the Harbor dataset.

    Exact compatibility is proved separately against Harbor's pinned Packager. This
    local implementation is only a download-free structural consistency check.
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


def _write_evaluators(
    resolved: ResolvedExport, root: Path, *, kitaru_version: str
) -> None:
    evaluator_dir = root / "evaluators"
    evaluator_dir.mkdir()
    requirements: list[str] = []
    metadata: list[dict[str, object]] = []
    total_script_bytes = 0
    for index, evaluator in enumerate(
        sorted(resolved.evaluators, key=lambda item: item.name)
    ):
        script_path: str | None = None
        source = evaluator.version.source
        if isinstance(source, ScriptPluginSource):
            if evaluator.script is None:
                raise ExportError(
                    "missing_evaluator_source",
                    f"Evaluator {evaluator.name!r} has no materialized script.",
                )
            total_script_bytes += len(evaluator.script)
            if len(evaluator.script) > V1_EXPORT_BUDGETS.max_evaluator_bytes:
                raise ExportError(
                    "evaluator_too_large",
                    "An evaluator source blob exceeds the 10 MiB export limit.",
                )
            if total_script_bytes > V1_EXPORT_BUDGETS.max_total_evaluator_bytes:
                raise ExportError(
                    "evaluators_too_large",
                    "Evaluator source blobs exceed the 100 MiB aggregate export limit.",
                )
            script_path = f"evaluator-{index}.py"
            (evaluator_dir / script_path).write_bytes(evaluator.script)
        elif isinstance(source, PackagePluginSource):
            if any(character in source.requirement for character in "\r\n\0"):
                raise ExportError(
                    "invalid_dependency_metadata",
                    "Evaluator requirements must be one inert requirement per line.",
                )
            validate_kitaru_requirement(
                source.requirement,
                kitaru_version,
                subject="An evaluator",
            )
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
    (root / "evaluator-requirements.txt").write_text(
        "".join(f"{requirement}\n" for requirement in sorted(requirements))
    )


def _dependency_install(plan: DependencyPlan) -> str:
    if plan.manifests == ("pyproject.toml", "uv.lock") and plan.status == "locked":
        return "RUN uv sync --project /workspace --frozen --no-dev --no-editable"
    if plan.manifests == ("pyproject.toml",) and plan.status == "declared":
        return "RUN uv sync --project /workspace --no-dev --no-editable"
    if plan.manifests == ("requirements.txt",):
        hash_option = " --require-hashes" if plan.status == "locked" else ""
        return (
            "RUN uv venv /workspace/.venv && uv pip install"
            f" --python /workspace/.venv/bin/python{hash_option}"
            " -r /workspace/requirements.txt"
        )
    raise ExportError(
        "invalid_dependency_plan",
        "The agent dependency plan is not a supported locked or declared shape.",
    )


def _dockerfile(plan: DependencyPlan) -> str:
    dependency_install = _dependency_install(plan)
    return f"""# syntax=docker/dockerfile:1
ARG UV_VERSION={_UV_VERSION}
FROM docker.io/astral/uv:${{UV_VERSION}} AS uv

FROM python:3.12-slim

COPY --from=uv /uv /uvx /bin/
ENV UV_NO_CACHE=1

WORKDIR /workspace
COPY agent_source/ /workspace/
{dependency_install}
COPY bridge-requirements.txt evaluator-requirements.txt /opt/kitaru-export/
RUN uv pip install --system -r /opt/kitaru-export/bridge-requirements.txt \
    -r /opt/kitaru-export/evaluator-requirements.txt
COPY bridge/ /opt/kitaru-export/bridge/
COPY evaluators/ /opt/kitaru-export/evaluators/
COPY agent-runtime.json evaluators.json evaluate.py /opt/kitaru-export/
ENV PYTHONPATH=/opt/kitaru-export
"""


_AGENT_SOURCE = '''"""Harbor adapter for one inert Kitaru run specification."""

import json
import shlex
from pathlib import Path

from harbor.agents.base import BaseAgent

_RUNTIME_PATH = (
    Path(__file__).resolve().parents[1] / "agent_image" / "agent-runtime.json"
)


def _load_runtime():
    return json.loads(_RUNTIME_PATH.read_text())


class KitaruAgent(BaseAgent):
    """Run the exported agent command inside Harbor's task environment."""

    SUPPORTS_ATIF = __KITARU_SUPPORTS_ATIF__

    @staticmethod
    def name() -> str:
        return "kitaru-export"

    def version(self) -> str:
        return "1"

    async def setup(self, environment) -> None:
        return None

    async def run(self, instruction, environment, context) -> None:
        runtime = _load_runtime()
        env = dict(runtime["environment"])
        env.update(self.extra_env)
        env["PATH"] = "/workspace/.venv/bin:" + env.get(
            "PATH", "/usr/local/bin:/usr/bin:/bin"
        )
        env["KITARU_TASK_INPUTS"] = instruction
        env["KITARU_TRACE_PATH"] = runtime["trace_path"]
        result = await environment.exec(
            shlex.join(runtime["command_argv"]),
            cwd=runtime["working_directory"],
            env=env,
            timeout_sec=runtime["agent_timeout_seconds"],
        )
        if result.return_code != 0:
            raise RuntimeError(
                f"Exported agent failed with exit code {result.return_code}"
            )
        destination = "/logs/agent/" + runtime["trace_log_name"]
        copied = await environment.exec(
            "mkdir -p /logs/agent && cp -- "
            + shlex.quote(runtime["trace_path"])
            + " "
            + shlex.quote(destination),
            timeout_sec=30,
        )
        if copied.return_code != 0:
            raise RuntimeError("Exported agent did not produce its declared trace")

    def populate_context_post_run(self, context) -> None:
        if not self.SUPPORTS_ATIF:
            return
        try:
            trajectory = json.loads((self.logs_dir / "trajectory.json").read_text())
        except (OSError, json.JSONDecodeError):
            return
        metrics = trajectory.get("final_metrics")
        if not isinstance(metrics, dict):
            return
        fields = {
            "n_input_tokens": "total_prompt_tokens",
            "n_cache_tokens": "total_cached_tokens",
            "n_output_tokens": "total_completion_tokens",
            "cost_usd": "total_cost_usd",
        }
        for context_name, metric_name in fields.items():
            value = metrics.get(metric_name)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                setattr(context, context_name, value)
'''


_EVALUATE_SOURCE = '''"""Evaluate a Harbor trace with pinned evaluators."""

import json
import os
import sys
import tempfile
from pathlib import Path

from bridge._sanitize import EphemeralSanitizer
from bridge.evaluators import run_evaluator_process

_BRIDGE_ROOT = Path("/opt/kitaru-export")
_MAX_RESULT_BYTES = 1024 * 1024


def _read_result(path: Path) -> dict[str, object]:
    if not path.is_file() or path.stat().st_size > _MAX_RESULT_BYTES:
        raise RuntimeError("Evaluator did not produce a bounded result")
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise RuntimeError("Evaluator result is not a JSON object")
    return value


def main() -> None:
    task = json.loads(Path(sys.argv[1]).read_text())
    required_names = task["required_environment_names"]
    missing = [name for name in required_names if name not in os.environ]
    if missing:
        raise RuntimeError(
            "Missing required evaluator environment names: " + ", ".join(missing)
        )
    minimal_env = {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "PYTHONPATH": str(_BRIDGE_ROOT),
    }
    minimal_env.update({name: os.environ[name] for name in required_names})
    secrets = [os.environ[name] for name in required_names]
    sanitizer = EphemeralSanitizer.for_runtime(secrets)
    with tempfile.TemporaryDirectory(prefix="kitaru-evaluator-") as temporary:
        cwd = Path(temporary)
        process = run_evaluator_process(
            [
                sys.executable,
                "-m",
                "bridge.runtime",
                sys.argv[1],
                sys.argv[2],
                str(_BRIDGE_ROOT / "evaluators.json"),
            ],
            cwd=cwd,
            env=minimal_env,
            timeout_seconds=task["evaluator_timeout_seconds"],
            max_output_bytes=task["evaluator_max_output_bytes"],
        )
        result = _read_result(cwd / "result.json")
    if process.return_code != 0 or result.get("ok") is not True:
        message = result.get("message")
        if not isinstance(message, str):
            captured = process.stderr or process.stdout
            message = str(sanitizer.sanitize(captured)) if captured else "no detail"
        raise RuntimeError("Evaluator failed: " + message[:65536])
    reward = result.get("reward")
    metrics = result.get("metrics")
    if isinstance(reward, bool) or not isinstance(reward, (int, float)):
        raise RuntimeError("Evaluator reward is not numeric")
    if not isinstance(metrics, dict):
        raise RuntimeError("Evaluator metrics are not an object")
    Path("/logs/verifier/reward.txt").write_text(str(reward))
    Path("/logs/verifier/metrics.json").write_text(
        json.dumps(metrics, sort_keys=True, allow_nan=False) + "\\n"
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
/usr/local/bin/python /opt/kitaru-export/evaluate.py /tests/task.json \"$trace\"
"""


def _task_toml(
    *,
    task_name: str,
    image: str,
    timeout: int,
    required_environment_names: tuple[str, ...],
) -> str:
    verifier_timeout = SCORING_TIMEOUT_SECONDS + _VERIFIER_TIMEOUT_OVERHEAD_SECONDS
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
        f"timeout_sec = {float(verifier_timeout)}",
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


def preflight_harbor_export(
    resolved: ResolvedExport,
    *,
    trace_format: Literal["atif", "kitaru"] | str,
    trace_path: str,
    required_environment_names: tuple[str, ...] = (),
) -> tuple[tuple[str, ...], str]:
    """Validate Harbor-specific inputs without writing an artifact."""
    _validate_trace_declaration(trace_format, trace_path)
    if resolved.agent_version.run_spec is None:
        raise ExportError("missing_run_spec", "Agent version has no run specification.")
    if not resolved.command_argv:
        raise ExportError(
            "missing_run_command",
            "Agent command argv was not validated during preflight.",
        )
    dependency_plan = resolved.dependency_plan
    if dependency_plan is None:
        raise ExportError(
            "missing_dependency_plan",
            "Agent dependencies were not classified during preflight.",
        )
    try:
        normalized_names = normalize_environment_names(
            (*resolved.required_environment_names, *required_environment_names)
        )
    except ValueError as error:
        raise ExportError(
            "invalid_environment_name",
            "Required environment names must be unique identifiers.",
        ) from error
    runtime_version = get_runtime_bridge_version()
    for requirement in dependency_plan.requirements:
        if requirement.project == "kitaru":
            validate_kitaru_requirement(
                requirement.requirement,
                runtime_version,
                subject="A declared Kitaru",
            )
    for evaluator in resolved.evaluators:
        source = evaluator.version.source
        if isinstance(source, PackagePluginSource):
            validate_kitaru_requirement(
                source.requirement,
                runtime_version,
                subject="An evaluator",
            )
    return normalized_names, runtime_version


def render_harbor(
    resolved: ResolvedExport,
    destination: Path,
    *,
    trace_format: Literal["atif", "kitaru"] | str,
    trace_path: str,
    required_environment_names: tuple[str, ...] = (),
) -> ExportManifest:
    """Render a complete Harbor dataset into an empty destination directory."""
    required_environment_names, runtime_version = preflight_harbor_export(
        resolved,
        trace_format=trace_format,
        trace_path=trace_path,
        required_environment_names=required_environment_names,
    )
    if destination.exists() and (
        not destination.is_dir() or next(destination.iterdir(), None) is not None
    ):
        raise ExportError(
            "destination_conflict", f"Destination is not empty: {destination}"
        )
    run_spec = resolved.agent_version.run_spec
    assert run_spec is not None
    dependency_plan = resolved.dependency_plan
    assert dependency_plan is not None

    destination.mkdir(parents=True, exist_ok=True)
    agent_image = destination / "agent_image"
    copy_source(resolved.source, agent_image / "agent_source")
    bridge = materialize_runtime_bridge(agent_image / "bridge")
    assert bridge.originating_kitaru_version == runtime_version
    (agent_image / "bridge-requirements.txt").write_text(
        f"kitaru=={bridge.originating_kitaru_version}\n"
    )
    _write_evaluators(
        resolved,
        agent_image,
        kitaru_version=bridge.originating_kitaru_version,
    )
    trace_log_name = (
        "trajectory.json" if trace_format == "atif" else "kitaru-session.json"
    )
    write_canonical_json(
        agent_image / "agent-runtime.json",
        {
            "agent_timeout_seconds": run_spec.timeout_seconds,
            "command_argv": list(resolved.command_argv),
            "environment": run_spec.env,
            "required_environment_names": list(required_environment_names),
            "trace_format": trace_format,
            "trace_log_name": trace_log_name,
            "trace_path": trace_path,
            "working_directory": (
                f"/workspace/{run_spec.working_dir}"
                if run_spec.working_dir
                else "/workspace"
            ),
        },
    )
    (agent_image / "evaluate.py").write_text(_EVALUATE_SOURCE)
    (agent_image / "Dockerfile").write_text(_dockerfile(dependency_plan))

    agent_dir = destination / "agent"
    agent_dir.mkdir()
    (agent_dir / "__init__.py").write_text("")
    (agent_dir / "kitaru_agent.py").write_text(
        _AGENT_SOURCE.replace("__KITARU_SUPPORTS_ATIF__", repr(trace_format == "atif"))
    )

    image = f"{_IMAGE_PREFIX}:{directory_digest(agent_image)[:12]}"
    dataset_dir = destination / "dataset"
    dataset_dir.mkdir()
    references: list[tuple[str, str]] = []
    for index, session in enumerate(
        sorted(resolved.sessions, key=lambda item: str(item.session.id)), start=1
    ):
        task_id = f"task-{index:05d}-{str(session.session.id)[:8]}"
        task_name = f"kitaru/{task_id}"
        task_dir = dataset_dir / task_id
        (task_dir / "environment").mkdir(parents=True)
        (task_dir / "environment/.gitkeep").write_text("")
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
            "evaluator_max_output_bytes": _MAX_EVALUATOR_OUTPUT_BYTES,
            "evaluator_timeout_seconds": SCORING_TIMEOUT_SECONDS,
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

`harbor` is the Harbor CLI. `dataset` is the generated task dataset, and
`agent.kitaru_agent:KitaruAgent` is the generated adapter that starts the
registered agent source and command. It is not the user's agent name or class.

The agent must write its {trace_format} trace to `{trace_path}`. Missing or
malformed traces and evaluator failures fail the task. There is no fallback reward.

Agent dependencies are classified as {dependency_plan.status}. Export performed
structural validation only; exact Harbor {HARBOR_VERSION} execution is release-level
evidence and is not a claim that this user artifact was executed.

Content omissions: {", ".join(resolved.content_policy.omit) or "none"}.
Registered environment handling: {resolved.environment_policy.mode}.
Explicit source includes: {", ".join(resolved.source_policy.include) or "none"}.
Explicit source exclusions: {", ".join(resolved.source_policy.exclude) or "none"}.
Current resolved attached-secret values and protected local files are excluded;
runtime values must be supplied through Harbor's environment mechanism.
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
        evaluator_version_ids=tuple(
            item.version.id
            for item in sorted(resolved.evaluators, key=lambda item: item.name)
        ),
        primary_reward=(
            f"{resolved.reward.evaluator}:{resolved.reward.result}:{resolved.reward.field}"
        ),
        source_digest=resolved.source.digest,
        generated_files=file_digests(destination),
        required_environment_names=required_environment_names,
        exclusions=resolved.source.excluded,
        content_policy=resolved.content_policy,
        environment_policy=resolved.environment_policy,
        source_policy=resolved.source_policy,
        dependencies=DependencyReceipt(
            status=dependency_plan.status,
            requirement_digest=dependency_plan.requirement_digest,
        ),
        runtime_bridge=bridge,
        validation=ValidationReceipt(
            level="structural", status="passed", target_version=HARBOR_VERSION
        ),
    )
    write_canonical_json(
        destination / "kitaru-export.json", manifest.model_dump(mode="json")
    )
    validate_generated_resources(destination)
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
    validate_runtime_bridge(
        root / "agent_image/bridge",
        manifest.get("runtime_bridge"),
        error_code="invalid_harbor_bundle",
    )
    expected_files = manifest.get("generated_files")
    if not isinstance(expected_files, dict):
        raise ExportError(
            "invalid_harbor_bundle", "Manifest generated file inventory is invalid."
        )
    actual_files = file_digests(root)
    actual_files.pop("kitaru-export.json", None)
    if actual_files != expected_files:
        raise ExportError(
            "invalid_harbor_bundle", "Generated file bytes do not match manifest."
        )
    for path in (
        root / "agent/kitaru_agent.py",
        root / "agent_image/evaluate.py",
        *(root / "agent_image/bridge").glob("*.py"),
    ):
        source = path.read_text()
        if "kitaru.exports" in source:
            raise ExportError(
                "invalid_harbor_bundle",
                "Generated runtime imports private Kitaru exporter modules.",
            )
        compile(source, str(path.relative_to(root)), "exec")
    validate_generated_resources(root)
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
