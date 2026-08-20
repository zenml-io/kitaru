#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Render a frozen experiment as a complete Verifiers 0.3 v1 plugin."""

import ast
import hashlib
import json
import shutil
import tomllib
from pathlib import Path
from typing import Any

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

from kitaru.api_models.v1.plugin import PackagePluginSource, ScriptPluginSource
from kitaru.exports._bridge import materialize_runtime_bridge
from kitaru.exports.config import normalize_environment_names
from kitaru.exports.models import (
    V1_EXPORT_BUDGETS,
    ArtifactProvenance,
    DependencyPlan,
    DependencyReceipt,
    ExportError,
    ExportManifest,
    ResolvedExport,
    RuntimeBridgeReceipt,
    RuntimeRequirements,
    TaskProvenance,
    ValidationReceipt,
)
from kitaru.exports.source import copy_source
from kitaru.exports.writer import (
    canonical_json_bytes,
    file_digest,
    file_digests,
    write_canonical_json,
)

VERIFIERS_VERSION = "0.3.0"
PRIME_RL_VERSION = "0.8.0"
GENERATED_ARTIFACT_SCHEMA_VERSION = 1
SCORING_BRIDGE_SCHEMA_VERSION = 1
SCORING_TIMEOUT_SECONDS = 300
_SCORING_TIMEOUT_OVERHEAD_SECONDS = 30
_MAX_EVALUATOR_OUTPUT_BYTES = 64 * 1024
_UV_VERSION = "0.12.1"
_PLUGIN_PREFIX_LENGTH = 24
_RUNTIME_TEMPLATE_VERSION = 1
_PYTHON_RUNTIME_IMAGE = "python:3.12-slim"

_TASK_DATA_KEYS = {
    "idx",
    "inputs",
    "kitaru_content_digest",
    "kitaru_session_id",
    "name",
    "prompt",
}


def _canonical_digest(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _quote_toml(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _validate_kitaru_requirement(requirement: str, kitaru_version: str) -> None:
    try:
        parsed = Requirement(requirement)
    except InvalidRequirement as error:
        raise ExportError(
            "invalid_dependency_metadata", "An evaluator requirement is invalid."
        ) from error
    if canonicalize_name(parsed.name) != "kitaru":
        return
    if parsed.url is not None or not parsed.specifier.contains(
        kitaru_version, prereleases=True
    ):
        raise ExportError(
            "dependency_conflict",
            f"Generated scoring requires Kitaru {kitaru_version}; an evaluator "
            "requirement excludes that version.",
        )


def _evaluator_requirements(
    resolved: ResolvedExport, *, kitaru_version: str
) -> tuple[str, ...]:
    requirements: dict[str, str] = {}
    for evaluator in sorted(resolved.evaluators, key=lambda item: item.name):
        source = evaluator.version.source
        if isinstance(source, PackagePluginSource):
            if any(character in source.requirement for character in "\r\n\0"):
                raise ExportError(
                    "invalid_dependency_metadata",
                    "Evaluator requirements must be inert requirement strings.",
                )
            _validate_kitaru_requirement(source.requirement, kitaru_version)
            parsed = Requirement(source.requirement)
            project = canonicalize_name(parsed.name)
            if project == "kitaru":
                continue
            normalized = " ".join(source.requirement.split())
            previous = requirements.get(project)
            if previous is not None and previous != normalized:
                raise ExportError(
                    "dependency_conflict",
                    "Multiple non-identical evaluator requirements target one "
                    "normalized project.",
                )
            requirements[project] = normalized
        elif not isinstance(source, ScriptPluginSource):
            raise ExportError(
                "unsupported_evaluator_source",
                f"Evaluator {evaluator.name!r} has an unsupported source.",
            )
    return tuple(requirements[project] for project in sorted(requirements))


def _pyproject(
    distribution_name: str,
    module_name: str,
    resolved: ResolvedExport,
    *,
    kitaru_version: str,
) -> str:
    dependencies = [
        f"kitaru=={kitaru_version}",
        f"verifiers=={VERIFIERS_VERSION}",
        *_evaluator_requirements(resolved, kitaru_version=kitaru_version),
    ]
    rendered_dependencies = ", ".join(
        _quote_toml(dependency) for dependency in dependencies
    )
    return f'''[project]
name = "{distribution_name}"
version = "0.1.0"
description = "Frozen Kitaru benchmark and bundled Verifiers Harness."
requires-python = ">=3.12,<3.14"
dependencies = [{rendered_dependencies}]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["{module_name}"]
'''


def _init_module() -> str:
    return '''"""Generated Kitaru Taskset and bundled Harness plugin."""

from .plugin import KitaruHarness, KitaruTaskset

__all__ = ["KitaruHarness", "KitaruTaskset"]
'''


def _plugin_module() -> str:
    return '''"""Native Verifiers Taskset, Task, and bundled Harness."""

import json
import os
from pathlib import Path, PurePosixPath
from typing import Any

import verifiers.v1 as vf

from .scoring import evaluate

_PACKAGE_ROOT = Path(__file__).resolve().parent
_RUNTIME_ROOT = PurePosixPath("/workspace/kitaru-agent")
_RUNTIME = json.loads((_PACKAGE_ROOT / "runtime.json").read_text())
_SCORING_REQUIREMENTS = tuple(
    json.loads((_PACKAGE_ROOT / "scoring" / "requirements.json").read_text())
)


def _missing(names: tuple[str, ...], values: dict[str, str] | None = None) -> list[str]:
    available = os.environ if values is None else values
    return sorted(name for name in names if not available.get(name))


class KitaruData(vf.TaskData):
    inputs: Any
    kitaru_session_id: str
    kitaru_content_digest: str


class KitaruTask(vf.Task[KitaruData]):
    NEEDS_CONTAINER = True

    @vf.reward(weight=1.0)
    async def kitaru_reward(self, trace: vf.Trace) -> float:
        reward, metrics = evaluate(trace.model_dump(mode="json"), self.data)
        trace.record_metrics(metrics)
        return reward


class KitaruTaskset(vf.Taskset[KitaruTask, vf.TasksetConfig]):
    def load(self) -> list[KitaruTask]:
        missing = _missing(_SCORING_REQUIREMENTS)
        if missing:
            raise RuntimeError(
                "Missing required scoring environment names: "
                + ", ".join(missing)
            )
        tasks = []
        for line in (_PACKAGE_ROOT / "tasks.jsonl").read_text().splitlines():
            tasks.append(
                KitaruTask(
                    KitaruData.model_validate(json.loads(line)), self.config.task
                )
            )
        return tasks


class KitaruHarness(vf.Harness[vf.HarnessConfig]):
    APPENDS_SYSTEM_PROMPT = True
    SUPPORTS_MCP = True
    NEEDS_CONTAINER = True

    def __init__(self, config: vf.HarnessConfig) -> None:
        super().__init__(config)
        missing = _missing(
            tuple(_RUNTIME["required_environment_names"]), config.resolved_env
        )
        if missing:
            raise RuntimeError(
                "Missing required bundled Harness environment names: "
                + ", ".join(missing)
            )

    async def setup(self, runtime: vf.Runtime) -> None:
        source = _PACKAGE_ROOT / "agent_source"
        for path in sorted(item for item in source.rglob("*") if item.is_file()):
            target = (_RUNTIME_ROOT / path.relative_to(source).as_posix()).as_posix()
            await runtime.write(target, path.read_bytes())
        result = await runtime.run(
            ["python", "-m", "pip", "install", "uv==" + _RUNTIME["uv_version"]],
            {},
        )
        if result.exit_code != 0:
            raise RuntimeError("Could not install the pinned uv runtime")
        for argv in _RUNTIME["setup_argv"]:
            result = await runtime.run(argv, {})
            if result.exit_code != 0:
                raise RuntimeError(
                    "Agent dependency setup failed: "
                    + (result.stderr or result.stdout)[-2000:]
                )
        executable_paths = [
            (_RUNTIME_ROOT / path).as_posix()
            for path in _RUNTIME["executable_paths"]
        ]
        if executable_paths:
            result = await runtime.run(["chmod", "+x", *executable_paths], {})
            if result.exit_code != 0:
                raise RuntimeError("Could not restore executable agent source modes")

    async def launch(
        self,
        ctx: vf.ModelContext,
        trace: vf.Trace,
        runtime: vf.Runtime,
        endpoint: str,
        secret: str,
        mcp_urls: dict[str, str],
        data: vf.TaskData,
    ) -> vf.ProgramResult:
        if not isinstance(data, KitaruData):
            raise TypeError("KitaruHarness requires KitaruData")
        env = {
            **_RUNTIME["environment"],
            **self.config.resolved_env,
            "KITARU_EXTERNAL_EVALUATION": "1",
            "KITARU_TASK_INPUTS": json.dumps(
                data.inputs, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ),
            "KITARU_MCP_URLS": json.dumps(
                mcp_urls, sort_keys=True, separators=(",", ":")
            ),
            "OPENAI_BASE_URL": endpoint,
            "OPENAI_API_KEY": secret,
            "OPENAI_MODEL": ctx.model,
        }
        existing_path = env.get("PATH", "/usr/local/bin:/usr/bin:/bin")
        env["PATH"] = (_RUNTIME_ROOT / ".venv" / "bin").as_posix() + ":" + existing_path
        return await runtime.run_program(
            [
                "/usr/bin/env",
                "-C",
                (_RUNTIME_ROOT / _RUNTIME["working_directory"]).as_posix(),
                *_RUNTIME["command_argv"],
            ],
            env,
        )
'''


def _scoring_module(module_name: str) -> str:
    return f'''"""Host-private Kitaru scoring adapter."""

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from .bridge._sanitize import EphemeralSanitizer
from .bridge.evaluators import run_evaluator_process

_PACKAGE_ROOT = Path(__file__).resolve().parent
_SCORING_ROOT = _PACKAGE_ROOT / "scoring"
_MAX_RESULT_BYTES = 1024 * 1024
_TIMEOUT_SECONDS = {SCORING_TIMEOUT_SECONDS}
_MAX_OUTPUT_BYTES = {_MAX_EVALUATOR_OUTPUT_BYTES}


def _write_json(path: Path, value: Any) -> None:
    serialized = json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    path.write_text(serialized + "\\n")


def evaluate(trace: dict[str, Any], data: Any) -> tuple[float, dict[str, float]]:
    session_id = data.kitaru_session_id
    task_path = _SCORING_ROOT / "tasks" / (session_id + ".json")
    task = json.loads(task_path.read_text())
    if task["content_digest"] != data.kitaru_content_digest:
        raise RuntimeError("Kitaru task provenance does not match private scoring data")
    required_names = tuple(task["required_environment_names"])
    missing = sorted(name for name in required_names if not os.environ.get(name))
    if missing:
        raise RuntimeError(
            "Missing required scoring environment names: " + ", ".join(missing)
        )
    minimal_env = {{
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        **{{name: os.environ[name] for name in required_names}},
    }}
    secrets = [minimal_env[name] for name in required_names]
    sanitizer = EphemeralSanitizer(secrets)
    with tempfile.TemporaryDirectory(prefix="kitaru-verifiers-score-") as temporary:
        workdir = Path(temporary)
        private_task = workdir / "task.json"
        trace_path = workdir / "trace.json"
        _write_json(private_task, task["bridge_task"])
        _write_json(trace_path, trace)
        process = run_evaluator_process(
            [
                sys.executable,
                "-m",
                "{module_name}.bridge.runtime",
                str(private_task),
                str(trace_path),
                str(_SCORING_ROOT / "evaluators.json"),
            ],
            cwd=workdir,
            env=minimal_env,
            timeout_seconds=_TIMEOUT_SECONDS,
            max_output_bytes=_MAX_OUTPUT_BYTES,
        )
        result_path = workdir / "result.json"
        if not result_path.is_file() or result_path.stat().st_size > _MAX_RESULT_BYTES:
            raise RuntimeError("Evaluator did not produce a bounded result")
        result = json.loads(result_path.read_bytes())
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
    if not isinstance(metrics, dict) or any(
        isinstance(value, bool) or not isinstance(value, (int, float))
        for value in metrics.values()
    ):
        raise RuntimeError("Evaluator metrics are not numeric")
    return float(reward), {{name: float(value) for name, value in metrics.items()}}
'''


def _dependency_setup(plan: DependencyPlan) -> list[list[str]]:
    root = "/workspace/kitaru-agent"
    if plan.manifests == ("pyproject.toml", "uv.lock") and plan.status == "locked":
        return [
            ["uv", "sync", "--project", root, "--frozen", "--no-dev", "--no-editable"]
        ]
    if plan.manifests == ("pyproject.toml",) and plan.status == "declared":
        return [["uv", "sync", "--project", root, "--no-dev", "--no-editable"]]
    if plan.manifests == ("requirements.txt",):
        install = [
            "uv",
            "pip",
            "install",
            "--python",
            f"{root}/.venv/bin/python",
        ]
        if plan.status == "locked":
            install.append("--require-hashes")
        install.extend(["-r", f"{root}/requirements.txt"])
        return [["uv", "venv", f"{root}/.venv"], install]
    raise ExportError(
        "invalid_dependency_plan",
        "The agent dependency plan is not a supported locked or declared shape.",
    )


def _eval_toml(
    plugin_id: str,
    bundled_requirements: tuple[str, ...],
    *,
    agent_timeout_seconds: float,
) -> str:
    lines = [
        "[env.taskset]",
        f"id = {_quote_toml(plugin_id)}",
        "",
        "[env.agent.harness]",
        f"id = {_quote_toml(plugin_id)}",
    ]
    if bundled_requirements:
        rendered = ", ".join(_quote_toml(name) for name in bundled_requirements)
        lines.append(f"forward_env = [{rendered}]")
    lines.extend(
        [
            "",
            "[env.agent.runtime]",
            'type = "docker"',
            f"image = {_quote_toml(_PYTHON_RUNTIME_IMAGE)}",
            "",
            "[env.agent.timeout]",
            f"rollout = {agent_timeout_seconds:g}",
            "scoring = "
            f"{SCORING_TIMEOUT_SECONDS + _SCORING_TIMEOUT_OVERHEAD_SECONDS:g}",
        ]
    )
    return "\n".join(lines) + "\n"


def _prime_rl_toml(
    plugin_id: str,
    bundled_requirements: tuple[str, ...],
    *,
    agent_timeout_seconds: float,
) -> str:
    lines = [
        "[[orchestrator.train.source]]",
        'name = "kitaru-export"',
        "ratio = 1",
        "",
        "[orchestrator.train.source.env.taskset]",
        f"id = {_quote_toml(plugin_id)}",
        "",
        "[orchestrator.train.source.env.agent.harness]",
        f"id = {_quote_toml(plugin_id)}",
    ]
    if bundled_requirements:
        rendered = ", ".join(_quote_toml(name) for name in bundled_requirements)
        lines.append(f"forward_env = [{rendered}]")
    lines.extend(
        [
            "",
            "[orchestrator.train.source.env.agent.runtime]",
            'type = "docker"',
            f"image = {_quote_toml(_PYTHON_RUNTIME_IMAGE)}",
            "",
            "[orchestrator.train.source.env.agent.timeout]",
            f"rollout = {agent_timeout_seconds:g}",
            "scoring = "
            f"{SCORING_TIMEOUT_SECONDS + _SCORING_TIMEOUT_OVERHEAD_SECONDS:g}",
        ]
    )
    return "\n".join(lines) + "\n"


def _readme(
    plugin_id: str,
    dependency_status: str,
    runtime_requirements: RuntimeRequirements,
) -> str:
    requirements = (
        ", ".join(runtime_requirements.all) if runtime_requirements.all else "none"
    )
    return (
        "# Kitaru Verifiers export\n\n"
        "This complete plugin contains one frozen Taskset and one bundled default "
        f"Harness for Verifiers {VERIFIERS_VERSION}. Install it with `uv sync`, "
        "supply the manifest-declared runtime environment variables, and run "
        "`uv run eval @ eval.toml --model MODEL`.\n\n"
        f"The native plugin ID is `{plugin_id}`. `eval.toml` explicitly pairs its "
        "Taskset and bundled Harness and selects Docker. Removing the "
        "`[env.agent.harness]` block uses Verifiers' bundled-Harness discovery. "
        "To select an independently installed compatible Harness, change only "
        "`env.agent.harness.id`; an unknown ID fails through Verifiers' loader "
        "without falling back.\n\n"
        "Harness-visible TaskData contains only prompt and inputs plus stable Kitaru "
        "session provenance. Historical output, reasoning, tool results, evaluator "
        "evidence, reward selection, runtime requirements, and evaluator "
        "implementations remain host-private scoring assets and are not copied into "
        "the Docker rollout.\n\n"
        f"Agent dependencies are classified as {dependency_status} and are installed "
        f"from the copied source inside Docker. Runtime requirements: {requirements}. "
        "Task-private scoring requirements remain active for every Harness; "
        "bundled-Harness requirements are dropped when another Harness is selected, "
        "whose own requirements are delegated to its package and Verifiers.\n\n"
        f"`prime-rl.toml` is the exact PrimeRL {PRIME_RL_VERSION} training-source "
        "composition for the same Taskset, bundled Harness, and Docker runtime. Add "
        "trainer-owned model, optimizer, and hardware settings around it. Resume is "
        "supported only with this saved configuration and exact generated artifact. "
        "Create and export a new immutable cohort version to add or remove benchmark "
        "cases.\n\n"
        "Kitaru performed structural validation without importing or executing this "
        "plugin. Exact Verifiers and PrimeRL compatibility is release-level evidence, "
        "not a claim that this user artifact was executed.\n"
    )


def _session_content(session: Any) -> tuple[dict[str, Any], str]:
    context = session.model_dump(mode="json")
    return context, _canonical_digest(context)


def _task_rows(
    resolved: ResolvedExport,
) -> tuple[list[dict[str, Any]], list[tuple[Any, dict[str, Any], str]]]:
    rows: list[dict[str, Any]] = []
    private: list[tuple[Any, dict[str, Any], str]] = []
    for index, session in enumerate(
        sorted(resolved.sessions, key=lambda item: str(item.session.id))
    ):
        context, content_digest = _session_content(session)
        session_id = str(session.session.id)
        inputs = session.session.inputs
        rows.append(
            {
                "idx": index,
                "inputs": inputs,
                "kitaru_content_digest": content_digest,
                "kitaru_session_id": session_id,
                "name": f"kitaru-session-{session_id}",
                "prompt": json.dumps(
                    inputs,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                ),
            }
        )
        private.append((session, context, content_digest))
    return rows, private


def _benchmark_digest(
    resolved: ResolvedExport, private: list[tuple[Any, dict[str, Any], str]]
) -> str:
    return _canonical_digest(
        {
            "cohort_version_id": str(resolved.cohort_version.id),
            "content_policy": {"omit": []},
            "evaluator_versions": [
                {
                    "id": str(evaluator.version.id),
                    "name": evaluator.name,
                    "params": evaluator.params,
                    "source_sha256": evaluator.source_sha256,
                    "version": evaluator.version.version,
                }
                for evaluator in sorted(resolved.evaluators, key=lambda item: item.name)
            ],
            "generated_artifact_schema_version": GENERATED_ARTIFACT_SCHEMA_VERSION,
            "primary_reward": {
                "evaluator": resolved.reward.evaluator,
                "field": resolved.reward.field,
                "result": resolved.reward.result,
            },
            "scoring_bridge_schema_version": SCORING_BRIDGE_SCHEMA_VERSION,
            "target": "verifiers-v1",
            "target_version": VERIFIERS_VERSION,
            "task_content_digests": [
                content_digest for _, _, content_digest in private
            ],
        }
    )


def _default_harness_digest(
    resolved: ResolvedExport, dependency_plan: DependencyPlan
) -> str:
    run_spec = resolved.agent_version.run_spec
    assert run_spec is not None
    return _canonical_digest(
        {
            "agent_version_id": str(resolved.agent_version.id),
            "command_argv": list(resolved.command_argv),
            "dependency_plan": {
                "manifests": dependency_plan.manifests,
                "requirement_digest": dependency_plan.requirement_digest,
                "status": dependency_plan.status,
            },
            "environment": dict(sorted(run_spec.env.items())),
            "environment_handling": "include",
            "required_environment_names": sorted(resolved.required_environment_names),
            "source_digest": resolved.source.digest,
            "timeout_seconds": run_spec.timeout_seconds,
            "working_directory": run_spec.working_dir or "",
        }
    )


def _runtime_bundle_digest(bridge: RuntimeBridgeReceipt) -> str:
    templates = {
        "init": _init_module(),
        "plugin": _plugin_module(),
        "scoring": _scoring_module("{module_name}"),
    }
    return _canonical_digest(
        {
            "bridge_schema_version": bridge.schema_version,
            "bridge_sha256": bridge.sha256,
            "configuration_schemas": {
                "eval": 1,
                "prime_rl": 1,
            },
            "renderer_templates": templates,
            "runtime_template_version": _RUNTIME_TEMPLATE_VERSION,
            "target_version": VERIFIERS_VERSION,
        }
    )


def _artifact_provenance(
    benchmark_digest: str,
    default_harness_digest: str,
    runtime_bundle_digest: str,
) -> ArtifactProvenance:
    artifact_digest = _canonical_digest(
        {
            "benchmark_digest": benchmark_digest,
            "default_harness_digest": default_harness_digest,
            "generated_artifact_schema_version": GENERATED_ARTIFACT_SCHEMA_VERSION,
            "runtime_bundle_digest": runtime_bundle_digest,
        }
    )
    suffix = artifact_digest[:_PLUGIN_PREFIX_LENGTH]
    return ArtifactProvenance(
        artifact_digest=artifact_digest,
        benchmark_digest=benchmark_digest,
        default_harness_digest=default_harness_digest,
        runtime_bundle_digest=runtime_bundle_digest,
        plugin_id=f"kitaru_verifiers_{suffix}",
        distribution_name=f"kitaru-verifiers-{suffix}",
        module_name=f"kitaru_verifiers_{suffix}",
    )


def _write_evaluators(module: Path, resolved: ResolvedExport) -> None:
    evaluator_root = module / "scoring" / "evaluators"
    evaluator_root.mkdir(parents=True)
    records: list[dict[str, Any]] = []
    total_script_bytes = 0
    for index, evaluator in enumerate(
        sorted(resolved.evaluators, key=lambda item: item.name)
    ):
        script_path: str | None = None
        if isinstance(evaluator.version.source, ScriptPluginSource):
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
            (evaluator_root / script_path).write_bytes(evaluator.script)
        records.append(
            {
                "name": evaluator.name,
                "params": evaluator.params,
                "script_path": script_path,
                "source_sha256": evaluator.source_sha256,
                "version": evaluator.version.model_dump(mode="json"),
            }
        )
    write_canonical_json(module / "scoring" / "evaluators.json", records)


def _write_tasks(
    module: Path,
    resolved: ResolvedExport,
    rows: list[dict[str, Any]],
    private: list[tuple[Any, dict[str, Any], str]],
    task_private_requirements: tuple[str, ...],
) -> tuple[TaskProvenance, ...]:
    (module / "tasks.jsonl").write_bytes(
        b"".join(canonical_json_bytes(row) for row in rows)
    )
    private_root = module / "scoring" / "tasks"
    private_root.mkdir(parents=True)
    selector = resolved.reward
    provenance: list[TaskProvenance] = []
    for session, context, content_digest in private:
        session_id = str(session.session.id)
        write_canonical_json(
            private_root / f"{session_id}.json",
            {
                "bridge_task": {
                    "context": context,
                    "primary_reward": {
                        "evaluator": selector.evaluator,
                        "field": selector.field,
                        "result": selector.result,
                    },
                    "required_environment_names": list(task_private_requirements),
                    "trace_format": "verifiers-v1",
                },
                "content_digest": content_digest,
                "session_id": session_id,
            },
        )
        provenance.append(
            TaskProvenance(session_id=session.session.id, content_digest=content_digest)
        )
    return tuple(provenance)


def _write_runtime(
    module: Path, resolved: ResolvedExport, plan: DependencyPlan
) -> None:
    run_spec = resolved.agent_version.run_spec
    assert run_spec is not None
    write_canonical_json(
        module / "runtime.json",
        {
            "agent_source": "agent_source",
            "command_argv": list(resolved.command_argv),
            "environment": dict(sorted(run_spec.env.items())),
            "executable_paths": [
                file.path for file in resolved.source.files if file.mode == 0o755
            ],
            "required_environment_names": list(resolved.required_environment_names),
            "setup_argv": _dependency_setup(plan),
            "uv_version": _UV_VERSION,
            "working_directory": run_spec.working_dir or "",
        },
    )


def _validate_generated_resources(root: Path) -> None:
    file_count = 0
    total_bytes = 0
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ExportError(
                "unsupported_bundle_symlink",
                "Generated bundles cannot contain symlinks.",
            )
        if not path.is_file():
            continue
        file_count += 1
        if file_count > V1_EXPORT_BUDGETS.max_generated_files:
            raise ExportError(
                "generated_files_limit",
                f"Generated file count {file_count} exceeds limit "
                f"{V1_EXPORT_BUDGETS.max_generated_files}.",
            )
        relative = path.relative_to(root).as_posix()
        path_bytes = len(relative.encode("utf-8"))
        if path_bytes > V1_EXPORT_BUDGETS.max_relative_path_bytes:
            raise ExportError(
                "generated_path_limit",
                f"Generated path length {path_bytes} exceeds limit "
                f"{V1_EXPORT_BUDGETS.max_relative_path_bytes} bytes.",
            )
        total_bytes += path.stat().st_size
        if total_bytes > V1_EXPORT_BUDGETS.max_artifact_bytes:
            raise ExportError(
                "artifact_size_limit",
                f"Generated artifact bytes {total_bytes} exceeds limit "
                f"{V1_EXPORT_BUDGETS.max_artifact_bytes}.",
            )


def render_verifiers_v1(
    resolved: ResolvedExport,
    root: Path,
    *,
    required_environment_names: tuple[str, ...] = (),
) -> ExportManifest:
    """Render one installable Verifiers 0.3.0 Taskset and bundled Harness."""
    if root.exists() and (not root.is_dir() or next(root.iterdir(), None) is not None):
        raise ExportError(
            "invalid_destination", "Verifiers staging directory must be empty."
        )
    run_spec = resolved.agent_version.run_spec
    if run_spec is None:
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
    if not resolved.sessions or len(resolved.sessions) > V1_EXPORT_BUDGETS.max_sessions:
        raise ExportError(
            "sessions_limit",
            "Verifiers export requires between 1 and 1,000 frozen sessions.",
        )
    try:
        task_private_requirements = normalize_environment_names(
            required_environment_names
        )
        bundled_requirements = normalize_environment_names(
            resolved.required_environment_names
        )
    except ValueError as error:
        raise ExportError(
            "invalid_environment_name",
            "Required environment names must be unique identifiers.",
        ) from error

    root.mkdir(parents=True, exist_ok=True)
    bridge_staging = root / ".bridge-staging"
    bridge = materialize_runtime_bridge(bridge_staging)
    for requirement in dependency_plan.requirements:
        if requirement.project == "kitaru":
            _validate_kitaru_requirement(
                requirement.requirement, bridge.originating_kitaru_version
            )
    rows, private = _task_rows(resolved)
    provenance = _artifact_provenance(
        _benchmark_digest(resolved, private),
        _default_harness_digest(resolved, dependency_plan),
        _runtime_bundle_digest(bridge),
    )
    module = root / provenance.module_name
    module.mkdir()
    shutil.move(bridge_staging.as_posix(), (module / "bridge").as_posix())

    (module / "__init__.py").write_text(_init_module())
    (module / "plugin.py").write_text(_plugin_module())
    (module / "scoring.py").write_text(_scoring_module(provenance.module_name))
    copy_source(resolved.source, module / "agent_source")
    _write_runtime(module, resolved, dependency_plan)
    _write_evaluators(module, resolved)
    task_provenance = _write_tasks(
        module,
        resolved,
        rows,
        private,
        task_private_requirements,
    )
    write_canonical_json(
        module / "scoring" / "requirements.json", list(task_private_requirements)
    )

    runtime_requirements = RuntimeRequirements(
        task_private=task_private_requirements,
        bundled_harness=bundled_requirements,
    )
    (root / "pyproject.toml").write_text(
        _pyproject(
            provenance.distribution_name,
            provenance.module_name,
            resolved,
            kitaru_version=bridge.originating_kitaru_version,
        )
    )
    (root / "eval.toml").write_text(
        _eval_toml(
            provenance.plugin_id,
            bundled_requirements,
            agent_timeout_seconds=run_spec.timeout_seconds,
        )
    )
    (root / "prime-rl.toml").write_text(
        _prime_rl_toml(
            provenance.plugin_id,
            bundled_requirements,
            agent_timeout_seconds=run_spec.timeout_seconds,
        )
    )
    (root / "README.md").write_text(
        _readme(provenance.plugin_id, dependency_plan.status, runtime_requirements)
    )

    _validate_generated_resources(root)
    validation = ValidationReceipt(
        level="structural", status="passed", target_version=VERIFIERS_VERSION
    )
    manifest = ExportManifest(
        format="verifiers-v1",
        target_version=VERIFIERS_VERSION,
        experiment_id=resolved.experiment.id,
        cohort_version_id=resolved.cohort_version.id,
        agent_version_id=resolved.agent_version.id,
        evaluator_version_ids=tuple(
            evaluator.version.id
            for evaluator in sorted(resolved.evaluators, key=lambda item: item.name)
        ),
        primary_reward=(
            f"{resolved.reward.evaluator}:{resolved.reward.result}:{resolved.reward.field}"
        ),
        source_digest=resolved.source.digest,
        generated_files=file_digests(root),
        required_environment_names=runtime_requirements.all,
        exclusions=resolved.source.excluded,
        dependencies=DependencyReceipt(
            status=dependency_plan.status,
            requirement_digest=dependency_plan.requirement_digest,
        ),
        runtime_bridge=bridge,
        provenance=provenance,
        runtime_requirements=runtime_requirements,
        task_provenance=task_provenance,
        validation=validation,
    )
    write_canonical_json(root / "kitaru-export.json", manifest.model_dump(mode="json"))
    validate_verifiers_v1(root)
    return manifest


def _validate_runtime_bridge(
    root: Path, module_name: str, receipt_data: object
) -> None:
    try:
        receipt = RuntimeBridgeReceipt.model_validate(receipt_data)
    except ValueError as error:
        raise ExportError(
            "invalid_verifiers_bundle", "Runtime bridge receipt is invalid."
        ) from error
    bridge_root = root / module_name / "bridge"
    aggregate = hashlib.sha256()
    for relative, expected in receipt.files.items():
        path = bridge_root / relative
        if not path.is_file() or file_digest(path) != expected:
            raise ExportError(
                "invalid_verifiers_bundle",
                "Runtime bridge bytes do not match manifest.",
            )
        aggregate.update(relative.encode("utf-8"))
        aggregate.update(b"\0")
        aggregate.update(path.read_bytes())
        aggregate.update(b"\n")
    if aggregate.hexdigest() != receipt.sha256:
        raise ExportError(
            "invalid_verifiers_bundle", "Runtime bridge digest does not match manifest."
        )


def _exported_names(module: Path) -> list[str]:
    tree = ast.parse((module / "__init__.py").read_text())
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "__all__"
            for target in node.targets
        ):
            continue
        if isinstance(node.value, (ast.List, ast.Tuple)) and all(
            isinstance(item, ast.Constant) and isinstance(item.value, str)
            for item in node.value.elts
        ):
            exported = ast.literal_eval(node.value)
            if isinstance(exported, list) and all(
                isinstance(item, str) for item in exported
            ):
                return exported
    raise ValueError("generated module has no static __all__")


def validate_verifiers_v1(root: Path) -> ValidationReceipt:
    """Validate generated structure and boundaries without importing plugin code."""
    try:
        manifest_data = json.loads((root / "kitaru-export.json").read_text())
        manifest = ExportManifest.model_validate(manifest_data)
        provenance = manifest.provenance
        if provenance is None or manifest.runtime_bridge is None:
            raise ValueError("manifest has no Verifiers provenance or bridge receipt")
        module = root / provenance.module_name
        required = (
            root / "README.md",
            root / "pyproject.toml",
            root / "eval.toml",
            root / "prime-rl.toml",
            module / "__init__.py",
            module / "plugin.py",
            module / "scoring.py",
            module / "runtime.json",
            module / "tasks.jsonl",
            module / "scoring/evaluators.json",
            module / "scoring/requirements.json",
        )
        missing = [
            path.relative_to(root).as_posix() for path in required if not path.is_file()
        ]
        if missing:
            raise ValueError("missing files: " + ", ".join(missing))
        project = tomllib.loads((root / "pyproject.toml").read_text())
        evaluation = tomllib.loads((root / "eval.toml").read_text())
        training = tomllib.loads((root / "prime-rl.toml").read_text())
        runtime = json.loads((module / "runtime.json").read_text())

        project_data = project["project"]
        dependencies = project_data["dependencies"]
        if (
            project_data["name"] != provenance.distribution_name
            or project_data["requires-python"] != ">=3.12,<3.14"
            or f"verifiers=={VERIFIERS_VERSION}" not in dependencies
            or f"kitaru=={manifest.runtime_bridge.originating_kitaru_version}"
            not in dependencies
        ):
            raise ValueError("generated project identity or exact pins do not match")
        if provenance.plugin_id != provenance.module_name:
            raise ValueError("plugin ID must equal the importable generated module")
        if evaluation["env"]["taskset"] != {"id": provenance.plugin_id}:
            raise ValueError("eval taskset selection does not match the plugin")
        harness_config = evaluation["env"]["agent"]["harness"]
        if (
            set(harness_config) - {"id", "forward_env"}
            or harness_config["id"] != provenance.plugin_id
        ):
            raise ValueError("eval Harness block is not base-compatible")
        if evaluation["env"]["agent"]["runtime"].get("type") != "docker":
            raise ValueError("eval runtime is not Docker")
        timeout_config = evaluation["env"]["agent"].get("timeout")
        if (
            not isinstance(timeout_config, dict)
            or timeout_config.get("scoring")
            != SCORING_TIMEOUT_SECONDS + _SCORING_TIMEOUT_OVERHEAD_SECONDS
            or not isinstance(timeout_config.get("rollout"), (int, float))
        ):
            raise ValueError("eval agent and scoring timeouts are invalid")
        sources = training["orchestrator"]["train"]["source"]
        if len(sources) != 1:
            raise ValueError("PrimeRL configuration must contain one training source")
        training_env = sources[0]["env"]
        if (
            training_env["taskset"] != {"id": provenance.plugin_id}
            or training_env["agent"]["harness"].get("id") != provenance.plugin_id
            or training_env["agent"]["runtime"].get("type") != "docker"
            or training_env["agent"].get("timeout") != timeout_config
        ):
            raise ValueError("PrimeRL training composition does not match the plugin")
        if set(training_env["agent"]["harness"]) - {"id", "forward_env"}:
            raise ValueError("PrimeRL Harness block is not base-compatible")
        if runtime.get("agent_source") != "agent_source":
            raise ValueError("runtime source-copy declaration is invalid")
        if (
            not isinstance(runtime.get("command_argv"), list)
            or not runtime["command_argv"]
        ):
            raise ValueError("runtime command argv is invalid")

        rows = [
            json.loads(line)
            for line in (module / "tasks.jsonl").read_text().splitlines()
            if line.strip()
        ]
        if not rows or len(rows) > V1_EXPORT_BUDGETS.max_sessions:
            raise ValueError("task count is outside the supported range")
        session_ids: list[str] = []
        task_provenance: list[TaskProvenance] = []
        for index, row in enumerate(rows):
            if set(row) != _TASK_DATA_KEYS or row["idx"] != index:
                raise ValueError(
                    "TaskData fields or indices violate the public contract"
                )
            if row["prompt"] != json.dumps(
                row["inputs"],
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ):
                raise ValueError("TaskData prompt and inputs do not match")
            session_id = row["kitaru_session_id"]
            private_task = json.loads(
                (module / "scoring/tasks" / f"{session_id}.json").read_text()
            )
            if (
                private_task["session_id"] != session_id
                or private_task["content_digest"] != row["kitaru_content_digest"]
                or _canonical_digest(private_task["bridge_task"]["context"])
                != row["kitaru_content_digest"]
            ):
                raise ValueError(
                    "TaskData provenance does not match private scoring data"
                )
            session_ids.append(session_id)
            task_provenance.append(
                TaskProvenance(
                    session_id=session_id,
                    content_digest=row["kitaru_content_digest"],
                )
            )
        if session_ids != sorted(session_ids) or len(set(session_ids)) != len(
            session_ids
        ):
            raise ValueError("tasks are not canonically ordered by unique session ID")
        if tuple(task_provenance) != manifest.task_provenance:
            raise ValueError("manifest task provenance does not match TaskData")
        scoring_requirements = tuple(
            json.loads((module / "scoring/requirements.json").read_text())
        )
        if scoring_requirements != manifest.runtime_requirements.task_private:
            raise ValueError("Task-private requirement ownership does not match")
        if (
            tuple(harness_config.get("forward_env", []))
            != manifest.runtime_requirements.bundled_harness
        ):
            raise ValueError("bundled Harness requirement ownership does not match")
        if _exported_names(module) != ["KitaruHarness", "KitaruTaskset"]:
            raise ValueError(
                "generated __all__ must expose one Harness and one Taskset"
            )

        for path in (
            module / "__init__.py",
            module / "plugin.py",
            module / "scoring.py",
            *(module / "bridge").glob("*.py"),
        ):
            source = path.read_text()
            if "kitaru.exports" in source:
                raise ValueError(
                    "generated Python imports private Kitaru exporter modules"
                )
            ast.parse(source, filename=path.relative_to(root).as_posix())
        _validate_runtime_bridge(
            root, provenance.module_name, manifest_data.get("runtime_bridge")
        )
        recorded = manifest_data.get("generated_files")
        actual = file_digests(root)
        actual.pop("kitaru-export.json", None)
        if not isinstance(recorded, dict) or recorded != actual:
            raise ValueError("generated file digests do not match")
        _validate_generated_resources(root)
    except ExportError:
        raise
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        json.JSONDecodeError,
        tomllib.TOMLDecodeError,
    ) as error:
        raise ExportError(
            "invalid_verifiers_bundle",
            f"Generated Verifiers bundle is invalid: {error}",
        ) from error
    return ValidationReceipt(
        level="structural", status="passed", target_version=VERIFIERS_VERSION
    )
