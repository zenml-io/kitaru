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
"""Render a frozen experiment as a Verifiers 0.3 v1 environment."""

import ast
import json
import tomllib
from importlib.metadata import version
from pathlib import Path

from kitaru.api_models.v1.plugin import PackagePluginSource
from kitaru.exports.config import normalize_environment_names
from kitaru.exports.models import (
    ExportError,
    ExportManifest,
    ResolvedExport,
    ValidationReceipt,
)
from kitaru.exports.source import copy_source
from kitaru.exports.writer import file_digests, write_canonical_json

_TARGET_VERSION = "0.3.0"
_PACKAGE = "kitaru_verifiers_v1"
_KITARU_VERSION = version("kitaru")


def _quote_toml(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _pyproject(resolved: ResolvedExport) -> str:
    dependencies = [f"kitaru=={_KITARU_VERSION}", f"verifiers=={_TARGET_VERSION}"]
    dependencies.extend(
        source.requirement
        for evaluator in resolved.evaluators
        if isinstance((source := evaluator.version.source), PackagePluginSource)
    )
    rendered_dependencies = ", ".join(
        _quote_toml(dependency) for dependency in dependencies
    )
    return f'''[project]
name = "kitaru-verifiers-v1"
version = "0.1.0"
description = "Kitaru experiment exported for Verifiers v1."
requires-python = ">=3.11"
dependencies = [{rendered_dependencies}]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["{_PACKAGE}"]

[tool.hatch.build.targets.wheel.force-include]
"agent_source" = "{_PACKAGE}/agent_source"
"data" = "{_PACKAGE}/data"
"evaluators" = "{_PACKAGE}/evaluators"
'''


def _init_module() -> str:
    return """from kitaru_verifiers_v1.harness import KitaruHarness
from kitaru_verifiers_v1.taskset import KitaruTaskset

__all__ = ["KitaruHarness", "KitaruTaskset"]
"""


def _bridge_module() -> str:
    return """import json
import os
from pathlib import Path
from typing import Any

from kitaru.exports.evaluators import evaluate_session, load_evaluator
from kitaru.exports.models import MaterializedEvaluator, RewardSelector
from kitaru.exports.trace import convert_trace
from kitaru.api_models.v1.evaluator import EvaluatorVersionResponse


PACKAGE_ROOT = Path(__file__).resolve().parent
BUNDLE_ROOT = PACKAGE_ROOT.parent


def asset_root(name: str) -> Path:
    packaged = PACKAGE_ROOT / name
    return packaged if packaged.exists() else BUNDLE_ROOT / name


def evaluate(trace: dict[str, Any], data: Any) -> tuple[float, dict[str, float]]:
    records = json.loads((asset_root("evaluators") / "evaluators.json").read_text())
    loaded = []
    for index, record in enumerate(records):
        script_path = (
            asset_root("evaluators") / f"{index}.py"
            if record["script"]
            else None
        )
        materialized = MaterializedEvaluator(
            name=record["name"],
            version=EvaluatorVersionResponse.model_validate(record["version"]),
            params=record["params"],
            script=script_path.read_bytes() if script_path is not None else None,
            source_sha256=record["source_sha256"],
        )
        loaded.append(
            (materialized, load_evaluator(materialized, script_path=script_path))
        )
    secrets = [
        value
        for name in data.required_environment_names
        if (value := os.environ.get(name))
    ]
    session = convert_trace(
        trace,
        format="verifiers-v1",
        context=data.get_context(),
        secret_values=secrets,
    )
    outcome = evaluate_session(
        loaded,
        RewardSelector.parse(data.reward_selector),
        session,
        secret_values=secrets,
    )
    return outcome.reward, outcome.metrics
"""


def _taskset_module() -> str:
    return """import json
from typing import Any

import verifiers.v1 as vf
from kitaru.api_models.v1.session_node import SessionWithNodesResponse

from kitaru_verifiers_v1.bridge import asset_root, evaluate


class KitaruData(vf.TaskData):
    inputs: Any
    context: dict[str, Any]
    reward_selector: str
    required_environment_names: list[str]

    def get_context(self) -> SessionWithNodesResponse:
        return SessionWithNodesResponse.model_validate(self.context)


class KitaruTask(vf.Task[KitaruData]):
    NEEDS_CONTAINER = True

    @vf.reward(weight=1.0)
    async def kitaru_reward(self, trace: vf.Trace) -> float:
        reward, metrics = evaluate(trace.model_dump(mode="json"), self.data)
        trace.record_metrics(metrics)
        return reward


class KitaruTaskset(vf.Taskset[KitaruTask, vf.TasksetConfig]):
    def load(self) -> list[KitaruTask]:
        tasks = []
        path = asset_root("data") / "tasks.jsonl"
        for line in path.read_text().splitlines():
            row = json.loads(line)
            row["timeout"] = vf.TaskTimeout(agent=row.pop("timeout_seconds"))
            tasks.append(KitaruTask(KitaruData.model_validate(row), self.config.task))
        return tasks
"""


def _harness_module(resolved: ResolvedExport) -> str:
    run_spec = resolved.agent_version.run_spec
    assert run_spec is not None
    command = repr(run_spec.command)
    working_directory = repr(run_spec.working_dir or "")
    environment = repr(dict(sorted(run_spec.env.items())))
    executable_paths = repr(
        [file.path for file in resolved.source.files if file.mode == 0o755]
    )
    return f"""import json
import shlex
from pathlib import PurePosixPath

import verifiers.v1 as vf

from kitaru_verifiers_v1.bridge import asset_root


_COMMAND = {command}
_WORKING_DIRECTORY = {working_directory}
_ENVIRONMENT = {environment}
_EXECUTABLE_PATHS = {executable_paths}
_RUNTIME_ROOT = PurePosixPath("/workspace/kitaru-agent")


class KitaruHarnessConfig(vf.HarnessConfig):
    pass


class KitaruHarness(vf.Harness[KitaruHarnessConfig]):
    APPENDS_SYSTEM_PROMPT = True
    SUPPORTS_MCP = True

    async def setup(self, runtime: vf.Runtime) -> None:
        source = asset_root("agent_source")
        for path in sorted(item for item in source.rglob("*") if item.is_file()):
            target = (_RUNTIME_ROOT / path.relative_to(source).as_posix()).as_posix()
            await runtime.write(target, path.read_bytes())
        if _EXECUTABLE_PATHS:
            await runtime.run(
                [
                    "chmod",
                    "+x",
                    *[
                        (_RUNTIME_ROOT / path).as_posix()
                        for path in _EXECUTABLE_PATHS
                    ],
                ],
                {{}},
            )

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
        working_directory = _RUNTIME_ROOT / _WORKING_DIRECTORY
        command = (
            f"cd {{shlex.quote(working_directory.as_posix())}} "
            f"&& exec {{_COMMAND}}"
        )
        env = {{
            **_ENVIRONMENT,
            **self.config.resolved_env,
            "KITARU_TASK_INPUTS": json.dumps(
                data.inputs, sort_keys=True, separators=(",", ":")
            ),
            "KITARU_MCP_URLS": json.dumps(
                mcp_urls, sort_keys=True, separators=(",", ":")
            ),
            "OPENAI_BASE_URL": endpoint,
            "OPENAI_API_KEY": secret,
            "OPENAI_MODEL": ctx.model,
        }}
        return await runtime.run_program(["sh", "-lc", command], env)


from kitaru_verifiers_v1.taskset import KitaruData
"""


def _readme(required_environment_names: tuple[str, ...]) -> str:
    forwarded = "".join(
        f" \\\n  --env.agent.harness.forward-env {name}"
        for name in required_environment_names
    )
    return f"""# Kitaru Verifiers v1 export

This environment contains a frozen Kitaru cohort, the registered agent source and
command, and the pinned Kitaru evaluators used for reward.

Install the environment and run it with Verifiers 0.3.0:

```bash
uv sync
uv run eval \\
  --env.taskset.id kitaru-verifiers-v1 \\
  --model MODEL \\
  --env.agent.runtime.type docker{forwarded}
```

`eval` is the Verifiers CLI. `kitaru-verifiers-v1` is the fixed local taskset
package generated by Kitaru, not the user's agent name. Replace `MODEL` with the
model identifier to evaluate. Verifiers supplies the Runtime sandbox and intercepts
the model calls that form the v1 trace. The generated harness copies the user's
`agent_source/` into the runtime, sets `KITARU_TASK_INPUTS`, model endpoint
variables, and MCP URLs, then runs the registered shell command unchanged.

The task fails if the trace cannot be converted, an evaluator fails, or the selected
evaluator result does not produce a numeric reward.
"""


def _write_tasks(
    root: Path,
    resolved: ResolvedExport,
    required_environment_names: tuple[str, ...],
) -> None:
    path = root / "data" / "tasks.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    run_spec = resolved.agent_version.run_spec
    assert run_spec is not None
    selector = resolved.reward
    reward = f"{selector.evaluator}:{selector.result}:{selector.field}"
    with path.open("wb") as tasks:
        for index, session in enumerate(resolved.sessions):
            row = {
                "idx": index,
                "name": session.session.name or str(session.session.id),
                "prompt": json.dumps(
                    session.session.inputs,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ),
                "inputs": session.session.inputs,
                "context": session.model_dump(mode="json"),
                "reward_selector": reward,
                "required_environment_names": list(required_environment_names),
                "timeout_seconds": run_spec.timeout_seconds,
            }
            tasks.write(
                (
                    json.dumps(
                        row,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                        allow_nan=False,
                    )
                    + "\n"
                ).encode()
            )


def _write_evaluators(root: Path, resolved: ResolvedExport) -> None:
    records = []
    directory = root / "evaluators"
    directory.mkdir(parents=True, exist_ok=True)
    for index, evaluator in enumerate(resolved.evaluators):
        if evaluator.script is not None:
            (directory / f"{index}.py").write_bytes(evaluator.script)
        records.append(
            {
                "name": evaluator.name,
                "version": evaluator.version.model_dump(mode="json"),
                "params": evaluator.params,
                "script": evaluator.script is not None,
                "source_sha256": evaluator.source_sha256,
            }
        )
    write_canonical_json(directory / "evaluators.json", records)


def render_verifiers_v1(
    resolved: ResolvedExport,
    root: Path,
    *,
    required_environment_names: tuple[str, ...] = (),
) -> ExportManifest:
    """Render a complete Verifiers 0.3.0 v1 environment into an empty root."""
    try:
        required_names = normalize_environment_names(required_environment_names)
    except ValueError as error:
        raise ExportError(
            "invalid_environment_name",
            "Required environment names must be unique identifiers.",
        ) from error
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        raise ExportError(
            "invalid_destination", "Verifiers staging directory must be empty."
        )
    (root / "README.md").write_text(_readme(required_names))
    (root / "pyproject.toml").write_text(_pyproject(resolved))
    package = root / _PACKAGE
    package.mkdir()
    (package / "__init__.py").write_text(_init_module())
    (package / "bridge.py").write_text(_bridge_module())
    (package / "taskset.py").write_text(_taskset_module())
    (package / "harness.py").write_text(_harness_module(resolved))
    copy_source(resolved.source, root / "agent_source")
    _write_tasks(root, resolved, required_names)
    _write_evaluators(root, resolved)

    receipt = ValidationReceipt(
        level="structural", status="passed", target_version=_TARGET_VERSION
    )
    selector = resolved.reward
    generated = file_digests(root)
    manifest = ExportManifest(
        format="verifiers-v1",
        target_version=_TARGET_VERSION,
        experiment_id=resolved.experiment.id,
        cohort_version_id=resolved.cohort_version.id,
        agent_version_id=resolved.agent_version.id,
        evaluator_version_ids=tuple(
            evaluator.version.id for evaluator in resolved.evaluators
        ),
        primary_reward=(f"{selector.evaluator}:{selector.result}:{selector.field}"),
        source_digest=resolved.source.digest,
        generated_files=generated,
        required_environment_names=required_names,
        exclusions=resolved.source.excluded,
        validation=receipt,
    )
    write_canonical_json(root / "kitaru-export.json", manifest.model_dump(mode="json"))
    validate_verifiers_v1(root)
    return manifest


def validate_verifiers_v1(root: Path) -> ValidationReceipt:
    """Validate the generated structure without importing exported code."""
    required = {
        "README.md",
        "pyproject.toml",
        "kitaru-export.json",
        "data/tasks.jsonl",
        "evaluators/evaluators.json",
        f"{_PACKAGE}/__init__.py",
        f"{_PACKAGE}/bridge.py",
        f"{_PACKAGE}/harness.py",
        f"{_PACKAGE}/taskset.py",
    }
    missing = sorted(path for path in required if not (root / path).is_file())
    try:
        project = tomllib.loads((root / "pyproject.toml").read_text())
        manifest = json.loads((root / "kitaru-export.json").read_text())
        task_count = 0
        with (root / "data" / "tasks.jsonl").open() as task_file:
            for line in task_file:
                if not line.strip():
                    continue
                task = json.loads(line)
                if task.get("idx") != task_count:
                    raise ValueError("task indices are not contiguous")
                task_count += 1
    except (OSError, ValueError, KeyError, TypeError, tomllib.TOMLDecodeError) as error:
        raise ExportError(
            "invalid_verifiers_bundle", f"Could not parse Verifiers bundle: {error}"
        ) from error

    dependencies = project.get("project", {}).get("dependencies", [])
    if (
        missing
        or manifest.get("format") != "verifiers-v1"
        or manifest.get("target_version") != _TARGET_VERSION
        or f"verifiers=={_TARGET_VERSION}" not in dependencies
        or task_count == 0
    ):
        detail = f" Missing files: {', '.join(missing)}." if missing else ""
        raise ExportError(
            "invalid_verifiers_bundle",
            f"Generated artifact does not match the Verifiers v1 contract.{detail}",
        )

    try:
        for filename in ("__init__.py", "bridge.py", "harness.py", "taskset.py"):
            path = root / _PACKAGE / filename
            ast.parse(path.read_text(), filename=path.as_posix())
    except (OSError, SyntaxError) as error:
        raise ExportError(
            "invalid_verifiers_bundle",
            f"Generated Python source is invalid: {error}",
        ) from error

    recorded = manifest.get("generated_files")
    actual = {
        path: digest
        for path, digest in file_digests(root).items()
        if path != "kitaru-export.json"
    }
    if not isinstance(recorded, dict) or recorded != actual:
        raise ExportError(
            "invalid_verifiers_bundle", "Generated file digests do not match."
        )
    return ValidationReceipt(
        level="structural", status="passed", target_version=_TARGET_VERSION
    )
