"""Verify generated exporters against their exact target releases."""

import argparse
import asyncio
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import tomllib
import uuid
import zipfile
from dataclasses import replace
from datetime import UTC, datetime
from email.parser import BytesParser
from pathlib import Path
from typing import Any

HARBOR_VERSION = "0.20.0"
VERIFIERS_VERSION = "0.3.0"
PRIME_RL_VERSION = "0.8.0"
KITARU_TEMPLATE_COMMIT = "2e5606198d560a9d412e89de8f4629952d439602"
PRIME_RL_ARCHIVE = (
    "https://github.com/PrimeIntellect-ai/prime-rl/archive/refs/tags/"
    f"v{PRIME_RL_VERSION}.tar.gz"
)
_TIMEOUT_SECONDS = 600
_SCRUBBED_ENVIRONMENT_VARIABLES = {
    "CONDA_DEFAULT_ENV",
    "CONDA_PREFIX",
    "PIPENV_ACTIVE",
    "POETRY_ACTIVE",
    "PYTHONHOME",
    "PYTHONPATH",
    "UV_ACTIVE",
    "UV_PROJECT_ENVIRONMENT",
    "VIRTUAL_ENV",
    "VIRTUAL_ENV_PROMPT",
}
_PRIVATE_SENTINELS = (
    "HISTORICAL_ANSWER",
    "HISTORICAL_EVALUATOR_RESULT",
    "HISTORICAL_REASONING",
    "HISTORICAL_TOOL_RESULT",
)


class SmokeFailure(RuntimeError):
    """Report one failed exact-target assertion."""


def _run(
    command: list[str | Path],
    *,
    cwd: Path,
    environment: dict[str, str],
    timeout: int = _TIMEOUT_SECONDS,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [str(part) for part in command],
        cwd=cwd,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )
    if result.returncode != 0:
        output = (result.stderr or result.stdout).strip()
        raise SmokeFailure(
            f"command failed ({result.returncode}): {' '.join(map(str, command))}"
            + (f"\n{output[-8000:]}" if output else "")
        )
    return result


def _isolated_environment(root: Path) -> dict[str, str]:
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("KITARU_")
        and name not in _SCRUBBED_ENVIRONMENT_VARIABLES
    }
    directories = {
        "HOME": root / "home",
        "XDG_CACHE_HOME": root / "xdg/cache",
        "XDG_CONFIG_HOME": root / "xdg/config",
        "XDG_DATA_HOME": root / "xdg/data",
        "XDG_STATE_HOME": root / "xdg/state",
    }
    for name, directory in directories.items():
        directory.mkdir(parents=True, exist_ok=True)
        environment[name] = str(directory)
    return environment


def _python(venv: Path) -> Path:
    return venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def _console(venv: Path, name: str) -> Path:
    suffix = ".exe" if os.name == "nt" else ""
    return venv / ("Scripts" if os.name == "nt" else "bin") / f"{name}{suffix}"


def _create_environment(
    uv: str, root: Path, requirements: list[str | Path]
) -> tuple[Path, dict[str, str]]:
    environment = _isolated_environment(root)
    venv = root / "venv"
    _run(
        [uv, "venv", "--python", "3.12", venv],
        cwd=root,
        environment=environment,
    )
    python = _python(venv)
    _run(
        [uv, "pip", "install", "--python", python, *requirements],
        cwd=root,
        environment=environment,
    )
    return python, environment


def _wheel_metadata(wheel: Path) -> Any:
    with zipfile.ZipFile(wheel) as archive:
        names = [name for name in archive.namelist() if name.endswith("/METADATA")]
        if len(names) != 1:
            raise SmokeFailure("candidate wheel must contain exactly one METADATA file")
        return BytesParser().parsebytes(archive.read(names[0]))


def _assert_core_metadata(repository: Path, wheel: Path) -> None:
    targets = {"harbor", "prime-rl", "verifiers"}
    metadata = _wheel_metadata(wheel)
    requirements = metadata.get_all("Requires-Dist", [])
    normalized = {
        requirement.split(";", 1)[0]
        .split("[", 1)[0]
        .split(" ", 1)[0]
        .strip()
        .lower()
        .replace("_", "-")
        for requirement in requirements
    }
    overlap = targets & normalized
    if overlap:
        raise SmokeFailure(f"core wheel depends on target packages: {sorted(overlap)}")

    project = tomllib.loads((repository / "pyproject.toml").read_text())["project"]
    optional_dependencies = [
        requirement
        for requirements in project.get("optional-dependencies", {}).values()
        for requirement in requirements
    ]
    root_dependencies = "\n".join(
        [*project.get("dependencies", []), *optional_dependencies]
    ).lower()
    if any(target in root_dependencies for target in targets):
        raise SmokeFailure("root Kitaru metadata contains target dependencies")


def _assert_distribution_source(name: str, expected: str) -> None:
    distributions = [
        distribution
        for distribution in importlib.metadata.distributions()
        if (distribution.metadata["Name"] or "").lower().replace("_", "-") == name
    ]
    if len(distributions) != 1:
        raise SmokeFailure(f"expected one installed {name} distribution")
    direct_url = distributions[0].read_text("direct_url.json")
    if direct_url is None or expected not in direct_url:
        raise SmokeFailure(f"{name} was not installed from {expected}")


def _assert_exporters_absent() -> None:
    """Require target consumer environments to omit generator plugins."""
    for distribution, module in (
        ("kitaru-harbor-exporter", "kitaru_harbor_exporter"),
        ("kitaru-verifiers-exporter", "kitaru_verifiers_exporter"),
    ):
        try:
            importlib.metadata.version(distribution)
        except importlib.metadata.PackageNotFoundError:
            pass
        else:
            raise SmokeFailure(f"target environment contains {distribution}")
        if importlib.util.find_spec(module) is not None:
            raise SmokeFailure(f"target environment can import {module}")


def _verify_installed_exporters(expected: set[str]) -> None:
    """Verify installed discovery and exact missing-plugin behavior."""
    from kitaru.exports.models import ExportError
    from kitaru.exports.plugin import resolve_exporter

    for format_name, distribution in (
        ("harbor", "kitaru-harbor-exporter"),
        ("verifiers-v1", "kitaru-verifiers-exporter"),
    ):
        if format_name in expected:
            loaded = resolve_exporter(format_name)  # type: ignore[arg-type]
            if loaded.metadata.distribution_name != distribution:
                raise SmokeFailure(f"{format_name} resolved the wrong distribution")
            continue
        try:
            resolve_exporter(format_name)  # type: ignore[arg-type]
        except ExportError as error:
            if (
                error.code != "exporter_not_installed"
                or distribution not in error.message
            ):
                raise SmokeFailure(
                    f"{format_name} did not return its exact missing-package error"
                ) from error
        else:
            raise SmokeFailure(f"{format_name} unexpectedly resolved")


def _fixture_agent_source() -> str:
    return textwrap.dedent(
        '''
        """Provider-free exact-target smoke agent."""

        import json
        import os
        import urllib.request
        from pathlib import Path


        def main() -> None:
            inputs = json.loads(os.environ["KITARU_TASK_INPUTS"])
            trace_path = os.environ.get("KITARU_TRACE_PATH")
            if trace_path:
                trace = {
                    "schema_version": "ATIF-v1.7",
                    "agent": {"name": "kitaru-smoke", "version": "1"},
                    "steps": [
                        {"step_id": 1, "source": "user", "message": inputs["question"]},
                        {
                            "step_id": 2,
                            "source": "agent",
                            "message": "The answer is 42.",
                        },
                    ],
                }
                Path(trace_path).write_text(json.dumps(trace))
                return

            request = urllib.request.Request(
                os.environ["OPENAI_BASE_URL"].rstrip("/") + "/chat/completions",
                data=json.dumps(
                    {
                        "model": os.environ["OPENAI_MODEL"],
                        "messages": [{"role": "user", "content": inputs["question"]}],
                    }
                ).encode(),
                headers={
                    "Authorization": "Bearer " + os.environ["OPENAI_API_KEY"],
                    "Content-Type": "application/json",
                },
            )
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.loads(response.read())
            print(payload["choices"][0]["message"]["content"])


        if __name__ == "__main__":
            main()
        '''
    ).lstrip()


def _fixture_evaluator_source() -> bytes:
    return (
        textwrap.dedent(
            """
        from kitaru.api_models.v1.evaluation import EvaluationResult


        def evaluate(session):
            output = str(session.session.outputs).strip()
            return EvaluationResult(
                name="correctness", score=1.0 if output.endswith("42.") else 0.0
            )
        """
        )
        .lstrip()
        .encode()
    )


def _template_evaluator_source() -> bytes:
    return (
        textwrap.dedent(
            """
        import json

        from kitaru.api_models.v1.evaluation import EvaluationResult


        def evaluate(session):
            try:
                value = session.session.outputs
                output = value if isinstance(value, dict) else json.loads(str(value))
            except (TypeError, ValueError):
                output = {}
            valid = output.get("action") in {
                "refund", "replacement", "escalate", "reject"
            }
            return EvaluationResult(
                name="valid_resolution", score=1.0 if valid else 0.0
            )
        """
        )
        .lstrip()
        .encode()
    )


def _copy_template_source(source: Path, destination: Path) -> None:
    source = source.resolve()
    archive = destination.parent / "kitaru-template.tar"
    _run(
        [
            "git",
            "-C",
            source,
            "archive",
            "--format=tar",
            "--output",
            archive,
            KITARU_TEMPLATE_COMMIT,
        ],
        cwd=source,
        environment=os.environ.copy(),
    )
    destination.mkdir()
    with tarfile.open(archive) as template:
        template.extractall(destination, filter="data")


def _generate_artifacts(
    root: Path,
    *,
    candidate_wheel: Path | None = None,
    harbor_exporter_wheel: Path | None = None,
    verifiers_exporter_wheel: Path | None = None,
    template_source: Path | None = None,
) -> None:
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
    from kitaru.exports._dependencies import classify_dependencies
    from kitaru.exports.models import (
        MaterializedEvaluator,
        ResolvedExport,
        RewardSelector,
    )
    from kitaru.exports.plugin import (
        ExporterContext,
        ExporterOptions,
        resolve_exporter,
    )
    from kitaru.exports.source import inventory_source

    now = datetime(2026, 1, 1, tzinfo=UTC)
    evaluator_script = _fixture_evaluator_source()

    if candidate_wheel is not None:
        _assert_distribution_source("kitaru", candidate_wheel.name)
    if harbor_exporter_wheel is not None:
        _assert_distribution_source(
            "kitaru-harbor-exporter", harbor_exporter_wheel.name
        )
    if verifiers_exporter_wheel is not None:
        _assert_distribution_source(
            "kitaru-verifiers-exporter", verifiers_exporter_wheel.name
        )

    def resolved(source_root: Path, agent_version: int) -> Any:
        source_root.mkdir(parents=True)
        (source_root / "agent.py").write_text(_fixture_agent_source())
        (source_root / "pyproject.toml").write_text(
            '[project]\nname = "kitaru-export-smoke-agent"\nversion = "1.0.0"\n'
        )
        source = inventory_source(source_root)
        sessions = tuple(
            SessionWithNodesResponse(
                session=SessionResponse(
                    id=uuid.UUID(int=1000 + index),
                    owner_id=uuid.UUID(int=2),
                    agent_id=uuid.UUID(int=3),
                    agent_version_id=uuid.UUID(int=99),
                    number=index + 1,
                    origin=SessionOrigin.RECORDED,
                    status=SessionStatus.COMPLETED,
                    inputs={"question": f"What is six times seven? Case {index}."},
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
            for index in range(2)
        )
        evaluator = MaterializedEvaluator(
            name="quality",
            version=EvaluatorVersionResponse.model_construct(
                id=uuid.UUID(int=20),
                evaluator_id=uuid.UUID(int=21),
                version=1,
                display_version="1",
                source=ScriptPluginSource(
                    blob_id=uuid.UUID(int=22), entrypoint="evaluate"
                ),
                created=now,
                updated=now,
            ),
            params={},
            script=evaluator_script,
            source_sha256=hashlib.sha256(evaluator_script).hexdigest(),
        )
        return ResolvedExport(
            experiment=ExperimentResponse.model_construct(
                id=uuid.UUID(int=1), name="Export smoke"
            ),
            cohort_version=CohortVersionResponse.model_construct(id=uuid.UUID(int=2)),
            agent_version=AgentVersionResponse.model_construct(
                id=uuid.UUID(int=agent_version),
                run_spec=RunSpec(
                    command="python agent.py",
                    env={},
                    timeout_seconds=90,
                ),
            ),
            sessions=sessions,
            evaluators=(evaluator,),
            reward=RewardSelector.parse("quality:correctness:score"),
            source=source,
            command_argv=("python", "agent.py"),
            dependency_plan=classify_dependencies(source),
        )

    first = resolved(root / "source-first", 30)
    second = resolved(root / "source-second", 31)
    harbor = resolve_exporter("harbor")
    harbor_context = ExporterContext(
        exporter=harbor.provenance, cancellation_checkpoint=lambda: None
    )
    harbor_options = ExporterOptions(
        trace_format="atif", trace_path="/workspace/trajectory.json"
    )
    harbor.implementation.preflight(
        first, options=harbor_options, context=harbor_context
    )
    harbor.implementation.render(
        first,
        root / "harbor",
        options=harbor_options,
        context=harbor_context,
    )
    verifiers = resolve_exporter("verifiers-v1")
    verifiers_context = ExporterContext(
        exporter=verifiers.provenance, cancellation_checkpoint=lambda: None
    )
    verifiers_options = ExporterOptions()
    for resolved_export, destination in (
        (first, root / "verifiers-first"),
        (second, root / "verifiers-second"),
    ):
        verifiers.implementation.preflight(
            resolved_export,
            options=verifiers_options,
            context=verifiers_context,
        )
        verifiers.implementation.render(
            resolved_export,
            destination,
            options=verifiers_options,
            context=verifiers_context,
        )
    if template_source is not None:
        if candidate_wheel is None:
            raise SmokeFailure("template generation requires the candidate wheel")
        template_root = root / "source-template"
        _copy_template_source(template_source, template_root)
        (template_root / "uv.lock").unlink()
        wheel_directory = template_root / "candidate-wheels"
        wheel_directory.mkdir()
        shutil.copy2(candidate_wheel, wheel_directory / candidate_wheel.name)
        project_path = template_root / "pyproject.toml"
        project = project_path.read_text()
        if '"kitaru[cli,mcp,server,worker]>=0.22.0",' not in project:
            raise SmokeFailure("pinned template Kitaru requirement changed")
        project_path.write_text(
            project
            + "\n[tool.uv.sources]\n"
            + f'kitaru = {{ path = "candidate-wheels/{candidate_wheel.name}" }}\n'
        )
        template_evaluator_script = _template_evaluator_source()
        template_session = first.sessions[0].model_copy(
            update={
                "session": first.sessions[0].session.model_copy(
                    update={
                        "id": uuid.UUID(int=2000),
                        "inputs": {
                            "ticket_id": "ticket-001",
                            "customer_name": "Dana",
                            "email": "dana@example.test",
                            "subject": "Hole in my Merino Runners",
                            "body": (
                                "Order #48213 arrived with a hole in the left shoe. "
                                "Please refund it."
                            ),
                        },
                        "number": 1,
                    }
                )
            }
        )
        template = replace(
            first,
            agent_version=first.agent_version.model_copy(
                update={
                    "id": uuid.UUID(int=32),
                    "run_spec": RunSpec(
                        command="python -m returns_agent.agent",
                        env={},
                        timeout_seconds=90,
                    ),
                }
            ),
            sessions=(template_session,),
            evaluators=(
                replace(
                    first.evaluators[0],
                    script=template_evaluator_script,
                    source_sha256=hashlib.sha256(template_evaluator_script).hexdigest(),
                ),
            ),
            reward=RewardSelector.parse("quality:valid_resolution:score"),
            source=inventory_source(template_root),
            command_argv=("python", "-m", "returns_agent.agent"),
            dependency_plan=classify_dependencies(inventory_source(template_root)),
        )
        verifiers.implementation.preflight(
            template, options=verifiers_options, context=verifiers_context
        )
        verifiers.implementation.render(
            template,
            root / "verifiers-template",
            options=verifiers_options,
            context=verifiers_context,
        )


def _verify_harbor(root: Path, candidate_wheel: Path, runtime: bool) -> None:
    from harbor.models.dataset.manifest import DatasetManifest
    from harbor.models.job.result import JobResult
    from harbor.models.task.config import TaskConfig
    from harbor.models.task.task import Task
    from harbor.models.trial.result import TrialResult
    from harbor.publisher.packager import Packager

    if importlib.metadata.version("harbor") != HARBOR_VERSION:
        raise SmokeFailure("wrong Harbor version installed")
    _assert_exporters_absent()
    _assert_distribution_source("kitaru", candidate_wheel.name)
    manifest = DatasetManifest.from_toml_file(root / "dataset/dataset.toml")
    task_directories = sorted((root / "dataset").glob("task-*"))
    if len(manifest.tasks) != 2 or len(task_directories) != 2:
        raise SmokeFailure("Harbor dataset does not contain two tasks")
    for reference, task_dir in zip(manifest.tasks, task_directories, strict=True):
        task_config = TaskConfig.model_validate_toml(
            (task_dir / "task.toml").read_text()
        )
        digest, _ = Packager.compute_content_hash(task_dir)
        if task_config.schema_version != "1.3":
            raise SmokeFailure("Harbor task did not parse as schema 1.3")
        if reference.digest != f"sha256:{digest}":
            raise SmokeFailure("Harbor official digest differs from manifest")
        if not Task.is_valid_dir(task_dir):
            raise SmokeFailure("Harbor runner rejects a generated task directory")
    if not runtime:
        return

    runtime_data = json.loads((root / "agent_image/agent-runtime.json").read_text())
    image = TaskConfig.model_validate_toml(
        (task_directories[0] / "task.toml").read_text()
    ).environment.docker_image
    if not image:
        raise SmokeFailure("Harbor task has no Docker image")
    wheel_dir = root / "agent_image/candidate-wheels"
    wheel_dir.mkdir()
    shutil.copy2(candidate_wheel, wheel_dir / candidate_wheel.name)
    bridge_requirements = root / "agent_image/bridge-requirements.txt"
    bridge_lines = bridge_requirements.read_text().splitlines()
    bridge_requirements.write_text(
        "\n".join(line for line in bridge_lines if not line.startswith("kitaru=="))
    )
    dockerfile = root / "agent_image/Dockerfile"
    source = dockerfile.read_text()
    install_start = source.find(
        "COPY bridge-requirements.txt evaluator-requirements.txt /opt/kitaru-export/"
    )
    if install_start < 0:
        raise SmokeFailure("could not find the Harbor dependency install block")
    source = (
        source[:install_start]
        + "COPY candidate-wheels/ /opt/kitaru-candidate/\n"
        + f"RUN uv pip install --system /opt/kitaru-candidate/{candidate_wheel.name}\n"
        + source[install_start:]
    )
    dockerfile.write_text(source)
    environment = os.environ.copy()
    _run(
        ["docker", "build", "-t", image, "agent_image"],
        cwd=root,
        environment=environment,
    )
    _run(
        [
            "docker",
            "run",
            "--rm",
            image,
            "python",
            "-c",
            (
                "import importlib.metadata as m; "
                f"assert m.version('kitaru') == "
                f"{importlib.metadata.version('kitaru')!r}; "
                f"assert {candidate_wheel.name!r} in "
                "m.distribution('kitaru').read_text('direct_url.json')"
            ),
        ],
        cwd=root,
        environment=environment,
    )
    jobs = root / "runtime-results"
    run_environment = environment | {"PYTHONPATH": str(root)}
    harbor = Path(sys.executable).parent / (
        "harbor.exe" if os.name == "nt" else "harbor"
    )
    _run(
        [
            harbor,
            "run",
            "-p",
            "dataset",
            "--agent",
            "agent.kitaru_agent:KitaruAgent",
            "-l",
            "1",
            "-n",
            "1",
            "-q",
            "-y",
            "-o",
            jobs,
        ],
        cwd=root,
        environment=run_environment,
    )
    result_files = list(jobs.glob("*/result.json"))
    if len(result_files) != 1:
        raise SmokeFailure("Harbor runtime did not produce one official job result")
    job_result = JobResult.model_validate_json(result_files[0].read_text())
    trial_files = [
        path
        for path in jobs.rglob("result.json")
        if len(path.relative_to(jobs).parts) > 2
    ]
    if len(trial_files) != 1:
        raise SmokeFailure("Harbor runtime did not complete exactly one trial")
    trial_result = TrialResult.model_validate_json(trial_files[0].read_text())
    if trial_result.exception_info is not None:
        raise SmokeFailure(
            "Harbor trial failed: "
            f"{trial_result.exception_info.exception_type}: "
            f"{trial_result.exception_info.exception_message}"
        )
    if trial_result.verifier_result is None or trial_result.verifier_result.rewards != {
        "reward": 1.0
    }:
        raise SmokeFailure("Harbor runtime did not produce the expected reward")
    if (
        job_result.stats.n_completed_trials != 1
        or job_result.stats.n_errored_trials != 0
    ):
        raise SmokeFailure("Harbor aggregate result did not record one clean trial")
    if runtime_data["trace_format"] != "atif":
        raise SmokeFailure("Harbor runtime did not execute the ATIF fixture")


def _write_override_package(root: Path) -> None:
    module = root / "kitaru_smoke_harness"
    module.mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        textwrap.dedent(
            """
            [project]
            name = "kitaru-smoke-harness"
            version = "0.1.0"
            requires-python = ">=3.12,<3.14"
            dependencies = ["verifiers==0.3.0"]

            [build-system]
            requires = ["hatchling"]
            build-backend = "hatchling.build"

            [tool.hatch.build.targets.wheel]
            packages = ["kitaru_smoke_harness"]
            """
        ).lstrip()
    )
    (module / "__init__.py").write_text(
        textwrap.dedent(
            """
            import verifiers.v1 as vf


            class SmokeHarness(vf.Harness[vf.HarnessConfig]):
                NEEDS_CONTAINER = True

                async def launch(
                    self, ctx, trace, runtime, endpoint, secret, mcp_urls, data
                ):
                    return vf.ProgramResult(exit_code=0, stdout="", stderr="")


            __all__ = ["SmokeHarness"]
            """
        ).lstrip()
    )


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _openai_stub_source(port: int) -> str:
    return textwrap.dedent(
        f"""
        import json
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                request = json.loads(self.rfile.read(length))
                if self.path.endswith("/responses"):
                    arguments = json.dumps({{
                        "action": "refund",
                        "amount": 98,
                        "reason": "Defective item",
                        "customer_reply": "Dana, your refund was issued.",
                    }})
                    payload = {{
                        "id": "resp-kitaru-smoke",
                        "object": "response",
                        "created_at": 1,
                        "status": "completed",
                        "error": None,
                        "incomplete_details": None,
                        "instructions": None,
                        "max_output_tokens": None,
                        "model": request["model"],
                        "output": [{{
                            "id": "fc-kitaru-smoke",
                            "call_id": "call-kitaru-smoke",
                            "type": "function_call",
                            "name": "final_result",
                            "arguments": arguments,
                            "status": "completed",
                        }}],
                        "parallel_tool_calls": True,
                        "previous_response_id": None,
                        "reasoning": {{"effort": None, "summary": None}},
                        "store": False,
                        "temperature": 1.0,
                        "text": {{"format": {{"type": "text"}}}},
                        "tool_choice": "auto",
                        "tools": [],
                        "top_p": 1.0,
                        "truncation": "disabled",
                        "usage": {{
                            "input_tokens": 4,
                            "output_tokens": 5,
                            "total_tokens": 9,
                            "input_tokens_details": {{"cached_tokens": 0}},
                            "output_tokens_details": {{"reasoning_tokens": 0}},
                        }},
                        "metadata": {{}},
                    }}
                else:
                    payload = {{
                        "id": "chatcmpl-kitaru-smoke",
                        "object": "chat.completion",
                        "created": 1,
                        "model": request["model"],
                        "choices": [{{
                            "index": 0,
                            "message": {{
                                "role": "assistant", "content": "The answer is 42."
                            }},
                            "finish_reason": "stop",
                        }}],
                        "usage": {{
                            "prompt_tokens": 4,
                            "completion_tokens": 5,
                            "total_tokens": 9,
                        }},
                    }}
                body = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                return

        ThreadingHTTPServer(("127.0.0.1", {port}), Handler).serve_forever()
        """
    ).lstrip()


def _install_prime_env_import_stubs() -> None:
    import logging
    import types

    algo = types.ModuleType("prime_rl.orchestrator.algo")
    algo.__dict__["Algorithm"] = object
    algo.__dict__["build_algorithm"] = lambda *args, **kwargs: None
    sampler = types.ModuleType("prime_rl.orchestrator.sampler")
    sampler.__dict__["Sampler"] = object
    transport = types.ModuleType("prime_rl.transport")

    class TrainingSample:
        pass

    transport.__dict__["TrainingSample"] = TrainingSample
    logger = types.ModuleType("prime_rl.utils.logger")
    logger.__dict__["get_logger"] = lambda: logging.getLogger("prime-rl-smoke")
    sys.modules[algo.__name__] = algo
    sys.modules[sampler.__name__] = sampler
    sys.modules[transport.__name__] = transport
    sys.modules[logger.__name__] = logger


async def _prime_handshake(source: Any, port: int, *, model_name: str = "stub") -> None:
    import multiprocessing

    import verifiers.v1 as vf
    from verifiers.v1.serve import EnvServer

    _install_prime_env_import_stubs()
    from prime_rl.orchestrator.envs import Env as PrimeEnv

    address = f"tcp://127.0.0.1:{_free_port()}"
    process = multiprocessing.Process(
        target=EnvServer.run_server,
        kwargs={"address": address, "config": source.env, "max_concurrent": 1},
    )
    process.start()
    environment = PrimeEnv(source, address)
    try:
        await environment.start()
        if environment.tasks is None:
            raise SmokeFailure("PrimeRL did not load the generated Taskset")
        task = next(environment.tasks)
        client = vf.EvalClientConfig(
            base_url=f"http://127.0.0.1:{port}/v1", api_key_var="PRIME_API_KEY"
        )
        rollouts = await environment.run(
            client,
            model_name=model_name,
            cache_salt=None,
            task_data=task.data.model_dump(mode="json"),
        )
        if len(rollouts) != 1 or rollouts[0].reward != 1.0:
            details = [
                {
                    "errors": rollout.errors,
                    "ok": rollout.ok,
                    "rewards": rollout.rewards,
                }
                for rollout in rollouts
            ]
            raise SmokeFailure(
                f"PrimeRL environment handshake did not return reward 1: {details!r}"
            )
    finally:
        if environment._env_client is not None:
            await environment._env_client.close()
        process.terminate()
        process.join(30)
        if process.is_alive():
            process.kill()
            process.join()


def _verify_verifiers(
    first: Path,
    second: Path,
    candidate_wheel: Path,
    runtime: bool,
    template: Path | None = None,
) -> None:
    from prime_rl.configs.orchestrator import OrchestratorConfig
    from pydantic import ValidationError
    from verifiers.v1.cli.resume import task_key
    from verifiers.v1.utils.loaders import (
        default_harness_id,
        load_harness,
        load_taskset,
        resolve_env_config,
    )

    if importlib.metadata.version("verifiers") != VERIFIERS_VERSION:
        raise SmokeFailure("wrong Verifiers version installed")
    if importlib.metadata.version("prime-rl") != PRIME_RL_VERSION:
        raise SmokeFailure("wrong PrimeRL version installed")
    _assert_exporters_absent()
    _assert_distribution_source("kitaru", candidate_wheel.name)
    _assert_distribution_source("prime-rl", f"v{PRIME_RL_VERSION}.tar.gz")
    _assert_distribution_source("prime-rl-configs", f"v{PRIME_RL_VERSION}.tar.gz")

    manifests = [
        json.loads((root / "kitaru-export.json").read_text())
        for root in (first, second)
    ]
    provenances = [manifest["provenance"] for manifest in manifests]
    if provenances[0]["benchmark_digest"] != provenances[1]["benchmark_digest"]:
        raise SmokeFailure("same benchmark produced different benchmark digests")
    if provenances[0]["artifact_digest"] == provenances[1]["artifact_digest"]:
        raise SmokeFailure("different agents produced the same artifact identity")

    task_bytes: list[bytes] = []
    task_keys: list[list[str]] = []
    for root, provenance in zip((first, second), provenances, strict=True):
        module = root / provenance["module_name"]
        public = (module / "tasks.jsonl").read_bytes()
        task_bytes.append(public)
        for sentinel in _PRIVATE_SENTINELS:
            if sentinel.encode() in public:
                raise SmokeFailure("private scoring evidence leaked into TaskData")
        config_data = tomllib.loads((root / "eval.toml").read_text())["env"]
        config = resolve_env_config(config_data)
        taskset = load_taskset(config.taskset)
        harness = load_harness(config.agent.harness)
        tasks = list(taskset.load())
        if len(tasks) != 2 or type(harness).__name__ != "KitaruHarness":
            raise SmokeFailure("explicit default composition did not load")
        task_keys.append(
            [task_key(task.data.model_dump(mode="json")) for task in tasks]
        )
        if default_harness_id(provenance["plugin_id"]) != provenance["plugin_id"]:
            raise SmokeFailure("omitted-Harness discovery did not select the bundle")

        omitted = json.loads(json.dumps(config_data))
        del omitted["agent"]["harness"]
        discovered = resolve_env_config(omitted)
        discovered_harness = discovered.agent_harnesses()["agent"]
        if discovered_harness.id != provenance["plugin_id"]:
            raise SmokeFailure(
                "official config parser did not discover bundled Harness"
            )

        override = json.loads(json.dumps(config_data))
        override["agent"]["harness"] = {"id": "kitaru_smoke_harness"}
        overridden = resolve_env_config(override)
        if type(load_harness(overridden.agent.harness)).__name__ != "SmokeHarness":
            raise SmokeFailure("native Harness override did not resolve")
        unknown = json.loads(json.dumps(config_data))
        unknown["agent"]["harness"] = {"id": "kitaru_missing_harness"}
        try:
            resolve_env_config(unknown)
        except (ModuleNotFoundError, ValidationError):
            pass
        else:
            raise SmokeFailure("unknown Harness silently fell back")

        training = tomllib.loads((root / "prime-rl.toml").read_text())
        prime = OrchestratorConfig.model_validate(training["orchestrator"])
        source = prime.train.source[0]
        if source.env.taskset.id != provenance["plugin_id"]:
            raise SmokeFailure("PrimeRL config selected the wrong Taskset")
        if source.env.agent.runtime.type != "docker":
            raise SmokeFailure("PrimeRL config did not preserve Docker runtime")

    if task_bytes[0] != task_bytes[1] or task_keys[0] != task_keys[1]:
        raise SmokeFailure(
            "benchmark TaskData or official task keys changed with agent"
        )

    installed_files: list[set[str]] = []
    for provenance in provenances:
        distribution = importlib.metadata.distribution(provenance["distribution_name"])
        installed_files.append(
            {
                str(path)
                for path in distribution.files or []
                if ".dist-info/" not in str(path)
            }
        )
    if installed_files[0] & installed_files[1]:
        raise SmokeFailure("side-by-side Verifiers artifacts overlap installed files")

    template_source = None
    if template is not None:
        template_manifest = json.loads((template / "kitaru-export.json").read_text())
        template_module = template / template_manifest["provenance"]["module_name"]
        template_runtime = json.loads((template_module / "runtime.json").read_text())
        if template_runtime["command_argv"] != [
            "python",
            "-m",
            "returns_agent.agent",
        ]:
            raise SmokeFailure("template export changed its canonical entrypoint")
        template_project = (template_module / "agent_source/pyproject.toml").read_text()
        if candidate_wheel.name not in template_project:
            raise SmokeFailure("template export does not pin the candidate wheel")
        template_config_data = tomllib.loads((template / "eval.toml").read_text())[
            "env"
        ]
        template_config = resolve_env_config(template_config_data)
        template_tasks = list(load_taskset(template_config.taskset).load())
        if (
            len(template_tasks) != 1
            or type(load_harness(template_config.agent.harness)).__name__
            != "KitaruHarness"
        ):
            raise SmokeFailure("template Taskset and Harness did not load")
        template_training = tomllib.loads((template / "prime-rl.toml").read_text())
        template_source = OrchestratorConfig.model_validate(
            template_training["orchestrator"]
        ).train.source[0]

    if not runtime:
        return
    port = _free_port()
    stub = subprocess.Popen(
        [sys.executable, "-c", _openai_stub_source(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    os.environ["PRIME_API_KEY"] = "provider-free-smoke"
    try:
        training = tomllib.loads((first / "prime-rl.toml").read_text())
        source = OrchestratorConfig.model_validate(
            training["orchestrator"]
        ).train.source[0]
        asyncio.run(_prime_handshake(source, port))
        if template_source is not None:
            asyncio.run(
                _prime_handshake(template_source, port, model_name="openai:stub")
            )
    finally:
        stub.terminate()
        try:
            stub.wait(10)
        except subprocess.TimeoutExpired:
            stub.kill()
            stub.wait()


def _build_wheel(
    uv: str, source: Path, output: Path, environment: dict[str, str]
) -> Path:
    output.mkdir(parents=True, exist_ok=True)
    _run(
        [uv, "build", "--wheel", "--out-dir", output, source],
        cwd=source,
        environment=environment,
    )
    wheels = list(output.glob("*.whl"))
    if len(wheels) != 1:
        raise SmokeFailure(f"expected one wheel under {output}")
    return wheels[0]


def _main(runtime: bool, template_source: Path | None) -> None:
    if sys.version_info[:2] != (3, 12):
        raise SmokeFailure("export artifact smoke must run on Python 3.12")
    repository = Path(__file__).resolve().parents[1]
    uv = shutil.which("uv")
    if uv is None:
        raise SmokeFailure("uv is required")
    temporary_root = Path(tempfile.gettempdir()).resolve()
    with tempfile.TemporaryDirectory(
        prefix="kitaru-export-smoke-", dir=temporary_root
    ) as temporary:
        root = Path(temporary)
        build_environment = _isolated_environment(root / "build-home")
        candidate = _build_wheel(
            uv, repository, root / "candidate-wheel", build_environment
        )
        harbor_exporter = _build_wheel(
            uv,
            repository / "plugins/packages/harbor-exporter",
            root / "harbor-exporter-wheel",
            build_environment,
        )
        verifiers_exporter = _build_wheel(
            uv,
            repository / "plugins/packages/verifiers-exporter",
            root / "verifiers-exporter-wheel",
            build_environment,
        )
        _assert_core_metadata(repository, candidate)

        core_python, core_environment = _create_environment(
            uv, root / "core-only", [candidate]
        )
        _run(
            [core_python, __file__, "--verify-installed-exporters"],
            cwd=repository,
            environment=core_environment,
        )
        for format_name, exporter_wheel in (
            ("harbor", harbor_exporter),
            ("verifiers-v1", verifiers_exporter),
        ):
            one_python, one_environment = _create_environment(
                uv, root / f"one-{format_name}", [candidate, exporter_wheel]
            )
            _run(
                [
                    one_python,
                    __file__,
                    "--verify-installed-exporters",
                    "--installed-exporter",
                    format_name,
                ],
                cwd=repository,
                environment=one_environment,
            )

        generator_python, generator_environment = _create_environment(
            uv,
            root / "generator",
            [candidate, harbor_exporter, verifiers_exporter],
        )
        artifacts = root / "artifacts"
        _run(
            [
                generator_python,
                __file__,
                "--generate",
                artifacts,
                "--candidate-wheel",
                candidate,
                "--harbor-exporter-wheel",
                harbor_exporter,
                "--verifiers-exporter-wheel",
                verifiers_exporter,
                *(
                    ["--template-source", template_source]
                    if template_source is not None
                    else []
                ),
            ],
            cwd=repository,
            environment=generator_environment,
        )

        harbor_python, harbor_environment = _create_environment(
            uv,
            root / "harbor-target",
            [candidate, f"harbor=={HARBOR_VERSION}"],
        )
        local_compose = Path.home() / ".docker/cli-plugins/docker-compose"
        if runtime and local_compose.is_file():
            isolated_plugins = Path(harbor_environment["HOME"]) / ".docker/cli-plugins"
            isolated_plugins.mkdir(parents=True)
            shutil.copy2(local_compose, isolated_plugins / "docker-compose")
        _run(
            [
                harbor_python,
                __file__,
                "--verify-harbor",
                artifacts / "harbor",
                "--candidate-wheel",
                candidate,
                *(["--runtime"] if runtime else []),
            ],
            cwd=repository,
            environment=harbor_environment,
        )

        target_python, target_environment = _create_environment(
            uv,
            root / "verifiers-target",
            [candidate, f"verifiers=={VERIFIERS_VERSION}"],
        )
        _run(
            [
                uv,
                "pip",
                "install",
                "--python",
                target_python,
                "--no-deps",
                "prime-rl-configs @ "
                f"{PRIME_RL_ARCHIVE}#subdirectory=packages/prime-rl-configs",
                f"prime-rl @ {PRIME_RL_ARCHIVE}",
            ],
            cwd=repository,
            environment=target_environment,
        )
        generated_wheels = root / "generated-wheels"
        first_wheel = _build_wheel(
            uv,
            artifacts / "verifiers-first",
            generated_wheels / "first",
            target_environment,
        )
        second_wheel = _build_wheel(
            uv,
            artifacts / "verifiers-second",
            generated_wheels / "second",
            target_environment,
        )
        template_wheel = (
            _build_wheel(
                uv,
                artifacts / "verifiers-template",
                generated_wheels / "template",
                target_environment,
            )
            if template_source is not None
            else None
        )
        override_source = root / "override-source"
        _write_override_package(override_source)
        override_wheel = _build_wheel(
            uv, override_source, generated_wheels / "override", target_environment
        )
        _run(
            [
                uv,
                "pip",
                "install",
                "--python",
                target_python,
                "--no-deps",
                first_wheel,
                second_wheel,
                *([template_wheel] if template_wheel is not None else []),
                override_wheel,
            ],
            cwd=repository,
            environment=target_environment,
        )
        _run(
            [
                target_python,
                __file__,
                "--verify-verifiers",
                artifacts / "verifiers-first",
                artifacts / "verifiers-second",
                "--candidate-wheel",
                candidate,
                *(
                    ["--template-artifact", artifacts / "verifiers-template"]
                    if template_source is not None
                    else []
                ),
                *(["--runtime"] if runtime else []),
            ],
            cwd=repository,
            environment=target_environment,
        )
    level = "runtime" if runtime else "model-only"
    print(
        f"Exact-target export smoke passed ({level}): Harbor {HARBOR_VERSION}, "
        f"Verifiers {VERIFIERS_VERSION}, PrimeRL {PRIME_RL_VERSION}."
    )


def main() -> int:
    """Run the requested exact-target smoke stage."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-only", action="store_true")
    parser.add_argument("--runtime", action="store_true")
    parser.add_argument("--generate", type=Path)
    parser.add_argument("--verify-harbor", type=Path)
    parser.add_argument("--verify-verifiers", nargs=2, type=Path)
    parser.add_argument("--verify-installed-exporters", action="store_true")
    parser.add_argument(
        "--installed-exporter",
        action="append",
        default=[],
        choices=("harbor", "verifiers-v1"),
    )
    parser.add_argument("--candidate-wheel", type=Path)
    parser.add_argument("--harbor-exporter-wheel", type=Path)
    parser.add_argument("--verifiers-exporter-wheel", type=Path)
    parser.add_argument("--template-artifact", type=Path)
    parser.add_argument("--template-source", type=Path)
    arguments = parser.parse_args()
    if arguments.generate is not None:
        _generate_artifacts(
            arguments.generate,
            candidate_wheel=arguments.candidate_wheel,
            harbor_exporter_wheel=arguments.harbor_exporter_wheel,
            verifiers_exporter_wheel=arguments.verifiers_exporter_wheel,
            template_source=arguments.template_source,
        )
        return 0
    if arguments.verify_installed_exporters:
        _verify_installed_exporters(set(arguments.installed_exporter))
        return 0
    if arguments.verify_harbor is not None:
        if arguments.candidate_wheel is None:
            parser.error("--candidate-wheel is required")
        _verify_harbor(
            arguments.verify_harbor, arguments.candidate_wheel, arguments.runtime
        )
        return 0
    if arguments.verify_verifiers is not None:
        if arguments.candidate_wheel is None:
            parser.error("--candidate-wheel is required")
        _verify_verifiers(
            arguments.verify_verifiers[0],
            arguments.verify_verifiers[1],
            arguments.candidate_wheel,
            arguments.runtime,
            arguments.template_artifact,
        )
        return 0
    if arguments.model_only and arguments.runtime:
        parser.error("choose either --model-only or --runtime")
    _main(
        runtime=not arguments.model_only,
        template_source=arguments.template_source,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
