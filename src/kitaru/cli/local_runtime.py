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
"""Manage the user-scoped local Kitaru Docker Compose deployment."""

import asyncio
import contextlib
import importlib.resources
import os
import re
import secrets
import shutil
import socket
import webbrowser
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

import httpx
from packaging.version import InvalidVersion, Version
from pydantic import BaseModel, ConfigDict

from kitaru.cli.output import CLIError
from kitaru.client.config import (
    DIRECTORY_MODE,
    FILE_MODE,
    get_config_directory,
    write_json_file,
)

LOCAL_SERVER_URL = "http://localhost:8000"
LOCAL_DASHBOARD_URL = LOCAL_SERVER_URL
LOCAL_PROJECT_NAME = "kitaru-local"
LOCAL_IMAGE_ENV = "KITARU_LOCAL_IMAGE"
POSTGRES_IMAGE = "postgres:16-alpine"
_IMAGE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/@:+-]*$")
_INSTALL_HINT = (
    "Install Docker from https://docs.docker.com/get-docker/, or use "
    "Kitaru Cloud at https://cloud.zenml.io/."
)


class LocalRuntimeState(BaseModel):
    """Persisted identity of the CLI-owned local deployment."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    project: Literal["kitaru-local"] = "kitaru-local"
    server_image: str


@dataclass(frozen=True, slots=True)
class LocalRuntimePaths:
    """Filesystem paths belonging to the local deployment."""

    directory: Path
    compose: Path
    environment: Path
    state: Path
    lock: Path


@dataclass(frozen=True, slots=True)
class ProcessResult:
    """Captured subprocess result."""

    returncode: int
    stdout: str
    stderr: str


class DockerCommandRunner(Protocol):
    """Structural interface for Docker command execution."""

    async def run(self, *arguments: str, timeout: float = 120) -> ProcessResult:
        """Run one Docker command."""

    def stream(self, *arguments: str) -> AsyncIterator[str]:
        """Stream output from one Docker command."""


class DockerRunner:
    """Execute Docker without introducing a Docker SDK dependency."""

    def __init__(self, executable: str) -> None:
        """Initialize the runner with the resolved Docker executable."""
        self.executable = executable

    async def run(self, *arguments: str, timeout: float = 120) -> ProcessResult:
        """Run one bounded Docker command and capture its output."""
        try:
            process = await asyncio.create_subprocess_exec(
                self.executable,
                *arguments,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError as error:
            raise CLIError(
                "invalid_configuration",
                f"Docker could not be executed: {error}",
                hint=_INSTALL_HINT,
            ) from error
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout)
        except TimeoutError as error:
            process.kill()
            await process.communicate()
            raise CLIError(
                "timeout", "Docker did not finish before the timeout expired."
            ) from error
        except BaseException:
            await _terminate_process(process)
            raise
        return ProcessResult(
            returncode=process.returncode or 0,
            stdout=_decode_output(stdout),
            stderr=_decode_output(stderr),
        )

    async def stream(self, *arguments: str) -> AsyncIterator[str]:
        """Yield merged output lines from a running Docker command."""
        try:
            process = await asyncio.create_subprocess_exec(
                self.executable,
                *arguments,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except OSError as error:
            raise CLIError(
                "invalid_configuration",
                f"Docker could not be executed: {error}",
                hint=_INSTALL_HINT,
            ) from error
        assert process.stdout is not None
        try:
            while line := await process.stdout.readline():
                yield line.decode("utf-8", errors="replace").rstrip("\r\n")
            returncode = await process.wait()
            if returncode:
                raise CLIError("internal_error", "Docker Compose logs failed.")
        finally:
            await _terminate_process(process)


def get_local_runtime_paths() -> LocalRuntimePaths:
    """Return paths in the user configuration directory."""
    directory = get_config_directory() / "local"
    return LocalRuntimePaths(
        directory=directory,
        compose=directory / "compose.yaml",
        environment=directory / "runtime.env",
        state=directory / "state.json",
        lock=directory / "operation.lock",
    )


async def start_local_runtime(
    *,
    package_version: str,
    upgrade: bool,
    timeout: float,
    runner: DockerCommandRunner | None = None,
    paths: LocalRuntimePaths | None = None,
) -> tuple[dict[str, object], list[str]]:
    """Create or reuse the local deployment and wait for its server."""
    paths = paths or get_local_runtime_paths()
    runner = runner or await _get_docker_runner()
    image, overridden = _get_server_image(package_version)
    with _operation_lock(paths):
        await _validate_docker(runner)
        state = _read_state(paths.state)
        if upgrade and state is None:
            raise CLIError(
                "invalid_configuration",
                "There is no local Kitaru deployment to upgrade.",
                hint="Run `kitaru login --local` first.",
            )
        if state is None:
            await _reject_unowned_resources(runner)
            await asyncio.to_thread(_reject_occupied_port)
        elif state.server_image != image and not upgrade:
            raise CLIError(
                "conflict",
                "The local deployment uses a different Kitaru server image.",
                hint="Run `kitaru login --local --upgrade` to replace it.",
                details={"current_image": state.server_image, "requested_image": image},
            )
        if state is None:
            _write_runtime_files(paths, image=image, existing_state=None)
            running = False
        else:
            running = await _is_running(runner, paths)
        if running and not upgrade:
            action = "reused"
        else:
            await _ensure_image(
                runner,
                image,
                pull_if_missing=not overridden,
                refresh=upgrade and not overridden,
                platform="linux/amd64",
            )
            await _ensure_image(runner, POSTGRES_IMAGE, pull_if_missing=True)
            if state is not None and state.server_image != image:
                _write_runtime_files(paths, image=image, existing_state=state)
            action = "upgraded" if upgrade else ("started" if state else "created")
            try:
                await _run_compose(
                    runner,
                    paths,
                    "up",
                    "-d",
                    "--pull",
                    "never",
                    "--remove-orphans",
                    timeout=max(timeout, 120),
                )
                await _wait_for_health(max(timeout, 120))
            except BaseException:
                if state is None:
                    with contextlib.suppress(CLIError):
                        await _run_compose(runner, paths, "down", timeout=60)
                raise

        if running and not upgrade:
            await _wait_for_health(max(timeout, 120))
        return (
            {
                "server_url": LOCAL_SERVER_URL,
                "server_image": image,
                "deployment": action,
                "auth_scheme": "none",
                "authentication": "not_required",
                "credential_kind": "none",
                "credential_stored": False,
            },
            [],
        )


async def stop_local_runtime(
    *,
    delete_volumes: bool,
    runner: DockerCommandRunner | None = None,
    paths: LocalRuntimePaths | None = None,
) -> dict[str, object]:
    """Stop a CLI-owned deployment and optionally delete its data."""
    paths = paths or get_local_runtime_paths()
    state = _read_state(paths.state)
    if state is None:
        raise CLIError(
            "invalid_configuration",
            "No CLI-owned local Kitaru deployment was found.",
        )
    runner = runner or await _get_docker_runner()
    with _operation_lock(paths):
        await _validate_docker(runner)
        arguments = ["down"]
        if delete_volumes:
            arguments.append("--volumes")
        await _run_compose(runner, paths, *arguments, timeout=120)
        if delete_volumes:
            paths.environment.unlink(missing_ok=True)
            paths.compose.unlink(missing_ok=True)
            paths.state.unlink(missing_ok=True)
        return {
            "server_url": LOCAL_SERVER_URL,
            "deployment": "deleted" if delete_volumes else "stopped",
            "data_deleted": delete_volumes,
        }


def is_local_runtime_owned(paths: LocalRuntimePaths | None = None) -> bool:
    """Return whether local deployment ownership state exists."""
    return _read_state((paths or get_local_runtime_paths()).state) is not None


async def open_local_dashboard() -> bool:
    """Open the local dashboard in the default browser."""
    return await asyncio.to_thread(webbrowser.open, LOCAL_DASHBOARD_URL)


async def get_local_logs(
    *,
    service: str | None,
    tail: int,
    follow: bool,
    runner: DockerCommandRunner | None = None,
    paths: LocalRuntimePaths | None = None,
) -> list[str] | AsyncIterator[str]:
    """Return or stream logs from the CLI-owned local deployment."""
    if tail < 0:
        raise CLIError("invalid_arguments", "--tail cannot be negative.")
    if service not in {None, "server", "db"}:
        raise CLIError(
            "invalid_arguments", "--service must be either 'server' or 'db'."
        )
    paths = paths or get_local_runtime_paths()
    if _read_state(paths.state) is None:
        raise CLIError(
            "invalid_configuration",
            "No CLI-owned local Kitaru deployment was found.",
        )
    runner = runner or await _get_docker_runner()
    await _validate_docker(runner)
    arguments = [*_compose_arguments(paths), "logs", "--tail", str(tail)]
    if follow:
        arguments.append("--follow")
    if service:
        arguments.append(service)
    if follow:
        return runner.stream(*arguments)
    result = await runner.run(*arguments, timeout=60)
    _raise_for_docker(result, "Docker Compose logs failed.")
    return result.stdout.splitlines()


async def _get_docker_runner() -> DockerRunner:
    executable = shutil.which("docker")
    if executable is None:
        raise CLIError(
            "invalid_configuration",
            "Docker with Compose v2 is required to run Kitaru locally.",
            hint=_INSTALL_HINT,
        )
    return DockerRunner(executable)


async def _validate_docker(runner: DockerCommandRunner) -> None:
    compose = await runner.run("compose", "version", timeout=15)
    if compose.returncode:
        raise CLIError(
            "invalid_configuration",
            "Docker Compose v2 is required to run Kitaru locally.",
            hint=_INSTALL_HINT,
        )
    info = await runner.run("info", timeout=15)
    if info.returncode:
        raise CLIError(
            "invalid_configuration",
            "The Docker daemon is unavailable.",
            hint="Start Docker, then retry the command.",
            details=_docker_details(info),
        )
    context = await runner.run(
        "context", "inspect", "--format", "{{json .Endpoints.docker.Host}}", timeout=15
    )
    if context.returncode:
        raise CLIError(
            "invalid_configuration",
            "The active Docker context could not be inspected.",
            details=_docker_details(context),
        )
    host = context.stdout.strip().strip('"')
    if host.startswith(("ssh://", "tcp://", "http://", "https://")):
        raise CLIError(
            "invalid_configuration",
            "The active Docker context points to a remote daemon.",
            hint="Select a local Docker context, then retry the command.",
        )


def _get_server_image(package_version: str) -> tuple[str, bool]:
    override = os.environ.get(LOCAL_IMAGE_ENV)
    if override:
        image = override.strip()
        if not _IMAGE_PATTERN.fullmatch(image):
            raise CLIError(
                "invalid_configuration", f"{LOCAL_IMAGE_ENV} is not a valid image."
            )
        return image, True
    try:
        parsed_version = Version(package_version)
    except InvalidVersion as error:
        raise CLIError(
            "invalid_configuration",
            f"The installed Kitaru version {package_version!r} is invalid.",
        ) from error
    if parsed_version.is_devrelease or parsed_version.local is not None:
        raise CLIError(
            "invalid_configuration",
            "No published local server image is available for this development build.",
            hint=f"Set {LOCAL_IMAGE_ENV} to a compatible local image.",
        )
    image_version = package_version
    if parsed_version.pre is not None and parsed_version.pre[0] == "rc":
        image_version = f"{parsed_version.base_version}-rc.{parsed_version.pre[1]}"
    return f"zenmldocker/kitaru-server:{image_version}", False


async def _ensure_image(
    runner: DockerCommandRunner,
    image: str,
    *,
    pull_if_missing: bool,
    refresh: bool = False,
    platform: str | None = None,
) -> None:
    inspection = await runner.run("image", "inspect", image, timeout=30)
    if inspection.returncode == 0 and not refresh:
        return
    if inspection.returncode != 0 and not pull_if_missing:
        raise CLIError(
            "invalid_configuration",
            f"The developer image {image!r} is not available locally.",
            hint=f"Build the image or change {LOCAL_IMAGE_ENV}, then retry.",
        )
    arguments = ["pull"]
    if platform:
        arguments.extend(("--platform", platform))
    arguments.append(image)
    pulled = await runner.run(*arguments, timeout=600)
    _raise_for_docker(pulled, f"Docker could not pull {image!r}.")


async def _reject_unowned_resources(runner: DockerCommandRunner) -> None:
    filters = (
        ("ps", "--all", "--quiet", "--filter"),
        ("network", "ls", "--quiet", "--filter"),
        ("volume", "ls", "--quiet", "--filter"),
    )
    for prefix in filters:
        result = await runner.run(
            *prefix,
            f"label=com.docker.compose.project={LOCAL_PROJECT_NAME}",
            timeout=30,
        )
        _raise_for_docker(result, "Docker resources could not be inspected.")
        if result.stdout.strip():
            raise CLIError(
                "conflict",
                "Docker resources named for Kitaru exist without CLI ownership state.",
                hint="Remove or rename those resources before retrying.",
            )


def _reject_occupied_port() -> None:
    try:
        with socket.create_connection(("127.0.0.1", 8000), timeout=0.25):
            pass
    except OSError:
        return
    raise CLIError(
        "conflict",
        "Port 8000 is already in use by a deployment Kitaru does not own.",
        hint="Stop that service or use `kitaru login SERVER` to connect to it.",
    )


async def _is_running(runner: DockerCommandRunner, paths: LocalRuntimePaths) -> bool:
    result = await _run_compose(
        runner, paths, "ps", "--status", "running", "--quiet", timeout=30
    )
    return len(result.stdout.splitlines()) >= 2


async def _wait_for_health(timeout: float) -> None:
    deadline = asyncio.get_running_loop().time() + max(timeout, 1)
    async with httpx.AsyncClient(timeout=min(timeout, 5)) as client:
        while True:
            try:
                response = await client.get(f"{LOCAL_SERVER_URL}/health/live")
                if response.is_success:
                    return
            except httpx.HTTPError:
                pass
            if asyncio.get_running_loop().time() >= deadline:
                raise CLIError(
                    "timeout",
                    "The local Kitaru server did not become healthy in time.",
                    hint="Run `kitaru local logs` to inspect startup failures.",
                )
            await asyncio.sleep(0.5)


async def _run_compose(
    runner: DockerCommandRunner,
    paths: LocalRuntimePaths,
    *arguments: str,
    timeout: float,
) -> ProcessResult:
    result = await runner.run(*_compose_arguments(paths), *arguments, timeout=timeout)
    _raise_for_docker(result, f"Docker Compose {' '.join(arguments)} failed.")
    return result


def _compose_arguments(paths: LocalRuntimePaths) -> tuple[str, ...]:
    return (
        "compose",
        "--project-name",
        LOCAL_PROJECT_NAME,
        "--env-file",
        str(paths.environment),
        "--file",
        str(paths.compose),
    )


def _write_runtime_files(
    paths: LocalRuntimePaths,
    *,
    image: str,
    existing_state: LocalRuntimeState | None,
) -> None:
    paths.directory.mkdir(parents=True, exist_ok=True, mode=DIRECTORY_MODE)
    os.chmod(paths.directory, DIRECTORY_MODE)
    compose = importlib.resources.files("kitaru.cli.resources").joinpath(
        "local-compose.yaml"
    )
    _write_private_text(paths.compose, compose.read_text(encoding="utf-8"))
    if not paths.environment.exists():
        values = {
            "KITARU_LOCAL_SERVER_IMAGE": image,
            "KITARU_LOCAL_DB_PASSWORD": secrets.token_urlsafe(32),
            "KITARU_LOCAL_JWT_SIGNING_KEY": secrets.token_urlsafe(48),
            "KITARU_LOCAL_SECRET_ENCRYPTION_KEY": secrets.token_urlsafe(48),
        }
        _write_private_text(
            paths.environment,
            "".join(f"{key}={value}\n" for key, value in values.items()),
        )
    elif existing_state is not None and existing_state.server_image != image:
        lines = paths.environment.read_text(encoding="utf-8").splitlines()
        replaced = [
            f"KITARU_LOCAL_SERVER_IMAGE={image}"
            if line.startswith("KITARU_LOCAL_SERVER_IMAGE=")
            else line
            for line in lines
        ]
        _write_private_text(paths.environment, "\n".join(replaced) + "\n")
    write_json_file(paths.state, LocalRuntimeState(server_image=image).model_dump())


def _write_private_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=DIRECTORY_MODE)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}")
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, FILE_MODE)
        with os.fdopen(descriptor, "w", encoding="utf-8") as file:
            file.write(value)
        os.replace(temporary, path)
        os.chmod(path, FILE_MODE)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise


def _read_state(path: Path) -> LocalRuntimeState | None:
    if not path.is_file():
        return None
    try:
        return LocalRuntimeState.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise CLIError(
            "invalid_configuration",
            "The local Kitaru deployment state is invalid.",
            hint="Remove the local runtime state after verifying Docker resources.",
        ) from error


@contextmanager
def _operation_lock(paths: LocalRuntimePaths) -> Iterator[None]:
    paths.directory.mkdir(parents=True, exist_ok=True, mode=DIRECTORY_MODE)
    os.chmod(paths.directory, DIRECTORY_MODE)
    descriptor: int | None = None
    for attempt in range(2):
        try:
            descriptor = os.open(
                paths.lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, FILE_MODE
            )
            break
        except FileExistsError as error:
            if attempt == 0 and _remove_stale_lock(paths.lock):
                continue
            raise CLIError(
                "conflict", "Another local Kitaru operation is already running."
            ) from error
    assert descriptor is not None
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as file:
            file.write(str(os.getpid()))
        yield
    finally:
        paths.lock.unlink(missing_ok=True)


def _remove_stale_lock(path: Path) -> bool:
    try:
        process_id = int(path.read_text(encoding="utf-8"))
        if process_id <= 0:
            raise ValueError
    except (OSError, ValueError):
        path.unlink(missing_ok=True)
        return True
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        path.unlink(missing_ok=True)
        return True
    except PermissionError:
        return False
    return False


def _raise_for_docker(result: ProcessResult, message: str) -> None:
    if result.returncode:
        raise CLIError(
            "internal_error", message, details=_docker_details(result), retryable=True
        )


def _docker_details(result: ProcessResult) -> dict[str, str]:
    detail = (result.stderr or result.stdout).strip()
    return {"docker_output": detail[-4000:]}


def _decode_output(value: bytes) -> str:
    return value.decode("utf-8", errors="replace").strip()


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        await process.wait()
