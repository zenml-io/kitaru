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
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol
from urllib.parse import urlsplit

import httpx
from packaging.version import InvalidVersion, Version
from pydantic import BaseModel, ConfigDict, Field

from kitaru.cli.output import CLIError
from kitaru.client.config import (
    DIRECTORY_MODE,
    FILE_MODE,
    get_config_directory,
    normalize_server_url,
    write_json_file,
)

DEFAULT_LOCAL_PORT = 8000
LOCAL_PORT_ENV = "KITARU_LOCAL_PORT"
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
    port: int = Field(default=DEFAULT_LOCAL_PORT, ge=1, le=65535)

    @property
    def server_url(self) -> str:
        """Return the loopback URL exposed by this deployment."""
        return _get_local_server_url(self.port)


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

    def stream(
        self,
        *arguments: str,
        failure_message: str = "Docker command failed.",
    ) -> AsyncIterator[str]:
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

    async def stream(
        self,
        *arguments: str,
        failure_message: str = "Docker command failed.",
    ) -> AsyncIterator[str]:
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
                raise CLIError("internal_error", failure_message)
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
    port: int | None = None,
    progress: Callable[[str], None] | None = None,
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
        resolved_port = _resolve_local_port(port, state)
        server_url = _get_local_server_url(resolved_port)
        port_changed = state is not None and state.port != resolved_port
        if upgrade and state is None:
            raise CLIError(
                "invalid_configuration",
                "There is no local Kitaru deployment to upgrade.",
                hint="Run `kitaru login --local` first.",
            )
        if state is None:
            await _reject_unowned_resources(runner)
        elif state.server_image != image and not upgrade:
            raise CLIError(
                "conflict",
                f"Your local Kitaru server uses {state.server_image}, but this "
                f"login expects {image}. Kitaru will not replace the server "
                "container without your approval.",
                hint=(
                    "Run `kitaru login --local --upgrade` to use the expected "
                    "image. Your local database will be kept."
                ),
                details={"current_image": state.server_image, "requested_image": image},
            )
        if state is None or port_changed:
            await asyncio.to_thread(_reject_occupied_port, resolved_port)
        if state is None:
            _write_runtime_files(paths, image=image, port=resolved_port)
            running = False
        else:
            running = await _is_running(runner, paths)
        previous_environment = (
            paths.environment.read_text(encoding="utf-8")
            if state is not None and (state.server_image != image or port_changed)
            else None
        )
        previous_state = (
            paths.state.read_text(encoding="utf-8")
            if previous_environment is not None
            else None
        )
        if running and not upgrade and not port_changed:
            action = "reused"
        else:
            await _ensure_image(
                runner,
                image,
                pull_if_missing=not overridden,
                refresh=upgrade and not overridden,
                platform="linux/amd64",
                progress=progress,
            )
            await _ensure_image(
                runner,
                POSTGRES_IMAGE,
                pull_if_missing=True,
                progress=progress,
            )
            if state is not None and (state.server_image != image or port_changed):
                _write_runtime_files(paths, image=image, port=resolved_port)
            if upgrade:
                action = "upgraded"
            elif port_changed:
                action = "reconfigured"
            elif state is None:
                action = "created"
            else:
                action = "started"
            try:
                compose_arguments = (
                    "up",
                    "-d",
                    "--pull",
                    "never",
                    "--remove-orphans",
                )
                if progress is None:
                    await _run_compose(
                        runner,
                        paths,
                        *compose_arguments,
                        timeout=max(timeout, 120),
                    )
                else:
                    base_arguments = _compose_arguments(paths)
                    await _stream_docker_command(
                        runner,
                        (
                            base_arguments[0],
                            "--progress",
                            "plain",
                            *base_arguments[1:],
                            *compose_arguments,
                        ),
                        progress=progress,
                        timeout=max(timeout, 120),
                        failure_message="Docker Compose up failed.",
                    )
                await _wait_for_health(server_url, max(timeout, 120))
            except BaseException:
                if state is None:
                    with contextlib.suppress(CLIError):
                        await _run_compose(runner, paths, "down", timeout=60)
                elif previous_environment is not None and previous_state is not None:
                    with contextlib.suppress(OSError):
                        _write_private_text(paths.environment, previous_environment)
                    with contextlib.suppress(OSError):
                        _write_private_text(paths.state, previous_state)
                    with contextlib.suppress(CLIError):
                        await _run_compose(
                            runner,
                            paths,
                            *compose_arguments,
                            timeout=max(timeout, 120),
                        )
                raise

        if running and not upgrade and not port_changed:
            await _wait_for_health(server_url, max(timeout, 120))
        return (
            {
                "server_url": server_url,
                "port": resolved_port,
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
    if state is None and not delete_volumes:
        raise CLIError(
            "invalid_configuration",
            "No CLI-owned local Kitaru deployment was found.",
        )
    runner = runner or await _get_docker_runner()
    if state is None:
        with _operation_lock(paths):
            await _validate_docker(runner)
            removed = await _remove_labeled_resources(runner)
            if not removed:
                raise CLIError(
                    "invalid_configuration",
                    "No CLI-owned local Kitaru deployment was found.",
                )
            paths.environment.unlink(missing_ok=True)
            paths.compose.unlink(missing_ok=True)
            return {
                "server_url": _get_local_server_url(DEFAULT_LOCAL_PORT),
                "deployment": "deleted",
                "data_deleted": True,
            }
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
            "server_url": state.server_url,
            "deployment": "deleted" if delete_volumes else "stopped",
            "data_deleted": delete_volumes,
        }


def is_local_runtime_url(
    server_url: str, paths: LocalRuntimePaths | None = None
) -> bool:
    """Return whether a URL identifies the CLI-owned local deployment."""
    normalized_server_url = normalize_server_url(server_url)
    parsed_server_url = urlsplit(normalized_server_url)
    if parsed_server_url.scheme != "http" or parsed_server_url.hostname != "localhost":
        return False
    state = _read_state((paths or get_local_runtime_paths()).state)
    expected_url = (
        state.server_url
        if state is not None
        else _get_local_server_url(DEFAULT_LOCAL_PORT)
    )
    return normalized_server_url == expected_url


async def open_local_dashboard(server_url: str) -> bool:
    """Open the local dashboard in the default browser."""
    return await asyncio.to_thread(webbrowser.open, server_url)


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
        return runner.stream(*arguments, failure_message="Docker Compose logs failed.")
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


def _format_image_version(version: Version) -> str:
    """Format a PEP 440 version as a Docker-compatible image tag."""
    canonical_version = str(version)
    base_version = version.base_version
    image_base_version = base_version.replace("!", ".epoch.")
    suffix = canonical_version.removeprefix(base_version)
    if not suffix:
        return image_base_version

    public_suffix, local_separator, local_suffix = suffix.partition("+")
    public_suffix = re.sub(r"([A-Za-z]+)([0-9]+)", r"\1.\2", public_suffix).strip(".")
    suffix_parts = [public_suffix] if public_suffix else []
    if local_separator:
        suffix_parts.append(f"local.{local_suffix}")
    return f"{image_base_version}-{'.'.join(suffix_parts)}"


def _get_local_server_url(port: int) -> str:
    """Build the loopback URL for a local host port."""
    return f"http://localhost:{port}"


def _resolve_local_port(
    explicit_port: int | None, state: LocalRuntimeState | None
) -> int:
    """Resolve and validate the host port for the local deployment."""
    if explicit_port is not None:
        return _validate_local_port(explicit_port, configuration=False)
    environment = os.environ.get(LOCAL_PORT_ENV)
    if environment:
        try:
            port = int(environment)
        except ValueError as error:
            raise CLIError(
                "invalid_configuration",
                f"{LOCAL_PORT_ENV} must be an integer between 1 and 65535.",
            ) from error
        return _validate_local_port(port, configuration=True)
    if state is not None:
        return state.port
    return DEFAULT_LOCAL_PORT


def _validate_local_port(port: int, *, configuration: bool) -> int:
    """Validate one local host port with source-appropriate errors."""
    if 1 <= port <= 65535:
        return port
    kind = "invalid_configuration" if configuration else "invalid_arguments"
    source = LOCAL_PORT_ENV if configuration else "--port"
    raise CLIError(kind, f"{source} must be between 1 and 65535.")


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
    image_version = _format_image_version(parsed_version)
    return f"zenmldocker/kitaru-server:{image_version}", False


async def _ensure_image(
    runner: DockerCommandRunner,
    image: str,
    *,
    pull_if_missing: bool,
    refresh: bool = False,
    platform: str | None = None,
    progress: Callable[[str], None] | None = None,
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
    if progress is not None:
        await _stream_docker_command(
            runner,
            tuple(arguments),
            progress=progress,
            timeout=600,
            failure_message=f"Docker could not pull {image!r}.",
        )
        return
    pulled = await runner.run(*arguments, timeout=600)
    _raise_for_docker(pulled, f"Docker could not pull {image!r}.")


async def _stream_docker_command(
    runner: DockerCommandRunner,
    arguments: tuple[str, ...],
    *,
    progress: Callable[[str], None],
    timeout: float,
    failure_message: str,
) -> None:
    """Forward one bounded Docker command's output as it arrives."""
    try:
        async with asyncio.timeout(timeout):
            async for line in runner.stream(
                *arguments, failure_message=failure_message
            ):
                progress(line)
    except TimeoutError as error:
        raise CLIError(
            "timeout", "Docker did not finish before the timeout expired."
        ) from error


_RESOURCE_QUERIES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("containers", ("ps", "--all", "--quiet", "--filter")),
    ("networks", ("network", "ls", "--quiet", "--filter")),
    ("volumes", ("volume", "ls", "--quiet", "--filter")),
)


async def _find_labeled_resources(
    runner: DockerCommandRunner,
) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for kind, prefix in _RESOURCE_QUERIES:
        result = await runner.run(
            *prefix,
            f"label=com.docker.compose.project={LOCAL_PROJECT_NAME}",
            timeout=30,
        )
        _raise_for_docker(result, "Docker resources could not be inspected.")
        identifiers = result.stdout.split()
        if identifiers:
            found[kind] = identifiers
    return found


async def _reject_unowned_resources(runner: DockerCommandRunner) -> None:
    found = await _find_labeled_resources(runner)
    if not found:
        return
    # The database password is regenerated along with the runtime files, and
    # Postgres only applies it when it initializes an empty data directory, so
    # an adopted volume would leave the server unable to authenticate.
    raise CLIError(
        "conflict",
        "Docker resources named for Kitaru exist without CLI ownership state.",
        hint="Run `kitaru logout --volumes` to delete them, then retry.",
        details={kind: sorted(values) for kind, values in found.items()},
    )


async def _remove_labeled_resources(runner: DockerCommandRunner) -> dict[str, object]:
    found = await _find_labeled_resources(runner)
    removals = (
        ("containers", ("rm", "--force")),
        ("networks", ("network", "rm")),
        ("volumes", ("volume", "rm")),
    )
    for kind, command in removals:
        identifiers = found.get(kind)
        if not identifiers:
            continue
        result = await runner.run(*command, *identifiers, timeout=120)
        _raise_for_docker(result, f"Docker {kind} could not be removed.")
    return {kind: sorted(values) for kind, values in found.items()}


def _reject_occupied_port(port: int) -> None:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.25):
            pass
    except OSError:
        return
    raise CLIError(
        "conflict",
        f"Port {port} is already in use by a deployment Kitaru does not own.",
        hint="Stop that service or use `kitaru login SERVER` to connect to it.",
    )


async def _is_running(runner: DockerCommandRunner, paths: LocalRuntimePaths) -> bool:
    result = await _run_compose(
        runner, paths, "ps", "--status", "running", "--quiet", timeout=30
    )
    return len(result.stdout.splitlines()) >= 2


async def _wait_for_health(server_url: str, timeout: float) -> None:
    deadline = asyncio.get_running_loop().time() + max(timeout, 1)
    async with httpx.AsyncClient(timeout=min(timeout, 5)) as client:
        while True:
            try:
                response = await client.get(f"{server_url}/health/live")
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
    port: int,
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
            "KITARU_LOCAL_HOST_PORT": str(port),
            "KITARU_LOCAL_DB_PASSWORD": secrets.token_urlsafe(32),
            "KITARU_LOCAL_JWT_SIGNING_KEY": secrets.token_urlsafe(48),
            "KITARU_LOCAL_SECRET_ENCRYPTION_KEY": secrets.token_urlsafe(48),
        }
        _write_private_text(
            paths.environment,
            "".join(f"{key}={value}\n" for key, value in values.items()),
        )
    else:
        lines = paths.environment.read_text(encoding="utf-8").splitlines()
        updates = {
            "KITARU_LOCAL_SERVER_IMAGE": image,
            "KITARU_LOCAL_HOST_PORT": str(port),
        }
        replaced: list[str] = []
        found: set[str] = set()
        for line in lines:
            key, separator, _ = line.partition("=")
            if separator and key in updates:
                replaced.append(f"{key}={updates[key]}")
                found.add(key)
            else:
                replaced.append(line)
        replaced.extend(
            f"{key}={value}" for key, value in updates.items() if key not in found
        )
        _write_private_text(paths.environment, "\n".join(replaced) + "\n")
    write_json_file(
        paths.state,
        LocalRuntimeState(server_image=image, port=port).model_dump(),
    )


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
