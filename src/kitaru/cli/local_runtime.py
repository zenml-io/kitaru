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
"""Manage the user-scoped local Kitaru Compose deployment."""

import asyncio
import contextlib
import importlib.resources
import ipaddress
import json
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
from packaging.version import InvalidVersion
from pydantic import BaseModel, ConfigDict, Field

from kitaru.cli.output import CLIError
from kitaru.client.config import (
    DIRECTORY_MODE,
    FILE_MODE,
    get_config_directory,
    normalize_server_url,
    write_json_file,
)
from kitaru.images import SERVER_IMAGE_REPOSITORY, get_image

DEFAULT_LOCAL_PORT = 8000
LOCAL_PORT_ENV = "KITARU_LOCAL_PORT"
LOCAL_PROJECT_NAME = "kitaru-local"
LOCAL_IMAGE_ENV = "KITARU_LOCAL_IMAGE"
POSTGRES_IMAGE = "postgres:16-alpine"
_IMAGE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/@:+-]*$")
_ANSI_ESCAPE_PATTERN = re.compile(r"\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
_INSTALL_HINT = (
    "Install Docker from https://docs.docker.com/get-docker/ or Podman from "
    "https://podman.io/docs/installation, or use Kitaru Cloud at "
    "https://cloud.zenml.io/."
)
ContainerRuntime = Literal["docker", "podman"]


class LocalRuntimeState(BaseModel):
    """Persisted identity of the CLI-owned local deployment."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    project: Literal["kitaru-local"] = "kitaru-local"
    server_image: str
    port: int = Field(default=DEFAULT_LOCAL_PORT, ge=1, le=65535)
    runtime: ContainerRuntime = "docker"

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


class ContainerCommandRunner(Protocol):
    """Structural interface for container runtime command execution."""

    runtime: ContainerRuntime

    async def run(self, *arguments: str, timeout: float = 120) -> ProcessResult:
        """Run one container runtime command."""

    def stream(
        self,
        *arguments: str,
        failure_message: str = "Container runtime command failed.",
    ) -> AsyncIterator[str]:
        """Stream output from one container runtime command."""


class ContainerRunner:
    """Execute a container runtime without introducing an SDK dependency."""

    def __init__(self, executable: str, runtime: ContainerRuntime) -> None:
        """Initialize the runner with the resolved runtime executable."""
        self.executable = executable
        self.runtime = runtime

    @property
    def display_name(self) -> str:
        """Return the runtime name for user-facing messages."""
        return self.runtime.title()

    async def run(self, *arguments: str, timeout: float = 120) -> ProcessResult:
        """Run one bounded container runtime command and capture its output."""
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
                f"{self.display_name} could not be executed: {error}",
                hint=_INSTALL_HINT,
            ) from error
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout)
        except TimeoutError as error:
            process.kill()
            await process.communicate()
            raise CLIError(
                "timeout",
                f"{self.display_name} did not finish before the timeout expired.",
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
        failure_message: str = "Container runtime command failed.",
    ) -> AsyncIterator[str]:
        """Yield merged output lines from a running container runtime command."""
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
                f"{self.display_name} could not be executed: {error}",
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
    runner: ContainerCommandRunner | None = None,
    paths: LocalRuntimePaths | None = None,
) -> tuple[dict[str, object], list[str]]:
    """Create or reuse the local deployment and wait for its server."""
    paths = paths or get_local_runtime_paths()
    image, overridden = _get_server_image(package_version)
    with _operation_lock(paths):
        state = _read_state(paths.state)
        if runner is None:
            if state is None:
                available_runners = await _get_available_container_runners()
                runner = available_runners[0]
            else:
                runner = await _get_container_runner(state.runtime)
                available_runners = [runner]
        else:
            await _validate_container_runtime(runner)
            available_runners = [runner]
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
            for available_runner in available_runners:
                await _reject_unowned_resources(available_runner)
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
            _write_runtime_files(
                paths, image=image, port=resolved_port, runtime=runner.runtime
            )
            running = False
        else:
            running = await _is_running(runner)
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
            if upgrade:
                action = "upgraded"
            elif port_changed:
                action = "reconfigured"
            elif state is None:
                action = "created"
            else:
                action = "started"
            compose_arguments = (
                "up",
                "-d",
                "--pull",
                "never",
                "--remove-orphans",
            )
            try:
                if state is not None and (state.server_image != image or port_changed):
                    _write_runtime_files(
                        paths,
                        image=image,
                        port=resolved_port,
                        runtime=runner.runtime,
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
                    stream_arguments = (*base_arguments, *compose_arguments)
                    if runner.runtime == "docker":
                        stream_arguments = (
                            base_arguments[0],
                            "--progress",
                            "plain",
                            *base_arguments[1:],
                            *compose_arguments,
                        )
                    await _stream_container_command(
                        runner,
                        stream_arguments,
                        progress=progress,
                        timeout=max(timeout, 120),
                        failure_message="Compose up failed.",
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
    runner: ContainerCommandRunner | None = None,
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
    if state is None:
        if runner is None:
            runners = await _get_available_container_runners()
        else:
            await _validate_container_runtime(runner)
            runners = [runner]
        with _operation_lock(paths):
            removed = False
            for available_runner in runners:
                removed = (
                    bool(await _remove_labeled_resources(available_runner)) or removed
                )
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
        if runner is None:
            runner = await _get_container_runner(state.runtime)
        else:
            await _validate_container_runtime(runner)
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
    return state is not None and normalized_server_url == state.server_url


def has_local_runtime_state(paths: LocalRuntimePaths | None = None) -> bool:
    """Return whether local deployment ownership state exists."""
    return (paths or get_local_runtime_paths()).state.exists()


async def open_local_dashboard(server_url: str) -> bool:
    """Open the local dashboard in the default browser."""
    return await asyncio.to_thread(webbrowser.open, server_url)


async def get_local_logs(
    *,
    service: str | None,
    tail: int,
    follow: bool,
    runner: ContainerCommandRunner | None = None,
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
    state = _read_state(paths.state)
    if state is None:
        raise CLIError(
            "invalid_configuration",
            "No CLI-owned local Kitaru deployment was found.",
        )
    if runner is None:
        runner = await _get_container_runner(state.runtime)
    else:
        await _validate_container_runtime(runner)
    arguments = [*_compose_arguments(paths), "logs", "--tail", str(tail)]
    if follow:
        arguments.append("--follow")
    if service:
        arguments.append(service)
    if follow:
        return _strip_ansi_stream(
            runner.stream(*arguments, failure_message="Compose logs failed.")
        )
    result = await runner.run(*arguments, timeout=60)
    _raise_for_runtime(result, "Compose logs failed.")
    return _strip_ansi(result.stdout).splitlines()


async def _get_container_runner(
    required_runtime: ContainerRuntime | None = None,
) -> ContainerRunner:
    runtimes: tuple[ContainerRuntime, ...] = (
        (required_runtime,) if required_runtime is not None else ("docker", "podman")
    )
    validation_error: CLIError | None = None
    for runtime in runtimes:
        if executable := shutil.which(runtime):
            runner = ContainerRunner(executable, runtime)
            try:
                await _validate_container_runtime(runner)
            except CLIError as error:
                if required_runtime is not None:
                    raise
                validation_error = error
                continue
            return runner
    if validation_error is not None:
        raise validation_error
    if required_runtime is not None:
        raise CLIError(
            "invalid_configuration",
            f"This local Kitaru deployment uses {required_runtime.title()}, but "
            f"the {required_runtime!r} executable was not found.",
            hint=_INSTALL_HINT,
        )
    raise CLIError(
        "invalid_configuration",
        "Docker or Podman with Compose support is required to run Kitaru locally.",
        hint=_INSTALL_HINT,
    )


async def _get_available_container_runners() -> list[ContainerRunner]:
    """Return every installed and healthy local container runtime."""
    runners: list[ContainerRunner] = []
    validation_error: CLIError | None = None
    runtimes: tuple[ContainerRuntime, ...] = ("docker", "podman")
    for runtime in runtimes:
        if executable := shutil.which(runtime):
            runner = ContainerRunner(executable, runtime)
            try:
                await _validate_container_runtime(runner)
            except CLIError as error:
                validation_error = error
                continue
            runners.append(runner)
    if runners:
        return runners
    if validation_error is not None:
        raise validation_error
    raise CLIError(
        "invalid_configuration",
        "Docker or Podman with Compose support is required to run Kitaru locally.",
        hint=_INSTALL_HINT,
    )


async def _validate_container_runtime(runner: ContainerCommandRunner) -> None:
    compose = await runner.run("compose", "version", timeout=15)
    if compose.returncode:
        compose_name = (
            "Docker Compose v2"
            if runner.runtime == "docker"
            else "Podman Compose support"
        )
        raise CLIError(
            "invalid_configuration",
            f"{compose_name} is required to run Kitaru locally.",
            hint=_INSTALL_HINT,
        )
    info = await runner.run("info", timeout=15)
    if info.returncode:
        hint = (
            "Start Docker, then retry the command."
            if runner.runtime == "docker"
            else "Start a Podman machine, then retry the command."
        )
        raise CLIError(
            "invalid_configuration",
            f"The {runner.runtime.title()} service is unavailable.",
            hint=hint,
            details=_runtime_details(info),
        )
    if runner.runtime == "podman":
        connection_name = os.environ.get("CONTAINER_CONNECTION")
        connection_uri = (
            None if connection_name is not None else os.environ.get("CONTAINER_HOST")
        )
        if connection_name is not None or connection_uri is None:
            connections = await runner.run(
                "system", "connection", "list", "--format", "json", timeout=15
            )
            if connections.returncode:
                raise CLIError(
                    "invalid_configuration",
                    "The active Podman connection could not be inspected.",
                    details=_runtime_details(connections),
                )
            try:
                configured_connections = json.loads(connections.stdout or "[]")
                connection_uri = next(
                    (
                        connection["URI"]
                        for connection in configured_connections
                        if connection.get("Name") == connection_name
                        or (connection_name is None and connection.get("Default"))
                    ),
                    None,
                )
            except (AttributeError, KeyError, TypeError, json.JSONDecodeError) as error:
                raise CLIError(
                    "invalid_configuration",
                    "The active Podman connection could not be inspected.",
                    details=_runtime_details(connections),
                ) from error
        if connection_uri is not None and not _is_local_container_host(connection_uri):
            raise CLIError(
                "invalid_configuration",
                "The active Podman connection points to a remote daemon.",
                hint="Select a local Podman connection, then retry the command.",
            )
        return
    context = await runner.run(
        "context", "inspect", "--format", "{{json .Endpoints.docker.Host}}", timeout=15
    )
    if context.returncode:
        raise CLIError(
            "invalid_configuration",
            "The active Docker context could not be inspected.",
            details=_runtime_details(context),
        )
    host = context.stdout.strip().strip('"')
    if host.startswith(("ssh://", "tcp://", "http://", "https://")):
        raise CLIError(
            "invalid_configuration",
            "The active Docker context points to a remote daemon.",
            hint="Select a local Docker context, then retry the command.",
        )


def _is_local_container_host(host: str) -> bool:
    """Check whether a container service URI is reachable on this host."""
    parsed = urlsplit(host)
    if parsed.scheme == "unix":
        return True
    if parsed.hostname == "localhost":
        return True
    try:
        return ipaddress.ip_address(parsed.hostname or "").is_loopback
    except ValueError:
        return False


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
        image = get_image(SERVER_IMAGE_REPOSITORY, package_version)
    except InvalidVersion as error:
        raise CLIError(
            "invalid_configuration",
            f"The installed Kitaru version {package_version!r} is invalid.",
        ) from error
    except ValueError as error:
        raise CLIError(
            "invalid_configuration",
            "No published local server image is available for this development build.",
            hint=f"Set {LOCAL_IMAGE_ENV} to a compatible local image.",
        ) from error
    return image, False


async def _ensure_image(
    runner: ContainerCommandRunner,
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
        await _stream_container_command(
            runner,
            tuple(arguments),
            progress=progress,
            timeout=600,
            failure_message=f"{runner.runtime.title()} could not pull {image!r}.",
        )
        return
    pulled = await runner.run(*arguments, timeout=600)
    _raise_for_runtime(pulled, f"{runner.runtime.title()} could not pull {image!r}.")


async def _stream_container_command(
    runner: ContainerCommandRunner,
    arguments: tuple[str, ...],
    *,
    progress: Callable[[str], None],
    timeout: float,
    failure_message: str,
) -> None:
    """Forward one bounded container runtime command's output as it arrives."""
    try:
        async with asyncio.timeout(timeout):
            async for line in runner.stream(
                *arguments, failure_message=failure_message
            ):
                progress(line)
    except TimeoutError as error:
        raise CLIError(
            "timeout",
            f"{runner.runtime.title()} did not finish before the timeout expired.",
        ) from error


_RESOURCE_QUERIES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("containers", ("ps", "--all", "--quiet", "--filter")),
    ("networks", ("network", "ls", "--quiet", "--filter")),
    ("volumes", ("volume", "ls", "--quiet", "--filter")),
)


async def _find_labeled_resources(
    runner: ContainerCommandRunner,
) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for kind, prefix in _RESOURCE_QUERIES:
        result = await runner.run(
            *prefix,
            f"label=com.docker.compose.project={LOCAL_PROJECT_NAME}",
            timeout=30,
        )
        _raise_for_runtime(
            result, "Container runtime resources could not be inspected."
        )
        identifiers = result.stdout.split()
        if identifiers:
            found[kind] = identifiers
    return found


async def _reject_unowned_resources(runner: ContainerCommandRunner) -> None:
    found = await _find_labeled_resources(runner)
    if not found:
        return
    # The database password is regenerated along with the runtime files, and
    # Postgres only applies it when it initializes an empty data directory, so
    # an adopted volume would leave the server unable to authenticate.
    raise CLIError(
        "conflict",
        "Container runtime resources named for Kitaru exist without CLI ownership "
        "state.",
        hint="Run `kitaru logout --volumes` to delete them, then retry.",
        details={kind: sorted(values) for kind, values in found.items()},
    )


async def _remove_labeled_resources(
    runner: ContainerCommandRunner,
) -> dict[str, object]:
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
        _raise_for_runtime(
            result, f"{runner.runtime.title()} {kind} could not be removed."
        )
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


async def _is_running(runner: ContainerCommandRunner) -> bool:
    result = await runner.run(
        "ps",
        "--quiet",
        "--filter",
        f"label=com.docker.compose.project={LOCAL_PROJECT_NAME}",
        "--filter",
        "status=running",
        timeout=30,
    )
    _raise_for_runtime(result, "Container runtime state could not be inspected.")
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
    runner: ContainerCommandRunner,
    paths: LocalRuntimePaths,
    *arguments: str,
    timeout: float,
) -> ProcessResult:
    result = await runner.run(*_compose_arguments(paths), *arguments, timeout=timeout)
    _raise_for_runtime(result, f"Compose {' '.join(arguments)} failed.")
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
    runtime: ContainerRuntime = "docker",
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
        LocalRuntimeState(server_image=image, port=port, runtime=runtime).model_dump(),
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
            hint=(
                "Remove the local runtime state after verifying container runtime "
                "resources."
            ),
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


def _raise_for_runtime(result: ProcessResult, message: str) -> None:
    if result.returncode:
        raise CLIError(
            "internal_error", message, details=_runtime_details(result), retryable=True
        )


def _runtime_details(result: ProcessResult) -> dict[str, str]:
    detail = (result.stderr or result.stdout).strip()
    return {"docker_output": detail[-4000:]}


def _decode_output(value: bytes) -> str:
    return value.decode("utf-8", errors="replace").strip()


def _strip_ansi(value: str) -> str:
    """Remove terminal escape sequences from captured output."""
    return _ANSI_ESCAPE_PATTERN.sub("", value)


async def _strip_ansi_stream(lines: AsyncIterator[str]) -> AsyncIterator[str]:
    """Remove terminal escape sequences from streamed output."""
    async for line in lines:
        yield _strip_ansi(line)


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
