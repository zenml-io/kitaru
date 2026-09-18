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
"""Tests for the CLI-owned local Docker Compose runtime."""

import json
import os
from pathlib import Path

import pytest

from kitaru.cli import local_runtime
from kitaru.cli.local_runtime import (
    LocalRuntimePaths,
    ProcessResult,
)
from kitaru.cli.output import CLIError


class FakeDockerRunner:
    """Record Docker commands and return configurable results."""

    def __init__(self, runtime: local_runtime.ContainerRuntime = "docker") -> None:
        """Initialize a successful local container runtime."""
        self.runtime = runtime
        self.calls: list[tuple[str, ...]] = []
        self.stream_calls: list[tuple[str, ...]] = []
        self.stream_lines = ("server ready", "db ready")
        self.results: dict[tuple[str, ...], ProcessResult] = {}

    async def run(self, *arguments: str, timeout: float = 120) -> ProcessResult:
        """Return the configured result for the exact argument list."""
        del timeout
        self.calls.append(arguments)
        result = self.results.get(arguments)
        if result is not None:
            return result
        if arguments[:2] == ("context", "inspect"):
            return ProcessResult(0, '"unix:///var/run/docker.sock"', "")
        return ProcessResult(0, "", "")

    async def stream(
        self,
        *arguments: str,
        failure_message: str = "Docker command failed.",
    ):
        """Yield representative Compose log lines."""
        del failure_message
        self.calls.append(arguments)
        self.stream_calls.append(arguments)
        for line in self.stream_lines:
            yield line


@pytest.fixture
def runtime_paths(tmp_path: Path) -> LocalRuntimePaths:
    """Build isolated local runtime paths."""
    directory = tmp_path / "local"
    return LocalRuntimePaths(
        directory=directory,
        compose=directory / "compose.yaml",
        environment=directory / "runtime.env",
        state=directory / "state.json",
        lock=directory / "operation.lock",
    )


@pytest.fixture(autouse=True)
def clear_local_port_environment(monkeypatch) -> None:
    """Keep local-runtime tests independent of the developer environment."""
    monkeypatch.delenv(local_runtime.LOCAL_PORT_ENV, raising=False)
    monkeypatch.delenv("CONTAINER_CONNECTION", raising=False)
    monkeypatch.delenv("CONTAINER_HOST", raising=False)


async def test_first_start_writes_private_state_and_starts_compose(
    runtime_paths, monkeypatch
) -> None:
    """First login creates private runtime files and a Compose deployment."""
    runner = FakeDockerRunner()
    server_image = "zenmldocker/kitaru-server:0.21.0"
    runner.results[("image", "inspect", server_image)] = ProcessResult(1, "", "")
    monkeypatch.setattr(local_runtime, "_reject_occupied_port", lambda _: None)

    async def healthy(server_url: str, timeout: float) -> None:
        assert server_url == "http://localhost:8000"
        assert timeout == 120

    monkeypatch.setattr(local_runtime, "_wait_for_health", healthy)
    item, warnings = await local_runtime.start_local_runtime(
        package_version="0.21.0",
        upgrade=False,
        timeout=30,
        runner=runner,
        paths=runtime_paths,
    )

    assert item["deployment"] == "created"
    assert item["authentication"] == "not_required"
    assert warnings == []
    assert json.loads(runtime_paths.state.read_text())["server_image"] == server_image
    assert "KITARU_LOCAL_DB_PASSWORD=" in runtime_paths.environment.read_text()
    assert runtime_paths.environment.stat().st_mode & 0o777 == 0o600
    assert ("pull", "--platform", "linux/amd64", server_image) in runner.calls
    assert ("pull", local_runtime.POSTGRES_IMAGE) not in runner.calls
    assert any("up" in call and "--pull" in call for call in runner.calls)
    assert runner.stream_calls == []


async def test_custom_port_is_persisted_and_used_for_startup(
    runtime_paths, monkeypatch
) -> None:
    """A requested host port controls Compose and the reported local URL."""
    runner = FakeDockerRunner()
    checked_ports: list[int] = []
    health_checks: list[tuple[str, float]] = []
    monkeypatch.setattr(local_runtime, "_reject_occupied_port", checked_ports.append)

    async def healthy(server_url: str, timeout: float) -> None:
        health_checks.append((server_url, timeout))

    monkeypatch.setattr(local_runtime, "_wait_for_health", healthy)

    item, _ = await local_runtime.start_local_runtime(
        package_version="0.21.0",
        upgrade=False,
        timeout=30,
        port=9010,
        runner=runner,
        paths=runtime_paths,
    )

    state = json.loads(runtime_paths.state.read_text())
    assert state["port"] == 9010
    assert "KITARU_LOCAL_HOST_PORT=9010" in runtime_paths.environment.read_text()
    assert item["server_url"] == "http://localhost:9010"
    assert item["port"] == 9010
    assert checked_ports == [9010]
    assert health_checks == [("http://localhost:9010", 120)]


def test_local_port_precedence_and_validation(runtime_paths, monkeypatch) -> None:
    """The flag wins over the environment, which wins over stored state."""
    state = local_runtime.LocalRuntimeState(
        server_image="zenmldocker/kitaru-server:0.21.0", port=9001
    )
    monkeypatch.setenv(local_runtime.LOCAL_PORT_ENV, "9002")

    assert local_runtime._resolve_local_port(9003, state) == 9003
    assert local_runtime._resolve_local_port(None, state) == 9002
    monkeypatch.delenv(local_runtime.LOCAL_PORT_ENV)
    assert local_runtime._resolve_local_port(None, state) == 9001
    assert local_runtime._resolve_local_port(None, None) == 8000

    with pytest.raises(CLIError) as explicit_error:
        local_runtime._resolve_local_port(0, state)
    assert explicit_error.value.kind == "invalid_arguments"

    monkeypatch.setenv(local_runtime.LOCAL_PORT_ENV, "not-a-port")
    with pytest.raises(CLIError) as environment_error:
        local_runtime._resolve_local_port(None, state)
    assert environment_error.value.kind == "invalid_configuration"


def test_legacy_runtime_state_defaults_to_port_8000(runtime_paths) -> None:
    """State written before configurable ports remains readable."""
    runtime_paths.directory.mkdir(parents=True)
    runtime_paths.state.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "project": "kitaru-local",
                "server_image": "zenmldocker/kitaru-server:0.21.0",
            }
        )
    )

    state = local_runtime._read_state(runtime_paths.state)

    assert state is not None
    assert state.port == 8000
    assert state.runtime == "docker"
    assert state.server_url == "http://localhost:8000"


def test_local_runtime_url_uses_persisted_custom_port(runtime_paths) -> None:
    """Owned-runtime detection follows the persisted port and normalizes URLs."""
    local_runtime._write_runtime_files(
        runtime_paths,
        image="zenmldocker/kitaru-server:0.21.0",
        port=9010,
    )

    assert local_runtime.is_local_runtime_url(
        "http://localhost:9010/", paths=runtime_paths
    )
    assert not local_runtime.is_local_runtime_url(
        "http://localhost:8000", paths=runtime_paths
    )


def test_local_runtime_url_requires_ownership_state(runtime_paths) -> None:
    """Orphan cleanup does not guess a URL when ownership state is missing."""
    assert not local_runtime.is_local_runtime_url(
        "http://localhost:8000", paths=runtime_paths
    )
    assert not local_runtime.is_local_runtime_url(
        "http://localhost:9010", paths=runtime_paths
    )


def test_managed_url_does_not_read_malformed_local_state(runtime_paths) -> None:
    """Unrelated logout targets are independent of local state integrity."""
    runtime_paths.directory.mkdir(parents=True)
    runtime_paths.state.write_text("not json")

    assert not local_runtime.is_local_runtime_url(
        "https://managed.example.com", paths=runtime_paths
    )


async def test_changed_port_reconfigures_running_deployment(
    runtime_paths, monkeypatch
) -> None:
    """Changing the requested port recreates the server without deleting data."""
    image = "zenmldocker/kitaru-server:0.21.0"
    local_runtime._write_runtime_files(runtime_paths, image=image, port=8000)
    runner = FakeDockerRunner()
    runner.results[
        (
            "ps",
            "--quiet",
            "--filter",
            "label=com.docker.compose.project=kitaru-local",
            "--filter",
            "status=running",
        )
    ] = ProcessResult(0, "server\ndb\n", "")
    checked_ports: list[int] = []
    monkeypatch.setattr(local_runtime, "_reject_occupied_port", checked_ports.append)

    async def healthy(server_url: str, timeout: float) -> None:
        assert server_url == "http://localhost:9010"

    monkeypatch.setattr(local_runtime, "_wait_for_health", healthy)

    item, _ = await local_runtime.start_local_runtime(
        package_version="0.21.0",
        upgrade=False,
        timeout=30,
        port=9010,
        runner=runner,
        paths=runtime_paths,
    )

    assert item["deployment"] == "reconfigured"
    assert checked_ports == [9010]
    assert any("up" in call for call in runner.calls)
    assert not any("--volumes" in call for call in runner.calls)
    assert json.loads(runtime_paths.state.read_text())["port"] == 9010


async def test_failed_port_reconfiguration_restores_running_deployment(
    runtime_paths, monkeypatch
) -> None:
    """A failed port change restores the previous files and Compose mapping."""
    image = "zenmldocker/kitaru-server:0.21.0"
    local_runtime._write_runtime_files(runtime_paths, image=image, port=8000)
    previous_environment = runtime_paths.environment.read_text()
    previous_state = runtime_paths.state.read_text()
    runner = FakeDockerRunner()
    runner.results[
        (
            "ps",
            "--quiet",
            "--filter",
            "label=com.docker.compose.project=kitaru-local",
            "--filter",
            "status=running",
        )
    ] = ProcessResult(0, "server\ndb\n", "")
    monkeypatch.setattr(local_runtime, "_reject_occupied_port", lambda _: None)

    async def health(server_url: str, timeout: float) -> None:
        del timeout
        if server_url == "http://localhost:9010":
            raise CLIError("timeout", "unhealthy")

    monkeypatch.setattr(local_runtime, "_wait_for_health", health)

    with pytest.raises(CLIError, match="unhealthy"):
        await local_runtime.start_local_runtime(
            package_version="0.21.0",
            upgrade=False,
            timeout=30,
            port=9010,
            runner=runner,
            paths=runtime_paths,
        )

    assert runtime_paths.environment.read_text() == previous_environment
    assert runtime_paths.state.read_text() == previous_state
    compose_ups = [
        call
        for call in runner.calls
        if call[-5:] == ("up", "-d", "--pull", "never", "--remove-orphans")
    ]
    assert len(compose_ups) == 2
    assert not any("down" in call or "--volumes" in call for call in runner.calls)

    item, _ = await local_runtime.start_local_runtime(
        package_version="0.21.0",
        upgrade=False,
        timeout=30,
        runner=runner,
        paths=runtime_paths,
    )
    assert item["deployment"] == "reused"
    assert item["server_url"] == "http://localhost:8000"


async def test_failed_runtime_file_update_restores_previous_files(
    runtime_paths, monkeypatch
) -> None:
    """A partial reconfiguration write restores both runtime files."""
    image = "zenmldocker/kitaru-server:0.21.0"
    local_runtime._write_runtime_files(runtime_paths, image=image, port=8000)
    previous_environment = runtime_paths.environment.read_text()
    previous_state = runtime_paths.state.read_text()
    runner = FakeDockerRunner()
    runner.results[
        (
            "ps",
            "--quiet",
            "--filter",
            "label=com.docker.compose.project=kitaru-local",
            "--filter",
            "status=running",
        )
    ] = ProcessResult(0, "server\ndb\n", "")
    monkeypatch.setattr(local_runtime, "_reject_occupied_port", lambda _: None)
    original_write = local_runtime.write_json_file
    failed = False

    def write(path: Path, content: dict[str, object]) -> None:
        nonlocal failed
        if path == runtime_paths.state and content.get("port") == 9010 and not failed:
            failed = True
            raise OSError("interrupted state write")
        original_write(path, content)

    monkeypatch.setattr(local_runtime, "write_json_file", write)

    with pytest.raises(OSError, match="interrupted state write"):
        await local_runtime.start_local_runtime(
            package_version="0.21.0",
            upgrade=False,
            timeout=30,
            port=9010,
            runner=runner,
            paths=runtime_paths,
        )

    assert runtime_paths.environment.read_text() == previous_environment
    assert runtime_paths.state.read_text() == previous_state


async def test_interactive_start_streams_pull_and_compose_progress(
    runtime_paths, monkeypatch
) -> None:
    """Interactive startup forwards long-running Docker output."""
    runner = FakeDockerRunner()
    server_image = "zenmldocker/kitaru-server:0.21.0"
    runner.results[("image", "inspect", server_image)] = ProcessResult(1, "", "")
    monkeypatch.setattr(local_runtime, "_reject_occupied_port", lambda _: None)

    async def healthy(server_url: str, timeout: float) -> None:
        del server_url, timeout

    monkeypatch.setattr(local_runtime, "_wait_for_health", healthy)
    progress: list[str] = []
    await local_runtime.start_local_runtime(
        package_version="0.21.0",
        upgrade=False,
        timeout=30,
        progress=progress.append,
        runner=runner,
        paths=runtime_paths,
    )

    assert progress == ["server ready", "db ready", "server ready", "db ready"]
    assert ("pull", "--platform", "linux/amd64", server_image) in runner.stream_calls
    compose_call = next(call for call in runner.stream_calls if "up" in call)
    assert compose_call[:3] == ("compose", "--progress", "plain")


async def test_podman_interactive_start_omits_docker_progress_option(
    runtime_paths, monkeypatch
) -> None:
    """Podman Compose starts without Docker's global progress option."""
    runner = FakeDockerRunner(runtime="podman")
    monkeypatch.setattr(local_runtime, "_reject_occupied_port", lambda _: None)

    async def healthy(server_url: str, timeout: float) -> None:
        del server_url, timeout

    monkeypatch.setattr(local_runtime, "_wait_for_health", healthy)
    await local_runtime.start_local_runtime(
        package_version="0.21.0",
        upgrade=False,
        timeout=30,
        progress=lambda _: None,
        runner=runner,
        paths=runtime_paths,
    )

    compose_call = next(call for call in runner.stream_calls if "up" in call)
    assert compose_call[:2] == ("compose", "--project-name")
    assert "--progress" not in compose_call
    assert json.loads(runtime_paths.state.read_text())["runtime"] == "podman"


async def test_missing_compose_has_install_and_cloud_hint() -> None:
    """A Docker installation without Compose v2 fails with both alternatives."""
    runner = FakeDockerRunner()
    runner.results[("compose", "version")] = ProcessResult(1, "", "missing")

    with pytest.raises(CLIError, match="Compose v2") as raised:
        await local_runtime._validate_container_runtime(runner)

    assert "docs.docker.com" in str(raised.value.hint)
    assert "cloud.zenml.io" in str(raised.value.hint)


async def test_missing_container_runtime_has_install_and_cloud_hint(
    monkeypatch,
) -> None:
    """Absent container CLIs produce an actionable local-or-Cloud choice."""
    monkeypatch.setattr(local_runtime.shutil, "which", lambda executable: None)

    with pytest.raises(CLIError, match="Docker or Podman") as raised:
        await local_runtime._get_container_runner()

    assert "docs.docker.com" in str(raised.value.hint)
    assert "podman.io" in str(raised.value.hint)
    assert "cloud.zenml.io" in str(raised.value.hint)


@pytest.mark.parametrize(
    ("available", "expected"),
    [
        ({"docker": "/usr/bin/docker", "podman": "/usr/bin/podman"}, "docker"),
        ({"podman": "/usr/bin/podman"}, "podman"),
    ],
)
async def test_container_runtime_resolution_prefers_docker_then_podman(
    monkeypatch, available: dict[str, str], expected: str
) -> None:
    """Runtime discovery remains deterministic when both CLIs are installed."""
    monkeypatch.setattr(local_runtime.shutil, "which", available.get)

    async def valid(runner) -> None:
        del runner

    monkeypatch.setattr(local_runtime, "_validate_container_runtime", valid)

    runner = await local_runtime._get_container_runner()

    assert runner.runtime == expected


async def test_required_runtime_does_not_switch_engines(monkeypatch) -> None:
    """An owned deployment never silently moves to another engine's storage."""
    monkeypatch.setattr(
        local_runtime.shutil,
        "which",
        {"docker": "/usr/bin/docker", "podman": "/usr/bin/podman"}.get,
    )

    async def valid(runner) -> None:
        del runner

    monkeypatch.setattr(local_runtime, "_validate_container_runtime", valid)

    runner = await local_runtime._get_container_runner("podman")

    assert runner.runtime == "podman"


async def test_new_deployment_falls_back_from_broken_docker_to_podman(
    monkeypatch,
) -> None:
    """A stopped Docker installation does not block a healthy Podman machine."""
    monkeypatch.setattr(
        local_runtime.shutil,
        "which",
        {"docker": "/usr/bin/docker", "podman": "/usr/bin/podman"}.get,
    )
    validated: list[local_runtime.ContainerRuntime] = []

    async def validate(runner) -> None:
        validated.append(runner.runtime)
        if runner.runtime == "docker":
            raise CLIError("invalid_configuration", "Docker is unavailable.")

    monkeypatch.setattr(local_runtime, "_validate_container_runtime", validate)

    runner = await local_runtime._get_container_runner()

    assert validated == ["docker", "podman"]
    assert runner.runtime == "podman"


async def test_existing_deployment_resolves_its_persisted_runtime(
    runtime_paths, monkeypatch
) -> None:
    """Later logins keep using the engine that owns the deployment's data."""
    local_runtime._write_runtime_files(
        runtime_paths,
        image="zenmldocker/kitaru-server:0.21.0",
        port=8000,
        runtime="podman",
    )
    runner = FakeDockerRunner(runtime="podman")
    running_call = (
        "ps",
        "--quiet",
        "--filter",
        "label=com.docker.compose.project=kitaru-local",
        "--filter",
        "status=running",
    )
    runner.results[running_call] = ProcessResult(0, "server\ndb\n", "")
    requested: list[local_runtime.ContainerRuntime | None] = []

    async def resolve(
        required_runtime: local_runtime.ContainerRuntime | None = None,
    ) -> FakeDockerRunner:
        requested.append(required_runtime)
        return runner

    async def healthy(server_url: str, timeout: float) -> None:
        del server_url, timeout

    monkeypatch.setattr(local_runtime, "_get_container_runner", resolve)
    monkeypatch.setattr(local_runtime, "_wait_for_health", healthy)

    item, _ = await local_runtime.start_local_runtime(
        package_version="0.21.0",
        upgrade=False,
        timeout=30,
        paths=runtime_paths,
    )

    assert requested == ["podman"]
    assert item["deployment"] == "reused"


async def test_remote_docker_context_is_rejected() -> None:
    """A remote daemon cannot expose the fixed localhost URL correctly."""
    runner = FakeDockerRunner()
    runner.results[
        ("context", "inspect", "--format", "{{json .Endpoints.docker.Host}}")
    ] = ProcessResult(0, '"ssh://docker.example.com"', "")

    with pytest.raises(CLIError, match="remote daemon"):
        await local_runtime._validate_container_runtime(runner)


async def test_podman_validation_skips_docker_context_inspection() -> None:
    """A Podman machine's loopback SSH connection remains valid."""
    runner = FakeDockerRunner(runtime="podman")
    runner.results[("system", "connection", "list", "--format", "json")] = (
        ProcessResult(
            0,
            json.dumps(
                [
                    {
                        "Name": "podman-machine-default",
                        "URI": "ssh://core@127.0.0.1:53298/run/user/501/podman.sock",
                        "Default": True,
                    }
                ]
            ),
            "",
        )
    )

    await local_runtime._validate_container_runtime(runner)

    assert runner.calls == [
        ("compose", "version"),
        ("info",),
        ("system", "connection", "list", "--format", "json"),
    ]


@pytest.mark.parametrize(
    "uri",
    [
        "ssh://root@podman.example.com/run/podman/podman.sock",
        "tcp://192.0.2.10:1234",
    ],
)
async def test_remote_podman_connection_is_rejected(uri: str) -> None:
    """A remote Podman service cannot expose ports on the local host."""
    runner = FakeDockerRunner(runtime="podman")
    runner.results[("system", "connection", "list", "--format", "json")] = (
        ProcessResult(
            0,
            json.dumps([{"Name": "remote", "URI": uri, "Default": True}]),
            "",
        )
    )

    with pytest.raises(CLIError, match="remote daemon"):
        await local_runtime._validate_container_runtime(runner)


async def test_podman_named_connection_takes_precedence_over_host(
    monkeypatch,
) -> None:
    """A named Podman connection overrides the configured container host."""
    runner = FakeDockerRunner(runtime="podman")
    runner.results[("system", "connection", "list", "--format", "json")] = (
        ProcessResult(
            0,
            json.dumps(
                [
                    {
                        "Name": "remote",
                        "URI": "ssh://root@podman.example.com/run/podman/podman.sock",
                        "Default": False,
                    }
                ]
            ),
            "",
        )
    )
    monkeypatch.setenv("CONTAINER_CONNECTION", "remote")
    monkeypatch.setenv("CONTAINER_HOST", "unix:///run/user/501/podman.sock")

    with pytest.raises(CLIError, match="remote daemon"):
        await local_runtime._validate_container_runtime(runner)

    assert ("system", "connection", "list", "--format", "json") in runner.calls


async def test_developer_override_must_exist_locally(
    runtime_paths, monkeypatch
) -> None:
    """A developer image override never causes an implicit registry pull."""
    runner = FakeDockerRunner()
    runner.results[("image", "inspect", "kitaru-dev:test")] = ProcessResult(
        1, "", "missing"
    )
    monkeypatch.setenv(local_runtime.LOCAL_IMAGE_ENV, "kitaru-dev:test")
    monkeypatch.setattr(local_runtime, "_reject_occupied_port", lambda _: None)

    with pytest.raises(CLIError, match="not available locally"):
        await local_runtime.start_local_runtime(
            package_version="0.21.0",
            upgrade=False,
            timeout=30,
            runner=runner,
            paths=runtime_paths,
        )

    assert not any(call and call[0] == "pull" for call in runner.calls)


async def test_version_mismatch_requires_explicit_upgrade(
    runtime_paths, monkeypatch
) -> None:
    """A normal login does not replace a differently versioned server."""
    local_runtime._write_runtime_files(
        runtime_paths,
        image="zenmldocker/kitaru-server:0.20.0",
        port=8000,
    )
    monkeypatch.delenv(local_runtime.LOCAL_IMAGE_ENV, raising=False)

    with pytest.raises(CLIError, match="Your local Kitaru server uses") as raised:
        await local_runtime.start_local_runtime(
            package_version="0.21.0",
            upgrade=False,
            timeout=30,
            runner=FakeDockerRunner(),
            paths=runtime_paths,
        )

    assert raised.value.kind == "conflict"
    assert "zenmldocker/kitaru-server:0.20.0" in raised.value.message
    assert "zenmldocker/kitaru-server:0.21.0" in raised.value.message
    assert "without your approval" in raised.value.message
    assert "--upgrade" in str(raised.value.hint)
    assert "database will be kept" in str(raised.value.hint)
    assert raised.value.details == {
        "current_image": "zenmldocker/kitaru-server:0.20.0",
        "requested_image": "zenmldocker/kitaru-server:0.21.0",
    }


async def test_upgrade_refreshes_release_image(runtime_paths, monkeypatch) -> None:
    """An explicit upgrade pulls and recreates a cached release image."""
    image = "zenmldocker/kitaru-server:0.21.0"
    local_runtime._write_runtime_files(runtime_paths, image=image, port=8000)
    runner = FakeDockerRunner()

    async def healthy(server_url: str, timeout: float) -> None:
        del server_url, timeout

    monkeypatch.setattr(local_runtime, "_wait_for_health", healthy)
    item, _ = await local_runtime.start_local_runtime(
        package_version="0.21.0",
        upgrade=True,
        timeout=30,
        runner=runner,
        paths=runtime_paths,
    )

    assert item["deployment"] == "upgraded"
    assert ("pull", "--platform", "linux/amd64", image) in runner.calls


async def test_failed_first_start_removes_containers_but_keeps_data(
    runtime_paths, monkeypatch
) -> None:
    """A failed first startup runs Compose down without deleting volumes."""
    runner = FakeDockerRunner()
    monkeypatch.setattr(local_runtime, "_reject_occupied_port", lambda _: None)

    async def unhealthy(server_url: str, timeout: float) -> None:
        del server_url, timeout
        raise CLIError("timeout", "unhealthy")

    monkeypatch.setattr(local_runtime, "_wait_for_health", unhealthy)

    with pytest.raises(CLIError, match="unhealthy"):
        await local_runtime.start_local_runtime(
            package_version="0.21.0",
            upgrade=False,
            timeout=1,
            runner=runner,
            paths=runtime_paths,
        )

    down = [call for call in runner.calls if "down" in call]
    assert len(down) == 1
    assert "--volumes" not in down[0]
    assert runtime_paths.environment.exists()
    assert runtime_paths.state.exists()


async def test_stop_with_volumes_deletes_runtime_state(runtime_paths) -> None:
    """Volume deletion also removes the secrets and ownership files."""
    image = "zenmldocker/kitaru-server:0.21.0"
    local_runtime._write_runtime_files(runtime_paths, image=image, port=8000)
    result = await local_runtime.stop_local_runtime(
        delete_volumes=True,
        runner=FakeDockerRunner(),
        paths=runtime_paths,
    )

    assert result["data_deleted"] is True
    assert not runtime_paths.environment.exists()
    assert not runtime_paths.compose.exists()
    assert not runtime_paths.state.exists()


async def test_logs_use_bounded_tail_and_service(runtime_paths) -> None:
    """Snapshot logs pass the requested bound and service to Compose."""
    local_runtime._write_runtime_files(
        runtime_paths,
        image="zenmldocker/kitaru-server:0.21.0",
        port=8000,
    )
    runner = FakeDockerRunner()
    compose = local_runtime._compose_arguments(runtime_paths)
    runner.results[(*compose, "logs", "--tail", "25", "server")] = ProcessResult(
        0, "\x1b[32mready\x1b[0m\nserving", ""
    )

    result = await local_runtime.get_local_logs(
        service="server",
        tail=25,
        follow=False,
        runner=runner,
        paths=runtime_paths,
    )

    assert result == ["ready", "serving"]


async def test_followed_logs_strip_ansi_sequences(runtime_paths) -> None:
    """Followed logs remove terminal formatting from every streamed line."""
    local_runtime._write_runtime_files(
        runtime_paths,
        image="zenmldocker/kitaru-server:0.21.0",
        port=8000,
    )
    runner = FakeDockerRunner(runtime="podman")
    runner.stream_lines = ("\x1b[32mready\x1b[0m", "\x1b[1mserving\x1b[0m")

    result = await local_runtime.get_local_logs(
        service="server",
        tail=25,
        follow=True,
        runner=runner,
        paths=runtime_paths,
    )

    assert not isinstance(result, list)
    assert [line async for line in result] == ["ready", "serving"]
    assert runner.stream_calls == [
        (
            *local_runtime._compose_arguments(runtime_paths),
            "logs",
            "--tail",
            "25",
            "--follow",
            "server",
        )
    ]


async def test_running_state_uses_engine_filters() -> None:
    """Running-state checks do not depend on a Compose provider's ps flags."""
    runner = FakeDockerRunner(runtime="podman")
    call = (
        "ps",
        "--quiet",
        "--filter",
        "label=com.docker.compose.project=kitaru-local",
        "--filter",
        "status=running",
    )
    runner.results[call] = ProcessResult(0, "server\ndb\n", "")

    assert await local_runtime._is_running(runner)
    assert runner.calls == [call]


def test_stale_operation_lock_is_reclaimed(runtime_paths) -> None:
    """A lock left by a dead process does not block future commands."""
    runtime_paths.directory.mkdir(parents=True)
    runtime_paths.lock.write_text("999999999", encoding="utf-8")

    with local_runtime._operation_lock(runtime_paths):
        assert runtime_paths.lock.exists()

    assert not runtime_paths.lock.exists()


def test_development_version_requires_an_override(monkeypatch) -> None:
    """Unpublished development builds fail before selecting a release image."""
    monkeypatch.delenv(local_runtime.LOCAL_IMAGE_ENV, raising=False)
    with pytest.raises(CLIError, match="development build"):
        local_runtime._get_server_image("0.22.0.dev1")


@pytest.mark.parametrize(
    ("package_version", "image_version"),
    [
        ("0.22.0a1", "0.22.0-a.1"),
        ("0.22.0b3", "0.22.0-b.3"),
        ("0.22.0rc5", "0.22.0-rc.5"),
        ("0.22.0.post2", "0.22.0-post.2"),
        ("0.22.0rc5.post2", "0.22.0-rc.5.post.2"),
    ],
)
def test_release_suffix_uses_docker_image_tag(
    monkeypatch, package_version: str, image_version: str
) -> None:
    """PEP 440 release suffixes map to the Docker tag format."""
    monkeypatch.delenv(local_runtime.LOCAL_IMAGE_ENV, raising=False)

    image, overridden = local_runtime._get_server_image(package_version)

    assert image == f"zenmldocker/kitaru-server:{image_version}"
    assert overridden is False


def test_runtime_files_contain_no_world_readable_secrets(runtime_paths) -> None:
    """Generated runtime secrets are restricted to the current user."""
    local_runtime._write_runtime_files(
        runtime_paths,
        image="zenmldocker/kitaru-server:0.21.0",
        port=8000,
    )
    assert os.stat(runtime_paths.directory).st_mode & 0o777 == 0o700
    assert os.stat(runtime_paths.environment).st_mode & 0o777 == 0o600
    assert os.stat(runtime_paths.state).st_mode & 0o777 == 0o600


def _orphan_volume_runner() -> FakeDockerRunner:
    """Build a runner reporting a leftover volume and no other resources.

    Returns:
        Runner instance.
    """
    runner = FakeDockerRunner()
    label = f"label=com.docker.compose.project={local_runtime.LOCAL_PROJECT_NAME}"
    runner.results[("volume", "ls", "--quiet", "--filter", label)] = ProcessResult(
        0, "kitaru-local_postgres_data\n", ""
    )
    return runner


async def test_orphaned_resources_name_themselves_and_point_at_the_cleanup(
    runtime_paths, monkeypatch
) -> None:
    """A leftover volume without state reports what blocks the login."""
    monkeypatch.delenv(local_runtime.LOCAL_IMAGE_ENV, raising=False)

    with pytest.raises(CLIError, match="without CLI ownership state") as raised:
        await local_runtime.start_local_runtime(
            package_version="0.21.0",
            upgrade=False,
            timeout=30,
            runner=_orphan_volume_runner(),
            paths=runtime_paths,
        )

    assert raised.value.kind == "conflict"
    assert "kitaru logout --volumes" in str(raised.value.hint)
    assert raised.value.details == {"volumes": ["kitaru-local_postgres_data"]}


async def test_first_start_checks_every_healthy_runtime(
    runtime_paths, monkeypatch
) -> None:
    """A first start finds Podman resources when Docker is also healthy."""
    docker = FakeDockerRunner()
    podman = _orphan_volume_runner()
    podman.runtime = "podman"
    runners = {"docker": docker, "podman": podman}
    monkeypatch.setattr(
        local_runtime.shutil,
        "which",
        {"docker": "/usr/bin/docker", "podman": "/usr/bin/podman"}.get,
    )
    monkeypatch.setattr(
        local_runtime,
        "ContainerRunner",
        lambda _executable, runtime: runners[runtime],
    )

    with pytest.raises(CLIError, match="without CLI ownership state") as raised:
        await local_runtime.start_local_runtime(
            package_version="0.21.0",
            upgrade=False,
            timeout=30,
            paths=runtime_paths,
        )

    assert raised.value.details == {"volumes": ["kitaru-local_postgres_data"]}
    label = f"label=com.docker.compose.project={local_runtime.LOCAL_PROJECT_NAME}"
    assert ("volume", "ls", "--quiet", "--filter", label) in docker.calls
    assert not runtime_paths.state.exists()


async def test_deleting_volumes_removes_orphaned_resources(runtime_paths) -> None:
    """A stop with data deletion clears resources the state no longer tracks."""
    runner = _orphan_volume_runner()

    item = await local_runtime.stop_local_runtime(
        delete_volumes=True, runner=runner, paths=runtime_paths
    )

    assert item["deployment"] == "deleted"
    assert item["data_deleted"] is True
    assert ("volume", "rm", "kitaru-local_postgres_data") in runner.calls


async def test_deleting_volumes_checks_every_healthy_runtime(
    runtime_paths, monkeypatch
) -> None:
    """State-less cleanup finds Podman resources when Docker is also healthy."""
    docker = FakeDockerRunner()
    podman = _orphan_volume_runner()
    podman.runtime = "podman"
    runners = {"docker": docker, "podman": podman}
    monkeypatch.setattr(
        local_runtime.shutil,
        "which",
        {"docker": "/usr/bin/docker", "podman": "/usr/bin/podman"}.get,
    )
    monkeypatch.setattr(
        local_runtime,
        "ContainerRunner",
        lambda _executable, runtime: runners[runtime],
    )

    item = await local_runtime.stop_local_runtime(
        delete_volumes=True, paths=runtime_paths
    )

    assert item["deployment"] == "deleted"
    assert ("volume", "rm", "kitaru-local_postgres_data") in podman.calls
    label = f"label=com.docker.compose.project={local_runtime.LOCAL_PROJECT_NAME}"
    assert ("volume", "ls", "--quiet", "--filter", label) in docker.calls


async def test_stop_without_resources_reports_no_deployment(runtime_paths) -> None:
    """A stop finds nothing to delete when no resources carry the label."""
    with pytest.raises(CLIError, match="No CLI-owned local Kitaru deployment"):
        await local_runtime.stop_local_runtime(
            delete_volumes=True, runner=FakeDockerRunner(), paths=runtime_paths
        )
