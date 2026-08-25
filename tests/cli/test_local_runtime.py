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
from packaging.version import Version

from kitaru.cli import local_runtime
from kitaru.cli.local_runtime import (
    LocalRuntimePaths,
    ProcessResult,
)
from kitaru.cli.output import CLIError


class FakeDockerRunner:
    """Record Docker commands and return configurable results."""

    def __init__(self) -> None:
        """Initialize a successful local Docker daemon."""
        self.calls: list[tuple[str, ...]] = []
        self.stream_calls: list[tuple[str, ...]] = []
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
        for line in ("server ready", "db ready"):
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
    """Keep local-port tests independent of the developer environment."""
    monkeypatch.delenv(local_runtime.LOCAL_PORT_ENV, raising=False)


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
    compose = local_runtime._compose_arguments(runtime_paths)
    runner.results[(*compose, "ps", "--status", "running", "--quiet")] = ProcessResult(
        0, "server\ndb\n", ""
    )
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
    compose = local_runtime._compose_arguments(runtime_paths)
    runner.results[(*compose, "ps", "--status", "running", "--quiet")] = ProcessResult(
        0, "server\ndb\n", ""
    )
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


async def test_missing_compose_has_install_and_cloud_hint() -> None:
    """A Docker installation without Compose v2 fails with both alternatives."""
    runner = FakeDockerRunner()
    runner.results[("compose", "version")] = ProcessResult(1, "", "missing")

    with pytest.raises(CLIError, match="Compose v2") as raised:
        await local_runtime._validate_docker(runner)

    assert "docs.docker.com" in str(raised.value.hint)
    assert "cloud.zenml.io" in str(raised.value.hint)


async def test_missing_docker_has_install_and_cloud_hint(monkeypatch) -> None:
    """An absent Docker CLI produces an actionable local-or-Cloud choice."""
    monkeypatch.setattr(local_runtime.shutil, "which", lambda executable: None)

    with pytest.raises(CLIError, match="Docker with Compose v2") as raised:
        await local_runtime._get_docker_runner()

    assert "docs.docker.com" in str(raised.value.hint)
    assert "cloud.zenml.io" in str(raised.value.hint)


async def test_remote_docker_context_is_rejected() -> None:
    """A remote daemon cannot expose the fixed localhost URL correctly."""
    runner = FakeDockerRunner()
    runner.results[
        ("context", "inspect", "--format", "{{json .Endpoints.docker.Host}}")
    ] = ProcessResult(0, '"ssh://docker.example.com"', "")

    with pytest.raises(CLIError, match="remote daemon"):
        await local_runtime._validate_docker(runner)


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
        0, "ready\nserving", ""
    )

    result = await local_runtime.get_local_logs(
        service="server",
        tail=25,
        follow=False,
        runner=runner,
        paths=runtime_paths,
    )

    assert result == ["ready", "serving"]


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


@pytest.mark.parametrize(
    ("package_version", "image_version"),
    [
        ("0.22.0", "0.22.0"),
        ("0.22.0rc5.post2", "0.22.0-rc.5.post.2"),
        ("0.22.0.dev3", "0.22.0-dev.3"),
        ("0.22.0+macos.arm64", "0.22.0-local.macos.arm64"),
        ("1!0.22.0rc5", "1.epoch.0.22.0-rc.5"),
        (
            "0.22.0rc5.post2.dev3+macos.arm64",
            "0.22.0-rc.5.post.2.dev.3.local.macos.arm64",
        ),
    ],
)
def test_image_version_formatter_supports_pep440_suffixes(
    package_version: str, image_version: str
) -> None:
    """All canonical PEP 440 suffixes produce Docker-compatible tags."""
    assert (
        local_runtime._format_image_version(Version(package_version)) == image_version
    )


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


async def test_deleting_volumes_removes_orphaned_resources(runtime_paths) -> None:
    """A stop with data deletion clears resources the state no longer tracks."""
    runner = _orphan_volume_runner()

    item = await local_runtime.stop_local_runtime(
        delete_volumes=True, runner=runner, paths=runtime_paths
    )

    assert item["deployment"] == "deleted"
    assert item["data_deleted"] is True
    assert ("volume", "rm", "kitaru-local_postgres_data") in runner.calls


async def test_stop_without_resources_reports_no_deployment(runtime_paths) -> None:
    """A stop finds nothing to delete when no resources carry the label."""
    with pytest.raises(CLIError, match="No CLI-owned local Kitaru deployment"):
        await local_runtime.stop_local_runtime(
            delete_volumes=True, runner=FakeDockerRunner(), paths=runtime_paths
        )
