"""DockerMockServices — a tiny FastAPI mock the agent's curl can hit.

Started alongside the proxy and sandbox in stage 4+. Attached to the
`agent_factory` Docker network with configurable host aliases (default
`wiki.local`, `webhook.local`) so requests from the sandbox to those
hostnames resolve to this container via Docker's embedded DNS.

Stage 5 onward also publishes the mock on a host-bound port for
`exec_service` host-side handlers — Docker network aliases only resolve
from inside the `agent_factory` network, so a host-process call needs
`http://localhost:<port>`. The runner asks Docker for an unused port
(`-p 0:80`) and exposes it via `host_base_url`; the runner also exports
`AGENT_FACTORY_MOCK_BASE_URL` in the host process environment so service
handlers can resolve it without taking the runner instance as a
dependency. Concurrent flow runs no longer collide on the host port.
"""

import os
import subprocess
import time
from types import TracebackType

from kitaru.errors import KitaruRuntimeError

_MOCK_IMAGE = "agent-factory-mock"
_NETWORK = "agent_factory"
_CONTAINER_NAME_PREFIX = "agent_factory_mock_"
_HOST_BASE_URL_ENV = "AGENT_FACTORY_MOCK_BASE_URL"
_STOP_TIMEOUT_SECONDS = 2
_READY_POLL_INTERVAL = 0.1
_READY_TIMEOUT_SECONDS = 10.0


class DockerMockServices:
    """Run the example's mock services container for the duration of a flow."""

    def __init__(
        self,
        *,
        execution_id: str,
        host_aliases: tuple[str, ...] = ("wiki.local", "webhook.local"),
    ) -> None:
        self._host_aliases = host_aliases
        self._container_name = f"{_CONTAINER_NAME_PREFIX}{execution_id}"
        self._container_id: str | None = None
        self._host_port: int | None = None
        self._previous_base_url_env: str | None = None

    @property
    def host_base_url(self) -> str:
        """`http://localhost:<host_port>` for host-side reachability."""
        if self._host_port is None:
            raise KitaruRuntimeError(
                "DockerMockServices is not started; use `with` to manage lifecycle"
            )
        return f"http://localhost:{self._host_port}"

    def __enter__(self) -> "DockerMockServices":
        self._ensure_image()
        self._ensure_network()
        self._start_container()
        self._read_published_port()
        self._wait_until_ready()
        # Export the base URL into the host env so `exec_service` handlers
        # can resolve it without depending on this instance directly.
        self._previous_base_url_env = os.environ.get(_HOST_BASE_URL_ENV)
        os.environ[_HOST_BASE_URL_ENV] = self.host_base_url
        print(
            f"[mock-services] Started container "
            f"{(self._container_id or '')[:12]} "
            f"(image={_MOCK_IMAGE}, "
            f"network aliases={list(self._host_aliases)}, "
            f"host={self.host_base_url})"
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._container_id is not None:
            print(
                f"[mock-services] Stopping container {self._container_id[:12]}"
            )
        self._stop_container()
        # Restore the base-URL env to whatever it was before we set it.
        if self._previous_base_url_env is None:
            os.environ.pop(_HOST_BASE_URL_ENV, None)
        else:
            os.environ[_HOST_BASE_URL_ENV] = self._previous_base_url_env

    def _ensure_image(self) -> None:
        result = subprocess.run(
            ["docker", "image", "inspect", _MOCK_IMAGE],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return
        raise KitaruRuntimeError(
            f"Mock image {_MOCK_IMAGE!r} is not built locally. "
            "Build it once (run from the example root) with:\n\n"
            f"    docker build -t {_MOCK_IMAGE} "
            "-f docker/mock.Dockerfile .\n\n"
            "Then re-run this stage."
        )

    def _ensure_network(self) -> None:
        # The proxy + sandbox runners also call ensure_network helpers;
        # but stages 5/6 enter the mock-services context manager FIRST in
        # the with-stack, so this needs to run here too — otherwise a
        # fresh host (no `agent_factory` network yet) fails at mock start.
        result = subprocess.run(
            ["docker", "network", "inspect", _NETWORK],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return
        # Race-safe create: don't fail on the loser of a concurrent race;
        # re-inspect to confirm the network is now up either way.
        subprocess.run(
            ["docker", "network", "create", _NETWORK],
            capture_output=True,
        )
        confirm = subprocess.run(
            ["docker", "network", "inspect", _NETWORK],
            capture_output=True,
            text=True,
        )
        if confirm.returncode != 0:
            raise KitaruRuntimeError(
                f"failed to create or find docker network {_NETWORK!r}"
            )

    def _start_container(self) -> None:
        # Container name is per-execution (suffixed with execution_id) and
        # the host port is `-p 0:80` (Docker assigns a free port), so
        # concurrent flow runs don't collide on either. --rm cleans up
        # our container on stop.
        args = [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            self._container_name,
            "--network",
            _NETWORK,
            "-p",
            "0:80",
        ]
        for alias in self._host_aliases:
            args.extend(["--network-alias", alias])
        args.append(_MOCK_IMAGE)
        completed = subprocess.run(args, capture_output=True, text=True)
        if completed.returncode != 0:
            raise KitaruRuntimeError(
                f"failed to start mock-services: {completed.stderr.strip()}"
            )
        self._container_id = completed.stdout.strip()

    def _read_published_port(self) -> None:
        """Ask Docker which host port it bound to the container's :80."""
        completed = subprocess.run(
            ["docker", "port", self._container_name, "80/tcp"],
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise KitaruRuntimeError(
                f"failed to read published port: {completed.stderr.strip()}"
            )
        # `docker port` output: "0.0.0.0:54321\n[::]:54321\n" or similar.
        first_line = completed.stdout.strip().splitlines()[0]
        try:
            self._host_port = int(first_line.rsplit(":", 1)[1])
        except (IndexError, ValueError) as exc:
            raise KitaruRuntimeError(
                f"could not parse `docker port` output: {first_line!r}"
            ) from exc

    def _wait_until_ready(self) -> None:
        deadline = time.monotonic() + _READY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    self._container_name,
                    "python",
                    "-c",
                    "import urllib.request,sys;"
                    "sys.exit(0 if urllib.request.urlopen('http://127.0.0.1/healthz', "
                    "timeout=1).status == 200 else 1)",
                ],
                capture_output=True,
            )
            if result.returncode == 0:
                return
            time.sleep(_READY_POLL_INTERVAL)
        raise KitaruRuntimeError(
            f"mock-services did not become ready within "
            f"{_READY_TIMEOUT_SECONDS}s"
        )

    def _stop_container(self) -> None:
        if self._container_id is None:
            return
        subprocess.run(
            [
                "docker",
                "stop",
                "--time",
                str(_STOP_TIMEOUT_SECONDS),
                self._container_name,
            ],
            capture_output=True,
            text=True,
        )
        self._container_id = None
