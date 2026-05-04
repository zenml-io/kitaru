"""DockerMockServices — a tiny FastAPI mock the agent's curl can hit.

Started alongside the proxy and sandbox in stage 4+. Attached to the
`agent_factory` Docker network with a configurable host alias (default
`wiki.local`) so requests from the sandbox to that hostname resolve
to this container via Docker's embedded DNS.
"""

import subprocess
import time
from types import TracebackType

from kitaru.errors import KitaruRuntimeError

_MOCK_IMAGE = "agent-factory-mock"
_NETWORK = "agent_factory"
_CONTAINER_NAME_PREFIX = "agent_factory_mock_"
_STOP_TIMEOUT_SECONDS = 2
_READY_POLL_INTERVAL = 0.1
_READY_TIMEOUT_SECONDS = 10.0


class DockerMockServices:
    """Run the example's mock services container for the duration of a flow."""

    def __init__(
        self,
        *,
        execution_id: str,
        host_aliases: tuple[str, ...] = ("wiki.local",),
    ) -> None:
        self._host_aliases = host_aliases
        self._container_name = f"{_CONTAINER_NAME_PREFIX}{execution_id}"
        self._container_id: str | None = None

    def __enter__(self) -> "DockerMockServices":
        self._ensure_image()
        self._start_container()
        self._wait_until_ready()
        print(
            f"[mock-services] Started container "
            f"{(self._container_id or '')[:12]} "
            f"(image={_MOCK_IMAGE}, "
            f"network aliases={list(self._host_aliases)})"
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

    def _start_container(self) -> None:
        # Container name is per-execution (suffixed with execution_id), so
        # concurrent flow runs don't collide. --rm cleans up our container
        # on stop.
        args = [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            self._container_name,
            "--network",
            _NETWORK,
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
