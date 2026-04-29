"""DockerWorker — sandbox for the agent's `exec` tool.

MVP shape: each `run(command)` shells out to `docker exec` with a fresh
bash. Persistent-shell + marker-completion (kami's pattern) is a follow-up
once we've verified the basic plumbing works.

The worker container is brought up at flow entry and torn down at flow
exit via the context-manager protocol. A named volume (one per execution)
gives the agent a durable `/workspace` that survives pause/resume.
"""

import subprocess
from types import TracebackType

from kitaru.errors import KitaruRuntimeError

from ..tools import ExecResult, _truncate

_WORKER_IMAGE = "agent-factory-worker"
_WORKER_NETWORK = "agent_factory"
_CONTAINER_NAME_PREFIX = "agent_factory_worker_"
_STOP_TIMEOUT_SECONDS = 2


class DockerWorker:
    """Runs the agent's shell commands inside an isolated Docker container."""

    def __init__(self, *, execution_id: str) -> None:
        self._execution_id = execution_id
        self._container_name = f"{_CONTAINER_NAME_PREFIX}{execution_id}"
        self._volume_name = f"agent_factory_workspace_{execution_id}"
        self._container_id: str | None = None

    def __enter__(self) -> "DockerWorker":
        self._ensure_network()
        self._start_container()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._stop_container()

    def run(self, command: str) -> ExecResult:
        """Execute a shell command in the worker container."""
        if self._container_id is None:
            raise KitaruRuntimeError(
                "DockerWorker is not started; use `with` to manage lifecycle"
            )
        completed = subprocess.run(
            ["docker", "exec", self._container_name, "bash", "-c", command],
            capture_output=True,
            text=True,
        )
        return ExecResult(
            exit_code=completed.returncode,
            stdout=_truncate(completed.stdout),
            stderr=_truncate(completed.stderr),
        )

    def _ensure_network(self) -> None:
        # `docker network create` errors loudly if the network exists, so check first.
        result = subprocess.run(
            ["docker", "network", "inspect", _WORKER_NETWORK],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return
        subprocess.run(
            ["docker", "network", "create", _WORKER_NETWORK],
            check=True,
            capture_output=True,
        )

    def _start_container(self) -> None:
        completed = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--rm",  # auto-clean on stop
                "--name",
                self._container_name,
                "--network",
                _WORKER_NETWORK,
                "-v",
                f"{self._volume_name}:/workspace",
                _WORKER_IMAGE,
            ],
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise KitaruRuntimeError(
                f"failed to start worker container: {completed.stderr.strip()}"
            )
        self._container_id = completed.stdout.strip()

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
