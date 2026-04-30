"""DockerSandbox — the agent's exec sandbox.

MVP shape: each `run(command)` shells out to `docker exec` with a fresh
bash. Persistent-shell + marker-completion (kami's pattern) is a follow-up
once we've verified the basic plumbing works.

The sandbox container is brought up at flow entry and torn down at flow
exit via the context-manager protocol. A named volume (one per execution)
gives the agent a durable `/workspace` that survives pause/resume.

Each lifecycle event and exec call prints a `[sandbox]` line to stdout
and (during a kitaru flow) attaches structured metadata via `kitaru.log`
so the dashboard shows the same picture the terminal does.
"""

import subprocess
from types import TracebackType

import kitaru
from kitaru.errors import KitaruRuntimeError

from ..tools import ExecResult, _truncate

_SANDBOX_IMAGE = "agent-factory-sandbox"
_SANDBOX_NETWORK = "agent_factory"
_CONTAINER_NAME_PREFIX = "agent_factory_sandbox_"
_STOP_TIMEOUT_SECONDS = 2
_COMMAND_DISPLAY_LIMIT = 80


def _summarize_command(command: str) -> str:
    """One-line display form for a shell command in `[sandbox]` log lines."""
    first_line = command.splitlines()[0] if command else ""
    if len(first_line) > _COMMAND_DISPLAY_LIMIT:
        first_line = first_line[: _COMMAND_DISPLAY_LIMIT - 1] + "…"
    if "\n" in command:
        first_line += " ⏎"
    return first_line


class DockerSandbox:
    """Runs the agent's shell commands inside an isolated Docker container."""

    def __init__(self, *, execution_id: str) -> None:
        self._execution_id = execution_id
        self._container_name = f"{_CONTAINER_NAME_PREFIX}{execution_id}"
        self._volume_name = f"agent_factory_workspace_{execution_id}"
        self._container_id: str | None = None

    def __enter__(self) -> "DockerSandbox":
        self._ensure_network()
        self._start_container()
        short_id = (self._container_id or "")[:12]
        short_exec = self._execution_id[:8]
        print(
            f"[sandbox] Started container {short_id} "
            f"(image={_SANDBOX_IMAGE}, "
            f"/workspace ← workspace_{short_exec})"
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._container_id is not None:
            short_id = self._container_id[:12]
            print(
                f"[sandbox] Stopping container {short_id} "
                f"(workspace volume preserved for pause/resume durability)"
            )
        self._stop_container()

    def run(self, command: str) -> ExecResult:
        """Execute a shell command in the sandbox container."""
        if self._container_id is None:
            raise KitaruRuntimeError(
                "DockerSandbox is not started; use `with` to manage lifecycle"
            )
        print(f"[sandbox] $ {_summarize_command(command)}")
        completed = subprocess.run(
            ["docker", "exec", self._container_name, "bash", "-c", command],
            capture_output=True,
            text=True,
        )
        result = ExecResult(
            exit_code=completed.returncode,
            stdout=_truncate(completed.stdout),
            stderr=_truncate(completed.stderr),
        )
        print(
            f"[sandbox]   → exit={result.exit_code}, "
            f"stdout={len(result.stdout)} chars, "
            f"stderr={len(result.stderr)} chars"
        )
        # Attach per-exec metadata to the surrounding kitaru checkpoint so
        # the same picture shows up in the dashboard. No-op outside a flow.
        kitaru.log(
            sandbox_container=self._container_name,
            command_length=len(command),
            exit_code=result.exit_code,
            stdout_chars=len(result.stdout),
            stderr_chars=len(result.stderr),
        )
        return result

    def _ensure_network(self) -> None:
        # `docker network create` errors loudly if the network exists, so check first.
        result = subprocess.run(
            ["docker", "network", "inspect", _SANDBOX_NETWORK],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return
        subprocess.run(
            ["docker", "network", "create", _SANDBOX_NETWORK],
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
                _SANDBOX_NETWORK,
                "-v",
                f"{self._volume_name}:/workspace",
                _SANDBOX_IMAGE,
            ],
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise KitaruRuntimeError(
                f"failed to start sandbox container: {completed.stderr.strip()}"
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
