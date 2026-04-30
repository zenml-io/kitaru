"""DockerSandbox — the agent's exec sandbox with a persistent shell.

The sandbox container is brought up at flow entry and torn down at flow
exit via the context-manager protocol. Inside the container, **one
long-lived `bash --noprofile --norc` process** handles every `run(command)`
call, so shell state (cwd, env vars, aliases, file descriptors,
background jobs) survives across calls — just like a normal interactive
shell.

This is a Docker port of `kami_agent/sandbox/modal_runtime.py:
ModalSandboxRuntime`. The persistent-shell + marker-completion pattern
is verbatim from kami; only the process-creation step differs (we use
`subprocess.Popen(["docker", "exec", "-i", ...])` instead of
`modal.Sandbox.exec`).

A named volume (one per execution) gives the agent a durable `/workspace`
that survives pause/resume.

Each lifecycle event and exec call prints a `[sandbox]` line to stdout
and (during a kitaru flow) attaches structured metadata via `kitaru.log`
so the dashboard shows the same picture the terminal does.
"""

import base64
import contextlib
import queue
import shlex
import subprocess
import threading
from types import TracebackType
from uuid import uuid4

import kitaru
from kitaru.errors import KitaruContextError, KitaruRuntimeError

from ..tools import ExecResult, _truncate

_SANDBOX_IMAGE = "agent-factory-sandbox"
_SANDBOX_NETWORK = "agent_factory"
_CONTAINER_NAME_PREFIX = "agent_factory_sandbox_"
_STOP_TIMEOUT_SECONDS = 2
_COMMAND_DISPLAY_LIMIT = 80
_SHELL_READ_TIMEOUT_SECONDS = 1.0

# Per-command snapshot of cwd + exported env vars, written by bash to a
# tmp path inside the container. Ephemeral — the durable copy lives in
# `kitaru.memory` and is restored into the container at sandbox start.
_SESSION_SNAPSHOT_PATH = "/tmp/.af_session.sh"


def _summarize_command(command: str) -> str:
    """One-line display form for a shell command in `[sandbox]` log lines."""
    first_line = command.splitlines()[0] if command else ""
    if len(first_line) > _COMMAND_DISPLAY_LIMIT:
        first_line = first_line[: _COMMAND_DISPLAY_LIMIT - 1] + "…"
    if "\n" in command:
        first_line += " ⏎"
    return first_line


class DockerSandbox:
    """Runs the agent's shell commands inside an isolated Docker container.

    Maintains one persistent `bash` process inside the container so shell
    state (cwd, env vars, aliases, etc.) survives across `run()` calls.
    """

    def __init__(
        self,
        *,
        execution_id: str,
        memory_key: str | None = "shell_session",
    ) -> None:
        self._execution_id = execution_id
        self._container_name = f"{_CONTAINER_NAME_PREFIX}{execution_id}"
        self._volume_name = f"agent_factory_workspace_{execution_id}"
        self._container_id: str | None = None
        self._shell_process: subprocess.Popen[bytes] | None = None
        self._shell_lock = threading.Lock()
        self._shell_stdout_queue: queue.Queue[str | None] | None = None
        self._shell_stdout_thread: threading.Thread | None = None
        # Auto-restore + auto-persist shell state across runs via
        # kitaru.memory. Set memory_key=None to disable.
        self._memory_key = memory_key
        self._restored = False

    def __enter__(self) -> "DockerSandbox":
        self._ensure_image()
        self._ensure_network()
        self._start_container()
        # Auto-restore prior shell state (cwd + exported env vars) from
        # kitaru.memory if any prior run of this flow saved one. Failures
        # are swallowed — restoring is best-effort, the sandbox still works
        # without a prior snapshot.
        prior_snapshot = self._load_prior_snapshot()
        if prior_snapshot:
            self._write_snapshot_to_container(prior_snapshot)
            self._restored = True
        short_id = (self._container_id or "")[:12]
        short_exec = self._execution_id[:8]
        restored = " (shell state restored)" if self._restored else ""
        print(
            f"[sandbox] Started container {short_id} "
            f"(image={_SANDBOX_IMAGE}, "
            f"/workspace ← workspace_{short_exec}){restored}"
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Auto-persist shell state to kitaru.memory before teardown so the
        # next run of this flow can restore it. Failures swallowed —
        # persistence is best-effort.
        captured = self._capture_and_persist_snapshot()
        if self._container_id is not None:
            short_id = self._container_id[:12]
            persisted = " (shell state persisted)" if captured else ""
            print(f"[sandbox] Stopping container {short_id}{persisted}")
        self._terminate_shell()
        self._stop_container()

    def run(self, command: str) -> ExecResult:
        """Execute a shell command in the sandbox container's persistent bash.

        Shell state (cwd, env vars, aliases) survives across calls because
        every `run()` writes into the same long-lived bash process's stdin.
        """
        if self._container_id is None:
            raise KitaruRuntimeError(
                "DockerSandbox is not started; use `with` to manage lifecycle"
            )
        print(f"[sandbox] $ {_summarize_command(command)}")

        # Encode the command in base64 so we can send arbitrary bytes
        # (including newlines, quotes, control chars) through stdin without
        # having to escape them. Then bash decodes and `eval`s the original.
        command_b64 = base64.b64encode(command.encode("utf-8")).decode("ascii")
        marker = f"__AGENT_FACTORY_DONE_{uuid4().hex}"
        # After the user's command runs, snapshot cwd + exported env vars to
        # /workspace (durable volume) so a fresh bash on a later flow run
        # can restore them — turn N's `cd /foo` still applies after a
        # crash + retry, even though the container itself was recreated.
        payload = (
            f'eval "$(printf %s {shlex.quote(command_b64)} | base64 -d)"\n'
            "__af_exit=$?\n"
            '__af_cwd="$(pwd)"\n'
            "{ "
            'printf "cd %q\\n" "$__af_cwd"; '
            "declare -px; "
            f"}} > {_SESSION_SNAPSHOT_PATH} 2>/dev/null || true\n"
            f'printf "{marker} %s %s\\n" "$__af_exit" "$__af_cwd"\n'
        )

        with self._shell_lock:
            # Two attempts: if the shell died unexpectedly, restart once.
            for attempt in range(2):
                self._ensure_shell_process()
                self._write_stdin(payload)
                try:
                    output, exit_code, cwd = self._read_stdout_until_marker(marker)
                    break
                except RuntimeError:
                    shell_exited = (
                        self._shell_process is None
                        or self._shell_process.poll() is not None
                    )
                    if not shell_exited or attempt == 1:
                        raise
                    self._terminate_shell()
            else:  # pragma: no cover — loop always breaks or raises
                raise KitaruRuntimeError("persistent shell command failed")

        truncated = _truncate(output)
        # The shell merges stderr into stdout (`exec 2>&1` at shell start),
        # so `stderr` is always empty; keeping the field for ExecResult
        # shape compatibility with stage 1's in-process variant.
        result = ExecResult(exit_code=exit_code, stdout=truncated, stderr="")
        print(
            f"[sandbox]   → exit={result.exit_code}, "
            f"stdout={len(result.stdout)} chars, "
            f"cwd={cwd}"
        )
        # Best-effort metadata for the surrounding kitaru checkpoint —
        # silently no-op when the sandbox is used outside a flow (e.g.
        # in tests / interactive scripts).
        with contextlib.suppress(KitaruContextError):
            kitaru.log(
                sandbox_container=self._container_name,
                command_length=len(command),
                exit_code=result.exit_code,
                stdout_chars=len(result.stdout),
                cwd=cwd,
            )
        return result

    # --- Image / network / container lifecycle -------------------------------

    def _ensure_image(self) -> None:
        result = subprocess.run(
            ["docker", "image", "inspect", _SANDBOX_IMAGE],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return
        raise KitaruRuntimeError(
            f"Sandbox image {_SANDBOX_IMAGE!r} is not built locally. "
            "Build it once with:\n\n"
            f"    docker build -t {_SANDBOX_IMAGE} "
            "-f docker/sandbox.Dockerfile docker/\n\n"
            "Then re-run this stage."
        )

    def _ensure_network(self) -> None:
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
                "--rm",
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

    # --- Shell-state persistence via kitaru.memory --------------------------

    def _load_prior_snapshot(self) -> str | None:
        """Load the prior run's shell snapshot from kitaru.memory.

        Best-effort: returns None on any error (no prior snapshot, outside
        a flow scope, kitaru store unreachable, materialization failure).
        """
        if self._memory_key is None:
            return None
        with contextlib.suppress(Exception):
            artifact = kitaru.memory.get(self._memory_key)
            if artifact is None:
                return None
            # Inside a flow, kitaru.memory.get returns an artifact handle
            # we have to materialize; outside a flow it returns the value
            # directly.
            value = artifact.load() if hasattr(artifact, "load") else artifact
            if isinstance(value, str) and value.strip():
                return value
        return None

    def _capture_and_persist_snapshot(self) -> bool:
        """Capture the current shell snapshot and write it to kitaru.memory.

        Returns True if a snapshot was captured AND persisted successfully.
        """
        if self._memory_key is None or self._container_id is None:
            return False
        snapshot = self._read_snapshot_from_container()
        if not snapshot:
            return False
        with contextlib.suppress(Exception):
            kitaru.memory.set(self._memory_key, snapshot)
            return True
        return False

    def _read_snapshot_from_container(self) -> str | None:
        """Read the shell snapshot file from inside the container."""
        if self._container_id is None:
            return None
        result = subprocess.run(
            ["docker", "exec", self._container_name, "cat", _SESSION_SNAPSHOT_PATH],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None
        return result.stdout

    def _write_snapshot_to_container(self, snapshot: str) -> None:
        """Write a shell snapshot into the container so bash can source it."""
        if self._container_id is None:
            return
        subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                self._container_name,
                "tee",
                _SESSION_SNAPSHOT_PATH,
            ],
            input=snapshot.encode("utf-8"),
            capture_output=True,
        )

    # --- Persistent shell ----------------------------------------------------

    def _ensure_shell_process(self) -> subprocess.Popen[bytes]:
        if self._shell_process is not None and self._shell_process.poll() is None:
            return self._shell_process
        if self._container_id is None:
            raise KitaruRuntimeError("cannot start shell: container is not running")
        self._shell_process = subprocess.Popen(
            [
                "docker",
                "exec",
                "-i",
                self._container_name,
                "bash",
                "--noprofile",
                "--norc",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
        )
        self._start_shell_stdout_reader()
        # Merge stderr into stdout once at shell start so marker parsing
        # only has one stream to worry about.
        self._write_stdin("exec 2>&1\n")
        # Restore cwd + exported env vars from the prior run, if any.
        # The snapshot lives in /workspace (durable across runs) so a
        # crashed-and-resumed flow picks up where it left off.
        self._write_stdin(
            f"[ -f {_SESSION_SNAPSHOT_PATH} ] && "
            f"source {_SESSION_SNAPSHOT_PATH} 2>/dev/null || true\n"
        )
        return self._shell_process

    def _start_shell_stdout_reader(self) -> None:
        assert self._shell_process is not None
        self._shell_stdout_queue = queue.Queue()
        stdout_queue = self._shell_stdout_queue
        process = self._shell_process

        def _reader() -> None:
            stdout_stream = process.stdout
            if stdout_stream is None:
                stdout_queue.put(None)
                return
            try:
                while True:
                    chunk = stdout_stream.readline()
                    if not chunk:
                        break
                    stdout_queue.put(chunk.decode("utf-8", errors="replace"))
            except Exception:
                pass
            finally:
                stdout_queue.put(None)

        self._shell_stdout_thread = threading.Thread(
            target=_reader,
            name=f"docker-sandbox-stdout-{self._execution_id[:8]}",
            daemon=True,
        )
        self._shell_stdout_thread.start()

    def _write_stdin(self, payload: str) -> None:
        assert self._shell_process is not None
        stdin = self._shell_process.stdin
        if stdin is None:
            raise KitaruRuntimeError("persistent shell has no stdin")
        stdin.write(payload.encode("utf-8"))
        stdin.flush()

    def _read_stdout_until_marker(self, marker: str) -> tuple[str, int, str]:
        assert self._shell_process is not None
        if self._shell_stdout_queue is None:
            raise KitaruRuntimeError("shell stdout reader not initialized")
        stdout_queue = self._shell_stdout_queue

        marker_prefix = f"{marker} "
        collected: list[str] = []
        buffer = ""

        def _try_parse_marker(line: str) -> tuple[int, str] | None:
            normalized = line.rstrip("\r")
            if not normalized.startswith(marker_prefix):
                return None
            parts = normalized.split(" ", 2)
            if len(parts) != 3:
                raise RuntimeError("malformed shell marker payload")
            return int(parts[1]), parts[2]

        while True:
            try:
                chunk = stdout_queue.get(timeout=_SHELL_READ_TIMEOUT_SECONDS)
            except queue.Empty:
                if self._shell_process.poll() is not None:
                    raise RuntimeError(
                        "persistent shell terminated before command completed"
                    ) from None
                continue
            if chunk is None:
                break
            buffer += chunk
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                marker_payload = _try_parse_marker(line)
                if marker_payload is not None:
                    exit_code, cwd = marker_payload
                    return "".join(collected), exit_code, cwd
                collected.append(f"{line}\n")

        raise RuntimeError("persistent shell terminated before marker")

    def _terminate_shell(self) -> None:
        if self._shell_process is None:
            return
        try:
            if self._shell_process.poll() is None:
                self._shell_process.terminate()
                try:
                    self._shell_process.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    self._shell_process.kill()
        finally:
            self._shell_process = None
            self._shell_stdout_queue = None
            self._shell_stdout_thread = None
