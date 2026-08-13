#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Offline importer/evaluator scaffolding and bounded local validation."""

import contextlib
import json
import os
import re
import signal
import stat
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any

from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.registration import parse_json_object, validate_script_source

_OUTPUT_LIMIT = 32 * 1024
_NAME_SANITIZER = re.compile(r"[^A-Za-z0-9_]+")

_IMPORTER_TEMPLATE = '''"""Kitaru importer scaffold."""

from collections.abc import Iterator
from typing import Any

from kitaru.task.importer import ImportFailure, ImportedNode, ImportedSession, Parser


def parse(
    payload: bytes, params: dict[str, Any]
) -> Iterator[ImportedSession | ImportFailure]:
    """Parse a provider payload into Kitaru sessions or failures."""
    del payload, params
    return iter(())


parser: Parser = parse
'''

_EVALUATOR_TEMPLATE = '''"""Kitaru evaluator scaffold."""

from typing import Any

from kitaru.task.evaluator import EvaluationResult, SessionView


def evaluate(
    session: SessionView, **params: Any
) -> EvaluationResult | list[EvaluationResult]:
    """Evaluate one Kitaru session."""
    del session, params
    return EvaluationResult(name="example", passed=True)
'''


def scaffold_asset(
    kind: str,
    name: str,
    *,
    path: Path | None,
    force: bool,
) -> CommandResult:
    """Write one minimal plugin script without touching any other path."""
    slug = _NAME_SANITIZER.sub("_", name).strip("_").lower()
    if not slug:
        raise CLIError(
            "invalid_arguments", "Scaffold NAME must contain a letter or digit."
        )
    target = path or Path(f"{slug}_{kind}.py")
    if target.suffix != ".py":
        raise CLIError("invalid_arguments", "Scaffold target must be a .py file.")
    if not target.parent.exists() or not target.parent.is_dir():
        raise CLIError(
            "invalid_arguments",
            f"Parent directory {str(target.parent)!r} does not exist.",
        )
    try:
        target_mode = target.lstat().st_mode
    except FileNotFoundError:
        existed = False
        target_mode = None
    except OSError as error:
        raise CLIError(
            "invalid_arguments",
            f"Cannot inspect scaffold target {str(target)!r}: {error}",
        ) from error
    else:
        existed = True
    if target_mode is not None and not stat.S_ISREG(target_mode):
        raise CLIError(
            "invalid_arguments",
            f"Scaffold target {str(target)!r} is not a regular file.",
        )
    if existed and not force:
        raise CLIError(
            "conflict",
            f"Scaffold target {str(target)!r} already exists.",
            hint="Pass --force to overwrite this exact file.",
        )
    template = _IMPORTER_TEMPLATE if kind == "importer" else _EVALUATOR_TEMPLATE
    _atomic_write(target, template)
    return CommandResult(
        item={
            "asset_type": kind,
            "name": name,
            "path": str(target),
            "entrypoint": "parse" if kind == "importer" else "evaluate",
            "overwritten": existed,
        }
    )


def test_importer(
    path: Path,
    *,
    entrypoint: str,
    payload: Path | None,
    params: str | None,
    timeout: float,
) -> CommandResult:
    """Load an importer and optionally validate its yielded items in a child process."""
    validate_script_source(path, entrypoint)
    parsed_params = parse_json_object(params, option="--params")
    if payload is not None and (not payload.exists() or not payload.is_file()):
        raise CLIError(
            "invalid_arguments", f"Payload {str(payload)!r} is not a regular file."
        )
    arguments = [
        "importer",
        str(path),
        entrypoint,
        "--params",
        json.dumps(parsed_params, separators=(",", ":")),
    ]
    if payload is not None:
        arguments.extend(["--payload", str(payload)])
    result = _run_plugin_test(arguments, timeout=timeout)
    item = {
        "asset_type": "importer",
        "path": str(path),
        "entrypoint": entrypoint,
        **result,
    }
    return CommandResult(
        item=item,
        warnings=[
            "This command executed local code in a child process; it is not a sandbox."
        ],
    )


def test_evaluator(
    path: Path,
    *,
    entrypoint: str,
    timeout: float,
) -> CommandResult:
    """Load an evaluator and validate its callable signature in a child process."""
    validate_script_source(path, entrypoint)
    result = _run_plugin_test(["evaluator", str(path), entrypoint], timeout=timeout)
    return CommandResult(
        item={
            "asset_type": "evaluator",
            "path": str(path),
            "entrypoint": entrypoint,
            **result,
        },
        warnings=[
            "This command executed local code in a child process; it is not a sandbox."
        ],
    )


def _run_plugin_test(arguments: list[str], *, timeout: float) -> dict[str, Any]:
    """Run the internal plugin validator with bounded captured output."""
    if timeout <= 0:
        raise CLIError("invalid_arguments", "--timeout must be positive.")
    with tempfile.TemporaryDirectory(prefix="kitaru-plugin-test-") as temporary:
        result_path = Path(temporary) / "result.json"
        command = [
            sys.executable,
            "-m",
            "kitaru.cli.plugin_test_runner",
            *arguments,
            "--result",
            str(result_path),
        ]
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=os.name == "posix",
        )
        stdout_tail = _TailBuffer()
        stderr_tail = _TailBuffer()
        threads = [
            threading.Thread(
                target=_drain_pipe,
                args=(process.stdout, stdout_tail),
                daemon=True,
            ),
            threading.Thread(
                target=_drain_pipe,
                args=(process.stderr, stderr_tail),
                daemon=True,
            ),
        ]
        for thread in threads:
            thread.start()
        timed_out = False
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
        finally:
            _kill_plugin_process(process)
            if process.poll() is None:
                process.wait()
            for thread in threads:
                thread.join()

        result_text = (
            result_path.read_text(encoding="utf-8") if result_path.exists() else None
        )

    stdout = stdout_tail.decode()
    stderr = stderr_tail.decode()
    if timed_out:
        raise CLIError(
            "timeout",
            f"Local plugin test exceeded {timeout:g} seconds.",
            details={"stdout": stdout, "stderr": stderr},
        )
    if process.returncode != 0:
        raise CLIError(
            "invalid_arguments",
            "Local plugin test failed.",
            details={
                "returncode": process.returncode,
                "stdout": stdout,
                "stderr": stderr,
            },
        )
    try:
        if result_text is None:
            raise ValueError("result file is missing")
        payload = json.loads(result_text)
    except (json.JSONDecodeError, ValueError) as error:
        raise CLIError(
            "internal_error",
            "Local plugin test returned an invalid result.",
            details={"stdout": stdout, "stderr": stderr},
        ) from error
    payload["stdout"] = _merge_output(payload.get("stdout", ""), stdout)
    payload["stderr"] = _merge_output(payload.get("stderr", ""), stderr)
    return payload


class _TailBuffer:
    """Bounded byte buffer retaining the most recent child output."""

    def __init__(self) -> None:
        self._data = bytearray()

    def write(self, chunk: bytes) -> None:
        """Append a chunk while discarding the oldest excess bytes."""
        self._data.extend(chunk)
        overflow = len(self._data) - _OUTPUT_LIMIT
        if overflow > 0:
            del self._data[:overflow]

    def decode(self) -> str:
        """Decode retained bytes with replacement for invalid UTF-8."""
        return self._data.decode("utf-8", errors="replace")


def _drain_pipe(stream: Any, tail: _TailBuffer) -> None:
    """Drain one child pipe into a bounded tail buffer."""
    if stream is None:
        return
    with stream:
        while chunk := stream.read(65536):
            tail.write(chunk)


def _kill_plugin_process(process: subprocess.Popen[bytes]) -> None:
    """Kill the owned validator process group where the platform supports it."""
    if os.name == "posix":
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
    elif process.poll() is None:
        process.kill()


def _merge_output(first: str, second: str) -> str:
    """Combine two output tails while retaining the stable byte bound."""
    separator = "\n" if first and second else ""
    encoded = f"{first}{separator}{second}".encode("utf-8", errors="replace")
    return encoded[-_OUTPUT_LIMIT:].decode("utf-8", errors="replace")


def _atomic_write(path: Path, content: str) -> None:
    """Replace one scaffold target atomically."""
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as file:
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary_path, path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise
