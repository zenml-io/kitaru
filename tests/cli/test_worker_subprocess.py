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
"""Minimal real-signal coverage for the foreground worker lifecycle."""

import json
import os
import select
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = textwrap.dedent(
    """
    import asyncio
    import sys

    from kitaru.cli.output import (
        OutputContext,
        emit_event,
        emit_result,
        reset_output_context,
        set_output_context,
    )
    from kitaru.cli.workers import ForegroundWorkerProcess


    class Worker:
        async def run(self, stop=None):
            assert stop is not None
            emit_event("ready")
            await stop.wait()
            if sys.argv[1] == "emergency":
                await asyncio.Event().wait()


    async def main():
        token = set_output_context(
            OutputContext(
                command="worker.start",
                mode="jsonl",
                machine=True,
                non_interactive=True,
                debug=False,
                traceback=False,
                stdout=sys.stdout,
                stderr=sys.stderr,
                rich=False,
            )
        )
        try:
            result = await ForegroundWorkerProcess(
                Worker(), {"name": "subprocess"}
            ).run()
            emit_result(result)
            return result.exit_code
        finally:
            reset_output_context(token)


    raise SystemExit(asyncio.run(main()))
    """
)

pytestmark = pytest.mark.skipif(
    os.name != "posix", reason="Real POSIX signal delivery is required"
)


def _start_worker(mode: str) -> subprocess.Popen[bytes]:
    """Start the isolated foreground-worker fixture."""
    return subprocess.Popen(
        [sys.executable, "-c", _SCRIPT, mode],
        cwd=Path(__file__).parents[2],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )


def _read_event(process: subprocess.Popen[bytes], timeout: float = 5) -> dict[str, Any]:
    """Read one flushed JSONL event without waiting indefinitely."""
    assert process.stdout is not None
    deadline = time.monotonic() + timeout
    line = bytearray()
    while not line.endswith(b"\n"):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError("Timed out waiting for worker event")
        readable, _, _ = select.select([process.stdout], [], [], remaining)
        if not readable:
            raise AssertionError("Timed out waiting for worker event")
        chunk = os.read(process.stdout.fileno(), 1)
        if not chunk:
            raise AssertionError(
                f"Worker exited before event with code {process.poll()}"
            )
        line.extend(chunk)
    return json.loads(line)


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    """Stop a fixture process left alive after a failed assertion."""
    if process.poll() is None:
        process.kill()
        process.wait(timeout=5)


@pytest.mark.parametrize(
    ("termination_signal", "reason", "exit_code"),
    [
        (signal.SIGINT, "sigint", 130),
        pytest.param(
            signal.SIGTERM,
            "sigterm",
            143,
            marks=pytest.mark.skipif(
                not hasattr(signal, "SIGTERM"), reason="SIGTERM is not available"
            ),
        ),
    ],
)
def test_graceful_signal_exit_and_terminal_result(
    termination_signal: signal.Signals, reason: str, exit_code: int
) -> None:
    """Real graceful signals preserve the terminal result and exit semantics."""
    process = _start_worker("graceful")
    try:
        assert _read_event(process)["event"] == "starting"
        assert _read_event(process)["event"] == "ready"
        process.send_signal(termination_signal)
        draining = _read_event(process)
        assert draining["event"] == "draining"
        assert draining["item"] == {"reason": reason}
        stdout, stderr = process.communicate(timeout=5)
        stdout = stdout.decode()
        stderr = stderr.decode()
    finally:
        _stop_process(process)

    assert process.returncode == exit_code
    assert stderr == ""
    terminal = json.loads(stdout)
    assert terminal["event"] == "stopped"
    assert terminal["item"]["stop_reason"] == reason
    assert terminal["item"]["server_record"] == "retained_until_stale"


def test_emergency_sigint_exits_after_flushed_prefix() -> None:
    """A second real SIGINT exits 130 without a terminal document."""
    process = _start_worker("emergency")
    try:
        assert _read_event(process)["event"] == "starting"
        assert _read_event(process)["event"] == "ready"
        process.send_signal(signal.SIGINT)
        draining = _read_event(process)
        assert draining["event"] == "draining"
        assert draining["item"] == {"reason": "sigint"}
        process.send_signal(signal.SIGINT)
        deadline = time.monotonic() + 5
        while process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        stdout, stderr = process.communicate(timeout=1)
        stdout = stdout.decode()
        stderr = stderr.decode()
    finally:
        _stop_process(process)

    assert process.returncode == 130
    assert stdout == ""
    assert stderr == ""
