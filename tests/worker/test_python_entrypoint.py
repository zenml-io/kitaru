#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Zero-argument Python agent entrypoint runner behavior."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from kitaru.worker.python_entrypoint import run_reference


def test_runner_invokes_sync_and_async_zero_argument_wrappers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both sync returns and awaitables complete without defining task semantics."""
    output = tmp_path / "output.txt"
    module = tmp_path / "runner_fixture.py"
    module.write_text(
        "from pathlib import Path\n"
        f"OUTPUT = Path({str(output)!r})\n"
        "def sync_wrapper():\n"
        "    OUTPUT.write_text('sync')\n"
        "async def async_wrapper():\n"
        "    OUTPUT.write_text('async')\n"
        "def returns_awaitable():\n"
        "    return async_wrapper()\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    run_reference("runner_fixture:sync_wrapper")
    assert output.read_text(encoding="utf-8") == "sync"

    run_reference("runner_fixture:async_wrapper")
    assert output.read_text(encoding="utf-8") == "async"

    output.unlink()
    run_reference("runner_fixture:returns_awaitable")
    assert output.read_text(encoding="utf-8") == "async"


def test_runner_wrapper_retrieves_task_inputs_itself(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The runner passes no arguments; wrappers opt into get_task_inputs()."""
    output = tmp_path / "inputs.json"
    module = tmp_path / "task_wrapper.py"
    module.write_text(
        "import json\n"
        "from pathlib import Path\n"
        "from kitaru.task import get_task_inputs\n"
        "def run():\n"
        f"    Path({str(output)!r}).write_text("
        "json.dumps(get_task_inputs(), sort_keys=True))\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("KITARU_TASK_INPUTS", '{"prompt":"hello"}')
    monkeypatch.setenv("KITARU_TASK_ID", "task")

    run_reference("task_wrapper:run")

    assert json.loads(output.read_text(encoding="utf-8")) == {"prompt": "hello"}


def test_runner_failures_produce_nonzero_process_exit(tmp_path: Path) -> None:
    """Wrapper exceptions escape for the worker process to observe."""
    module = tmp_path / "failing_wrapper.py"
    module.write_text(
        "def run():\n    raise RuntimeError('wrapper failed')\n", encoding="utf-8"
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(tmp_path), str(Path(__file__).parents[2] / "src")]
    )

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "kitaru.worker.python_entrypoint",
            "failing_wrapper:run",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert completed.returncode != 0
    assert "wrapper failed" in completed.stderr


def test_runner_module_does_not_import_worker_extra() -> None:
    """Loading the stdlib runner does not traverse optional worker exports."""
    code = """
import builtins
real_import = builtins.__import__
def blocked(name, *args, **kwargs):
    if name == 'pydantic_settings':
        raise AssertionError('worker extra imported')
    return real_import(name, *args, **kwargs)
builtins.__import__ = blocked
import kitaru.worker.python_entrypoint
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_runner_rejects_malformed_and_noncallable_references(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """References must resolve to one top-level callable."""
    module = tmp_path / "values.py"
    module.write_text("value = 1\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(ValueError, match="MODULE:ATTRIBUTE"):
        run_reference("values")
    with pytest.raises(ValueError, match="not callable"):
        run_reference("values:value")
