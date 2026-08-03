#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Importer/evaluator scaffold and bounded local-test behavior."""

import os
import time
from pathlib import Path

import pytest

from kitaru.cli import scaffold as scaffold_module
from kitaru.cli.output import CLIError
from kitaru.cli.scaffold import (
    scaffold_asset,
)
from kitaru.cli.scaffold import (
    test_evaluator as run_evaluator_test,
)
from kitaru.cli.scaffold import (
    test_importer as run_importer_test,
)


def test_scaffold_refuses_overwrite_and_force_changes_only_target(
    tmp_path: Path,
) -> None:
    """Scaffolding owns one exact file and requires force to replace it."""
    target = tmp_path / "provider.py"
    sibling = tmp_path / "keep.txt"
    sibling.write_text("keep", encoding="utf-8")

    result = scaffold_asset("importer", "Provider", path=target, force=False)
    assert result.item["entrypoint"] == "parse"
    assert "Parser" in target.read_text(encoding="utf-8")

    target.write_text("changed", encoding="utf-8")
    with pytest.raises(CLIError) as error:
        scaffold_asset("importer", "Provider", path=target, force=False)
    assert error.value.kind == "conflict"

    result = scaffold_asset("importer", "Provider", path=target, force=True)
    assert result.item["overwritten"] is True
    assert sibling.read_text(encoding="utf-8") == "keep"
    assert "def parse" in target.read_text(encoding="utf-8")


def test_scaffold_rejects_symlink_targets(tmp_path: Path) -> None:
    """Neither normal nor forced scaffolding replaces a symlink entry."""
    original = tmp_path / "original.py"
    original.write_text("keep", encoding="utf-8")
    link = tmp_path / "linked.py"
    link.symlink_to(original)

    for force in (False, True):
        with pytest.raises(CLIError, match="not a regular file"):
            scaffold_asset("importer", "Provider", path=link, force=force)
    assert link.is_symlink()
    assert original.read_text(encoding="utf-8") == "keep"

    dangling = tmp_path / "dangling.py"
    dangling.symlink_to(tmp_path / "missing.py")
    with pytest.raises(CLIError, match="not a regular file"):
        scaffold_asset("importer", "Provider", path=dangling, force=False)
    assert dangling.is_symlink()


def test_importer_test_loads_signature_and_validates_payload_items(
    tmp_path: Path,
) -> None:
    """Payload tests consume the parser through the current call_parser contract."""
    script = tmp_path / "parser.py"
    script.write_text(
        "from kitaru.task.importer import ImportFailure\n"
        "def parse(payload, params):\n"
        "    assert payload == b'payload'\n"
        "    assert params == {'mode': 'test'}\n"
        "    yield ImportFailure(line=1, external_id='x', error='bad')\n",
        encoding="utf-8",
    )
    payload = tmp_path / "payload.bin"
    payload.write_bytes(b"payload")

    result = run_importer_test(
        script,
        entrypoint="parse",
        payload=payload,
        params='{"mode":"test"}',
        timeout=5,
    )

    assert result.item["loaded"] is True
    assert result.item["invoked"] is True
    assert result.item["items"] == 1
    assert result.item["failures"] == 1
    assert "not a sandbox" in result.warnings[0]


def test_evaluator_test_validates_signature_without_invoking(tmp_path: Path) -> None:
    """Stage 1 evaluator tests load code but do not invent a SessionView fixture."""
    marker = tmp_path / "called"
    script = tmp_path / "evaluator.py"
    script.write_text(
        "from pathlib import Path\n"
        "def evaluate(session, **params):\n"
        f"    Path({str(marker)!r}).write_text('called')\n",
        encoding="utf-8",
    )

    result = run_evaluator_test(script, entrypoint="evaluate", timeout=5)

    assert result.item["loaded"] is True
    assert result.item["invoked"] is False
    assert not marker.exists()


def test_plugin_test_timeout_is_stable_error(tmp_path: Path) -> None:
    """A hanging plugin import is terminated by the configured local timeout."""
    script = tmp_path / "slow.py"
    script.write_text(
        "import time\n"
        "time.sleep(2)\n"
        "def parse(payload, params):\n"
        "    return iter(())\n",
        encoding="utf-8",
    )

    with pytest.raises(CLIError) as error:
        run_importer_test(
            script,
            entrypoint="parse",
            payload=None,
            params=None,
            timeout=0.05,
        )
    assert error.value.kind == "timeout"


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group enhancement")
def test_plugin_timeout_kills_descendants(tmp_path: Path) -> None:
    """A timed-out plugin cannot leave a descendant running in its session."""
    marker = tmp_path / "leaked"
    child_code = (
        "import time; from pathlib import Path; time.sleep(0.5); "
        f"Path({str(marker)!r}).write_text('leaked')"
    )
    script = tmp_path / "spawns.py"
    script.write_text(
        "import subprocess, sys, time\n"
        f"subprocess.Popen([sys.executable, '-c', {child_code!r}])\n"
        "time.sleep(30)\n"
        "def parse(payload, params):\n"
        "    return iter(())\n",
        encoding="utf-8",
    )

    with pytest.raises(CLIError) as error:
        run_importer_test(
            script,
            entrypoint="parse",
            payload=None,
            params=None,
            timeout=0.1,
        )
    assert error.value.kind == "timeout"
    time.sleep(0.7)
    assert not marker.exists()


def test_plugin_native_output_is_bounded_while_running(tmp_path: Path) -> None:
    """Writes that bypass Python stream redirection retain only a bounded tail."""
    script = tmp_path / "native_output.py"
    script.write_text(
        "import os\n"
        "os.write(1, b'x' * 100_000)\n"
        "def parse(payload, params):\n"
        "    return iter(())\n",
        encoding="utf-8",
    )

    result = run_importer_test(
        script,
        entrypoint="parse",
        payload=None,
        params=None,
        timeout=5,
    )

    assert len(result.item["stdout"].encode()) <= scaffold_module._OUTPUT_LIMIT


def test_plugin_test_rejects_bad_signature(tmp_path: Path) -> None:
    """The child validates the contract without hiding load errors."""
    script = tmp_path / "bad.py"
    script.write_text("def parse(payload):\n    return iter(())\n", encoding="utf-8")

    with pytest.raises(CLIError) as error:
        run_importer_test(
            script,
            entrypoint="parse",
            payload=None,
            params=None,
            timeout=5,
        )
    assert error.value.kind == "invalid_arguments"
    assert error.value.details["returncode"] != 0
