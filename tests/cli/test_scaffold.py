#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Importer/evaluator scaffold and bounded local-test behavior."""

import os
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
)
from kitaru.cli import scaffold as scaffold_module
from kitaru.cli.output import CLIError
from kitaru.cli.registration import ScriptSource, prepare_plugin_source
from kitaru.cli.scaffold import (
    scaffold_asset,
)
from kitaru.cli.scaffold import (
    test_evaluator as run_evaluator_test,
)
from kitaru.cli.scaffold import (
    test_importer as run_importer_test,
)
from kitaru.task.evaluator import SessionView, call_evaluator
from kitaru.task.plugins import load_plugin_entrypoint


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


@pytest.mark.parametrize("with_payload", [False, True])
def test_importer_test_accepts_importer_object(
    tmp_path: Path, with_payload: bool
) -> None:
    """Object entrypoints validate and invoke parse without fetching remote data."""
    script = tmp_path / "provider.py"
    script.write_text(
        "from kitaru.task.importer import ImportFailure\n"
        "class Provider:\n"
        "    def parse(self, payload, params):\n"
        "        assert payload == b'payload'\n"
        "        assert params == {'mode': 'test'}\n"
        "        yield ImportFailure(line=1, external_id='x', error='bad')\n"
        "    def fetch(self, query):\n"
        "        raise AssertionError('local tests must not fetch')\n"
        "importer = Provider()\n",
        encoding="utf-8",
    )
    payload = tmp_path / "payload.bin"
    payload.write_bytes(b"payload")

    result = run_importer_test(
        script,
        entrypoint="importer",
        payload=payload if with_payload else None,
        params='{"mode":"test"}',
        timeout=5,
    )

    assert result.item["loaded"] is True
    assert result.item["invoked"] is with_payload
    if with_payload:
        assert result.item["items"] == 1
        assert result.item["failures"] == 1


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


async def test_evaluator_scaffold_reads_session_nodes_and_validates(
    tmp_path: Path,
) -> None:
    """The scaffolded evaluator validates locally and inspects session.nodes."""
    target = tmp_path / "quality_evaluator.py"

    scaffolded = scaffold_asset("evaluator", "quality", path=target, force=False)
    assert scaffolded.item["entrypoint"] == "evaluate"

    tested = run_evaluator_test(target, entrypoint="evaluate", timeout=5)
    assert tested.item["loaded"] is True
    assert tested.item["invoked"] is False

    evaluate = load_plugin_entrypoint(target, "evaluate", "Evaluator")
    now = datetime.now(UTC)
    session_id = uuid.uuid4()
    view = SessionView(
        session=SessionDetailResponse(
            id=session_id,
            owner_id=uuid.uuid4(),
            agent_id=uuid.uuid4(),
            number=1,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
            inputs={"prompt": "Refund order #42"},
            outputs={"reply": "Done"},
            metadata={},
            llm_call_count=1,
            tool_call_count=2,
            created=now,
            updated=now,
        ),
        nodes=[
            SessionNodeResponse(
                id=uuid.uuid4(),
                session_id=session_id,
                external_id="llm-1",
                links=[],
                node_type=NodeType.LLM_CALL,
                name="chat",
                status=NodeStatus.COMPLETED,
                metadata={},
            ),
            SessionNodeResponse(
                id=uuid.uuid4(),
                session_id=session_id,
                external_id="tool-1",
                links=[],
                node_type=NodeType.TOOL_CALL,
                name="lookup_order",
                tool_name="lookup_order",
                status=NodeStatus.COMPLETED,
                inputs={"order_id": "42"},
                outputs={"eligible": True},
                metadata={},
            ),
            SessionNodeResponse(
                id=uuid.uuid4(),
                session_id=session_id,
                external_id="tool-2",
                links=[],
                node_type=NodeType.TOOL_CALL,
                name="refund_payment",
                tool_name="refund_payment",
                status=NodeStatus.COMPLETED,
                inputs={"order_id": "42"},
                outputs={"accepted": True},
                metadata={},
            ),
        ],
    )

    [evaluation] = await call_evaluator("quality", evaluate, view, {})
    assert evaluation.name == "example"
    assert evaluation.score == 2
    assert evaluation.passed is True
    assert evaluation.explanation == "Observed 2 tool call(s)."


@pytest.mark.parametrize(
    ("kind", "entrypoint"),
    [("importer", "parse"), ("evaluator", "evaluate")],
)
def test_scaffold_templates_do_not_inject_placeholder_description(
    tmp_path: Path, kind: str, entrypoint: str
) -> None:
    """Fresh scaffold files leave the extracted script description unset."""
    target = tmp_path / f"sample_{kind}.py"
    scaffold_asset(kind, "sample", path=target, force=False)

    source = prepare_plugin_source(script=target, package=None, entrypoint=entrypoint)
    assert isinstance(source, ScriptSource)
    assert source.description is None
