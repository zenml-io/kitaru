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
"""Real Node subprocess tests for the TypeScript evaluator bridge."""

import asyncio
import hashlib
import json
import os
import shutil
import traceback
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import NodeStatus, NodeType, SessionNodeResponse
from kitaru.task.evaluator import EvaluationError, SessionView
from kitaru.task.typescript import run_typescript_evaluator


@pytest.fixture
def node() -> str:
    """Find Node or skip when the optional runtime is unavailable."""
    executable = shutil.which("node")
    if executable is None:
        pytest.skip("Node is not installed")
    return executable


@pytest.fixture
def view() -> SessionView:
    """Build a session with message, tool, metadata, and payload fields."""
    now = datetime.now(UTC)
    session = SessionDetailResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        number=1,
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.COMPLETED,
        inputs={"messages": [{"role": "user", "content": "Find my order"}]},
        outputs={"answer": "Shipped"},
        metadata={"locale": "en"},
        llm_call_count=1,
        tool_call_count=1,
        created=now,
        updated=now,
    )
    nodes = [
        SessionNodeResponse(
            id=uuid.uuid4(),
            session_id=session.id,
            index=index,
            parent_index=None,
            secondary_parent_indexes=[],
            secondary_parent_ids=[],
            node_type=kind,
            name=kind,
            status=NodeStatus.COMPLETED,
            inputs={"messages": session.inputs["messages"]}
            if index == 0
            else {"order": 42},
            outputs={"text": "Shipped"} if index == 0 else {"status": "shipped"},
            tool_name="lookup" if index == 1 else None,
            attributes={"provider": {"nested": True}},
            metadata={"attempt": 1},
        )
        for index, kind in enumerate([NodeType.LLM_CALL, NodeType.TOOL_CALL])
    ]
    return SessionView(session=session, nodes=nodes)


def _write_artifact(tmp_path: Path, source: str) -> tuple[Path, str]:
    """Write a JavaScript artifact and return its path and digest."""
    path = tmp_path / "evaluator with spaces.mjs"
    path.write_text(source)
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


async def test_full_session_and_config_round_trip(
    tmp_path: Path, node: str, view: SessionView
) -> None:
    """Preserve all session fields and score configuration across stdin."""
    artifact, digest = _write_artifact(
        tmp_path,
        """
let input = '';
for await (const chunk of process.stdin) input += chunk;
const request = JSON.parse(input);
process.stdout.write(JSON.stringify({schema_version: 1, results: [
  {name: 'roundtrip', value: JSON.stringify(request)},
  {name: 'quality', score: request.params.threshold, passed: true}
]}));
""",
    )
    params = {"threshold": 0.7, "model": "judge", "nested": {"labels": ["a"]}}
    results = await run_typescript_evaluator(
        view, artifact=artifact, sha256=digest, params=params, node=node
    )
    assert isinstance(results[0], EvaluationResult)
    assert json.loads(results[0].value or "") == {
        "schema_version": 1,
        "session": view.model_dump(mode="json"),
        "params": params,
    }
    assert results[1].score == 0.7
    assert results[1].passed is True


@pytest.mark.parametrize(
    "response",
    [
        "secret-provider-payload",
        "{}",
        "[]",
        '{"schema_version":true,"results":[{"name":"x","score":1}]}',
        '{"schema_version":2,"results":[{"name":"x","score":1}]}',
        '{"schema_version":1,"results":[]}',
        '{"schema_version":1,"results":[{"name":"x","score":1}],"extra":true}',
        '{"schema_version":1,"results":[{"name":"x","score":1},{"name":"x","score":0}]}',
        '{"schema_version":1,"results":[{"name":"","score":1}]}',
        '{"schema_version":1,"results":[{"name":"x","value":{"secret":"payload"}}]}',
        '{"schema_version":1,"results":[{"name":"x","score":1,"unknown":"secret"}]}',
        '{"schema_version":1,"results":[{"name":"x","score":NaN}]}',
        '{"schema_version":1,"results":[{"name":"x","score":"1"}]}',
        '{"schema_version":1,"results":[{"name":"x","value":1}]}',
        '{"schema_version":1,"results":[{"name":"x","score":1,"passed":"true"}]}',
        '{"schema_version":1,"results":[{"name":"x","score":1,"min_score":true}]}',
        '{"schema_version":1,"schema_version":1,"results":[{"name":"x","score":1}]}',
        '{"schema_version":1,"results":[{"name":"x"}]}',
    ],
)
async def test_rejects_invalid_response_without_disclosing_payload(
    tmp_path: Path, node: str, view: SessionView, response: str
) -> None:
    """Reject invalid protocol or result values with sanitized errors."""
    artifact, digest = _write_artifact(
        tmp_path, f"process.stdout.write({json.dumps(response)});"
    )
    with pytest.raises(EvaluationError) as error:
        await run_typescript_evaluator(
            view, artifact=artifact, sha256=digest, params={}, node=node
        )
    assert "secret" not in "".join(traceback.format_exception(error.value))


async def test_nonzero_exit_hides_output(
    tmp_path: Path, node: str, view: SessionView
) -> None:
    """Reject an unsuccessful process even if it printed valid results."""
    artifact, digest = _write_artifact(
        tmp_path,
        """
process.stdout.write('{"schema_version":1,"results":[{"name":"x","score":1}]}');
console.error('secret-provider-payload');
process.exitCode = 7;
""",
    )
    with pytest.raises(EvaluationError, match="exit") as error:
        await run_typescript_evaluator(
            view, artifact=artifact, sha256=digest, params={}, node=node
        )
    assert "secret" not in "".join(traceback.format_exception(error.value))


@pytest.mark.parametrize("failure", ["hash", "missing", "relative", "runtime"])
async def test_preflight_failures(
    tmp_path: Path, node: str, view: SessionView, failure: str
) -> None:
    """Fail closed on missing artifacts, wrong digests, paths, or runtimes."""
    marker = tmp_path / "executed"
    artifact, digest = _write_artifact(
        tmp_path,
        "import {writeFileSync} from 'node:fs'; "
        f"writeFileSync({json.dumps(str(marker))}, 'yes');",
    )
    if failure == "hash":
        digest = "0" * 64
    elif failure == "missing":
        artifact.unlink()
    elif failure == "relative":
        artifact = Path(artifact.name)
    else:
        node = str(tmp_path / "missing-node")
    with pytest.raises(EvaluationError):
        await run_typescript_evaluator(
            view, artifact=artifact, sha256=digest, params={}, node=node
        )
    assert not marker.exists()


@pytest.mark.parametrize("cancel", [False, True])
async def test_timeout_and_cancellation_reap_process(
    tmp_path: Path, node: str, view: SessionView, cancel: bool
) -> None:
    """Stop and reap Node when the timeout expires or the caller cancels."""
    pid_file = tmp_path / "pid"
    artifact, digest = _write_artifact(
        tmp_path,
        f"""
import {{writeFileSync}} from 'node:fs';
writeFileSync({json.dumps(str(pid_file))}, String(process.pid));
setInterval(() => {{}}, 1000);
""",
    )
    task = asyncio.create_task(
        run_typescript_evaluator(
            view,
            artifact=artifact,
            sha256=digest,
            params={},
            node=node,
            timeout_seconds=10 if cancel else 0.5,
        )
    )
    async with asyncio.timeout(3):
        while not pid_file.exists():
            await asyncio.sleep(0.01)
    if cancel:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(EvaluationError, match="timed out"):
            await task
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_file.read_text()), 0)


async def test_stdout_size_is_bounded(
    tmp_path: Path, node: str, view: SessionView
) -> None:
    """Abort a process producing an oversized response without hanging."""
    artifact, digest = _write_artifact(
        tmp_path, "setInterval(() => process.stdout.write('x'.repeat(65536)), 1);"
    )
    async with asyncio.timeout(3):
        with pytest.raises(EvaluationError, match="size"):
            await run_typescript_evaluator(
                view, artifact=artifact, sha256=digest, params={}, node=node
            )


async def test_stderr_cannot_block_the_response(
    tmp_path: Path, node: str, view: SessionView
) -> None:
    """Discard large diagnostic output while accepting the protocol response."""
    artifact, digest = _write_artifact(
        tmp_path,
        "process.stderr.write('secret'.repeat(400000));"
        "process.stdout.write(JSON.stringify({schema_version:1,"
        "results:[{name:'quality',score:true}]}));",
    )
    results = await run_typescript_evaluator(
        view,
        artifact=artifact,
        sha256=digest,
        params={},
        node=node,
        timeout_seconds=3,
    )
    assert results[0].score is True


async def test_timeout_includes_blocked_stdin(
    tmp_path: Path, node: str, view: SessionView
) -> None:
    """Enforce the timeout even when Node never reads a large session."""
    artifact, digest = _write_artifact(tmp_path, "setInterval(() => {}, 1000);")
    view.session.inputs = {"text": "x" * 2_000_000}
    async with asyncio.timeout(3):
        with pytest.raises(EvaluationError, match="timed out"):
            await run_typescript_evaluator(
                view,
                artifact=artifact,
                sha256=digest,
                params={},
                node=node,
                timeout_seconds=0.2,
            )


@pytest.mark.parametrize("params", [{"x": float("nan")}, {"x": object()}, []])
async def test_rejects_non_json_configuration(
    tmp_path: Path, node: str, view: SessionView, params: Any
) -> None:
    """Reject configuration that cannot be encoded as a JSON object."""
    artifact, digest = _write_artifact(tmp_path, "process.exit(0)")
    with pytest.raises(EvaluationError):
        await run_typescript_evaluator(
            view, artifact=artifact, sha256=digest, params=params, node=node
        )
