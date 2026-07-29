"""Task process handler tests."""

import hashlib
import json
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from kitaru.api_models.v1.task import (
    AgentTaskDetails,
    EvaluationTaskDetails,
    ImportTaskDetails,
    PackagePluginSpec,
    PayloadSpec,
    ScriptPluginSpec,
    TaskKind,
    TaskRunSpec,
    TaskSpecResponse,
)
from kitaru.worker.blob_cache import BlobCache
from kitaru.worker.context import ExecutionContext
from kitaru.worker.handlers.agent import AgentHandler
from kitaru.worker.handlers.evaluation import EvaluationHandler
from kitaru.worker.handlers.imports import ImportHandler


class FakeBlobs:
    def __init__(self, content: dict[uuid.UUID, bytes]) -> None:
        self.content = content
        self.downloaded: list[uuid.UUID] = []

    async def download(self, blob_id: uuid.UUID) -> bytes:
        self.downloaded.append(blob_id)
        return self.content[blob_id]


def make_context(
    tmp_path: Path, content: dict[uuid.UUID, bytes]
) -> tuple[ExecutionContext, FakeBlobs]:
    blobs = FakeBlobs(content)
    client = cast(Any, SimpleNamespace(blobs=blobs))
    return (
        ExecutionContext(
            client=client,
            blob_cache=BlobCache(tmp_path / "code"),
            payload_cache=BlobCache(tmp_path / "payload"),
        ),
        blobs,
    )


async def test_agent_handler_uses_run_and_inlines_small_inputs(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("KITARU_API_URL", "https://api")
    task_id = uuid.uuid4()
    spec = TaskSpecResponse(
        task_id=task_id,
        kind=TaskKind.AGENT,
        timeout_seconds=17,
        run=TaskRunSpec(
            command="python agent.py",
            working_dir="/work",
            env={"RUN": "yes"},
        ),
        env={"EXTRA": "yes"},
        secret_env={"SECRET": "yes"},
        details=AgentTaskDetails(kind="agent", inputs={"prompt": "hello"}),
    )
    ctx, _ = make_context(tmp_path, {})

    process = await AgentHandler().prepare(ctx, task_id, spec)

    assert process.command == "python agent.py"
    assert process.working_dir == "/work"
    assert process.timeout_seconds == 17
    assert process.env["RUN"] == "yes"
    assert process.env["EXTRA"] == "yes"
    assert process.env["SECRET"] == "yes"
    assert json.loads(process.env["KITARU_TASK_INPUTS"]) == {"prompt": "hello"}


async def test_agent_handler_omits_inputs_over_limit(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("kitaru.worker.handlers.agent.MAX_INPUTS_ENV_BYTES", 1)
    task_id = uuid.uuid4()
    spec = TaskSpecResponse(
        task_id=task_id,
        kind=TaskKind.AGENT,
        timeout_seconds=10,
        run=TaskRunSpec(command="agent", working_dir=None, env={}),
        env={},
        secret_env={},
        details=AgentTaskDetails(kind="agent", inputs={"large": "value"}),
    )
    ctx, _ = make_context(tmp_path, {})

    process = await AgentHandler().prepare(ctx, task_id, spec)

    assert "KITARU_TASK_INPUTS" not in process.env


async def test_evaluation_script_materializes_code_and_uses_pep_723(
    tmp_path,
) -> None:
    task_id = uuid.uuid4()
    blob_id = uuid.uuid4()
    content = (
        b'# /// script\n# dependencies = ["judge==1"]\n# ///\ndef evaluate(): pass\n'
    )
    digest = hashlib.sha256(content).hexdigest()
    plugin = ScriptPluginSpec(
        type="script",
        entrypoint="evaluate",
        blob_id=blob_id,
        sha256=digest,
    )
    spec = TaskSpecResponse(
        task_id=task_id,
        kind=TaskKind.EVALUATOR,
        timeout_seconds=23,
        run=None,
        env={},
        secret_env={},
        details=EvaluationTaskDetails(
            kind="evaluator",
            evaluator_name="quality",
            params={},
            plugin=plugin,
            input_session_id=uuid.uuid4(),
        ),
    )
    ctx, blobs = make_context(tmp_path, {blob_id: content})

    process = await EvaluationHandler().prepare(ctx, task_id, spec)

    assert blobs.downloaded == [blob_id]
    assert process.timeout_seconds == 23
    assert process.working_dir is None
    assert process.env["KITARU_TASK_PLUGIN_PATH"] == str(ctx.blob_cache.path(digest))
    assert "--with judge==1" in process.command
    assert process.command.endswith("-m kitaru.task evaluate")


async def test_evaluation_package_uses_requirement_without_download(
    tmp_path,
) -> None:
    task_id = uuid.uuid4()
    spec = TaskSpecResponse(
        task_id=task_id,
        kind=TaskKind.EVALUATOR,
        timeout_seconds=10,
        run=None,
        env={},
        secret_env={},
        details=EvaluationTaskDetails(
            kind="evaluator",
            evaluator_name="quality",
            params={},
            plugin=PackagePluginSpec(
                type="package",
                entrypoint="pkg:evaluate",
                requirement="pkg==1",
            ),
            input_session_id=uuid.uuid4(),
        ),
    )
    ctx, blobs = make_context(tmp_path, {})

    process = await EvaluationHandler().prepare(ctx, task_id, spec)

    assert not blobs.downloaded
    assert "KITARU_TASK_PLUGIN_PATH" not in process.env
    assert "--with pkg==1" in process.command


async def test_import_script_materializes_code_and_payload(tmp_path) -> None:
    task_id = uuid.uuid4()
    code_id = uuid.uuid4()
    payload_id = uuid.uuid4()
    code = b"def parse(): pass\n"
    payload = b'{"sessions":[]}'
    code_hash = hashlib.sha256(code).hexdigest()
    payload_hash = hashlib.sha256(payload).hexdigest()
    spec = TaskSpecResponse(
        task_id=task_id,
        kind=TaskKind.IMPORTER,
        timeout_seconds=31,
        run=None,
        env={},
        secret_env={},
        details=ImportTaskDetails(
            kind="importer",
            plugin=ScriptPluginSpec(
                type="script",
                entrypoint="parse",
                blob_id=code_id,
                sha256=code_hash,
            ),
            payload=PayloadSpec(blob_id=payload_id, sha256=payload_hash),
            provider="test",
            agent_id=uuid.uuid4(),
            params={},
        ),
    )
    ctx, blobs = make_context(tmp_path, {code_id: code, payload_id: payload})

    process = await ImportHandler().prepare(ctx, task_id, spec)

    assert set(blobs.downloaded) == {code_id, payload_id}
    assert process.env["KITARU_TASK_PLUGIN_PATH"] == str(ctx.blob_cache.path(code_hash))
    assert process.env["KITARU_TASK_PAYLOAD_PATH"] == str(
        ctx.payload_cache.path(payload_hash)
    )
    assert process.timeout_seconds == 31
    assert process.command.endswith("-m kitaru.task import")


async def test_materialization_uses_cache_hit_without_download(tmp_path) -> None:
    task_id = uuid.uuid4()
    blob_id = uuid.uuid4()
    content = b"def evaluate(): pass\n"
    digest = hashlib.sha256(content).hexdigest()
    spec = TaskSpecResponse(
        task_id=task_id,
        kind=TaskKind.EVALUATOR,
        timeout_seconds=10,
        run=None,
        env={},
        secret_env={},
        details=EvaluationTaskDetails(
            kind="evaluator",
            evaluator_name="quality",
            params={},
            plugin=ScriptPluginSpec(
                type="script",
                entrypoint="evaluate",
                blob_id=blob_id,
                sha256=digest,
            ),
            input_session_id=uuid.uuid4(),
        ),
    )
    ctx, blobs = make_context(tmp_path, {blob_id: content})
    await ctx.blob_cache.put(digest, content)

    await EvaluationHandler().prepare(ctx, task_id, spec)

    assert not blobs.downloaded
