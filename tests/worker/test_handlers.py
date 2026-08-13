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
"""Tests for per-kind task handlers."""

import hashlib
import json
import uuid
from pathlib import Path

from fakes import (
    FakeKitaruAPIClient,
    as_client,
    make_agent_spec,
    make_evaluator_spec,
    make_importer_spec,
)

from kitaru.api_models.v1.task import (
    PackagePluginSpec,
    PayloadSpec,
    ScriptPluginSpec,
    TaskKind,
)
from kitaru.worker.blob_cache import BlobCache
from kitaru.worker.context import ExecutionContext
from kitaru.worker.handlers import HANDLERS
from kitaru.worker.handlers.agent import MAX_INPUTS_ENV_BYTES, AgentHandler
from kitaru.worker.handlers.evaluation import EvaluationHandler
from kitaru.worker.handlers.imports import ImportHandler


def _ctx(tmp_path: Path, client: FakeKitaruAPIClient) -> ExecutionContext:
    return ExecutionContext(
        client=as_client(client),
        blob_cache=BlobCache(tmp_path / "blobs"),
        payload_cache=BlobCache(tmp_path / "payloads"),
    )


def _digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def test_handlers_registry_covers_every_kind() -> None:
    """HANDLERS carries one entry per task kind."""
    assert set(HANDLERS) == {TaskKind.AGENT, TaskKind.EVALUATOR, TaskKind.IMPORTER}


async def test_agent_handler_builds_command_and_working_dir(tmp_path: Path) -> None:
    """The agent process uses the run spec's command and working dir verbatim."""
    task_id = uuid.uuid4()
    spec = make_agent_spec(
        task_id, command="run.sh --flag", working_dir="/srv/agent", inputs={"a": 1}
    )
    ctx = _ctx(tmp_path, FakeKitaruAPIClient())

    process = await AgentHandler().prepare(ctx, task_id, spec, "task-token")

    assert process.command == "run.sh --flag"
    assert process.working_dir == "/srv/agent"
    assert process.timeout_seconds == spec.timeout_seconds
    assert json.loads(process.env["KITARU_TASK_INPUTS"]) == {"a": 1}


async def test_agent_handler_omits_inputs_over_the_env_byte_threshold(
    tmp_path: Path,
) -> None:
    """Oversized inputs are left out of the environment."""
    task_id = uuid.uuid4()
    huge_inputs = {"payload": "x" * (MAX_INPUTS_ENV_BYTES + 100)}
    spec = make_agent_spec(task_id, inputs=huge_inputs)
    ctx = _ctx(tmp_path, FakeKitaruAPIClient())

    process = await AgentHandler().prepare(ctx, task_id, spec, "task-token")

    assert "KITARU_TASK_INPUTS" not in process.env


async def test_agent_handler_merges_extras_and_secrets(tmp_path: Path) -> None:
    """Creator-set extras and secrets reach the process environment."""
    task_id = uuid.uuid4()
    spec = make_agent_spec(
        task_id,
        run_env={"RUN_VAR": "1"},
        extra_env={"KITARU_SESSION_NAME": "run-1"},
        secret_env={"PROVIDER_KEY": "secret"},
    )
    ctx = _ctx(tmp_path, FakeKitaruAPIClient())

    process = await AgentHandler().prepare(ctx, task_id, spec, "task-token")

    assert process.env["RUN_VAR"] == "1"
    assert process.env["KITARU_SESSION_NAME"] == "run-1"
    assert process.env["PROVIDER_KEY"] == "secret"
    assert process.env["KITARU_API_TOKEN"] == "task-token"


async def test_agent_handler_sets_the_replay_id_from_the_details(
    tmp_path: Path,
) -> None:
    """A replay id on the details reaches the process environment."""
    task_id = uuid.uuid4()
    replay_id = uuid.uuid4()
    spec = make_agent_spec(task_id, replay_id=replay_id)
    ctx = _ctx(tmp_path, FakeKitaruAPIClient())

    process = await AgentHandler().prepare(ctx, task_id, spec, "task-token")

    assert process.env["KITARU_REPLAY_ID"] == str(replay_id)


async def test_agent_handler_omits_the_replay_id_without_one(
    tmp_path: Path,
) -> None:
    """A task outside a replay gets no replay id variable."""
    task_id = uuid.uuid4()
    spec = make_agent_spec(task_id)
    ctx = _ctx(tmp_path, FakeKitaruAPIClient())

    process = await AgentHandler().prepare(ctx, task_id, spec, "task-token")

    assert "KITARU_REPLAY_ID" not in process.env


async def test_evaluation_handler_script_plugin_materializes_and_sets_path(
    tmp_path: Path,
) -> None:
    """A script evaluator plugin is materialized and its path exported."""
    content = b"# /// script\n# dependencies = ['numpy']\n# ///\n"
    digest = _digest(content)
    blob_id = uuid.uuid4()
    plugin = ScriptPluginSpec(entrypoint="evaluate", blob_id=blob_id, sha256=digest)
    task_id = uuid.uuid4()
    spec = make_evaluator_spec(task_id, plugin=plugin)

    client = FakeKitaruAPIClient()
    client.blobs.content[blob_id] = content
    ctx = _ctx(tmp_path, client)

    process = await EvaluationHandler().prepare(ctx, task_id, spec, "task-token")

    plugin_path = Path(process.env["KITARU_TASK_PLUGIN_PATH"])
    assert plugin_path.read_bytes() == content
    assert client.blobs.download_calls == [blob_id]
    assert process.working_dir is None
    assert process.command == [
        "uv",
        "run",
        "--with",
        "numpy",
        "python",
        "-m",
        "kitaru.task",
        "evaluate",
    ]
    assert process.env["KITARU_API_TOKEN"] == "task-token"


async def test_evaluation_handler_package_plugin_skips_materialization(
    tmp_path: Path,
) -> None:
    """A package evaluator plugin needs no blob download."""
    plugin = PackagePluginSpec(entrypoint="pkg.mod:evaluate", requirement="pkg==1.0")
    task_id = uuid.uuid4()
    spec = make_evaluator_spec(task_id, plugin=plugin)
    client = FakeKitaruAPIClient()
    ctx = _ctx(tmp_path, client)

    process = await EvaluationHandler().prepare(ctx, task_id, spec, "task-token")

    assert "KITARU_TASK_PLUGIN_PATH" not in process.env
    assert client.blobs.download_calls == []
    assert process.command == [
        "uv",
        "run",
        "--with",
        "pkg==1.0",
        "python",
        "-m",
        "kitaru.task",
        "evaluate",
    ]


async def test_evaluation_handler_reuses_cached_plugin(tmp_path: Path) -> None:
    """A cached plugin blob is not re-downloaded."""
    content = b"def evaluate(session, **params):\n    pass\n"
    digest = _digest(content)
    blob_id = uuid.uuid4()
    plugin = ScriptPluginSpec(entrypoint="evaluate", blob_id=blob_id, sha256=digest)
    client = FakeKitaruAPIClient()
    ctx = _ctx(tmp_path, client)
    await ctx.blob_cache.put(digest, content)

    task_id = uuid.uuid4()
    spec = make_evaluator_spec(task_id, plugin=plugin)
    await EvaluationHandler().prepare(ctx, task_id, spec, "task-token")

    assert client.blobs.download_calls == []


async def test_import_handler_script_plugin_materializes_code_and_payload(
    tmp_path: Path,
) -> None:
    """A script importer plugin materializes both the code and payload blobs."""
    code_content = b"def parse(payload, params):\n    return iter(())\n"
    payload_content = b'{"sessions": []}'
    code_digest = _digest(code_content)
    payload_digest = _digest(payload_content)
    code_blob_id = uuid.uuid4()
    payload_blob_id = uuid.uuid4()

    plugin = ScriptPluginSpec(
        entrypoint="parse", blob_id=code_blob_id, sha256=code_digest
    )
    payload = PayloadSpec(blob_id=payload_blob_id, sha256=payload_digest)
    task_id = uuid.uuid4()
    spec = make_importer_spec(task_id, plugin=plugin, payload=payload)

    client = FakeKitaruAPIClient()
    client.blobs.content[code_blob_id] = code_content
    client.blobs.content[payload_blob_id] = payload_content
    ctx = _ctx(tmp_path, client)

    process = await ImportHandler().prepare(ctx, task_id, spec, "task-token")

    assert Path(process.env["KITARU_TASK_PLUGIN_PATH"]).read_bytes() == code_content
    assert Path(process.env["KITARU_TASK_PAYLOAD_PATH"]).read_bytes() == payload_content
    assert set(client.blobs.download_calls) == {code_blob_id, payload_blob_id}
    assert process.env["KITARU_API_TOKEN"] == "task-token"
    # The payload is cached under the payload cache root, the plugin under
    # the code cache root, never the other's directory.
    assert "payloads" in process.env["KITARU_TASK_PAYLOAD_PATH"]
    assert "blobs" in process.env["KITARU_TASK_PLUGIN_PATH"]


async def test_import_handler_package_plugin_materializes_only_the_payload(
    tmp_path: Path,
) -> None:
    """A package importer plugin only materializes the payload blob."""
    payload_content = b'{"sessions": []}'
    payload_digest = _digest(payload_content)
    payload_blob_id = uuid.uuid4()

    plugin = PackagePluginSpec(entrypoint="pkg.mod:parse", requirement="pkg==2.0")
    payload = PayloadSpec(blob_id=payload_blob_id, sha256=payload_digest)
    task_id = uuid.uuid4()
    spec = make_importer_spec(task_id, plugin=plugin, payload=payload)

    client = FakeKitaruAPIClient()
    client.blobs.content[payload_blob_id] = payload_content
    ctx = _ctx(tmp_path, client)

    process = await ImportHandler().prepare(ctx, task_id, spec, "task-token")

    assert "KITARU_TASK_PLUGIN_PATH" not in process.env
    assert client.blobs.download_calls == [payload_blob_id]
    assert process.command == [
        "uv",
        "run",
        "--with",
        "pkg==2.0",
        "python",
        "-m",
        "kitaru.task",
        "import",
    ]
