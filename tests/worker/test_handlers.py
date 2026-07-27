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
"""Tests for job kind handlers and blob materialization."""

import hashlib
import json
import sys
import uuid
from pathlib import Path
from typing import cast

import pytest
from fakes import (
    FakeClient,
    make_import_spec,
    make_payload,
    make_plugin,
    make_score_spec,
    make_spec,
)

from kitaru.blob_cache import BlobCache
from kitaru.client.api_client import KitaruAPIClient
from kitaru.worker.context import ExecutionContext
from kitaru.worker.handlers.agent import AgentHandler
from kitaru.worker.handlers.base import materialize_blob
from kitaru.worker.handlers.imports import IMPORT_TIMEOUT_SECONDS, ImportHandler
from kitaru.worker.handlers.score import SCORE_TIMEOUT_SECONDS, ScoreHandler

PLUGIN_CODE = b"def score(session):\n    return 0.5\n"
PLUGIN_SHA256 = hashlib.sha256(PLUGIN_CODE).hexdigest()

IMPORTER_CODE = b"def parse(payload):\n    return []\n"
IMPORTER_SHA256 = hashlib.sha256(IMPORTER_CODE).hexdigest()
TRACE_PAYLOAD = b'{"trace_id": "trace-1"}\n'
TRACE_SHA256 = hashlib.sha256(TRACE_PAYLOAD).hexdigest()


def make_ctx(fake: FakeClient, tmp_path: Path) -> ExecutionContext:
    """Build an execution context backed by a fake client and tmp caches."""
    return ExecutionContext(
        client=cast(KitaruAPIClient, fake),
        blob_cache=BlobCache(tmp_path / "blobs"),
        payload_cache=BlobCache(tmp_path / "payloads"),
    )


async def test_materialize_blob_downloads_on_miss(tmp_path: Path) -> None:
    """Download and cache a blob absent from the cache."""
    blob_id = uuid.uuid4()
    fake = FakeClient(blob_contents={blob_id: PLUGIN_CODE})
    ctx = make_ctx(fake, tmp_path)

    path = await materialize_blob(ctx, ctx.blob_cache, blob_id, PLUGIN_SHA256)

    assert path.read_bytes() == PLUGIN_CODE
    assert fake.blob_downloads == [blob_id]


async def test_materialize_blob_reuses_the_cache(tmp_path: Path) -> None:
    """Skip the download when the content is already cached."""
    blob_id = uuid.uuid4()
    fake = FakeClient(blob_contents={blob_id: PLUGIN_CODE})
    ctx = make_ctx(fake, tmp_path)
    ctx.blob_cache.put(PLUGIN_SHA256, PLUGIN_CODE)

    path = await materialize_blob(ctx, ctx.blob_cache, blob_id, PLUGIN_SHA256)

    assert path.read_bytes() == PLUGIN_CODE
    assert fake.blob_downloads == []


async def test_agent_handler_builds_the_run_command(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Build the agent process from the run spec, with inputs and session name."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    spec = make_spec(
        job_id,
        command="run-agent",
        inputs={"question": "hi"},
        working_dir="/work",
        timeout_seconds=45,
        name="smoke",
    )
    fake = FakeClient()
    ctx = make_ctx(fake, tmp_path)

    process = await AgentHandler().prepare(ctx, job_id, spec)

    assert process.command == "run-agent"
    assert process.working_dir == "/work"
    assert process.timeout_seconds == 45
    assert process.env["KITARU_JOB_SESSION_NAME"] == "smoke"
    assert json.loads(process.env["KITARU_JOB_INPUTS"]) == {"question": "hi"}
    assert process.env["KITARU_JOB_ID"] == str(job_id)


async def test_agent_handler_omits_inputs_over_the_threshold(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Omit KITARU_JOB_INPUTS when the encoded inputs exceed the threshold."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    spec = make_spec(job_id, inputs="x" * 40_000)
    fake = FakeClient()
    ctx = make_ctx(fake, tmp_path)

    process = await AgentHandler().prepare(ctx, job_id, spec)

    assert "KITARU_JOB_INPUTS" not in process.env


async def test_score_handler_registry_arm_materializes_the_plugin(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Materialize the code blob and run it with no working directory."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    plugin = make_plugin(PLUGIN_SHA256)
    spec = make_score_spec(job_id, uuid.uuid4(), plugin=plugin)
    fake = FakeClient(blob_contents={plugin.blob_id: PLUGIN_CODE})
    ctx = make_ctx(fake, tmp_path)

    process = await ScoreHandler().prepare(ctx, job_id, spec)

    assert process.command == f"{sys.executable} -m kitaru.job score"
    assert process.working_dir is None
    assert process.timeout_seconds == SCORE_TIMEOUT_SECONDS
    cached = tmp_path / "blobs" / PLUGIN_SHA256
    assert process.env["KITARU_JOB_PLUGIN_PATH"] == str(cached)
    assert cached.read_bytes() == PLUGIN_CODE


async def test_score_handler_source_arm_runs_in_the_agent_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Run the source scorer in the agent's run environment and working dir."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    spec = make_score_spec(
        job_id,
        uuid.uuid4(),
        plugin=None,
        working_dir=str(tmp_path),
        timeout_seconds=45,
    )
    fake = FakeClient()
    ctx = make_ctx(fake, tmp_path)

    process = await ScoreHandler().prepare(ctx, job_id, spec)

    assert process.command == f"{sys.executable} -m kitaru.job score"
    assert process.working_dir == str(tmp_path)
    assert process.timeout_seconds == 45
    assert "KITARU_JOB_PLUGIN_PATH" not in process.env
    assert fake.blob_downloads == []


async def test_score_handler_falls_back_to_the_default_timeout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Use SCORE_TIMEOUT_SECONDS when the run spec sets no timeout."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    spec = make_score_spec(job_id, uuid.uuid4(), plugin=None, timeout_seconds=None)
    fake = FakeClient()
    ctx = make_ctx(fake, tmp_path)

    process = await ScoreHandler().prepare(ctx, job_id, spec)

    assert process.timeout_seconds == SCORE_TIMEOUT_SECONDS


async def test_import_handler_materializes_code_and_payload_concurrently(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Materialize the importer code and payload and hand both paths over."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "key")
    job_id = uuid.uuid4()
    plugin = make_plugin(IMPORTER_SHA256, entrypoint="parse")
    payload = make_payload(TRACE_SHA256)
    spec = make_import_spec(job_id, plugin, payload)
    fake = FakeClient(
        blob_contents={plugin.blob_id: IMPORTER_CODE, payload.blob_id: TRACE_PAYLOAD}
    )
    ctx = make_ctx(fake, tmp_path)

    process = await ImportHandler().prepare(ctx, job_id, spec)

    assert process.command == f"{sys.executable} -m kitaru.job import"
    assert process.working_dir is None
    assert process.timeout_seconds == IMPORT_TIMEOUT_SECONDS
    code_path = tmp_path / "blobs" / IMPORTER_SHA256
    payload_path = tmp_path / "payloads" / TRACE_SHA256
    assert process.env["KITARU_JOB_PLUGIN_PATH"] == str(code_path)
    assert process.env["KITARU_JOB_PAYLOAD_PATH"] == str(payload_path)
    assert code_path.read_bytes() == IMPORTER_CODE
    assert payload_path.read_bytes() == TRACE_PAYLOAD
    assert sorted(fake.blob_downloads) == sorted([plugin.blob_id, payload.blob_id])
