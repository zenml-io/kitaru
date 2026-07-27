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
"""Tests for WorkerConfig environment parsing."""

import uuid

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.jobs import JobKind, WorkerScope
from kitaru.worker.config import WorkerConfig


def test_defaults_without_env() -> None:
    """Fall back to unset name, an empty scope, and single concurrency."""
    config = WorkerConfig()

    assert config.name is None
    assert config.scope == WorkerScope()
    assert config.concurrency == 1
    assert config.claim_batch_size is None
    assert config.timeout is None
    assert config.blob_cache_root is None
    assert config.payload_cache_root is None


def test_env_parses_scalar_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """Parse scalar fields from their prefixed env vars."""
    monkeypatch.setenv("KITARU_WORKER_NAME", "worker-1")
    monkeypatch.setenv("KITARU_WORKER_CONCURRENCY", "4")
    monkeypatch.setenv("KITARU_WORKER_CLAIM_BATCH_SIZE", "10")
    monkeypatch.setenv("KITARU_WORKER_POLL_INTERVAL", "5.5")
    monkeypatch.setenv("KITARU_WORKER_HEARTBEAT_INTERVAL", "20")
    monkeypatch.setenv("KITARU_WORKER_TIMEOUT", "300")
    monkeypatch.setenv("KITARU_WORKER_BLOB_CACHE_ROOT", "/tmp/blobs")
    monkeypatch.setenv("KITARU_WORKER_PAYLOAD_CACHE_ROOT", "/tmp/payloads")

    config = WorkerConfig()

    assert config.name == "worker-1"
    assert config.concurrency == 4
    assert config.claim_batch_size == 10
    assert config.poll_interval == 5.5
    assert config.heartbeat_interval == 20
    assert config.timeout == 300
    assert str(config.blob_cache_root) == "/tmp/blobs"
    assert str(config.payload_cache_root) == "/tmp/payloads"


def test_env_parses_nested_scope_job_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Parse a scalar nested scope field through the delimiter."""
    job_id = uuid.uuid4()
    monkeypatch.setenv("KITARU_WORKER_SCOPE__JOB_ID", str(job_id))

    config = WorkerConfig()

    assert config.scope.job_id == job_id
    assert config.scope.experiment_run_id is None


def test_env_parses_nested_scope_experiment_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parse the experiment run scope field through the delimiter."""
    run_id = uuid.uuid4()
    monkeypatch.setenv("KITARU_WORKER_SCOPE__EXPERIMENT_RUN_ID", str(run_id))

    config = WorkerConfig()

    assert config.scope.experiment_run_id == run_id


def test_env_parses_comma_separated_kinds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Split a single comma-separated kinds value into a list."""
    monkeypatch.setenv("KITARU_WORKER_SCOPE__KINDS", "import")

    config = WorkerConfig()

    assert config.scope.kinds == [JobKind.IMPORT]


def test_env_parses_comma_separated_agent_version_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Split comma-separated agent version ids into a list of uuids."""
    first, second = uuid.uuid4(), uuid.uuid4()
    monkeypatch.setenv("KITARU_WORKER_SCOPE__AGENT_VERSION_IDS", f"{first},{second}")

    config = WorkerConfig()

    assert config.scope.agent_version_ids == [first, second]


def test_env_parses_json_array_scope_lists(monkeypatch: pytest.MonkeyPatch) -> None:
    """Accept the standard JSON array encoding for scope list fields too."""
    monkeypatch.setenv("KITARU_WORKER_SCOPE__KINDS", '["import", "score"]')

    config = WorkerConfig()

    assert config.scope.kinds == [JobKind.IMPORT, JobKind.SCORE]


def test_config_is_frozen() -> None:
    """Reject attribute assignment on a constructed config."""
    config = WorkerConfig()

    with pytest.raises(ValidationError, match="frozen"):
        config.concurrency = 2
