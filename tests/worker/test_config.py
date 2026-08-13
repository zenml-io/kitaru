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

from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import LabelSelector, WorkerScope
from kitaru.worker.config import WorkerConfig
from kitaru.worker.worker import default_worker_name


def test_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fields fall back to their defaults when no environment is set."""
    monkeypatch.delenv("KITARU_WORKER_CONCURRENCY", raising=False)
    config = WorkerConfig()
    assert config.name is None
    assert config.scope == WorkerScope()
    assert config.concurrency == 1
    assert config.claim_batch_size is None
    assert config.timeout is None
    assert config.metadata == {}


def test_concurrency_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """KITARU_WORKER_CONCURRENCY sets the concurrency field."""
    monkeypatch.setenv("KITARU_WORKER_CONCURRENCY", "5")
    config = WorkerConfig()
    assert config.concurrency == 5


def test_explicit_config_values_override_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit constructor value wins over the matching environment variable."""
    monkeypatch.setenv("KITARU_WORKER_CONCURRENCY", "4")
    config = WorkerConfig(concurrency=2)
    assert config.concurrency == 2


def test_scope_kinds_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """KITARU_WORKER_SCOPE__KINDS takes a JSON list of task kinds."""
    monkeypatch.setenv("KITARU_WORKER_SCOPE__KINDS", '["importer"]')
    config = WorkerConfig()
    assert config.scope.kinds == [TaskKind.IMPORTER]


def test_scope_selectors_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """KITARU_WORKER_SCOPE__SELECTORS takes a JSON list of selector objects."""
    monkeypatch.setenv(
        "KITARU_WORKER_SCOPE__SELECTORS",
        '[{"key": "agent_version", "values": ["v1"], "required": false}]',
    )
    config = WorkerConfig()
    assert config.scope.selectors == [
        LabelSelector(key="agent_version", values=["v1"], required=False)
    ]


def test_scope_job_id_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """KITARU_WORKER_SCOPE__JOB_ID takes a bare uuid value."""
    job_id = uuid.uuid4()
    monkeypatch.setenv("KITARU_WORKER_SCOPE__JOB_ID", str(job_id))
    config = WorkerConfig()
    assert config.scope.job_id == job_id


def test_metadata_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """KITARU_WORKER_METADATA takes a JSON object."""
    monkeypatch.setenv("KITARU_WORKER_METADATA", '{"region": "eu-west-1"}')
    config = WorkerConfig()
    assert config.metadata == {"region": "eu-west-1"}


def test_config_is_frozen(monkeypatch: pytest.MonkeyPatch) -> None:
    """The config cannot be mutated after construction."""
    config = WorkerConfig()
    with pytest.raises(Exception):  # noqa: B017 - pydantic raises ValidationError
        config.concurrency = 5


@pytest.mark.parametrize(
    ("hostname", "pid", "expected"),
    [
        ("worker-1.local", 42, "worker-1-local-42"),
        ("WORKER_1", 7, "WORKER_1-7"),
        ("--weird..host--", 1, "weird--host---1"),
    ],
)
def test_default_worker_name_sanitizes(
    monkeypatch: pytest.MonkeyPatch, hostname: str, pid: int, expected: str
) -> None:
    """The default name replaces disallowed characters and trims edges."""
    monkeypatch.setattr("socket.gethostname", lambda: hostname)
    monkeypatch.setattr("os.getpid", lambda: pid)
    assert default_worker_name() == expected
