"""Worker configuration and scope tests."""

import uuid

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.task import LabelSelector, TaskKind, WorkerScope
from kitaru.worker.config import WorkerConfig


def test_config_reads_nested_scope_and_metadata_from_env(monkeypatch) -> None:
    job_id = uuid.uuid4()
    monkeypatch.setenv("KITARU_WORKER_CONCURRENCY", "4")
    monkeypatch.setenv("KITARU_WORKER_SCOPE__KINDS", '["importer"]')
    monkeypatch.setenv(
        "KITARU_WORKER_SCOPE__SELECTORS",
        '[{"key":"region","values":["eu"],"required":true}]',
    )
    monkeypatch.setenv("KITARU_WORKER_SCOPE__JOB_ID", str(job_id))
    monkeypatch.setenv("KITARU_WORKER_METADATA", '{"pool":"batch"}')

    config = WorkerConfig()

    assert config.concurrency == 4
    assert config.scope.kinds == [TaskKind.IMPORTER]
    assert config.scope.selectors == [
        LabelSelector(key="region", values=["eu"], required=True)
    ]
    assert config.scope.job_id == job_id
    assert config.metadata == {"pool": "batch"}


def test_explicit_config_values_override_environment(monkeypatch) -> None:
    monkeypatch.setenv("KITARU_WORKER_CONCURRENCY", "4")

    config = WorkerConfig(concurrency=2)

    assert config.concurrency == 2


@pytest.mark.parametrize(
    "kwargs",
    [
        {"kinds": []},
        {"selectors": []},
        {
            "selectors": [
                LabelSelector(key="x", values=["a"]),
                LabelSelector(key="x", values=["b"]),
            ]
        },
    ],
)
def test_scope_rejects_empty_or_duplicate_constraints(kwargs) -> None:
    with pytest.raises(ValidationError):
        WorkerScope(**kwargs)


def test_selector_rejects_empty_values() -> None:
    with pytest.raises(ValidationError):
        LabelSelector(key="x", values=[])


def test_config_is_frozen() -> None:
    config = WorkerConfig()

    with pytest.raises(ValidationError):
        config.concurrency = 2
