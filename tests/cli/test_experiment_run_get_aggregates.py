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
"""Experiment-run CLI behavior for evaluation aggregates on `run get`."""

import json
import uuid

import pytest
from tests.cli.experiment_run_fakes import StubRunsClient, install_runs_client

from kitaru.api_models.v1.experiment_run import (
    EvaluationAggregateResponse,
    ExperimentRunStatus,
)
from kitaru.cli import app as app_module
from kitaru.client.exceptions import APIError


@pytest.fixture
def argv_client(monkeypatch: pytest.MonkeyPatch) -> StubRunsClient:
    """Route public run commands through one recording client."""
    return install_runs_client(monkeypatch)


@pytest.mark.parametrize("status", list(ExperimentRunStatus))
def test_public_run_get_preserves_default_inspection(
    argv_client: StubRunsClient,
    capsys: pytest.CaptureFixture[str],
    status: ExperimentRunStatus,
) -> None:
    """Default inspection returns only the run, regardless of terminal status."""
    client = argv_client
    run = client.created_run.model_copy(update={"status": status})
    client.get_responses = [run]

    assert (
        app_module.main(
            ["experiment", "run", "get", str(client.run_id), "--output", "json"]
        )
        == 0
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert captured.err == ""
    assert payload["command"] == "experiment.run.get"
    assert payload["item"] == run.model_dump(mode="json")
    assert client.get_calls == 1
    assert client.aggregate_calls == []


@pytest.mark.parametrize("status", list(ExperimentRunStatus))
def test_public_run_get_includes_evaluation_aggregates(
    argv_client: StubRunsClient,
    capsys: pytest.CaptureFixture[str],
    status: ExperimentRunStatus,
) -> None:
    """Aggregate inspection adds stored evaluation evidence to the run item."""
    client = argv_client
    client.get_responses = [client.created_run.model_copy(update={"status": status})]
    evaluator_version_id = uuid.uuid4()
    replay_id = uuid.uuid4()
    client.evaluation_aggregates = [
        EvaluationAggregateResponse.model_validate(
            {
                "name": "quality",
                "evaluator_version_id": evaluator_version_id,
                "evaluator_name": "quality-check",
                "evaluator_version": 2,
                "data_type": "bool",
                "baseline": {"count": 2, "mean": 0.5, "pass_rate": 0.5},
                "result": {"count": 2, "mean": 1.0, "pass_rate": 0.5},
                "replays": [
                    {
                        "replay_id": replay_id,
                        "baseline": {"score": False, "passed": False},
                        "result": {"score": True, "passed": False},
                    }
                ],
            }
        )
    ]

    assert (
        app_module.main(
            [
                "experiment",
                "run",
                "get",
                str(client.run_id),
                "--aggregate",
                "--output",
                "json",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert captured.err == ""
    assert payload["command"] == "experiment.run.get"
    assert payload["ok"] is True
    assert payload["item"]["id"] == str(client.run_id)
    assert payload["item"]["status"] == status.value
    [aggregate] = payload["item"]["evaluation_aggregates"]
    assert aggregate["name"] == "quality"
    assert aggregate["evaluator_version_id"] == str(evaluator_version_id)
    assert aggregate["evaluator_name"] == "quality-check"
    assert aggregate["evaluator_version"] == 2
    assert aggregate["data_type"] == "bool"
    assert aggregate["baseline"]["count"] == 2
    assert aggregate["baseline"]["mean"] == 0.5
    assert aggregate["result"]["count"] == 2
    assert aggregate["result"]["mean"] == 1.0
    assert aggregate["result"]["pass_rate"] == 0.5
    [replay] = aggregate["replays"]
    assert replay["replay_id"] == str(replay_id)
    assert replay["baseline"]["score"] is False
    assert replay["result"]["score"] is True
    assert replay["result"]["passed"] is False
    assert client.get_calls == 1
    assert client.aggregate_calls == [client.run_id]


def test_public_run_get_includes_empty_evaluation_aggregates(
    argv_client: StubRunsClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """A run with no evaluation groups remains a successful aggregate lookup."""
    client = argv_client

    assert (
        app_module.main(
            [
                "experiment",
                "run",
                "get",
                str(client.run_id),
                "--aggregate",
                "--output",
                "json",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert captured.err == ""
    assert payload["item"]["status"] == "running"
    assert payload["item"]["evaluation_aggregates"] == []
    assert client.aggregate_calls == [client.run_id]


@pytest.mark.parametrize("method", ["get", "get_evaluation_aggregates"])
@pytest.mark.parametrize(
    ("status_code", "kind", "exit_code"),
    [(404, "not_found", 4), (401, "authentication_failed", 3)],
)
def test_public_run_get_aggregate_errors_remain_structured(
    argv_client: StubRunsClient,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    method: str,
    status_code: int,
    kind: str,
    exit_code: int,
) -> None:
    """A failed run or aggregate request emits one error without partial output."""
    client = argv_client

    async def fail_get(run_id: uuid.UUID) -> None:
        assert run_id == client.run_id
        raise APIError(status_code, "Inspection failed")

    monkeypatch.setattr(client.experiment_runs, method, fail_get)
    assert (
        app_module.main(
            [
                "experiment",
                "run",
                "get",
                str(client.run_id),
                "--aggregate",
                "--output",
                "json",
            ]
        )
        == exit_code
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["command"] == "experiment.run.get"
    assert payload["error"]["kind"] == kind
    assert client.get_calls == (1 if method == "get_evaluation_aggregates" else 0)
    assert client.aggregate_calls == []
