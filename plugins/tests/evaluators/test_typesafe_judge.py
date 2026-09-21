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
"""Tests for the judge evaluator over a fake TypeSafe HTTP API."""

import json
import os
from collections.abc import Callable
from importlib.resources import files
from pathlib import Path
from typing import Any

import httpx2
import pytest
from typesafe_helpers import build_view_from_imported
from typesafe_sdk import RetryPolicy, TypeSafeClient

from kitaru.api_models.v1.session import SessionDetailResponse
from kitaru.task.evaluator import SessionView, call_evaluator
from kitaru.task.importer import ImportedSession
from kitaru_langfuse_importer.importer import parse
from kitaru_typesafe_evaluator import judge as judge_module
from kitaru_typesafe_evaluator.connection import TypeSafeConnection
from kitaru_typesafe_evaluator.judge import judge

_LIVE_TEST_ENV = "KITARU_TYPESAFE_LIVE_TEST"


def _live_test_skip_reason() -> str | None:
    """State what to set to run the live TypeSafe test, or None to run it."""
    if os.environ.get(_LIVE_TEST_ENV) != "1":
        return f"set {_LIVE_TEST_ENV}=1 to run the live TypeSafe test"
    if not os.environ.get("TYPESAFE_API_KEY"):
        return f"{_LIVE_TEST_ENV}=1 is set; also set TYPESAFE_API_KEY to run it"
    return None


PARAMS: dict[str, Any] = {
    "questions": {
        "invented_timeline": {
            "type": "noul",
            "instructions": "Does `final_answer` promise days?",
            "pass_when": "no",
        },
        "failure_mode": {
            "type": "choice",
            "instructions": "Which?",
            "criteria": {"fine": None, "invented_fact": None},
            "pass_when": ["fine"],
        },
    }
}
OK_BODY = {
    "model": "jev-1.13.0",
    "answers": {
        "invented_timeline": {"type": "noul", "noul": 0.94},
        "failure_mode": {
            "type": "choice",
            "choice": "invented_fact",
            "confidence": 0.9,
            "probabilities": {"fine": 0.1, "invented_fact": 0.9},
        },
    },
    "usage": {"input_tokens": 100, "output_tokens": 10},
}


def _view() -> SessionView:
    session = SessionDetailResponse.model_construct(
        inputs="Refund please", outputs="Done in 5-7 days.", input_text_selector=None
    )
    return SessionView(session=session, nodes=[])


@pytest.fixture
def fake_api(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[int, Any], list[dict[str, Any]]]:
    """Serve one canned HTTP response through the real SDK and record request bodies."""

    def install(status: int, body: Any) -> list[dict[str, Any]]:
        seen: list[dict[str, Any]] = []

        def handle(request: httpx2.Request) -> httpx2.Response:
            seen.append(json.loads(request.content))
            return (
                httpx2.Response(status, json=body)
                if not isinstance(body, str)
                else httpx2.Response(status, text=body)
            )

        monkeypatch.setattr(
            judge_module,
            "_build_client",
            lambda: TypeSafeClient(
                api_key="test",
                transport=httpx2.MockTransport(handle),
                retry=RetryPolicy(max_retries=0),
            ),
        )
        return seen

    return install


def test_returns_one_result_per_question(
    fake_api: Callable[..., list[dict[str, Any]]],
) -> None:
    seen = fake_api(200, OK_BODY)
    results = judge(_view(), **PARAMS)
    assert [(r.name, r.passed) for r in results] == [
        ("invented_timeline", False),
        ("failure_mode", False),
    ]
    assert seen[0]["state"] == {
        "request": "Refund please",
        "tool_calls": [],
        "final_answer": "Done in 5-7 days.",
    }
    assert seen[0]["questions"]["invented_timeline"] == {
        "type": "noul",
        "instructions": "Does `final_answer` promise days?",
    }
    assert seen[0]["model"] == "jev-latest"


def test_model_param_reaches_the_request(
    fake_api: Callable[..., list[dict[str, Any]]],
) -> None:
    seen = fake_api(200, OK_BODY)
    judge(_view(), **(PARAMS | {"model": "jev-1.13.0"}))
    assert seen[0]["model"] == "jev-1.13.0"


async def test_results_pass_the_worker_contract(
    fake_api: Callable[..., list[dict[str, Any]]],
) -> None:
    """The worker's own validation accepts what judge returns."""
    fake_api(200, OK_BODY)
    results = await call_evaluator("typesafe-judge", judge, _view(), PARAMS)
    assert len(results) == 2


def test_too_large_state_writes_unavailable_rows(
    fake_api: Callable[..., list[dict[str, Any]]],
) -> None:
    fake_api(400, {"detail": {"error_type": "max_tokens_exceeded"}})
    results = judge(_view(), **PARAMS)
    assert [(r.name, r.value) for r in results] == [
        ("invented_timeline", "unavailable"),
        ("failure_mode", "unavailable"),
    ]
    explanation = results[0].explanation
    assert explanation is not None
    assert explanation == (
        "The 'outcome' state of this session is over jev's input limit. "
        "Narrow it with include."
    )


def test_too_large_full_state_still_suggests_the_outcome_view(
    fake_api: Callable[..., list[dict[str, Any]]],
) -> None:
    fake_api(400, {"detail": {"error_type": "max_tokens_exceeded"}})
    results = judge(_view(), **(PARAMS | {"state": "full"}))
    explanation = results[0].explanation
    assert explanation is not None
    assert explanation == (
        "The 'full' state of this session is over jev's input limit. "
        "Use the 'outcome' view or narrow it with include."
    )


def test_other_bad_request_fails_the_task(
    fake_api: Callable[..., list[dict[str, Any]]],
) -> None:
    fake_api(400, "Too many score levels. Must have at most 10 levels.")
    with pytest.raises(Exception, match="Too many score levels"):
        judge(_view(), **PARAMS)


def test_rejected_key_names_the_env_var(
    fake_api: Callable[..., list[dict[str, Any]]],
) -> None:
    fake_api(401, {"detail": "invalid api key"})
    with pytest.raises(RuntimeError, match="TYPESAFE_API_KEY"):
        judge(_view(), **PARAMS)


def test_missing_key_names_the_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="TYPESAFE_API_KEY"):
        judge(_view(), **PARAMS)


def test_server_error_fails_the_task(
    fake_api: Callable[..., list[dict[str, Any]]],
) -> None:
    fake_api(500, {"detail": "boom"})
    with pytest.raises(Exception, match="500"):
        judge(_view(), **PARAMS)


def test_invalid_params_send_nothing(
    fake_api: Callable[..., list[dict[str, Any]]],
) -> None:
    seen = fake_api(200, OK_BODY)
    with pytest.raises(ValueError):
        judge(
            _view(),
            questions={"q": {"type": "noul", "instructions": "About `system_prompt`?"}},
        )
    assert seen == []


def test_missing_answer_fails_the_task(
    fake_api: Callable[..., list[dict[str, Any]]],
) -> None:
    fake_api(
        200,
        OK_BODY | {"answers": {"invented_timeline": {"type": "noul", "noul": 0.94}}},
    )
    with pytest.raises(RuntimeError, match="failure_mode"):
        judge(_view(), **PARAMS)


TRACES = (
    Path(__file__).parents[3]
    / "examples/python/pydantic_ai_ticket_resolver/traces/langfuse-traces.jsonl"
)


@pytest.mark.skipif(
    _live_test_skip_reason() is not None, reason=_live_test_skip_reason() or ""
)
def test_live_flags_the_invented_refund_timelines() -> None:
    """jev finds the five quickstart replies that promise days no tool returned."""
    question = {
        "type": "noul",
        "pass_when": "no",
        "instructions": (
            "Does `final_answer` promise the customer a specific number of days or a "
            "date, where that number or date does not appear in any `tool_calls` "
            "result?"
        ),
    }
    flagged: list[str] = []
    for imported in parse(TRACES.read_bytes(), {"source_instance": "live-test"}):
        assert isinstance(imported, ImportedSession), imported
        result = judge(
            build_view_from_imported(imported),
            questions={"invented_timeline": question},
        )[0]
        if result.passed is not True:
            flagged.append(imported.external_id[-3:])
    assert flagged == ["001", "003", "004", "007", "009"]


def test_shipped_connection_schema_matches_the_model() -> None:
    """The JSON file users pass to --connection-schema cannot drift from the model."""
    shipped = json.loads(
        files("kitaru_typesafe_evaluator")
        .joinpath("connection-schema.json")
        .read_text()
    )
    assert shipped == TypeSafeConnection.model_json_schema()
