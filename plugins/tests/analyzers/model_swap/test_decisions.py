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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Tests for decision-model judges (Jev, Clef, OpenAI Decisions)."""

import json
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from kitaru_model_swap.analyzer import run_model_swap
from kitaru_model_swap.decisions import (
    QUESTION,
    build_request,
    classify,
    get_decision_key,
    read_decision,
)
from kitaru_model_swap.endpoint import MissingCredentialError
from kitaru_model_swap.params import DecisionJudge, ModelSwapParams

from .helpers import StubClient, completion
from .test_analyzer import CHEAP, mirror, report

CLEF_URL = (
    "https://api.cloudflare.com/client/v4/accounts/acc/ai/run/@cf/cloudflare/clef"
)


def judge(**fields: Any) -> DecisionJudge:
    return DecisionJudge.model_validate(fields)


def test_jev_request_uses_system_one() -> None:
    url, body = build_request(judge(kind="jev"), "refund(98)", "escalate()")

    assert url == "https://api.typesafe.ai/v1/systemone"
    # The judge is never told which answer is today's.
    assert body["state"] == {"answer_a": "refund(98)", "answer_b": "escalate()"}
    assert body["questions"][QUESTION]["type"] == "noul"
    assert body["model"] == "jev-latest"


def test_clef_request_posts_to_the_workers_ai_url() -> None:
    url, body = build_request(
        judge(kind="clef", base_url=CLEF_URL, api_key_env="CLOUDFLARE_API_TOKEN"),
        "a",
        "b",
    )

    assert url == CLEF_URL
    assert body["model"] == "clef"
    assert body["questions"][QUESTION]["type"] == "noul"


def test_openai_decisions_request_uses_a_predicate() -> None:
    url, body = build_request(judge(kind="openai_decisions"), 'say "hi"', "b")

    assert url == "https://api.openai.com/v1/decisions"
    assert body["model"] == "gpt-6-luna"
    assert body["questions"] == [
        {
            "type": "predicate",
            "name": QUESTION,
            "instructions": body["questions"][0]["instructions"],
        }
    ]
    # Answers are quoted so they read as data.
    assert '"say \\"hi\\""' in body["input"]


def test_a_custom_url_needs_an_explicit_key_variable() -> None:
    with pytest.raises(ValidationError, match="api_key_env"):
        judge(kind="openai_decisions", base_url="https://proxy.example/v1")
    with pytest.raises(ValidationError, match="api_key_env"):
        judge(kind="clef", base_url=CLEF_URL)

    assert (
        judge(
            kind="openai_decisions",
            base_url="https://proxy.example/v1",
            api_key_env="K",
        ).api_key_env
        == "K"
    )


def test_explicit_nulls_keep_the_defaults() -> None:
    assert judge(kind="openai_decisions", model=None).model == "gpt-6-luna"
    assert judge(kind="jev", base_url=None).base_url == "https://api.typesafe.ai"


def test_judge_errors_name_one_judge_type() -> None:
    with pytest.raises(ValidationError) as error:
        ModelSwapParams.model_validate(
            {"candidates": [CHEAP], "judge": {"kind": "nope", "name": "x"}}
        )

    assert "judge.decision" in str(error.value)
    assert "judge.chat" not in str(error.value)


def test_clef_needs_an_account_url() -> None:
    with pytest.raises(ValidationError, match="base_url"):
        judge(kind="clef")


def test_decision_judges_follow_the_endpoint_rules() -> None:
    with pytest.raises(ValidationError):
        judge(kind="jev", api_key_env="KITARU_API_TOKEN")
    with pytest.raises(ValidationError):
        judge(kind="jev", base_url="http://remote.example")


@pytest.mark.parametrize(
    ("payload", "probability", "failure"),
    [
        ({"answers": {QUESTION: {"noul": 0.91}}}, 0.91, None),
        ({"answers": {QUESTION: 0.2}}, 0.2, None),
        (
            {"result": {"answers": {QUESTION: {"noul": 0.5}}}, "success": True},
            0.5,
            None,
        ),
        (
            {"answers": [{"type": "predicate", "name": QUESTION, "probability": 0.7}]},
            0.7,
            None,
        ),
        ({"answers": [{"type": "refusal", "name": QUESTION}]}, None, "refusal"),
        ({"answers": {QUESTION: {"noul": 1.4}}}, None, "unreadable_answer"),
        ({"answers": {}}, None, "unreadable_answer"),
        ([], None, "http_error"),
    ],
)
def test_reads_every_supported_answer_shape(
    payload: Any, probability: float | None, failure: str | None
) -> None:
    decision = read_decision(payload)

    assert decision.probability == probability
    assert decision.failure == failure


def test_missing_usage_is_charged_at_the_estimate() -> None:
    decision = read_decision(
        {"answers": {QUESTION: {"noul": 0.9}}}, estimated_tokens=321
    )

    assert decision.usage.input_tokens == 321


def test_billing_units_and_negative_counts() -> None:
    billed = read_decision(
        {"answers": {QUESTION: 0.9}, "usage": {"billing_units": 40}},
        estimated_tokens=999,
    )
    negative = read_decision(
        {"answers": {QUESTION: 0.9}, "usage": {"input_tokens": -50}}
    )

    # Billing units are not tokens, so the estimate is used.
    assert billed.usage.input_tokens == 999
    assert negative.usage.input_tokens == 0


def test_a_cloudflare_error_envelope_is_an_error() -> None:
    decision = read_decision({"success": False, "errors": [{"message": "no"}]})

    assert decision.failure == "http_error"


def test_usage_is_read_when_reported() -> None:
    decision = read_decision(
        {"answers": {QUESTION: {"noul": 0.9}}, "usage": {"input_tokens": 120}}
    )

    assert decision.usage.input_tokens == 120


@pytest.mark.parametrize(
    ("probability", "expected"),
    [(0.8, True), (0.95, True), (0.2, False), (0.05, False), (0.5, None), (None, None)],
)
def test_probabilities_map_to_equivalent_different_or_unsure(
    probability: float | None, expected: bool | None
) -> None:
    assert classify(probability, 0.8) is expected


def test_missing_key_names_the_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)

    with pytest.raises(MissingCredentialError, match="TYPESAFE_API_KEY"):
        get_decision_key(judge(kind="jev"))


def changed_arguments(sessions: list[Any]) -> Any:
    """Answer with the recorded tools but different arguments."""
    replay = mirror(sessions)

    def reply(body: dict[str, Any]) -> httpx.Response:
        message = replay(body).json()["choices"][0]["message"]
        return completion(
            tool_calls=[
                (call["function"]["name"], {"changed": True})
                for call in message.get("tool_calls") or []
            ],
            text=message["content"],
        )

    return reply


class DecisionServer:
    """Candidates change every argument; the judge returns a set probability.

    The judge is stable: asked again about the same two answers, in either
    order, it gives the same probability.
    """

    def __init__(self, sessions: list[Any], probabilities: list[float]) -> None:
        self.candidate = changed_arguments(sessions)
        self.probabilities = iter(probabilities)
        self.rulings: dict[frozenset[str], float] = {}
        self.judge_requests: list[dict[str, Any]] = []
        self.judge_headers: list[httpx.Headers] = []

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if request.url.path.endswith("/v1/systemone"):
            self.judge_requests.append(body)
            self.judge_headers.append(request.headers)
            return httpx.Response(
                200,
                json={
                    "model": "jev",
                    "answers": {QUESTION: {"noul": self._rule(body["state"])}},
                    "usage": {"input_tokens": 100},
                },
            )
        return self.candidate(body)

    def _rule(self, state: dict[str, str]) -> float:
        pair = frozenset((state["answer_a"], state["answer_b"]))
        if pair not in self.rulings:
            self.rulings[pair] = next(self.probabilities)
        return self.rulings[pair]


async def test_jev_judge_end_to_end(
    stub_client: StubClient,
    quickstart_sessions: list[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-test")
    server = DecisionServer(quickstart_sessions, [0.95, 0.5, 0.05])
    ids = list(stub_client._by_id)

    insights = await run_model_swap(
        ids,
        ModelSwapParams.model_validate(
            {
                "candidates": [CHEAP | {"model": "changed"}],
                "max_calls": 3,
                "max_spend_usd": "1",
                "judge": {
                    "kind": "jev",
                    "price": {"input_per_mtok": "0.042", "output_per_mtok": "0"},
                },
            }
        ),
        transport=server.transport(),
    )
    result = report({insight.name: insight for insight in insights})
    candidate = result["candidates"][0]

    # Three rulings, then the two definite ones asked again in the other order.
    assert len(server.judge_requests) == 5
    assert result["judge_check"] == {
        "checked": 2,
        "changed": 0,
        "unsure": 0,
        "unanswered": 0,
    }
    assert server.judge_headers[0]["authorization"] == "Bearer ts-test"
    assert result["judge"] == "jev (jev-latest)"
    assert candidate["judged_equivalent"] == 1
    assert candidate["judged_different"] == 1
    assert candidate["judged_unsure"] == 1
    probabilities = [d["judge_probability"] for d in result["disagreements"]]
    # Review starts with answers the judge found different.
    assert probabilities[0] == 0.05
    assert sorted(probabilities) == [0.05, 0.5, 0.95]


async def test_a_judge_failure_leaves_the_verdict_open(
    stub_client: StubClient,
    quickstart_sessions: list[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-test")
    candidate = changed_arguments(quickstart_sessions)

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/v1/systemone"):
            return httpx.Response(500)
        return candidate(json.loads(request.content))

    insights = await run_model_swap(
        list(stub_client._by_id),
        ModelSwapParams.model_validate(
            {
                "candidates": [CHEAP | {"model": "changed"}],
                "max_calls": 2,
                "judge": {"kind": "jev"},
            }
        ),
        transport=httpx.MockTransport(handle),
    )
    result = report({insight.name: insight for insight in insights})

    assert {d["judge_probability"] for d in result["disagreements"]} == {None}
    assert {d["judge_failure"] for d in result["disagreements"]} == {"http_error"}
    assert result["candidates"][0]["judge_failures"] == {"http_error": 2}
    assert result["candidates"][0]["agreement"] == {"same_tool_different_args": 2}


async def test_the_judge_does_not_choose_which_disagreements_are_kept(
    stub_client: StubClient,
    quickstart_sessions: list[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-test")
    params = {
        "candidates": [CHEAP | {"model": "changed"}],
        "max_calls": 4,
        "max_disagreements": 2,
    }

    async def kept(judged: bool) -> set[str]:
        server = DecisionServer(quickstart_sessions, [0.99, 0.99, 0.01, 0.01])
        extra = {"judge": {"kind": "jev"}} if judged else {}
        insights = await run_model_swap(
            list(stub_client._by_id),
            ModelSwapParams.model_validate(params | extra),
            transport=server.transport(),
        )
        result = report({insight.name: insight for insight in insights})
        return {d["node_id"] for d in result["disagreements"]}

    assert await kept(judged=True) == await kept(judged=False)
