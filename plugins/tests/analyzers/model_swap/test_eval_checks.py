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
"""Tests for the checks that keep a comparison honest.

Cut-off replies, prompt candidates, a blind and stable judge, self-judging,
a missing noise floor, and a best mix confirmed on held-out sessions.
"""

import hashlib
import json
from typing import Any

import httpx
import pytest

from kitaru.api_models.v1.insight import TextInsightData
from kitaru.api_models.v1.session_node import SessionWithNodesResponse
from kitaru_model_swap import SUMMARY_INSIGHT_NAME
from kitaru_model_swap.analyzer import run_model_swap
from kitaru_model_swap.calls import Collection
from kitaru_model_swap.endpoint import replace_system_prompt, send
from kitaru_model_swap.params import Candidate, ModelSwapParams
from kitaru_model_swap.report import REPORT_SECTION_KEY

from .helpers import MockOpenAIServer, StubClient, completion
from .test_analyzer import CHEAP, REFERENCE, escalator, mirror, report, run

JUDGE = {"name": "judge", "base_url": "http://mock/v1", "model": "judge"}


@pytest.fixture
def server(quickstart_sessions: list[SessionWithNodesResponse]) -> MockOpenAIServer:
    return MockOpenAIServer(
        {"mirror": mirror(quickstart_sessions), "escalator": escalator}
    )


def changed_arguments(sessions: list[SessionWithNodesResponse]) -> Any:
    """Call the recorded tools with different arguments, so every call is judged."""
    replay = mirror(sessions)

    def reply(body: dict[str, Any]) -> httpx.Response:
        message = replay(body).json()["choices"][0]["message"]
        calls = message.get("tool_calls") or []
        if not calls:
            return completion(text="something else entirely")
        return completion(
            tool_calls=[(c["function"]["name"], {"changed": True}) for c in calls]
        )

    return reply


def length_cut(_: dict[str, Any]) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "choices": [
                {
                    "message": {"role": "assistant", "content": "The refund is"},
                    "finish_reason": "length",
                }
            ]
        },
    )


async def test_cut_off_replies_are_failures_not_disagreements(
    stub_client: StubClient,
) -> None:
    server = MockOpenAIServer({"cut": length_cut})

    insights = await run(
        stub_client, server, candidates=[CHEAP | {"model": "cut"}], max_calls=3
    )
    candidate = report(insights)["candidates"][0]

    assert candidate["failures"] == {"truncated": 3}
    assert candidate["answered"] == 0


async def test_responses_api_replies_hitting_the_limit_are_truncated() -> None:
    def handle(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "incomplete",
                "incomplete_details": {"reason": "max_output_tokens"},
                "output": [],
            },
        )

    item = Candidate.model_validate(
        {
            "name": "c",
            "base_url": "https://mock/v1",
            "model": "m",
            "provider_settings": {"api": "responses"},
        }
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        result = await send(
            client,
            item,
            api_key=None,
            messages=[{"role": "user", "content": "hi"}],
            tools=[],
        )

    assert result.failure == "truncated"


def test_a_replacement_prompt_takes_the_place_of_every_system_message() -> None:
    messages = [
        {"role": "system", "content": "Old rules."},
        {"role": "developer", "content": "More old rules."},
        {"role": "user", "content": "hi"},
    ]

    assert replace_system_prompt(messages, "Be brief.") == [
        {"role": "system", "content": "Be brief."},
        {"role": "user", "content": "hi"},
    ]
    assert replace_system_prompt([{"role": "user", "content": "hi"}], "P")[0] == {
        "role": "system",
        "content": "P",
    }


async def test_a_candidate_prompt_reaches_only_the_nodes_it_names(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    everywhere = CHEAP | {"name": "everywhere", "system_prompt": "Short."}
    nowhere = CHEAP | {
        "name": "nowhere",
        "model": "mirror",
        "system_prompt": "Short.",
        "system_prompt_nodes": ["no-such-node"],
    }

    await run(stub_client, server, candidates=[everywhere, nowhere], max_calls=3)
    systems = {
        r["model"]: r["messages"][0]["content"]
        for r in server.requests
        if r["messages"][0]["role"] == "system"
    }

    assert systems["escalator"] == "Short."
    assert systems["mirror"] != "Short."


def test_the_reference_cannot_carry_its_own_prompt() -> None:
    with pytest.raises(ValueError, match="reference"):
        Candidate.model_validate(REFERENCE | {"system_prompt": "Short."})


async def test_prompt_candidates_are_noted(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    cleaned = CHEAP | {
        "name": "cleaned",
        "model": "escalator",
        "system_prompt": "Short.",
    }

    insights = await run(
        stub_client, server, candidates=[REFERENCE, cleaned], max_calls=3
    )

    assert any(
        "cleaned use their own system prompt" in n for n in report(insights)["notes"]
    )


async def test_the_judge_is_not_told_which_answer_is_today(
    stub_client: StubClient, quickstart_sessions: list[SessionWithNodesResponse]
) -> None:
    server = MockOpenAIServer(
        {
            "changed": changed_arguments(quickstart_sessions),
            "judge": lambda _: completion(text='{"equivalent": true}'),
        }
    )

    await run(
        stub_client,
        server,
        candidates=[CHEAP | {"model": "changed"}],
        max_calls=8,
        judge=JUDGE,
    )
    prompts = [
        r["messages"][-1]["content"] for r in server.requests if r["model"] == "judge"
    ]
    recorded_first = [
        p.index(_recorded_marker(p, quickstart_sessions)) < p.index("<answer_b>")
        for p in prompts
    ]

    assert prompts
    assert all("<answer_a>" in p and "recorded" not in p.lower() for p in prompts)
    # Today's answer is not always in the same slot.
    assert any(recorded_first) and not all(recorded_first)


def _recorded_marker(prompt: str, _: list[SessionWithNodesResponse]) -> str:
    """The candidate's answers carry `"changed"`; the recorded ones do not."""
    a = prompt[prompt.index("<answer_a>") : prompt.index("<answer_b>")]
    return "<answer_a>" if '"changed"' not in a else "<answer_b>"


async def test_a_judge_that_flips_with_the_order_is_flagged(
    stub_client: StubClient, quickstart_sessions: list[SessionWithNodesResponse]
) -> None:
    def order_bound(body: dict[str, Any]) -> httpx.Response:
        # Rules "equivalent" only when today's answer comes first.
        prompt = body["messages"][-1]["content"]
        first = _recorded_marker(prompt, quickstart_sessions) == "<answer_a>"
        return completion(text=f'{{"equivalent": {str(first).lower()}}}')

    server = MockOpenAIServer(
        {"changed": changed_arguments(quickstart_sessions), "judge": order_bound}
    )

    insights = await run(
        stub_client,
        server,
        candidates=[CHEAP | {"model": "changed"}],
        max_calls=8,
        judge=JUDGE,
    )
    result = report(insights)

    assert result["judge_check"]["changed"] == result["judge_check"]["checked"] > 4
    assert any("weak evidence" in note for note in result["notes"])


async def test_a_judge_that_is_also_a_candidate_is_noted(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    insights = await run(
        stub_client,
        server,
        candidates=[REFERENCE, CHEAP],
        max_calls=3,
        judge=JUDGE | {"model": "escalator"},
    )

    assert any(
        "judge runs the same model as cheap-open-model" in note
        for note in report(insights)["notes"]
    )


async def test_a_run_without_a_reference_says_it_has_no_noise_floor(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    insights = await run(stub_client, server, candidates=[CHEAP], max_calls=3)

    assert any("no noise floor" in note for note in report(insights)["notes"])


def _learning(session_id: Any) -> bool:
    return int(hashlib.sha256(str(session_id).encode()).hexdigest(), 16) % 2 == 0


def learning_only(sessions: list[SessionWithNodesResponse]) -> Any:
    """Match the recording on learning sessions and escalate on held-out ones."""
    replay = mirror(sessions)
    params = ModelSwapParams.model_validate({"candidates": [REFERENCE]})
    collection = Collection(max_per_group=100)
    for session in sessions:
        collection.add_session(session, params)
    session_of = {
        json.dumps(call.request.messages, sort_keys=True): call.session_id
        for call in collection.calls
    }

    def reply(body: dict[str, Any]) -> httpx.Response:
        if _learning(session_of[json.dumps(body["messages"], sort_keys=True)]):
            return replay(body)
        return escalator(body)

    return reply


async def test_a_mix_that_fails_on_held_out_sessions_is_not_recommended(
    stub_client: StubClient, quickstart_sessions: list[SessionWithNodesResponse]
) -> None:
    server = MockOpenAIServer(
        {
            "mirror": mirror(quickstart_sessions),
            "learner": learning_only(quickstart_sessions),
        }
    )
    learner = CHEAP | {"name": "learner", "model": "learner"}
    honest = CHEAP | {"name": "honest", "model": "mirror"}

    overfit = report(
        await run(stub_client, server, candidates=[REFERENCE, learner], min_mix_calls=1)
    )
    control = report(
        await run(stub_client, server, candidates=[REFERENCE, honest], min_mix_calls=1)
    )

    groups = overfit["best_mix"]["groups"]
    # Nothing moves, and the report shows why: the picks failed on held-out
    # sessions rather than finding no fit at all.
    assert not any(item["candidate"] for item in groups)
    assert any(item["held_out"] == "failed" for item in groups)
    assert not any(c["goal"] == "Best mix" for c in overfit["best_for"])
    # The same steps move when the candidate holds on both halves.
    assert control["best_mix"] is not None
    assert any(item["candidate"] == "honest" for item in control["best_mix"]["groups"])


async def test_a_judge_keeps_its_own_instructions(
    stub_client: StubClient, quickstart_sessions: list[SessionWithNodesResponse]
) -> None:
    server = MockOpenAIServer(
        {
            "changed": changed_arguments(quickstart_sessions),
            "judge": lambda _: completion(text='{"equivalent": true}'),
        }
    )

    insights = await run(
        stub_client,
        server,
        candidates=[CHEAP | {"model": "changed"}],
        max_calls=3,
        # Copied from a candidate block; the agent's prompt must not reach it.
        judge=JUDGE | {"system_prompt": "You are a support agent."},
    )
    systems = [
        r["messages"][0]["content"] for r in server.requests if r["model"] == "judge"
    ]

    assert systems
    assert all("<answer_a>" in s for s in systems)
    assert report(insights)["candidates"][0]["judged_equivalent"] == 3


async def test_one_session_cannot_be_split_and_the_mix_says_so(
    stub_client: StubClient, quickstart_sessions: list[SessionWithNodesResponse]
) -> None:
    server = MockOpenAIServer({"mirror": mirror(quickstart_sessions)})
    honest = CHEAP | {"name": "honest", "model": "mirror"}

    insights = await run_model_swap(
        list(stub_client._by_id)[:1],
        ModelSwapParams.model_validate(
            {"candidates": [REFERENCE, honest], "min_mix_calls": 1}
        ),
        transport=server.transport(),
    )
    mix = report({i.name: i for i in insights})["best_mix"]
    moved = [item for item in mix["groups"] if item["candidate"]]

    assert sorted(mix["sessions"].values()) == [0, 1]
    # Every pick stays, flagged as unchecked rather than silently confirmed.
    assert moved
    assert {item["held_out"] for item in moved} <= {"too_few", "no_split"}


async def test_the_report_says_the_screen_rules_out_and_replays_decide(
    stub_client: StubClient, server: MockOpenAIServer
) -> None:
    insights = await run(
        stub_client, server, candidates=[REFERENCE, CHEAP], max_calls=3
    )
    section = insights[SUMMARY_INSIGHT_NAME].metadata[REPORT_SECTION_KEY]
    summary = insights[SUMMARY_INSIGHT_NAME].data

    assert isinstance(section, dict) and isinstance(summary, TextInsightData)
    assert "rules candidates out but does not pick a winner" in str(section["html"])
    assert "Stage 1 of 2" in summary.content
