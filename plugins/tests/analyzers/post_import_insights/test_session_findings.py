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
"""Tests for the session findings analyzer."""

import re
import uuid
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru_post_import_insights import session_findings
from kitaru_post_import_insights.openai_generator import (
    MissingOpenAICredential,
    OpenAIInsightGenerationError,
)
from kitaru_post_import_insights.session_findings import (
    SESSION_FINDINGS_METADATA_KEY,
    Harm,
    _Check,
    _MergedFinding,
    _MergePlan,
    _ReaderPlan,
    analyze_session_findings,
    find_problems,
    render_session,
)

START = datetime(2026, 11, 2, 9, tzinfo=UTC)
OWNER_ID = uuid.UUID("01990000-0000-7000-8000-000000000001")
AGENT_ID = uuid.UUID("01990000-0000-7000-8000-000000000002")
SYSTEM_PROMPT = "Refund defective items. Never refund final-sale items."


def _node(session_id: uuid.UUID, position: int, **fields: Any) -> SessionNodeResponse:
    return SessionNodeResponse(
        id=uuid.uuid4(),
        session_id=session_id,
        external_id=f"node-{position}",
        parent_external_id=None,
        links=[],
        status=NodeStatus.COMPLETED,
        started_at=START,
        metadata={},
        **fields,
    )


def _session(
    number: int, reply: str, *, outputs: Any = None
) -> SessionWithNodesResponse:
    session_id = uuid.UUID(f"01990000-0000-7000-8000-{100 + number:012d}")
    message = f"Ticket {number}: the strap snapped."
    messages = {"messages": [{"content": SYSTEM_PROMPT}, {"content": message}]}
    return SessionWithNodesResponse(
        session=SessionDetailResponse(
            id=session_id,
            owner_id=OWNER_ID,
            created=START,
            updated=START,
            started_at=START + timedelta(minutes=number),
            agent_id=AGENT_ID,
            number=number,
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.COMPLETED,
            inputs=None,
            outputs=outputs,
            metadata={},
            cost=None,
            tokens=None,
            llm_call_count=2,
            tool_call_count=1,
        ),
        nodes=[
            _node(
                session_id,
                0,
                node_type=NodeType.LLM_CALL,
                name="chat",
                inputs=messages,
                outputs=None,
                input_text_selector="/messages/1/content",
                system_prompt_selector="/messages/0/content",
            ),
            _node(
                session_id,
                1,
                node_type=NodeType.TOOL_CALL,
                name="issue_refund",
                tool_name="issue_refund",
                inputs={"amount": 75},
                outputs={"accepted": True},
            ),
            _node(
                session_id,
                2,
                node_type=NodeType.LLM_CALL,
                name="chat",
                inputs=messages,
                outputs=reply,
                output_text_selector="",
            ),
        ],
    )


class FakeOpenAI:
    """OpenAI client returning queued structured outputs and fixed probabilities."""

    def __init__(self, plans: list[Any], held: dict[str, dict[int, float]]) -> None:
        self.plans = plans
        self.held = held
        self.prompts: list[str] = []
        self.cache_keys: list[str] = []
        self.responses = SimpleNamespace(parse=self._parse)
        self.decisions = SimpleNamespace(create=self._decide)

    async def _parse(self, *, input: str, **_kwargs: Any) -> SimpleNamespace:
        self.prompts.append(input)
        return SimpleNamespace(output_parsed=self.plans.pop(0))

    async def _decide(
        self,
        *,
        input: str,
        questions: list[dict[str, str]],
        prompt_cache_key: str,
        **_kwargs: Any,
    ) -> SimpleNamespace:
        self.cache_keys.append(prompt_cache_key)
        number = int(re.findall(r"Ticket (\d+):", input)[0])
        answers = []
        for question in questions:
            probability = self.held.get(question["name"], {}).get(number)
            answers.append(
                SimpleNamespace(type="refusal", name=question["name"])
                if probability is None
                else SimpleNamespace(
                    type="predicate", name=question["name"], probability=probability
                )
            )
        return SimpleNamespace(answers=answers)


def _check(name: str) -> _Check:
    return _Check(name=name, check=f"The session shows {name}.")


def _finding(
    checks: list[str], harm: Harm, drop: tuple[int, ...] = ()
) -> _MergedFinding:
    return _MergedFinding(
        title=f"{harm} finding",
        checks=checks,
        drop_sessions=list(drop),
        harm=harm,
        example=f"Example of {harm}.",
    )


def test_render_session_shows_the_whole_structured_answer() -> None:
    """Keep a structured answer's decision fields next to its reply."""
    answer = {"action": "refund", "reply": "Refund issued."}
    transcript = render_session(_session(1, "unused", outputs=answer))

    assert transcript.splitlines() == [
        "Started 2026-11-02 09:01 UTC",
        "User: Ticket 1: the strap snapped.",
        'Tool issue_refund({"amount": 75}) -> {"accepted": true}',
        'Agent answer: {"action": "refund", "reply": "Refund issued."}',
    ]


def test_render_session_falls_back_to_the_last_model_reply() -> None:
    """Read the reply through the last model call's root selector."""
    transcript = render_session(_session(1, "Refund issued."))

    assert transcript.splitlines()[-1] == "Agent answer: Refund issued."


async def test_findings_count_only_sessions_where_a_check_held() -> None:
    """Count confident answers only, and let code decide each finding's sessions."""
    transcripts = [render_session(_session(n, "Done.")) for n in range(4)]
    client = FakeOpenAI(
        plans=[
            _ReaderPlan(
                checks=[_check("final_sale"), _check("unsure"), _check("tone")]
            ),
            _MergePlan(
                findings=[
                    _finding(["tone"], "low"),
                    _finding(["final_sale", "invented"], "high", drop=(2,)),
                    _finding(["unsure"], "medium"),
                ]
            ),
        ],
        held={
            "final_sale": {0: 0.95, 1: 0.1, 2: 0.9, 3: 0.1},
            "unsure": {1: 0.5},
            "tone": {0: 0.9, 1: 0.9, 3: 0.85},
        },
    )

    findings = await find_problems(
        client,
        transcripts,
        SYSTEM_PROMPT,
        context="The refund tool skips the approval limit.",
        model="reader",
        decisions_model="decider",
    )

    assert [(f.harm, f.sessions) for f in findings] == [
        ("high", [0]),
        ("low", [0, 1, 3]),
    ]
    assert findings[0].checks == ["The session shows final_sale."]
    reader_prompt, merge_prompt = client.prompts
    assert "The refund tool skips the approval limit." in reader_prompt
    assert "- final_sale: The session shows final_sale. Held in sessions [0, 2]." in (
        merge_prompt
    )
    assert "unsure" not in merge_prompt
    assert len(client.cache_keys) == 4
    assert len(set(client.cache_keys)) == 1


async def test_analyzer_writes_one_insight_per_finding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Name the verified sessions by ID, oldest first, in each insight."""
    sessions = [_session(2, "Refunded."), _session(1, "Ok.")]
    by_id = {s.session.id: s for s in sessions}

    async def get_with_nodes(session_id: uuid.UUID) -> SessionWithNodesResponse:
        return by_id[session_id]

    kitaru = SimpleNamespace(sessions=SimpleNamespace(get_with_nodes=get_with_nodes))
    client = FakeOpenAI(
        plans=[
            _ReaderPlan(checks=[_check("final_sale")]),
            _MergePlan(findings=[_finding(["final_sale"], "high")]),
        ],
        held={"final_sale": {2: 0.97, 1: 0.05}},
    )
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(
        session_findings,
        "import_openai",
        lambda: SimpleNamespace(AsyncOpenAI=lambda **_kwargs: client),
    )
    monkeypatch.setattr(
        session_findings, "KitaruAPIClient", lambda: nullcontext(kitaru)
    )

    insights = await analyze_session_findings([s.session.id for s in sessions])

    [insight] = insights
    assert insight.name == "session-finding-1"
    assert insight.description == "Verified in 1 of 2 imported sessions. Harm: high."
    assert insight.data.model_dump() == {"type": "text", "content": "Example of high."}
    assert insight.metadata[SESSION_FINDINGS_METADATA_KEY] == {
        "harm": "high",
        "checks": ["The session shows final_sale."],
        "contributing_session_ids": [str(sessions[0].session.id)],
    }
    reader_prompt = client.prompts[0]
    assert reader_prompt.index("Started 2026-11-02 09:01") < (
        reader_prompt.index("Started 2026-11-02 09:02")
    )
    assert "Session 0" not in reader_prompt


async def test_missing_credential_fails_before_reading_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse to run without an OpenAI key instead of returning no findings."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(session_findings, "KitaruAPIClient", pytest.fail)

    with pytest.raises(MissingOpenAICredential):
        await analyze_session_findings([uuid.uuid4()])


async def test_failed_decisions_request_names_only_the_error_type() -> None:
    """Fail loudly without passing provider error text into task diagnostics."""

    async def refuse(**_kwargs: Any) -> None:
        raise PermissionError("organization org-secret lacks Decisions access")

    client = FakeOpenAI(plans=[_ReaderPlan(checks=[_check("final_sale")])], held={})
    client.decisions = SimpleNamespace(create=refuse)

    with pytest.raises(OpenAIInsightGenerationError) as raised:
        await find_problems(
            client,
            [render_session(_session(1, "Ok."))],
            SYSTEM_PROMPT,
            context=None,
            model="reader",
            decisions_model="decider",
        )

    assert str(raised.value) == "decisions request failed (PermissionError)"
