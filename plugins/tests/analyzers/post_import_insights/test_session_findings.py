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

from kitaru.api_models.v1.insight import (
    EvidenceArtifact,
    RateArtifact,
    StepStoryArtifact,
    TextArtifact,
)
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
    _StoryStepOutput,
    analyze_session_findings,
    find_problems,
    render_session,
    select_reader_indices,
)

START = datetime(2026, 11, 2, 9, tzinfo=UTC)
OWNER_ID = uuid.UUID("01990000-0000-7000-8000-000000000001")
AGENT_ID = uuid.UUID("01990000-0000-7000-8000-000000000002")
IMPORT_ID = uuid.UUID("01990000-0000-7000-8000-000000000003")
SYSTEM_PROMPT = "Refund defective items. Never refund final-sale items."


def _node(session_id: uuid.UUID, position: int, **fields: Any) -> SessionNodeResponse:
    return SessionNodeResponse(
        id=uuid.UUID(
            f"01990000-0000-7000-8000-{session_id.int % 10000:04d}{position:08d}"
        ),
        session_id=session_id,
        external_id=f"node-{position}",
        parent_external_id=None,
        links=[],
        status=fields.pop("status", NodeStatus.COMPLETED),
        started_at=START,
        metadata={},
        **fields,
    )


def _session(
    number: int,
    reply: str,
    *,
    outputs: Any = None,
    status: SessionStatus = SessionStatus.COMPLETED,
    tool_status: NodeStatus = NodeStatus.COMPLETED,
    origin: SessionOrigin = SessionOrigin.IMPORTED,
    imported_from: str | None = "langfuse",
    import_id: uuid.UUID | None = IMPORT_ID,
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
            import_id=import_id if origin is SessionOrigin.IMPORTED else None,
            imported_from=imported_from if origin is SessionOrigin.IMPORTED else None,
            origin=origin,
            status=status,
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
                status=tool_status,
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


def _check(name: str, text: str | None = None) -> _Check:
    return _Check(name=name, check=text or f"The session shows {name}.")


def _finding(
    checks: list[str],
    harm: Harm,
    drop: tuple[int, ...] = (),
    **extra: Any,
) -> _MergedFinding:
    return _MergedFinding(
        title=extra.pop("title", f"{harm} finding"),
        checks=checks,
        drop_sessions=list(drop),
        harm=harm,
        example=extra.pop("example", f"Example of {harm}."),
        **extra,
    )


def _stub_kitaru(
    sessions: list[SessionWithNodesResponse],
    *,
    agent_name: str = "returns-agent",
    base_url: str = "https://kitaru.example.test/",
) -> SimpleNamespace:
    by_id = {s.session.id: s for s in sessions}

    async def get_with_nodes(session_id: uuid.UUID) -> SessionWithNodesResponse:
        return by_id[session_id]

    async def get_agent(_agent_id: uuid.UUID) -> SimpleNamespace:
        return SimpleNamespace(name=agent_name)

    return SimpleNamespace(
        base_url=base_url,
        sessions=SimpleNamespace(get_with_nodes=get_with_nodes),
        agents=SimpleNamespace(get=get_agent),
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


def test_select_reader_indices_mixes_targeted_and_deterministic_random_baseline() -> (
    None
):
    """When budget is tight, prioritize older failures plus deterministic baseline."""
    # 20 sessions of 100 chars each = 2040 chars; budget fits ~8 sessions (820 chars).
    sections = [f"S{idx:02d}-" + ("x" * 96) for idx in range(20)]
    # Oldest 3 sessions (0, 1, 2) are failures; newest sessions are clean.
    targeted = {0, 1, 2}

    first = select_reader_indices(
        sections, targeted_indices=targeted, budget=820, seed="stable-seed"
    )
    second = select_reader_indices(
        sections, targeted_indices=targeted, budget=820, seed="stable-seed"
    )

    assert first == second
    assert first == sorted(first)
    # All 3 older targeted failures are preserved despite being at the start!
    assert {0, 1, 2} <= set(first)
    # Baseline sessions are also included for contrast
    assert len(set(first) - targeted) >= 3
    assert len(first) < len(sections)


async def test_findings_count_only_sessions_where_a_check_held() -> None:
    """Count confident answers only, drop false positives, and pick counterexamples."""
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
    # Sessions 1 and 3 had prob 0.1 (passing counterexamples); session 2 was dropped
    assert findings[0].counterexample_sessions == [1, 3]
    # Tone held on 0, 1, 3 and was refused/missing on 2 -> no passing counterexample
    assert findings[1].counterexample_sessions == []
    reader_prompt, merge_prompt = client.prompts
    assert "The refund tool skips the approval limit." in reader_prompt
    assert "- final_sale: The session shows final_sale. Held in sessions [0, 2]." in (
        merge_prompt
    )
    assert "unsure" not in merge_prompt
    assert len(client.cache_keys) == 4
    assert len(set(client.cache_keys)) == 1


async def test_analyzer_writes_one_structured_multi_finding_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Emit one InsightInput report with Rate, StepStory, Evidence + counterexamples."""
    sessions = [
        _session(2, "Refunded.", tool_status=NodeStatus.FAILED),
        _session(1, "Ok."),
    ]
    kitaru = _stub_kitaru(sessions)
    client = FakeOpenAI(
        plans=[
            _ReaderPlan(
                checks=[
                    _check("final_sale", "issue_refund refunded a final-sale item."),
                    _check("tone", "Agent used an abrupt closing."),
                ]
            ),
            _MergePlan(
                findings=[
                    _finding(
                        ["tone"],
                        "low",
                        title="Abrupt closing message",
                        example="Agent ended without offering further help.",
                    ),
                    _finding(
                        ["final_sale"],
                        "high",
                        title="Refunded final-sale item",
                        example=(
                            "Agent called issue_refund ($75) on a final-sale strap."
                        ),
                        check_first="Inspect issue_refund eligibility validation.",
                        example_session=1,
                        mechanism_steps=[
                            _StoryStepOutput(
                                text=(
                                    "Customer asks to refund a broken final-sale strap."
                                ),
                                role="user",
                            ),
                            _StoryStepOutput(
                                text=(
                                    "Agent calls issue_refund without checking policy."
                                ),
                                role="tool",
                                tool_name="issue_refund",
                                is_bad_step=True,
                                note="Refunded $75 despite final-sale flag.",
                            ),
                        ],
                        counterexample_note=(
                            "Passing session did not refund final-sale items."
                        ),
                        suggested_questions=[
                            "Which final-sale items were refunded?",
                            "Draft a policy guard for issue_refund",
                        ],
                    ),
                ]
            ),
        ],
        held={
            "final_sale": {2: 0.97, 1: 0.05},
            "tone": {2: 0.91, 1: 0.92},
        },
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

    reports = await analyze_session_findings([s.session.id for s in sessions])

    assert len(reports) == 1
    report = reports[0]
    assert report.name == "openai-session-findings"
    assert report.title == "Refunded final-sale item"
    assert report.source_provider == "langfuse"
    assert report.coverage is not None
    assert report.coverage.sessions_analyzed == 2
    assert report.coverage.sessions_available == 2
    assert report.coverage.started_after == START + timedelta(minutes=1)
    assert report.coverage.started_before == START + timedelta(minutes=2)
    assert report.highlight is not None
    assert report.highlight.finding_id == "session-finding-1"
    assert report.highlight.artifact_index == 0
    assert report.next_step is not None
    assert "returns-agent" in report.next_step.prompt
    assert str(IMPORT_ID) in report.next_step.prompt
    assert report.suggested_questions

    # Findings ordered most harmful first: high then low
    assert [f.id for f in report.findings] == [
        "session-finding-1",
        "session-finding-2",
    ]
    top = report.findings[0]
    assert top.severity == "high"
    assert top.check_first == "Inspect issue_refund eligibility validation."
    assert top.contributing_session_ids == [sessions[0].session.id]

    # Artifact 0: RateArtifact (share_bar)
    rate = top.artifacts[0]
    assert isinstance(rate, RateArtifact)
    assert rate.presentation == "share_bar"
    assert rate.numerator == 1
    assert rate.denominator == 2
    assert rate.tone == "danger"

    # Artifact 1: StepStoryArtifact linked to session 2 & its issue_refund node
    story = top.artifacts[1]
    assert isinstance(story, StepStoryArtifact)
    assert story.title == "How it happened"
    assert story.session_id == sessions[0].session.id
    assert story.session_label == "Session #2"
    assert story.steps[1].tone == "danger"
    assert story.steps[1].node_id == sessions[0].nodes[1].id

    # Artifact 2: EvidenceArtifact (table) with failing row + passing counterexample
    evidence = top.artifacts[2]
    assert isinstance(evidence, EvidenceArtifact)
    assert evidence.title == "Evidence"
    assert evidence.presentation == "table"
    assert evidence.total_sessions == 1
    assert len(evidence.rows) == 2
    fail_row, counter_row = evidence.rows
    assert fail_row.session_id == sessions[0].session.id
    assert fail_row.node_id == sessions[0].nodes[1].id
    assert fail_row.step_label == "step 2"
    assert fail_row.verdict == "fail"
    assert fail_row.is_counterexample is False
    assert counter_row.session_id == sessions[1].session.id
    assert counter_row.verdict == "pass"
    assert counter_row.is_counterexample is True
    assert counter_row.counterexample_note == (
        "Passing session did not refund final-sale items."
    )

    # Second finding has no mechanism_steps -> falls back to TextArtifact body
    second = report.findings[1]
    assert isinstance(second.artifacts[1], TextArtifact)
    assert second.artifacts[1].content == "Agent ended without offering further help."
    # All sessions failed "tone" -> no fabricated counterexample
    second_ev = second.artifacts[2]
    assert isinstance(second_ev, EvidenceArtifact)
    assert all(not row.is_counterexample for row in second_ev.rows)

    # Metadata stores schema_version 2 and exact session IDs
    meta = report.metadata[SESSION_FINDINGS_METADATA_KEY]
    assert isinstance(meta, dict)
    assert meta["schema_version"] == "2"
    assert meta["total_sessions"] == 2
    assert meta["reader_sessions_inspected"] == 2

    # Reader prompt stays oldest-first and unnumbered
    reader_prompt = client.prompts[0]
    assert reader_prompt.index("Started 2026-11-02 09:01") < (
        reader_prompt.index("Started 2026-11-02 09:02")
    )
    assert "Session 0" not in reader_prompt


async def test_analyzer_discloses_reader_sampling_caveat_when_budget_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Add an honest coverage caveat when reader sampling could not fit all sessions."""
    sessions = [
        _session(
            n,
            "Done.",
            status=SessionStatus.FAILED if n == 1 else SessionStatus.COMPLETED,
        )
        for n in range(1, 7)
    ]
    kitaru = _stub_kitaru(sessions)
    client = FakeOpenAI(
        plans=[
            _ReaderPlan(checks=[_check("final_sale")]),
            _MergePlan(findings=[_finding(["final_sale"], "high")]),
        ],
        held={"final_sale": {1: 0.95, 2: 0.1, 3: 0.1, 4: 0.1, 5: 0.1, 6: 0.1}},
    )
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(session_findings, "MAX_PROMPT_LENGTH", 500)
    monkeypatch.setattr(
        session_findings,
        "import_openai",
        lambda: SimpleNamespace(AsyncOpenAI=lambda **_kwargs: client),
    )
    monkeypatch.setattr(
        session_findings, "KitaruAPIClient", lambda: nullcontext(kitaru)
    )

    [report] = await analyze_session_findings([s.session.id for s in sessions])

    assert report.coverage is not None
    assert any(
        "targeted + random sample" in caveat
        and "verified across all 6 sessions" in caveat
        for caveat in report.coverage.caveats
    )
    # Failed session 1 was prioritized into the reader prompt despite tight budget
    assert "Ticket 1:" in client.prompts[0]


async def test_analyzer_handles_live_recorded_and_mixed_providers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve source_provider to 'recorded' or 'mixed' and validate max_findings."""
    recorded = _session(1, "Ok.", origin=SessionOrigin.RECORDED)
    imported = _session(
        2, "Ok.", origin=SessionOrigin.IMPORTED, imported_from="langfuse"
    )
    kitaru = _stub_kitaru([recorded, imported])
    client = FakeOpenAI(
        plans=[
            _ReaderPlan(checks=[_check("a"), _check("b")]),
            _MergePlan(
                findings=[
                    _finding(["a"], "high", title="First"),
                    _finding(["b"], "medium", title="Second"),
                ]
            ),
        ],
        held={"a": {1: 0.95, 2: 0.1}, "b": {1: 0.95, 2: 0.1}},
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

    [report] = await analyze_session_findings(
        [recorded.session.id, imported.session.id], max_findings=1
    )
    assert report.source_provider == "mixed"
    assert len(report.findings) == 1
    assert report.findings[0].title == "First"
    assert report.next_step is not None
    assert "Import id:" not in report.next_step.prompt

    with pytest.raises(ValueError, match="max_findings"):
        await analyze_session_findings([recorded.session.id], max_findings=0)


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
