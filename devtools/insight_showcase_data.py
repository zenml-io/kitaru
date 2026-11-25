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
"""Showcase Insight report payloads covering H1, R1, R2, and all 17 artifacts."""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from kitaru.api_models.v1.insight import (
    Bin,
    BinnedArtifact,
    CategoricalArtifact,
    CategoryValue,
    CycleStep,
    EvidenceArtifact,
    EvidenceRow,
    InsightAction,
    InsightCoverage,
    InsightFinding,
    InsightHighlight,
    InsightInput,
    InsightStatus,
    RateArtifact,
    StepStripArtifact,
    StepStripRow,
    SuggestedQuestion,
    ToolCycleArtifact,
)

BASE_TIME = datetime(2026, 10, 19, 9, 12, tzinfo=UTC)


@dataclass(frozen=True)
class SeededSession:
    """Real ingested session and its last tool node for evidence links."""

    session_id: uuid.UUID
    label: str
    node_id: uuid.UUID | None


def build_meridian_report(sessions: list[SeededSession]) -> InsightInput:
    """Build Zuri's flagship R1 / R2 report for meridian-support."""
    s1, s2, s3, s4 = sessions[:4]
    return InsightInput(
        name="meridian-policy-skip-report",
        title="Refunds skip the policy check when the order was edited",
        summary=(
            "After an order edit, the agent called issue_refund without "
            "check_refund_policy in 31 of 47 sessions (14% overall)."
        ),
        source_provider="Langfuse",
        coverage=InsightCoverage(
            sessions_analyzed=214,
            sessions_available=250,
            skipped=33,
            failed=3,
            started_after=BASE_TIME - timedelta(days=7),
            started_before=BASE_TIME,
            caveats=["stopped at the 250-trace limit"],
        ),
        highlight=InsightHighlight(finding_id="policy-check-skipped", artifact_index=0),
        next_step=InsightAction(
            label="Investigate this insight",
            summary="Run check_refund_policy again after every order edit",
            prompt=(
                "Investigate why meridian-support skipped check_refund_policy after "
                "edit_order in 31 of 47 edited-order sessions. Compare failing "
                f"session {s1.session_id} against counterexample {s4.session_id}."
            ),
        ),
        findings=[
            InsightFinding(
                id="policy-check-skipped",
                title="Refunds skip the policy check when the order was edited",
                summary="31 of 47 edited-order sessions skipped check_refund_policy.",
                body=(
                    "After an order edit, the agent called `issue_refund` without "
                    "`check_refund_policy` in **31 of 47 sessions**. Without an edit "
                    "it never skipped the check (0 of 167). 24 were scored Fail on "
                    "`policy_adherence`."
                ),
                tier="primary",
                kind="finding",
                severity="high",
                change_status="new",
                artifacts=[
                    RateArtifact(
                        presentation="range",
                        rate=0.14,
                        ci_low=0.10,
                        ci_high=0.20,
                        numerator=31,
                        denominator=214,
                        label="of sessions, between 10% and 20%",
                        basis="Every session read",
                        tone="risk",
                        chat_prompt="Why is the range 10% to 20% on the refund skip?",
                    ),
                    EvidenceArtifact(
                        presentation="table",
                        total_sessions=31,
                        rows=[
                            EvidenceRow(
                                session_id=s1.session_id,
                                session_label=s1.label,
                                node_id=s1.node_id,
                                step_label="step 7",
                                signal="issue_refund",
                                verdict="fail",
                            ),
                            EvidenceRow(
                                session_id=s2.session_id,
                                session_label=s2.label,
                                node_id=s2.node_id,
                                step_label="step 7",
                                signal="issue_refund",
                                verdict="fail",
                            ),
                            EvidenceRow(
                                session_id=s3.session_id,
                                session_label=s3.label,
                                node_id=s3.node_id,
                                step_label="step 6",
                                signal="issue_refund",
                                verdict="pass",
                            ),
                            EvidenceRow(
                                session_id=s4.session_id,
                                session_label=s4.label,
                                node_id=s4.node_id,
                                step_label="step 6",
                                signal="check_refund_policy",
                                verdict="pass",
                                is_counterexample=True,
                                counterexample_note="Counterexample, 1 of 16",
                            ),
                        ],
                    ),
                ],
                contributing_session_ids=[s.session_id for s in sessions[:3]],
                check_first=(
                    "Confirm whether order edits reset eligibility state in the "
                    "system prompt before calling issue_refund."
                ),
                action=InsightAction(
                    label="Investigate this insight",
                    prompt=(
                        "Open sessions where edit_order preceded issue_refund without "
                        "check_refund_policy and compare with the passing "
                        "counterexample."
                    ),
                ),
            ),
            InsightFinding(
                id="escalations-include-order-number",
                title="Escalations to a human always include the order number",
                summary="41 of 41 escalated sessions included the order number.",
                tier="primary",
                kind="bright_spot",
                change_status="recurring",
                artifacts=[
                    RateArtifact(
                        presentation="share_bar",
                        rate=1.0,
                        numerator=41,
                        denominator=41,
                        label="41 of 41 escalated sessions",
                        tone="good",
                    )
                ],
                contributing_session_ids=[s4.session_id],
            ),
            InsightFinding(
                id="where-failures-start",
                title="Where failures start",
                summary="52 failed sessions cluster at step 7 after order edits.",
                tier="primary",
                kind="finding",
                change_status="recurring",
                artifacts=[
                    StepStripArtifact(
                        presentation="rows",
                        total_steps=10,
                        rows=[
                            StepStripRow(
                                label="Policy check skipped",
                                session_count=24,
                                last_good_step=6,
                                first_wrong_step=7,
                                severity="high",
                                session_ids=[s1.session_id, s2.session_id],
                            ),
                            StepStripRow(
                                label="Wrong order looked up",
                                session_count=14,
                                last_good_step=2,
                                first_wrong_step=3,
                                severity="medium",
                                session_ids=[s3.session_id],
                            ),
                            StepStripRow(
                                label="Tool error",
                                session_count=9,
                                last_good_step=3,
                                first_wrong_step=4,
                                severity="medium",
                            ),
                            StepStripRow(label="Other", session_count=5),
                        ],
                    )
                ],
                contributing_session_ids=[s1.session_id, s2.session_id, s3.session_id],
            ),
            InsightFinding(
                id="how-sessions-ended",
                title="How sessions ended",
                summary="Resolved 158, Escalated 41, Abandoned 15.",
                tier="secondary",
                artifacts=[
                    CategoricalArtifact(
                        presentation="split_bar",
                        unit="sessions",
                        values=[
                            CategoryValue(label="Resolved", value=158, tone="good"),
                            CategoryValue(label="Escalated", value=41, tone="warn"),
                            CategoryValue(label="Abandoned", value=15, tone="neutral"),
                        ],
                    )
                ],
            ),
            InsightFinding(
                id="tool-calls-per-session",
                title="Tool calls per session",
                summary="Most sessions complete in 0-10 tool calls; 14 exceed 20.",
                tier="secondary",
                artifacts=[
                    BinnedArtifact(
                        presentation="micro",
                        unit="tool calls",
                        median=6,
                        p95=22,
                        bins=[
                            Bin(lower_bound=0, upper_bound=10, count=142),
                            Bin(lower_bound=10, upper_bound=20, count=58),
                            Bin(lower_bound=20, upper_bound=None, count=14, focus=True),
                        ],
                    )
                ],
            ),
        ],
        suggested_questions=[
            SuggestedQuestion(label="Why is the range this wide?"),
            SuggestedQuestion(label="Show the step where the check was skipped"),
            SuggestedQuestion(label="Compare with the counterexample"),
        ],
    )


def build_grid_card_reports(
    sessions_by_agent: dict[str, list[SeededSession]],
) -> list[tuple[str, InsightInput, InsightStatus]]:
    """Build the remaining 7 agents' reports from Zuri's H1 home grid."""
    return [
        (
            "harbor-returns",
            InsightInput(
                name="harbor-refund-without-lookup",
                title="Refund promised without a successful order lookup",
                summary="37% promised a refund before lookup_order succeeded.",
                source_provider="LangSmith",
                coverage=InsightCoverage(sessions_analyzed=1240),
                highlight=InsightHighlight(
                    finding_id="refund-promised", artifact_index=0
                ),
                findings=[
                    InsightFinding(
                        id="refund-promised",
                        title="Refund promised without a successful order lookup",
                        severity="high",
                        change_status="recurring",
                        artifacts=[
                            RateArtifact(
                                presentation="range",
                                rate=0.37,
                                ci_low=0.30,
                                ci_high=0.44,
                                label="between 30% and 44%",
                                tone="risk",
                            )
                        ],
                        contributing_session_ids=[
                            sessions_by_agent["harbor-returns"][0].session_id
                        ],
                    ),
                    InsightFinding(
                        id="harbor-outcomes",
                        title="Session outcomes",
                        tier="secondary",
                        artifacts=[
                            CategoricalArtifact(
                                presentation="split_bar",
                                unit="sessions",
                                values=[
                                    CategoryValue(
                                        label="Completed", value=780, tone="good"
                                    ),
                                    CategoryValue(
                                        label="Failed", value=460, tone="danger"
                                    ),
                                ],
                            )
                        ],
                    ),
                ],
            ),
            "reviewed",
        ),
        (
            "atlas-research",
            InsightInput(
                name="atlas-search-fetch-loop",
                title="search -> fetch_page repeats up to 14x",
                summary="23 of 402 research sessions looped on search and fetch_page.",
                source_provider="Braintrust",
                coverage=InsightCoverage(sessions_analyzed=402),
                highlight=InsightHighlight(finding_id="search-loop", artifact_index=0),
                findings=[
                    InsightFinding(
                        id="search-loop",
                        title="search -> fetch_page repeats up to 14x",
                        severity="medium",
                        artifacts=[
                            ToolCycleArtifact(
                                steps=[
                                    CycleStep(name="search", status="ok"),
                                    CycleStep(name="fetch_page", status="error"),
                                ],
                                repeat_count=14,
                                session_count=23,
                            )
                        ],
                        contributing_session_ids=[
                            sessions_by_agent["atlas-research"][0].session_id
                        ],
                    )
                ],
            ),
            "reviewed",
        ),
        (
            "relay-triage",
            InsightInput(
                name="relay-unread-attachment",
                title="Tickets routed without reading the attachment",
                summary="9% routed tickets before parsing the attachment.",
                source_provider="Langfuse",
                coverage=InsightCoverage(sessions_analyzed=400),
                highlight=InsightHighlight(
                    finding_id="unread-attachment", artifact_index=0
                ),
                findings=[
                    InsightFinding(
                        id="unread-attachment",
                        title="Tickets routed without reading the attachment",
                        severity="medium",
                        change_status="new",
                        artifacts=[
                            RateArtifact(
                                presentation="range",
                                rate=0.09,
                                ci_low=0.06,
                                ci_high=0.13,
                                label="between 6% and 13%",
                                tone="risk",
                            )
                        ],
                        contributing_session_ids=[
                            sessions_by_agent["relay-triage"][0].session_id
                        ],
                    )
                ],
            ),
            "new",
        ),
        (
            "quill-writer",
            InsightInput(
                name="quill-draft-failure-step",
                title="Drafts fail after the outline step",
                summary="Step 6 is last good; step 7 fails across 19 sessions.",
                source_provider="LangSmith",
                coverage=InsightCoverage(sessions_analyzed=96),
                highlight=InsightHighlight(
                    finding_id="outline-step-fail", artifact_index=0
                ),
                findings=[
                    InsightFinding(
                        id="outline-step-fail",
                        title="Drafts fail after the outline step",
                        severity="high",
                        artifacts=[
                            StepStripArtifact(
                                presentation="rows",
                                total_steps=10,
                                rows=[
                                    StepStripRow(
                                        label="Draft expansion failed",
                                        session_count=19,
                                        last_good_step=6,
                                        first_wrong_step=7,
                                        severity="high",
                                        session_ids=[
                                            sessions_by_agent["quill-writer"][
                                                0
                                            ].session_id
                                        ],
                                    )
                                ],
                            )
                        ],
                    ),
                    InsightFinding(
                        id="quill-draft-lengths",
                        title="Draft length spread",
                        tier="secondary",
                        artifacts=[
                            BinnedArtifact(
                                presentation="columns",
                                unit="tokens",
                                bins=[
                                    Bin(lower_bound=0, upper_bound=2000, count=54),
                                    Bin(lower_bound=2000, upper_bound=4000, count=30),
                                    Bin(lower_bound=4000, upper_bound=None, count=12),
                                ],
                            )
                        ],
                    ),
                ],
            ),
            "reviewed",
        ),
        (
            "cedar-sales",
            InsightInput(
                name="cedar-pricing-abandonment",
                title="Prospects abandon after the pricing question",
                summary="29 of 120 sales sessions abandoned right after pricing.",
                source_provider="Langfuse",
                coverage=InsightCoverage(sessions_analyzed=120),
                highlight=InsightHighlight(
                    finding_id="pricing-abandon", artifact_index=0
                ),
                findings=[
                    InsightFinding(
                        id="pricing-abandon",
                        title="Prospects abandon after the pricing question",
                        severity="medium",
                        artifacts=[
                            RateArtifact(
                                presentation="share_bar",
                                rate=29 / 120,
                                numerator=29,
                                denominator=120,
                                label="of 120 abandoned",
                                tone="danger",
                            )
                        ],
                        contributing_session_ids=[
                            sessions_by_agent["cedar-sales"][0].session_id
                        ],
                    ),
                    InsightFinding(
                        id="cedar-followup",
                        title="Follow-up prompt breakdown",
                        tier="secondary",
                        artifacts=[
                            CategoricalArtifact(
                                presentation="ranked_bars",
                                unit="sessions",
                                values=[
                                    CategoryValue(label="Enterprise tier", value=16),
                                    CategoryValue(label="Seat limits", value=9),
                                    CategoryValue(label="Other", value=4),
                                ],
                            )
                        ],
                    ),
                ],
            ),
            "reviewed",
        ),
        (
            "beacon-onboarding",
            InsightInput(
                name="beacon-one-session-setup",
                title="Invited users finish setup in one session",
                summary="92% of invited users finish workspace setup on the first try.",
                source_provider="Braintrust",
                coverage=InsightCoverage(sessions_analyzed=600),
                highlight=InsightHighlight(finding_id="one-session", artifact_index=0),
                findings=[
                    InsightFinding(
                        id="one-session",
                        title="Invited users finish setup in one session",
                        kind="bright_spot",
                        artifacts=[
                            RateArtifact(
                                presentation="range",
                                rate=0.92,
                                ci_low=0.88,
                                ci_high=0.95,
                                label="between 88% and 95%",
                                tone="good",
                            )
                        ],
                        contributing_session_ids=[
                            sessions_by_agent["beacon-onboarding"][0].session_id
                        ],
                    ),
                    InsightFinding(
                        id="beacon-steps",
                        title="Steps to complete setup",
                        tier="secondary",
                        artifacts=[
                            BinnedArtifact(
                                presentation="columns",
                                unit="steps",
                                bins=[
                                    Bin(lower_bound=1, upper_bound=4, count=410),
                                    Bin(lower_bound=4, upper_bound=8, count=142),
                                    Bin(lower_bound=8, upper_bound=None, count=48),
                                ],
                            )
                        ],
                    ),
                ],
            ),
            "reviewed",
        ),
        (
            "orbit-scheduler",
            InsightInput(
                name="orbit-timezone-corrections",
                title="Users correct the agent after a timezone change",
                summary="22 of 180 scheduling sessions required a user timezone fix.",
                source_provider="Langfuse",
                coverage=InsightCoverage(sessions_analyzed=180),
                highlight=InsightHighlight(
                    finding_id="timezone-corrections", artifact_index=0
                ),
                findings=[
                    InsightFinding(
                        id="timezone-corrections",
                        title="Users correct the agent after a timezone change",
                        severity="medium",
                        change_status="new",
                        artifacts=[
                            RateArtifact(
                                presentation="share_bar",
                                rate=22 / 180,
                                numerator=22,
                                denominator=180,
                                label="of 180 sessions",
                                tone="warn",
                            )
                        ],
                        contributing_session_ids=[
                            sessions_by_agent["orbit-scheduler"][0].session_id
                        ],
                    )
                ],
            ),
            "new",
        ),
    ]
