"""Save real delivery executions in bounded, versioned Kitaru test sets."""

import hashlib
import re
from typing import Any
from uuid import UUID

from kitaru.api_models.v1.cohort import CohortCreateRequest, CohortListParams
from kitaru.api_models.v1.cohort_version import (
    CohortVersionCreateRequest,
    CohortVersionListParams,
)
from kitaru.api_models.v1.filter import AndFilter, FilterCondition, FilterOp
from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionListParams,
    SessionOrigin,
    SessionStatus,
)
from kitaru.client import KitaruAPIClient
from kitaru.client.exceptions import APIError
from pydantic_ai.messages import ModelMessagesTypeAdapter

from .generation import Scenario as EditorScenario
from .models import RunResult, Scenario
from .persistence import SCHEMA_VERSION, get_runner_revision
from .runner import validate_history

MAX_CASES = 25
LIBRARY_SCHEMA = "delivery-date-test-set.v1"


def validate_case(session: SessionDetailResponse) -> Scenario:
    """Verify a finalized recording and its exact executable fixture inputs."""
    inputs = session.inputs
    if (
        session.origin != SessionOrigin.RECORDED
        or session.status not in {SessionStatus.COMPLETED, SessionStatus.FAILED}
        or session.framework != "pydantic-ai"
        or session.metadata.get("demo") != "delivery-date"
        or not isinstance(inputs, dict)
        or inputs.get("schema_version") != SCHEMA_VERSION
    ):
        raise ValueError("Test cases must be finalized delivery-date recordings")
    scenario = Scenario.model_validate(inputs.get("scenario_snapshot"))
    if scenario.calculate_hash() != inputs.get("scenario_sha256"):
        raise ValueError("Stored scenario snapshot does not match its SHA256")
    mode = inputs.get("mode")
    if mode not in {"full", "n-minus-one", "tool-boundary"}:
        raise ValueError("Recorded scenario has an invalid start mode")
    if not isinstance(inputs.get("continue_after_boundary", False), bool):
        raise ValueError("Recorded continuation flag must be a boolean")
    if mode not in scenario.seed_histories:
        raise ValueError("Recorded scenario has no captured history for its mode")
    seed = scenario.seed_histories[mode]
    validate_history(ModelMessagesTypeAdapter.validate_python(seed))
    if inputs.get("input_seed") != seed:
        raise ValueError("Recorded seed differs from the frozen scenario history")
    result = RunResult.model_validate(session.outputs)
    if (
        result.scenario_hash != scenario.calculate_hash()
        or result.scenario.calculate_hash() != scenario.calculate_hash()
        or result.mode != mode
        or result.input_seed != seed
    ):
        raise ValueError("Recorded result does not match its frozen inputs")
    if session.status == SessionStatus.COMPLETED and result.status not in {
        "completed",
        "boundary-completed",
    }:
        raise ValueError("Completed recording contains an incomplete simulation")
    if session.status == SessionStatus.FAILED and result.status in {
        "completed",
        "boundary-completed",
    }:
        raise ValueError("Failed recording contains a completed simulation")
    if inputs.get("editor_snapshot") is not None:
        editor = EditorScenario.model_validate(inputs["editor_snapshot"])
        if (
            editor.goal != scenario.goal
            or editor.opening != scenario.opening_message
            or editor.status != scenario.shipping.status
            or (editor.estimate or None) != scenario.shipping.estimated_delivery
            or editor.tracking != scenario.shipping.tracking_url
            or editor.maxTurns != scenario.max_agent_turns
            or [editor.acceptance] != scenario.acceptance_criteria
            or {"provided_facts": editor.knownFacts} != scenario.customer_known_facts
            or editor.start != mode
            or scenario.customer_policy
            != (
                f"Tone: {editor.tone}. Persistence: {editor.persistence}. "
                "Stay within the provided customer facts. Do not infer hidden tool evidence."
            )
        ):
            raise ValueError("Editor snapshot differs from the executable scenario")
    return scenario


def _present_case(session: SessionDetailResponse, scenario: Scenario) -> dict[str, Any]:
    """Expose editor fields and native evidence for a saved case."""
    inputs = session.inputs
    editor = inputs.get("editor_snapshot")
    if editor is None:
        editor = {
            "goal": scenario.goal,
            "knownFacts": "\n".join(
                f"{key}: {value}"
                for key, value in scenario.customer_known_facts.items()
            ),
            "opening": scenario.opening_message,
            "tone": "calm",
            "persistence": "asks-once",
            "status": scenario.shipping.status,
            "estimate": scenario.shipping.estimated_delivery or "",
            "tracking": scenario.shipping.tracking_url,
            "acceptance": "\n".join(scenario.acceptance_criteria),
            "maxTurns": min(scenario.max_agent_turns, 5),
            "start": inputs["mode"],
        }
    source_id = inputs.get("source_session_id") or str(session.id)
    return {
        "id": str(session.id),
        "sessionId": str(session.id),
        "sourceId": source_id,
        "sourceSessionId": str(session.id),
        "title": session.name or scenario.name,
        "scenario": editor,
        "editorSnapshot": inputs.get("editor_snapshot"),
        "scenarioSnapshot": scenario.model_dump(mode="json"),
        "scenarioHash": scenario.calculate_hash(),
        "mode": inputs["mode"],
        "continueAfterBoundary": inputs.get("continue_after_boundary", False),
        "runnerRevision": inputs.get("runner_revision"),
        "latestResult": session.outputs,
        "runs": [],
    }


async def read_test_set(
    client: KitaruAPIClient, cohort_version_id: UUID
) -> dict[str, Any]:
    """Read and verify at most 25 cases from one exact cohort version."""
    version = await client.cohort_versions.get(cohort_version_id)
    cohort = await client.cohorts.get(version.cohort_id)
    if cohort.metadata.get("schema_version") != LIBRARY_SCHEMA:
        raise ValueError("Cohort is not a delivery-date test set")
    if version.session_count > MAX_CASES:
        raise ValueError(f"Test set exceeds the {MAX_CASES}-case limit")
    cases = []
    cursor = None
    seen_cursors: set[str] = set()
    for _ in range(MAX_CASES):
        page = await client.sessions.list(
            SessionListParams(
                size=MAX_CASES,
                cursor=cursor,
                include_payloads=True,
                sort="created:asc",
                filter=FilterCondition(
                    field="cohort_version_id",
                    op=FilterOp.EQ,
                    value=str(cohort_version_id),
                ),
            )
        )
        for session in page.items:
            if not isinstance(session, SessionDetailResponse):
                raise ValueError("Test-set session payloads were not returned")
            if session.agent_id != cohort.agent_id:
                raise ValueError("Test set contains sessions from different agents")
            scenario = validate_case(session)
            cases.append(_present_case(session, scenario))
            if len(cases) > MAX_CASES:
                raise ValueError(f"Test set exceeds the {MAX_CASES}-case limit")
        cursor = page.next_cursor
        if cursor is None:
            break
        if not page.items or cursor in seen_cursors:
            raise ValueError("Test-set pagination did not make progress")
        seen_cursors.add(cursor)
    else:
        raise ValueError("Test-set pagination exceeded its bounded read limit")
    if len(cases) != version.session_count or len({c["id"] for c in cases}) != len(
        cases
    ):
        raise ValueError("Test-set membership is incomplete or duplicated")
    return {
        "cohortId": str(cohort.id),
        "cohortVersionId": str(version.id),
        "name": cohort.name,
        "agentId": str(cohort.agent_id),
        "runnerRevision": cohort.metadata.get("runner_revision"),
        "cases": cases,
    }


async def keep_case(
    client: KitaruAPIClient,
    session_id: UUID,
    cohort_id: UUID | None = None,
    name: str | None = None,
) -> dict[str, Any]:
    """Add a finalized execution to a versioned set, preserving repeat saves."""
    revision = get_runner_revision()
    if name is None:
        name = f"delivery-regression-tests-{revision[:8]}"
    if (
        len(name) > 255
        or re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9_-]*[A-Za-z0-9])?", name) is None
    ):
        raise ValueError(
            "Test-set name must contain only ASCII letters, digits, '-' and '_', "
            "start and end with a letter or digit, and contain at most 255 characters"
        )
    session = await client.sessions.get(session_id)
    validate_case(session)
    if session.inputs.get("runner_revision") != revision:
        raise ValueError("Recorded case uses a stale runner revision; run it again")
    if cohort_id is None:
        existing = await client.cohorts.list(
            CohortListParams(
                size=1,
                filter=AndFilter.model_validate(
                    {
                        "and": [
                            FilterCondition(field="name", op=FilterOp.EQ, value=name),
                            FilterCondition(
                                field="agent_id",
                                op=FilterOp.EQ,
                                value=str(session.agent_id),
                            ),
                        ]
                    }
                ),
            )
        )
        if existing.items:
            cohort = existing.items[0]
        else:
            try:
                cohort = await client.cohorts.create(
                    CohortCreateRequest(
                        name=name,
                        agent_id=session.agent_id,
                        description="Frozen delivery-date recordings for fresh native regression executions.",
                        metadata={
                            "schema_version": LIBRARY_SCHEMA,
                            "runner_revision": revision,
                        },
                    ),
                    idempotency_key="delivery-test-set-"
                    + hashlib.sha256(f"{session.agent_id}:{name}".encode()).hexdigest(),
                )
            except APIError as exc:
                if exc.status_code != 409:
                    raise
                existing = await client.cohorts.list(
                    CohortListParams(
                        size=1,
                        filter=FilterCondition(
                            field="name", op=FilterOp.EQ, value=name
                        ),
                    )
                )
                if not existing.items:
                    raise
                cohort = existing.items[0]
    else:
        cohort = await client.cohorts.get(cohort_id)
    if cohort.agent_id != session.agent_id:
        raise ValueError("Saved test set belongs to a different agent")
    if (
        cohort.metadata.get("schema_version") != LIBRARY_SCHEMA
        or cohort.metadata.get("runner_revision") != revision
    ):
        raise ValueError("Test set is incompatible or uses a stale runner revision")
    versions = await client.cohorts.list_versions(
        cohort.id, CohortVersionListParams(size=1)
    )
    baseline = versions.items[0] if versions.items else None
    if baseline and baseline.version != cohort.latest_version:
        raise ValueError(
            "Test set changed during save; retry against its latest version"
        )
    if baseline:
        current = await read_test_set(client, baseline.id)
        if any(case["id"] == str(session_id) for case in current["cases"]):
            return current
        if len(current["cases"]) >= MAX_CASES:
            raise ValueError(f"Test set exceeds the {MAX_CASES}-case limit")
    version = await client.cohorts.create_version(
        cohort.id,
        CohortVersionCreateRequest(
            baseline_id=baseline.id if baseline else None,
            add_session_ids=[session_id],
        ),
        idempotency_key=f"delivery-keep-{cohort.id}-{baseline.id if baseline else 'empty'}-{session_id}",
    )
    return await read_test_set(client, version.id)
