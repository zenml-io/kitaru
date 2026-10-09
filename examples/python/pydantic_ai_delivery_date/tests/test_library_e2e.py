"""Real-server saved membership and fresh native regression recordings."""

import asyncio
import os
from uuid import UUID, uuid4

import pytest
from kitaru.api_models.v1.cohort_version import CohortVersionListParams
from kitaru.api_models.v1.session import SessionStatus
from kitaru.client import KitaruAPIClient

from delivery_date.generation import Scenario as EditorScenario
from delivery_date.models import get_scenario
from delivery_date.persistence import record_result
from delivery_date.runner import run_scenario
from delivery_date.scenario_library import keep_case, read_test_set
from delivery_date.simulation import RunRequest, build_execution_scenario
from delivery_date.test_set import execute_test_set

SERVER = os.environ.get("KITARU_DELIVERY_E2E_URL", "")
pytestmark = pytest.mark.skipif(
    not SERVER, reason="Set KITARU_DELIVERY_E2E_URL to an isolated local test server."
)


def test_saved_set_versions_and_fresh_recorded_regression_execution() -> None:
    async def check() -> None:
        suffix = uuid4().hex
        agent_name = f"delivery-library-{suffix}"
        set_name = f"delivery-regression-tests-{suffix}"
        first = await run_scenario(get_scenario("missing-date"), variant="fix")
        async with KitaruAPIClient(base_url=SERVER, api_key="local-test") as client:
            first_id = await record_result(
                first, server_url=SERVER, agent_name=agent_name, client=client
            )
            original = await client.sessions.get(first_id)
            saved = await keep_case(client, first_id, name=set_name)
            cohort_id = UUID(saved["cohortId"])
            first_version_id = UUID(saved["cohortVersionId"])
            assert len(saved["cases"]) == 1
            assert saved["cases"][0]["sourceSessionId"] == str(first_id)
            assert saved["cases"][0]["scenarioHash"] == first.scenario_hash

            repeated = await keep_case(client, first_id, cohort_id)
            assert repeated["cohortVersionId"] == str(first_version_id)
            versions = await client.cohorts.list_versions(
                cohort_id, CohortVersionListParams(size=25)
            )
            assert len(versions.items) == 1

            editor = EditorScenario(
                goal="Find a supported delivery estimate",
                knownFacts="ORDER-1042 is my order",
                opening="When will ORDER-1042 arrive?",
                tone="skeptical",
                persistence="asks-once",
                status="In transit",
                estimate="2026-10-09",
                tracking="https://tracking.example.test/ORDER-1042",
                acceptance="Accept a supported estimate without a guarantee",
                maxTurns=3,
                start="tool-boundary",
            )
            second = await run_scenario(
                build_execution_scenario(
                    RunRequest(
                        title="Known estimate", sourceId=first_id, scenario=editor
                    ),
                    first.scenario,
                ),
                mode=editor.start,
                continue_after_boundary=True,
                variant="fix",
            )
            second_id = await record_result(
                second,
                server_url=SERVER,
                agent_name=agent_name,
                client=client,
                editor_snapshot=editor.model_dump(),
                source_session_id=first_id,
                title="Known estimate",
                continue_after_boundary=True,
            )
            updated = await keep_case(client, second_id, cohort_id)
            assert updated["cohortVersionId"] != str(first_version_id)
            assert {case["sourceSessionId"] for case in updated["cases"]} == {
                str(first_id),
                str(second_id),
            }
            saved_editor = next(
                case
                for case in updated["cases"]
                if case["sourceSessionId"] == str(second_id)
            )
            assert saved_editor["editorSnapshot"] == editor.model_dump()
            assert saved_editor["scenario"] == editor.model_dump()
            assert saved_editor["continueAfterBoundary"] is True
            versions = await client.cohorts.list_versions(
                cohort_id, CohortVersionListParams(size=25)
            )
            assert len(versions.items) == 2

            pinned = await read_test_set(client, first_version_id)
            assert [case["sourceSessionId"] for case in pinned["cases"]] == [
                str(first_id)
            ]
            receipt = await execute_test_set(
                client,
                first_version_id,
                backend="scripted",
                variant="fix",
                record=True,
                server_url=SERVER,
            )
            assert receipt["passed"] is True
            assert receipt["status"] == "complete"
            assert receipt["cohortVersionId"] == str(first_version_id)
            assert len(receipt["cases"]) == 1
            row = receipt["cases"][0]
            assert row["sourceSessionId"] == str(first_id)
            assert row["runId"] != str(first.run_id)
            assert row["sessionId"] is not None
            rerun_id = UUID(row["sessionId"])
            assert rerun_id not in {first_id, second_id}
            rerun = await client.sessions.get(rerun_id)
            assert rerun.status == SessionStatus.COMPLETED
            assert rerun.agent_id == original.agent_id
            assert rerun.inputs["source_session_id"] == str(first_id)
            assert rerun.inputs["scenario_sha256"] == first.scenario_hash
            assert rerun.outputs["input_seed"] == first.input_seed
            assert rerun.inputs["continue_after_boundary"] is False
            unchanged = await client.sessions.get(first_id)
            assert unchanged.inputs == original.inputs
            assert unchanged.outputs == original.outputs
            assert len((await read_test_set(client, first_version_id))["cases"]) == 1

    asyncio.run(check())
