"""Frozen membership, compatibility, and fresh regression-run behavior."""

import asyncio
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from kitaru.api_models.v1.filter import AndFilter
from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionOrigin,
    SessionResponse,
    SessionStatus,
)

from delivery_date.models import get_scenario
from delivery_date.persistence import SCHEMA_VERSION, get_runner_revision
from delivery_date.runner import run_scenario
from delivery_date.scenario_library import (
    LIBRARY_SCHEMA,
    keep_case,
    read_test_set,
)
from delivery_date.test_set import execute_test_set


class MemoryClient:
    """Implement the existing SDK operations used by the example library."""

    def __init__(self, session):
        self.saved_sessions = {session.id: session}
        self.saved_cohorts = {}
        self.saved_versions = {}
        self.members = {}
        self.created_versions = []
        self.sessions = SimpleNamespace(get=self.get_session, list=self.list_sessions)
        self.cohorts = SimpleNamespace(
            create=self.create_cohort,
            get=self.get_cohort,
            list=self.list_cohorts,
            list_versions=self.list_versions,
            create_version=self.create_version,
        )
        self.cohort_versions = SimpleNamespace(get=self.get_version)
        self.agents = SimpleNamespace(get=self.get_agent)
        self.pagination_size = 25

    async def get_agent(self, agent_id):
        return SimpleNamespace(id=agent_id, name="delivery-date-demo")

    async def get_session(self, session_id):
        return self.saved_sessions[session_id]

    async def list_sessions(self, params):
        members = self.members[UUID(params.filter.value)]
        offset = int(params.cursor or 0)
        stop = offset + self.pagination_size
        return SimpleNamespace(
            items=[self.saved_sessions[sid] for sid in members[offset:stop]],
            next_cursor=str(stop) if stop < len(members) else None,
        )

    async def create_cohort(self, request, idempotency_key=None):
        cohort = SimpleNamespace(
            id=uuid4(),
            name=request.name,
            agent_id=request.agent_id,
            metadata=request.metadata,
            latest_version=0,
        )
        self.saved_cohorts[cohort.id] = cohort
        return cohort

    async def get_cohort(self, cohort_id):
        return self.saved_cohorts[cohort_id]

    async def list_cohorts(self, params):
        conditions = (
            params.filter.and_
            if isinstance(params.filter, AndFilter)
            else [params.filter]
        )
        return SimpleNamespace(
            items=[
                cohort
                for cohort in self.saved_cohorts.values()
                if all(
                    str(getattr(cohort, condition.field)) == str(condition.value)
                    for condition in conditions
                )
            ],
            next_cursor=None,
        )

    async def get_version(self, version_id):
        return self.saved_versions[version_id]

    async def list_versions(self, cohort_id, params):
        versions = [v for v in self.saved_versions.values() if v.cohort_id == cohort_id]
        return SimpleNamespace(items=sorted(versions, key=lambda v: -v.version)[:1])

    async def create_version(self, cohort_id, request, idempotency_key=None):
        self.created_versions.append(request)
        members = list(self.members[request.baseline_id]) if request.baseline_id else []
        members.extend(request.add_session_ids)
        cohort = self.saved_cohorts[cohort_id]
        cohort.latest_version += 1
        version = SimpleNamespace(
            id=uuid4(),
            cohort_id=cohort_id,
            version=cohort.latest_version,
            session_count=len(members),
        )
        self.saved_versions[version.id] = version
        self.members[version.id] = members
        return version


async def make_session(*, mode="full", continue_after_boundary=False):
    result = await run_scenario(
        get_scenario("missing-date"),
        mode=mode,
        continue_after_boundary=continue_after_boundary,
    )
    return SessionDetailResponse.model_construct(
        id=uuid4(),
        agent_id=uuid4(),
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.COMPLETED,
        framework="pydantic-ai",
        metadata={"demo": "delivery-date"},
        name="Saved delivery case",
        inputs={
            "schema_version": SCHEMA_VERSION,
            "scenario_snapshot": result.scenario.model_dump(mode="json"),
            "scenario_sha256": result.scenario_hash,
            "input_seed": result.input_seed,
            "mode": result.mode,
            "continue_after_boundary": continue_after_boundary,
            "runner_revision": get_runner_revision(),
        },
        outputs=result.model_dump(mode="json"),
    )


def test_keep_is_idempotent_and_deltas_anchor_a_known_baseline():
    async def check():
        first = await make_session()
        client = MemoryClient(first)
        saved = await keep_case(client, first.id)
        assert saved["name"] == f"delivery-regression-tests-{get_runner_revision()[:8]}"
        assert saved == await keep_case(client, first.id)
        assert len(client.created_versions) == 1
        second = await make_session(mode="tool-boundary")
        second.agent_id = first.agent_id
        client.saved_sessions[second.id] = second
        updated = await keep_case(client, second.id, UUID(saved["cohortId"]))
        assert client.created_versions[-1].baseline_id == UUID(saved["cohortVersionId"])
        assert len(updated["cases"]) == 2
        assert (
            len((await read_test_set(client, UUID(saved["cohortVersionId"])))["cases"])
            == 1
        )
        assert updated["cases"][1]["mode"] == "tool-boundary"

    asyncio.run(check())


@pytest.mark.parametrize("invalid", ["hash", "seed", "unfinished", "foreign"])
def test_incompatible_cases_fail_before_any_cohort_write(invalid):
    async def check():
        session = await make_session()
        if invalid == "hash":
            session.inputs["scenario_sha256"] = "invalid"
        elif invalid == "seed":
            session.inputs["input_seed"] = [{"kind": "request", "parts": []}]
        elif invalid == "unfinished":
            session.status = SessionStatus.IN_PROGRESS
        else:
            session.metadata = {"demo": "another-demo"}
        client = MemoryClient(session)
        with pytest.raises(ValueError):
            await keep_case(client, session.id)
        assert not client.saved_cohorts
        assert not client.created_versions

    asyncio.run(check())


def test_mixed_agent_set_is_rejected_before_new_version():
    async def check():
        first = await make_session()
        client = MemoryClient(first)
        saved = await keep_case(client, first.id)
        foreign = await make_session()
        client.saved_sessions[foreign.id] = foreign
        with pytest.raises(ValueError, match="different agent"):
            await keep_case(client, foreign.id, UUID(saved["cohortId"]))
        assert len(client.created_versions) == 1
        client.members[UUID(saved["cohortVersionId"])].append(foreign.id)
        client.saved_versions[UUID(saved["cohortVersionId"])].session_count = 2
        with pytest.raises(ValueError, match="different agents"):
            await read_test_set(client, UUID(saved["cohortVersionId"]))

    asyncio.run(check())


def test_paginated_membership_and_limit():
    async def check():
        first = await make_session()
        client = MemoryClient(first)
        saved = await keep_case(client, first.id)
        second = await make_session()
        second.agent_id = first.agent_id
        client.saved_sessions[second.id] = second
        saved = await keep_case(client, second.id, UUID(saved["cohortId"]))
        client.pagination_size = 1
        version_id = UUID(saved["cohortVersionId"])
        assert len((await read_test_set(client, version_id))["cases"]) == 2
        client.saved_versions[version_id].session_count = 26
        with pytest.raises(ValueError, match="25-case limit"):
            await read_test_set(client, version_id)

    asyncio.run(check())


@pytest.mark.parametrize(
    "variant,expected", [("fix", "complete"), ("control", "check-failed")]
)
def test_ci_reexecutes_exact_version_and_enforces_date_checks(variant, expected):
    async def check():
        session = await make_session(mode="tool-boundary")
        original = session.outputs.copy()
        client = MemoryClient(session)
        saved = await keep_case(client, session.id)
        receipt = await execute_test_set(
            client,
            UUID(saved["cohortVersionId"]),
            backend="scripted",
            variant=variant,
        )
        assert receipt["status"] == expected
        assert receipt["passed"] == (expected == "complete")
        case = receipt["cases"][0]
        assert case["sourceSessionId"] == str(session.id)
        assert case["runId"] != original["run_id"]
        assert case["mode"] == "tool-boundary"
        assert not case["continueAfterBoundary"]
        assert case["result"]["input_seed"] == session.inputs["input_seed"]
        assert case["runnerRevision"] == get_runner_revision()
        assert session.outputs == original

    asyncio.run(check())


def test_invalid_second_case_aborts_before_first_model_call(monkeypatch):
    import delivery_date.test_set as test_set

    async def should_not_run(*args, **kwargs):
        pytest.fail("Input validation must precede all model execution")

    async def check():
        first = await make_session()
        client = MemoryClient(first)
        saved = await keep_case(client, first.id)
        second = await make_session()
        second.agent_id = first.agent_id
        client.saved_sessions[second.id] = second
        saved = await keep_case(client, second.id, UUID(saved["cohortId"]))
        second.inputs["scenario_sha256"] = "corrupt"
        monkeypatch.setattr(test_set, "run_scenario", should_not_run)
        with pytest.raises(ValueError, match="SHA256"):
            await execute_test_set(
                client, UUID(saved["cohortVersionId"]), backend="scripted"
            )

    asyncio.run(check())


def test_stale_revision_is_rejected_before_model_execution(monkeypatch):
    import delivery_date.test_set as test_set

    async def should_not_run(*args, **kwargs):
        pytest.fail("A stale pinned runner must not execute")

    async def check():
        session = await make_session()
        client = MemoryClient(session)
        saved = await keep_case(client, session.id)
        client.saved_cohorts[UUID(saved["cohortId"])].metadata["runner_revision"] = (
            "old"
        )
        monkeypatch.setattr(test_set, "run_scenario", should_not_run)
        with pytest.raises(ValueError, match="stale runner"):
            await execute_test_set(
                client, UUID(saved["cohortVersionId"]), backend="scripted"
            )

    asyncio.run(check())


def test_incomplete_execution_reports_partial_even_with_passing_checks(monkeypatch):
    import delivery_date.test_set as test_set

    async def incomplete(scenario, **kwargs):
        result = await run_scenario(scenario, **kwargs)
        return result.model_copy(update={"status": "turn-limit"})

    async def check():
        session = await make_session(mode="tool-boundary", continue_after_boundary=True)
        client = MemoryClient(session)
        saved = await keep_case(client, session.id)
        monkeypatch.setattr(test_set, "run_scenario", incomplete)
        receipt = await execute_test_set(
            client, UUID(saved["cohortVersionId"]), backend="scripted"
        )
        assert receipt["status"] == "partial"
        assert not receipt["passed"]
        assert receipt["cases"][0]["continueAfterBoundary"]
        assert all(receipt["cases"][0]["checks"].values())

    asyncio.run(check())


def test_test_sets_require_explicit_demo_schema():
    async def check():
        session = await make_session()
        client = MemoryClient(session)
        saved = await keep_case(client, session.id)
        cohort = client.saved_cohorts[UUID(saved["cohortId"])]
        assert cohort.metadata["schema_version"] == LIBRARY_SCHEMA
        cohort.metadata = {}
        with pytest.raises(ValueError, match="not a delivery-date"):
            await read_test_set(client, UUID(saved["cohortVersionId"]))

    asyncio.run(check())


@pytest.mark.parametrize("variant,exit_code", [("fix", 0), ("control", 1)])
def test_cli_exit_status_enforces_checks(monkeypatch, capsys, variant, exit_code):
    import json
    import sys

    import delivery_date.test_set as test_set

    async def check():
        session = await make_session()
        client = MemoryClient(session)
        saved = await keep_case(client, session.id)

        class ClientContext:
            async def __aenter__(self):
                return client

            async def __aexit__(self, *args):
                return None

        monkeypatch.setattr(
            test_set, "KitaruAPIClient", lambda **kwargs: ClientContext()
        )
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "delivery_date.test_set",
                "--cohort-version",
                saved["cohortVersionId"],
                "--backend",
                "scripted",
                "--variant",
                variant,
                "--check",
            ],
        )
        assert await test_set.main() == exit_code
        receipt = json.loads(capsys.readouterr().out)
        assert receipt["cohortVersionId"] == saved["cohortVersionId"]
        assert receipt["configuration"]["model"] == "gpt-6-luna"
        assert receipt["passed"] == (exit_code == 0)

    asyncio.run(check())


def test_mismatched_editor_snapshot_aborts_before_model_call(monkeypatch):
    import delivery_date.test_set as test_set

    async def should_not_run(*args, **kwargs):
        pytest.fail(
            "A displayed scenario that differs from executed inputs must not run"
        )

    async def check():
        session = await make_session()
        client = MemoryClient(session)
        saved = await keep_case(client, session.id)
        session.inputs["editor_snapshot"] = saved["cases"][0]["scenario"] | {
            "estimate": "2026-10-09"
        }
        monkeypatch.setattr(test_set, "run_scenario", should_not_run)
        with pytest.raises(ValueError, match="Editor snapshot differs"):
            await execute_test_set(
                client, UUID(saved["cohortVersionId"]), backend="scripted"
            )

    asyncio.run(check())


def test_reviewed_editor_snapshot_matches_saved_native_execution():
    from delivery_date.generation import Scenario as EditorScenario
    from delivery_date.models import RunResult
    from delivery_date.simulation import RunRequest, build_execution_scenario

    async def check():
        session = await make_session()
        source = RunResult.model_validate(session.outputs).scenario
        editor = EditorScenario(
            goal="Get an evidence-backed estimate",
            knownFacts="ORDER-1042 is my order",
            opening="When will ORDER-1042 arrive?",
            tone="skeptical",
            persistence="asks-once",
            status="In transit",
            estimate="",
            tracking="https://tracking.example.test/ORDER-1042",
            acceptance="Accept an honest unknown date and tracking",
            maxTurns=3,
            start="tool-boundary",
        )
        request = RunRequest(
            title="Reviewed variation", sourceId=session.id, scenario=editor
        )
        result = await run_scenario(
            build_execution_scenario(request, source), mode=editor.start
        )
        session.inputs.update(
            scenario_snapshot=result.scenario.model_dump(mode="json"),
            scenario_sha256=result.scenario_hash,
            input_seed=result.input_seed,
            mode=editor.start,
            editor_snapshot=editor.model_dump(),
        )
        session.outputs = result.model_dump(mode="json")
        client = MemoryClient(session)
        saved = await keep_case(client, session.id)
        assert saved["cases"][0]["editorSnapshot"] == editor.model_dump()
        assert saved["cases"][0]["scenario"] == editor.model_dump()
        receipt = await execute_test_set(
            client, UUID(saved["cohortVersionId"]), backend="scripted"
        )
        assert receipt["passed"]
        assert receipt["cases"][0]["result"]["scenario"] == result.scenario.model_dump(
            mode="json"
        )

    asyncio.run(check())


@pytest.mark.parametrize(
    "name",
    ["Delivery regression tests", "", "-tests", "tests_", "tests/example", "x" * 256],
)
def test_invalid_test_set_name_fails_before_cohort_creation(name):
    async def check():
        session = await make_session()
        client = MemoryClient(session)
        with pytest.raises(ValueError, match="Test-set name"):
            await keep_case(client, session.id, name=name)
        assert not client.saved_cohorts
        assert not client.created_versions

    asyncio.run(check())


def test_missing_member_payloads_are_rejected():
    async def check():
        session = await make_session()
        client = MemoryClient(session)
        saved = await keep_case(client, session.id)
        client.saved_sessions[session.id] = SessionResponse.model_construct(
            id=session.id, agent_id=session.agent_id
        )
        with pytest.raises(ValueError, match="payloads were not returned"):
            await read_test_set(client, UUID(saved["cohortVersionId"]))

    asyncio.run(check())


def test_default_test_set_name_changes_with_runner_revision(monkeypatch):
    import delivery_date.scenario_library as library

    async def check():
        first = await make_session()
        first.inputs["runner_revision"] = "a" * 64
        client = MemoryClient(first)
        monkeypatch.setattr(library, "get_runner_revision", lambda: "a" * 64)
        old = await keep_case(client, first.id)
        second = await make_session()
        second.agent_id = first.agent_id
        second.inputs["runner_revision"] = "b" * 64
        client.saved_sessions[second.id] = second
        monkeypatch.setattr(library, "get_runner_revision", lambda: "b" * 64)
        new = await keep_case(client, second.id)
        assert old["name"] == "delivery-regression-tests-aaaaaaaa"
        assert new["name"] == "delivery-regression-tests-bbbbbbbb"
        assert old["cohortId"] != new["cohortId"]
        assert (
            len((await read_test_set(client, UUID(old["cohortVersionId"])))["cases"])
            == 1
        )
        assert len(new["cases"]) == 1

    asyncio.run(check())
