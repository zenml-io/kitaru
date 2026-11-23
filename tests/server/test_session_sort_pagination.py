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
"""Sort and keyset-pagination tests for sessions across fake, Postgres, and REST."""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta, timezone

import httpx
import pytest

from conftest import (
    db_settings,
    lifespan_client,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    get_evaluation_service,
    get_session_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.application.pagination import encode_cursor
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.session import Session
from kitaru.server.filtering import FilterCondition
from kitaru_test_support import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeEvaluationRepository,
    FakeImportRepository,
    FakeReplayRepository,
    FakeSessionRepository,
    FakeTagRepository,
    FakeTaskRepository,
    build_payload_store,
    override_idempotency,
)

TwoAgentSetup = tuple[SessionRepository, uuid.UUID, uuid.UUID, uuid.UUID]
ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture(params=["fake", "postgres"])
async def two_agent_setup(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[TwoAgentSetup, None]:
    """Provide a session repository with one owner and two distinct agents."""
    if request.param == "fake":
        yield FakeSessionRepository(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        first_agent = await agents.create(Agent(owner_id=owner.id, name="agent-a"))
        second_agent = await agents.create(Agent(owner_id=owner.id, name="agent-b"))
        yield (
            SQLSessionRepository(session, engine),
            owner.id,
            first_agent.id,
            second_agent.id,
        )


async def _collect_pages(
    repository: SessionRepository,
    *,
    sort: str,
    size: int,
    expression: FilterCondition | None = None,
) -> list[Session]:
    """Walk every page for a query and return all returned sessions in order."""
    collected: list[Session] = []
    cursor: str | None = None
    for _ in range(50):
        page, next_cursor = await repository.query(
            SessionFilter(expression=expression, cursor=cursor, size=size, sort=sort),
            include_payloads=False,
        )
        assert len(page) <= size
        collected.extend(page)
        if next_cursor is None:
            return collected
        cursor = next_cursor
    raise AssertionError("Pagination loop did not terminate")


def _expected_start_order(
    sessions: list[Session], *, descending: bool
) -> list[uuid.UUID]:
    """Compute the expected NULLS LAST started_at order independently."""
    timed = [s for s in sessions if s.started_at is not None]
    untimed = [s for s in sessions if s.started_at is None]

    def timed_key(session: Session) -> tuple[datetime, uuid.UUID]:
        assert session.started_at is not None
        return session.started_at, session.id

    timed.sort(key=timed_key, reverse=descending)
    untimed.sort(key=lambda s: s.id, reverse=descending)
    return [s.id for s in (*timed, *untimed)]


@pytest.mark.parametrize(
    "sort",
    ["ended_at:asc", "name:desc", "started_at", "number:up", "created:sideways"],
)
def test_session_filter_rejects_invalid_sort(sort: str) -> None:
    """SessionFilter accepts only created, started_at, and number with asc/desc."""
    with pytest.raises(ValidationError, match="Invalid sort"):
        SessionFilter(sort=sort)


@pytest.mark.parametrize("sort", ["started_at:asc", "started_at:desc"])
@pytest.mark.parametrize("page_size", [1, 2, 3, 10])
async def test_query_sorts_by_started_at_across_pages(
    two_agent_setup: TwoAgentSetup,
    sort: str,
    page_size: int,
) -> None:
    """Sort out-of-order timestamps, ties, offsets, and NULLs across every page."""
    repository, owner_id, agent_id, _ = two_agent_setup
    tied_ts = datetime(2026, 2, 1, 12, 0, tzinfo=UTC)
    plus_two = timezone(timedelta(hours=2))
    # Creation order intentionally disagrees with started_at order.
    timestamps: list[datetime | None] = [
        datetime(2026, 3, 1, 10, 0, tzinfo=UTC),
        None,
        tied_ts,
        datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        tied_ts,
        None,
        # 2026-01-01T00:00:00+02:00 is 2025-12-31T22:00:00Z (earliest instant).
        datetime(2026, 1, 1, 0, 0, tzinfo=plus_two),
        None,
    ]
    created: list[Session] = []
    for index, started_at in enumerate(timestamps, start=1):
        created.append(
            await repository.create(
                Session(
                    owner_id=owner_id,
                    agent_id=agent_id,
                    number=index,
                    origin=SessionOrigin.IMPORTED,
                    started_at=started_at,
                )
            )
        )

    collected = await _collect_pages(repository, sort=sort, size=page_size)
    expected = _expected_start_order(created, descending=sort.endswith(":desc"))
    assert [s.id for s in collected] == expected
    assert len({s.id for s in collected}) == len(created)


@pytest.mark.parametrize("sort", ["started_at:asc", "started_at:desc"])
async def test_query_sorts_all_null_started_at_across_pages(
    two_agent_setup: TwoAgentSetup,
    sort: str,
) -> None:
    """Sessions whose started_at is null still paginate cleanly by id tie-breaker."""
    repository, owner_id, agent_id, _ = two_agent_setup
    created = [
        await repository.create(
            Session(
                owner_id=owner_id,
                agent_id=agent_id,
                number=index,
                origin=SessionOrigin.RECORDED,
                started_at=None,
            )
        )
        for index in range(1, 6)
    ]

    collected = await _collect_pages(repository, sort=sort, size=2)
    expected = sorted((s.id for s in created), reverse=sort.endswith(":desc"))
    assert [s.id for s in collected] == expected


@pytest.mark.parametrize("sort", ["number:asc", "number:desc"])
@pytest.mark.parametrize("page_size", [1, 2, 3])
async def test_query_sorts_by_number_with_duplicate_numbers_across_agents(
    two_agent_setup: TwoAgentSetup,
    sort: str,
    page_size: int,
) -> None:
    """Break duplicate session numbers across agents deterministically by id."""
    repository, owner_id, agent_a, agent_b = two_agent_setup
    created: list[Session] = []
    # Interleave agents and numbers so creation order differs from number order.
    pairs = [
        (agent_a, 3),
        (agent_b, 1),
        (agent_a, 1),
        (agent_b, 3),
        (agent_a, 2),
        (agent_b, 2),
    ]
    for agent_id, number in pairs:
        created.append(
            await repository.create(
                Session(
                    owner_id=owner_id,
                    agent_id=agent_id,
                    number=number,
                    origin=SessionOrigin.RECORDED,
                )
            )
        )

    collected = await _collect_pages(repository, sort=sort, size=page_size)
    expected = [
        s.id
        for s in sorted(
            created,
            key=lambda s: (s.number, s.id),
            reverse=sort.endswith(":desc"),
        )
    ]
    assert [s.id for s in collected] == expected
    assert len({s.id for s in collected}) == len(created)


@pytest.mark.parametrize(
    "sort",
    ["started_at:asc", "started_at:desc", "number:asc", "number:desc"],
)
async def test_query_combines_filter_and_sort_across_pages(
    two_agent_setup: TwoAgentSetup,
    sort: str,
) -> None:
    """Combine a filter expression with started_at and number pagination."""
    repository, owner_id, agent_id, _ = two_agent_setup
    imported = [
        await repository.create(
            Session(
                owner_id=owner_id,
                agent_id=agent_id,
                number=number,
                origin=SessionOrigin.IMPORTED,
                started_at=started_at,
            )
        )
        for number, started_at in (
            (3, datetime(2026, 3, 1, tzinfo=UTC)),
            (1, None),
            (2, datetime(2026, 1, 1, tzinfo=UTC)),
        )
    ]
    await repository.create(
        Session(
            owner_id=owner_id,
            agent_id=agent_id,
            number=4,
            origin=SessionOrigin.RECORDED,
            started_at=datetime(2026, 2, 1, tzinfo=UTC),
        )
    )

    expression = FilterCondition(
        field="origin", op=FilterOp.EQ, value=SessionOrigin.IMPORTED
    )
    collected = await _collect_pages(
        repository, sort=sort, size=1, expression=expression
    )
    descending = sort.endswith(":desc")
    if sort.startswith("started_at"):
        expected = _expected_start_order(imported, descending=descending)
    else:
        expected = [
            s.id
            for s in sorted(
                imported, key=lambda s: (s.number, s.id), reverse=descending
            )
        ]
    assert [s.id for s in collected] == expected


async def test_query_rejects_mismatched_and_malformed_sort_cursors(
    two_agent_setup: TwoAgentSetup,
) -> None:
    """Reject cross-sort cursors and malformed payloads with ValidationError."""
    repository, owner_id, agent_id, _ = two_agent_setup
    for number, started_at in (
        (1, datetime(2026, 1, 1, tzinfo=UTC)),
        (2, datetime(2026, 2, 1, tzinfo=UTC)),
    ):
        await repository.create(
            Session(
                owner_id=owner_id,
                agent_id=agent_id,
                number=number,
                origin=SessionOrigin.RECORDED,
                started_at=started_at,
            )
        )

    _, start_cursor = await repository.query(
        SessionFilter(size=1, sort="started_at:asc"), include_payloads=False
    )
    assert start_cursor is not None

    for other_sort in ("started_at:desc", "number:asc", "created:desc"):
        with pytest.raises(ValidationError, match="Invalid cursor"):
            await repository.query(
                SessionFilter(size=1, sort=other_sort, cursor=start_cursor),
                include_payloads=False,
            )

    filter_hash = SessionFilter().compute_filter_hash()
    valid_uuid = str(uuid.uuid4())
    bad_start_payloads = [
        "missing-separator",
        f"not-a-date|{valid_uuid}",
        f"2026-01-01T00:00:00|{valid_uuid}",
        "2026-01-01T00:00:00+00:00|not-a-uuid",
    ]
    for payload in bad_start_payloads:
        bad_cursor = encode_cursor("started_at:asc", payload, filter_hash)
        with pytest.raises(ValidationError, match="Invalid cursor"):
            await repository.query(
                SessionFilter(size=1, sort="started_at:asc", cursor=bad_cursor),
                include_payloads=False,
            )

    bad_number_payloads = [
        "missing-separator",
        f"|{valid_uuid}",
        f"abc|{valid_uuid}",
        f" 1|{valid_uuid}",
        f"2147483648|{valid_uuid}",
        f"-2147483649|{valid_uuid}",
        "1|not-a-uuid",
    ]
    for payload in bad_number_payloads:
        bad_cursor = encode_cursor("number:asc", payload, filter_hash)
        with pytest.raises(ValidationError, match="Invalid cursor"):
            await repository.query(
                SessionFilter(size=1, sort="number:asc", cursor=bad_cursor),
                include_payloads=False,
            )


async def test_sessions_api_and_ui_routes_support_started_at_and_number_sort() -> None:
    """Both /api/v1/sessions and /api/v1/ui/sessions paginate sorted sessions."""
    tags = FakeTagRepository()
    evaluations = FakeEvaluationRepository()
    sessions = FakeSessionRepository(tags=tags, evaluations=evaluations)
    agent_repo = FakeAgentRepository()
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    session_service = SessionService(
        repository=sessions,
        task_repository=FakeTaskRepository(),
        agent_version_repository=FakeAgentVersionRepository(agent_repo),
        replay_repository=FakeReplayRepository(),
        import_repository=FakeImportRepository(),
        payload_store=build_payload_store().store,
    )
    evaluation_service = EvaluationService(
        repository=evaluations, session_repository=sessions
    )
    app.dependency_overrides[get_session_service] = lambda: session_service
    app.dependency_overrides[get_evaluation_service] = lambda: evaluation_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    app.dependency_overrides[authorize_with_task] = lambda: AuthContext(account=ACCOUNT)
    override_idempotency(app, ACCOUNT)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        agent_id = str(uuid.uuid4())
        created = []
        for started_at in (
            "2026-03-01T00:00:00Z",
            None,
            "2026-01-01T00:00:00Z",
            "2026-02-01T00:00:00Z",
        ):
            response = await client.post(
                "/api/v1/sessions",
                json={
                    "agent_id": agent_id,
                    "origin": "imported",
                    "inputs": None,
                    "outputs": None,
                    "started_at": started_at,
                    "metadata": {},
                },
            )
            assert response.status_code == 201
            created.append(response.json())

        # started_at:asc puts Jan, Feb, Mar, then null.
        collected_ids: list[str] = []
        cursor: str | None = None
        while True:
            params: dict[str, str | int] = {"sort": "started_at:asc", "size": 2}
            if cursor is not None:
                params["cursor"] = cursor
            response = await client.get("/api/v1/sessions", params=params)
            assert response.status_code == 200
            body = response.json()
            collected_ids.extend(item["id"] for item in body["items"])
            cursor = body["next_cursor"]
            if cursor is None:
                break
        assert collected_ids == [
            created[2]["id"],
            created[3]["id"],
            created[0]["id"],
            created[1]["id"],
        ]

        # started_at:desc puts Mar, Feb, Jan, then null.
        response = await client.get(
            "/api/v1/ui/sessions", params={"sort": "started_at:desc", "size": 10}
        )
        assert response.status_code == 200
        assert [item["session"]["id"] for item in response.json()["items"]] == [
            created[0]["id"],
            created[3]["id"],
            created[2]["id"],
            created[1]["id"],
        ]

        # number:desc returns 4, 3, 2, 1.
        response = await client.get(
            "/api/v1/sessions", params={"sort": "number:desc", "size": 10}
        )
        assert response.status_code == 200
        assert [item["number"] for item in response.json()["items"]] == [4, 3, 2, 1]

        # Invalid sort field returns 422.
        invalid = await client.get("/api/v1/sessions", params={"sort": "ended_at:asc"})
        assert invalid.status_code == 422


async def test_sessions_api_pg_sort_pagination() -> None:
    """Verify started_at and number sorting end to end against migrated PostgreSQL."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    async with lifespan_client(db_settings()) as client:
        agent_a = (
            await client.post("/api/v1/agents", json={"name": "agent-a"})
        ).json()["id"]
        agent_b = (
            await client.post("/api/v1/agents", json={"name": "agent-b"})
        ).json()["id"]

        specs = [
            (agent_a, "2026-03-01T00:00:00Z"),
            (agent_b, None),
            (agent_a, "2026-01-01T00:00:00Z"),
            (agent_b, "2026-02-01T00:00:00Z"),
            (agent_a, None),
        ]
        created = []
        for agent_id, started_at in specs:
            resp = await client.post(
                "/api/v1/sessions",
                json={
                    "agent_id": agent_id,
                    "origin": "imported",
                    "inputs": None,
                    "outputs": None,
                    "started_at": started_at,
                    "metadata": {},
                },
            )
            assert resp.status_code == 201
            created.append(resp.json())

        async def walk_api(sort: str, size: int) -> list[dict[str, object]]:
            items: list[dict[str, object]] = []
            cursor: str | None = None
            while True:
                params: dict[str, str | int] = {"sort": sort, "size": size}
                if cursor is not None:
                    params["cursor"] = cursor
                response = await client.get("/api/v1/sessions", params=params)
                assert response.status_code == 200
                body = response.json()
                items.extend(body["items"])
                cursor = body["next_cursor"]
                if cursor is None:
                    return items

        asc_items = await walk_api("started_at:asc", size=2)
        assert [item["id"] for item in asc_items[:3]] == [
            created[2]["id"],
            created[3]["id"],
            created[0]["id"],
        ]
        assert {item["id"] for item in asc_items[3:]} == {
            created[1]["id"],
            created[4]["id"],
        }

        desc_items = await walk_api("started_at:desc", size=2)
        assert [item["id"] for item in desc_items[:3]] == [
            created[0]["id"],
            created[3]["id"],
            created[2]["id"],
        ]
        assert {item["id"] for item in desc_items[3:]} == {
            created[1]["id"],
            created[4]["id"],
        }

        num_asc = await walk_api("number:asc", size=2)
        assert [item["number"] for item in num_asc] == [1, 1, 2, 2, 3]
        assert len({item["id"] for item in num_asc}) == 5

        num_desc = await walk_api("number:desc", size=2)
        assert [item["number"] for item in num_desc] == [3, 2, 2, 1, 1]
        assert len({item["id"] for item in num_desc}) == 5

        invalid = await client.get("/api/v1/sessions", params={"sort": "ended_at:desc"})
        assert invalid.status_code == 422
