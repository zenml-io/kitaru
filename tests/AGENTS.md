# Test rules

Async tests and fixtures are plain `async def`. pytest-asyncio runs in auto
mode, so no decorators and no `asyncio.run` wrappers.

## Test surfaces per server resource

1. Service tests against the in-memory fake repository
   (`tests/server/test_<x>_service.py`).
2. REST tests over `httpx.ASGITransport` with the service dependency
   overridden to the fake (`tests/server/test_<x>s_api.py`).
3. Repository contract tests parametrized over the in-memory fake and the SQL
   implementation (`tests/server/test_<x>_repository.py`). One shared suite
   runs against both backends so the fake cannot drift from the real
   repository. The postgres parameter skips when the local database is
   unreachable.
4. An end-to-end PostgreSQL test that runs the app through its lifespan, which
   also executes the Alembic migrations
   (`tests/server/test_<x>s_api_pg.py`). Cross-request visibility proves the
   per-request commit.

The SDK gets a round-trip test in `tests/client/` routing the client through
`httpx.ASGITransport`.

## Fakes

Fake repositories live in `tests/conftest.py` and implement the full
repository Protocol, including domain error behavior and `updated` timestamp
renewal on update.

## PostgreSQL

Tests point at `localhost:5433` (override with `KITARU_TEST_DB_HOST`,
`KITARU_TEST_DB_PORT`, and `KITARU_TEST_DB_NAME`) and expect
`docker compose up -d db`. Each pg test recreates its database with
`force_drop`, so tests stay independent.
