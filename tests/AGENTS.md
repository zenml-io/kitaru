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

Tests point at `localhost:5433` (override with `KITARU_TEST_DB_HOST` and
`KITARU_TEST_DB_PORT`) and expect `docker compose up -d db`. Each pg test
creates a uniquely named database and drops it on teardown, so tests stay
independent and concurrent runs never share state. A session-scoped fixture
reaps databases left behind by a killed run, age-gated on the timestamp in
the database name so it never drops a concurrent run's live databases.

## Property-based tests

Hypothesis tests live next to the surface they cover: `plugins/tests/importers/test_fuzz_parse.py` (importer `parse()` contract), `tests/mcp/test_fuzz_tools.py` (MCP tool boundary, requests generated from each tool's JSON schema), `tests/cli/test_redaction_properties.py`, and `plugins/tests/adapters/langgraph/test_capture_properties.py`.

Three profiles are registered in each root's `conftest.py` and selected with `HYPOTHESIS_PROFILE`: `dev` (100 examples, default locally), `ci` (50 examples, fixed seed; default when `CI` is set, so PR runs are deterministic), and `nightly` (2000 examples, random; used by `just fuzz` and the `fuzz-nightly` workflow — `fuzz-importers` covers the plugins-tree property files and `fuzz-mcp` the core-tree ones, so all four files run nightly). `@given` tests are sync; call async code with `asyncio.run` inside the body.

Known bugs are pinned with `@pytest.mark.xfail(strict=True, reason="<issue>")` example tests, and the generators exclude the matching input class with a comment naming the same issue. Fixing the bug makes the xfail fail; remove the marker and the generator exclusion in the same PR. Failing examples are saved under `.hypothesis/examples` and replayed first on the next run.
