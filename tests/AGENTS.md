# Test rules

Ordinary async tests and fixtures are plain `async def`. pytest-asyncio runs in auto mode, so they need no decorators or `asyncio.run` wrappers. Synchronous Hypothesis `@given` tests are the exception: call async code with `asyncio.run` inside the test body.

## Test surfaces per server resource

Cover all four surfaces below for a new persistent public API resource. For changes to an existing resource, select the surfaces that prove the affected behavior. Transaction, locking, migration, and cross-request changes need PostgreSQL coverage; an in-memory fake cannot prove those contracts.

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

Set `KITARU_TEST_REQUIRE_POSTGRES=1` to fail before collection if PostgreSQL is unavailable. CI enables this for both Python 3.14 base jobs; local runs and the other Python versions keep optional database skips.

Repository sessions copy a session-scoped empty schema into each unique database instead of recreating every table for every test. The template has no open connections while it is copied and is dropped at session teardown. API lifespan and historical migration tests still initialize their own databases. CI stores the disposable PostgreSQL data directory in memory; it retains the normal PostgreSQL transaction and durability settings.

## CI test distribution

The Python 3.14 base suite runs on two independent runners, each with its own PostgreSQL service. Set both `KITARU_TEST_SHARD_INDEX` (zero-based) and `KITARU_TEST_SHARD_COUNT` to select a partition. Pytest assigns whole files using SHA-256 of their repository-relative POSIX paths modulo the count, preserving test order within each file and automatically including new files. With both variables unset or empty, pytest runs the full selection as usual. Invalid or incomplete settings fail instead of silently dropping tests.

For local reproduction, prefix the normal base pytest command with `KITARU_TEST_SHARD_INDEX=0 KITARU_TEST_SHARD_COUNT=2`, then repeat with index `1`. Both partitions together must contain exactly the unpartitioned collection, with no overlap. Database-required behavior is independent of the partition settings.

## Property-based tests

Hypothesis tests live next to the surface they cover: `plugins/tests/importers/test_fuzz_parse.py` and `test_normalization_properties.py` (importer parsing and normalization contracts), `plugins/tests/evaluators/test_numeric_properties.py`, `tests/api_models/test_wire_properties.py`, `tests/mcp/test_fuzz_tools.py` (MCP tool boundary, requests generated from each tool's JSON schema), `tests/cli/test_redaction_properties.py`, `tests/server/test_fuzz_filters.py` (recursive JSON list filters), and codec/capture property modules under `plugins/tests/adapters/`.

Three profiles are registered in each root's `conftest.py` and selected with `HYPOTHESIS_PROFILE`: `dev` (100 examples, default locally), `ci` (50 examples, fixed seed; default when `CI` is set, so PR runs are deterministic), and `nightly` (2000 examples, random; used by `just fuzz` and the `fuzz-nightly` workflow). The named recipes are `fuzz-importers`, `fuzz-evaluators`, `fuzz-api-models`, `fuzz-mcp`, `fuzz-adapters`, `fuzz-filters`, and `fuzz-api`. `@given` tests are sync; call async code with `asyncio.run` inside the body.

Known bugs are pinned with `@pytest.mark.xfail(strict=True, reason="<issue>")` example tests, and the generators exclude the matching input class with a comment naming the same issue. Fixing the bug makes the xfail fail; remove the marker and the generator exclusion in the same PR. Failing examples are saved under `.hypothesis/examples` and replayed first on the next run.

Mark generated-input MCP tests with `mcp_fuzz`. PR CI runs those properties on Python 3.14 only; fixed MCP regression tests continue on every supported Python version. The nightly MCP fuzz job remains on Python 3.12.

The CI profile omits Hypothesis's explanation phase only for the known-failing invalid-input MCP property. It still generates and shrinks examples; local and nightly profiles retain explanation.

## API fuzzing

`tests/server/test_fuzz_api.py` generates requests from `openapi/openapi.json` with Schemathesis and sends them to the real app. Opt in with `KITARU_FUZZ=1` (the suite skips otherwise) and start the database first with `docker compose up -d db`; `just fuzz-api` runs the deep nightly configuration. The `fuzz` dependency group carries `schemathesis`, so run under `uv run --extra server --group fuzz`.

`tests/server/fuzz_server.py` boots one real server per session on its own disposable database, with real Alembic migrations and a real bearer token from `POST /api/v1/login`. It runs under uvicorn in a background thread rather than an in-process ASGI transport, because those transports rerun the lifespan — migrations included — for every generated example. An ASGI wrapper records unhandled exceptions so a failure names the exception and not just the endpoint; a 500 body carries no detail on its own.

The only assertion is that the server never answers 5xx. One database is shared for the whole session, so an earlier operation's rows are visible to a later one, which makes "was this input rejected?" depend on run order while leaving "did the server crash?" well-posed. Schemathesis's `negative_data_rejection` is therefore deliberately not run. Negative generation is still on, so schema-violating input is still sent; only the rejection assertion is dropped.

Known defects are listed in `KNOWN_FAILURES` keyed by method and path, with the issue number as the reason, and skip rather than mask everything behind them in the same operation. Delete an entry as part of closing its issue.

`KITARU_FUZZ_MAX_EXAMPLES` sets depth (default 25) and `KITARU_FUZZ_RANDOM=1` turns off `derandomize` so a nightly explores fresh inputs; the default stays derandomized so a failure reproduces. `KITARU_FUZZ_CAPTURE` writes captured tracebacks to a JSONL file. Findings scale steeply with depth, so prefer nightly depth over a shallow gate.
