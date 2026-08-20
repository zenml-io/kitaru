# Kitaru devtools

Shared helpers for local development and testing: spin up servers and databases, generate dummy traces, record dummy sessions through a fake-LLM agent, and evaluate them. Everything is deterministic, so generated traces, live recordings, and replays line up with each other.

All commands run from the repo root with the server extra installed (`uv sync --extra server`). Docker is required for Postgres.

| File | Purpose |
|---|---|
| `stack.py` | Server, database, and worker lifecycle (CLI and importable helpers) |
| `traces.py` | Generate dummy JSONL traces for the built-in `kitaru/kitaru-jsonl` importer |
| `agent.py` | Dummy agent that simulates LLM and tool calls and records sessions |
| `evaluators.py` | Evaluator plugin file with one entrypoint per result shape |
| `importer.py` | JSONL importer plugin with configurable failure modes |
| `fixtures.py` | Register the dummy agent, importer, and evaluators against a running server |
| `seed.py` | One-command end-to-end seed: stack, import, live runs, experiment, evaluations |
| `load.py` | Concurrent read and write load generation with latency percentiles |
| `dbstats.py` | Postgres statistics reports, lock sampling, and index diagnostics |
| `overview.py` | Read-only dashboard of a running server's agents, sessions, jobs, and evaluations |
| `resilience.py` | Worker lifecycle scenarios: crashes, zombies, fleet death, retry exhaustion |
| `simulation.py` | Deterministic session simulation core shared by `traces.py` and `agent.py` |

## Stack

Start Postgres (via `docker compose up -d db`) and a server process from the local source tree:

```bash
uv run python devtools/stack.py up
```

The command prints `export KITARU_API_URL=...` lines for the shell. Useful flags:

- `--db-name NAME` picks the database (default `kitaru_dev`), `--fresh` drops it first.
- `--auth local` enables authentication and prints an API key export. The default is `none`.
- `--env KEY=VALUE` passes extra server settings, e.g. `--env KITARU_SERVER_DB_POOL_SIZE=20 --env KITARU_SERVER_DB_MAX_OVERFLOW=60` for load testing.
- `--docker` runs server and database from `docker-compose.yml` instead of a local process.

`down` stops the server (`--drop-db` also drops its database), `status` reports what is running, and `db` manages databases on the test Postgres:

- `db create|drop|list` for plain database management.
- `db snapshot NAME [--source DB]` copies a database into a snapshot, and `db restore NAME [--target DB]` recreates the target from it. Both default to the running stack's database and take about a second, so seed once, snapshot, and restore whenever a test trashed the data. Snapshots briefly terminate active connections on the involved databases, the server's pool reconnects on the next request. A snapshot is not a file: it is a regular database on the same Postgres, stored in the `kitaru_db` container's `postgres_data` volume, so it survives container restarts and disappears only when dropped or when the volume is removed. Name snapshots `kitaru_snap_<what>` so they show up in `db list` (which only lists `kitaru*` databases) and can be cleared with `db clean --prefix kitaru_snap_`.
- `db clean [--prefix P]` drops accumulated `kitaru_seed_*` databases, keeping the running stack's one.
- `db psql [NAME]` opens an interactive psql inside the `kitaru_db` container.

Jobs (imports, session runs, experiments) need a worker:

```bash
uv run python devtools/stack.py workers --count 2
```

## Generating traces

```bash
uv run python devtools/traces.py --count 100 --output traces.jsonl
```

Each line is a full session with nodes of every type (LLM calls, tool calls, subagent calls, spans) and every field populated: selectors, reasoning, tokens, costs, model params, attributes, and metadata including the expected outputs. Shape flags: `--min-turns`/`--max-turns` control session length, `--failure-rate` produces failed sessions, `--big-payload-every N` attaches a `--payload-bytes` sized context blob to every Nth session, `--malformed N` appends broken lines to exercise import failure handling, and `--seed`/`--start-index` produce distinct batches. Every generated line is validated against the importer schema, so imports cannot fail on schema drift.

Import the file with the built-in importer:

```bash
uv run kitaru session import traces.jsonl --importer 'kitaru/kitaru-jsonl@1' --agent 'dummy-agent@1' --wait
```

The built-in importer installs its plugin package from PyPI at task time. `fixtures.py` also registers `dummy-jsonl` from [importer.py](importer.py), a script plugin with the same parse contract that works in offline or version-pinned environments and adds failure modes (see below). Both references are exact `NAME@VERSION` pairs, a bare name is rejected. `seed.py` uses `dummy-jsonl` by default, `--importer kitaru/kitaru-jsonl` switches back.

## Dummy agent

`agent.py` simulates an agent loop without any real LLM calls. Tool results and completions are derived from hashes of their inputs, so the same session inputs always produce the same tool calls and outputs. Sessions generated by `traces.py` use the same simulation, which means a replay of an imported trace performs identical tool calls and history lookups hit the recorded baseline.

Registered through `fixtures.py`, the agent runs as a worker task for session runs and experiment replays. In replay mode it honors:

- `ReplayOverride.model` (string or mapping) and `ReplayOverride.model_params`, which change the recorded model fields and completion texts.
- `ReplayOverride.prompt`, which changes the question and thereby the tool inputs, making history lookups miss on purpose.
- Tool policies `passthrough`, `static` (exact and subset case matching), and `history` (all scopes, with baseline occurrence tracking). All `on_miss` behaviors are supported. The `llm` policy is rejected.

It can also record sessions directly against the API without any worker infrastructure:

```bash
uv run python devtools/agent.py --record 10 --agent-id <AGENT_ID>
```

`DUMMY_AGENT_LATENCY_SCALE` (or `--latency-scale`) makes the agent sleep a fraction of every simulated node duration for realistic timings.

## Evaluators

`fixtures.py` registers each entrypoint of `evaluators.py` as its own evaluator, so experiments can pick exactly the result shapes they need:

| Evaluator | Results |
|---|---|
| `dummy-outcome` | Pass or fail verdicts for completion and node health (bool) |
| `dummy-expected-match` | Outputs vs the expected outputs in session metadata (bool) |
| `dummy-efficiency` | Token, cost, latency, and tool usage scores (float) |
| `dummy-grade` | A grade label with a numeric score and verdict (categorical) |
| `dummy-notes` | A plain text description of the session (string) |
| `dummy-suite` | All of the above from a single evaluator |

`token_budget` can be passed via `EvaluatorConfig.params` to shift the efficiency and grade thresholds. The server also bootstraps the built-in `kitaru/*` evaluators, which work on these sessions too.

## Fixtures

Register the dummy agent and all evaluators against `KITARU_API_URL`:

```bash
uv run python devtools/fixtures.py
```

Registration is idempotent per name and adds a new plugin version on each run. It also marks the account's onboarding survey finished, so the dashboard opens on the seeded data instead of the sign-up form. A service account credential cannot do that and is reported as skipped.

## Seed

The whole flow in one command, useful for UI development and smoke testing:

```bash
uv run python devtools/seed.py --sessions 50 --keep
```

This starts a fresh stack (or targets `--base-url`), registers fixtures (including the onboarding skip), starts in-process workers, imports generated traces, records live sessions, builds a cohort, runs an experiment with a model override and a baseline history tool policy, evaluates the results, and prints a check summary. `--keep` leaves the server running for inspection, `--evaluators` selects which evaluators the experiment uses, and the trace shape flags from `traces.py` apply here as well.

## Failure modes

Each stage can inject failures deterministically, so the same configuration reproduces the same failures:

- **Agent** (environment variables, bake them into an agent version with `fixtures.py --agent-env KEY=VALUE --display-version NAME`): `DUMMY_AGENT_FAILURE_RATE` fails sessions through the simulated model, `DUMMY_AGENT_CRASH_RATE` kills the process before finalization and leaves the session in progress, `DUMMY_AGENT_CRASH_BEFORE_SESSION_RATE` kills it before the session exists, `DUMMY_AGENT_SLEEP_SECONDS` delays every task. All rates are decided per session inputs, so a given input either always fails or never does.
- **Evaluators** (via `EvaluatorConfig.params`): `failure_rate` raises for a deterministic fraction of sessions, `sleep_ms` delays every evaluation.
- **Importer** (`dummy-jsonl` via import params): `fail_line_rate` turns a deterministic fraction of valid lines into import failures, `sleep_ms_per_line` slows parsing, `crash` fails the whole import task.

`traces.py --failure-rate` and `--malformed` cover the remaining cases: sessions that completed with a failed status and lines the importer must reject.

## Load generation

Generate concurrent load against a running server:

```bash
uv run python devtools/load.py --duration 30 --concurrency 10 --write-rate 0.1
```

Read operations cover session lists, session node reads, agent lists, and evaluation lists. `--write-rate` mixes in full session recordings through the dummy agent (requires registered fixtures). The report prints per-operation counts, error outcomes, requests per second, and latency percentiles. `blast()` and `percentile()` are importable for ad hoc scripts.

## Inspecting a running server

Print a dashboard of what a server holds, resolved from `KITARU_API_URL` or the running stack:

```bash
uv run python devtools/overview.py
```

Sections cover agents, session counts by origin and status plus recent sessions, jobs, evaluations, cohorts, and experiments. Pass section names to print a subset, `--limit N` caps rows per table. For raw SQL access use `stack.py db psql`.

## Worker resilience scenarios

Verify task recovery when workers misbehave, each scenario on its own ephemeral database and server with fast sweeper settings:

```bash
uv run python devtools/resilience.py
```

- `worker-crash`: SIGKILL a worker mid-task, another worker completes the reclaimed task.
- `worker-zombie`: SIGSTOP a worker so heartbeats stop, its tasks are reclaimed, and its stale writes after SIGCONT are fenced off by the attempt check.
- `fleet-death`: SIGKILL every worker, a fresh worker drains the requeued tasks.
- `abandonment`: with the retry limit at one, a killed worker's task ends abandoned.

Pass scenario names to run a subset. Databases of failed scenarios are kept for inspection (`--keep` keeps all), worker and server logs land under `devtools/.run/resilience/`. `--db-name BASE` names the databases `BASE_<scenario>` instead of a random `kitaru_resil_*`, recreating and keeping them on every run so a kept database is easy to find again. The runner checks that the venv `kitaru` CLI starts before the first scenario, and a claim timeout prints the tail of every worker log. A SIGKILLed worker orphans its in-flight agent processes by design, they exit on their own once their API writes are rejected.

## DB statistics

Inspect what the workload did to Postgres:

```bash
uv run python devtools/dbstats.py report
```

`--db-name` defaults to the running stack's database and `--dsn` overrides both. A name that does not exist is reported as such instead of raising a connection error.

The report covers per-table sizes and scans, cache hit ratios, missing index candidates, unused and duplicate indexes, connection states, and index definitions, written as markdown plus raw JSON under `devtools/.run/reports/`. Query-level statistics need the `pg_stat_statements` extension, which the compose db does not preload: `dbstats.py enable` turns it on and restarts the db container, failing loudly when the restart did not apply the setting. The workflow for profiling a manual UI session is `reset`, click around, `report`. `sample --seconds 30` watches for lock waits and blocking queries during a workload, and the `LockSampler` class is importable for harnesses.
