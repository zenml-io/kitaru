# Devtools rules

Local harness for running Kitaru end to end: servers, databases, workers, deterministic sessions, and evaluations. Reach for it when a change needs a live stack or realistic data. Unit tests under `tests/` cover ordinary changes, so do not start a stack to verify something a test can prove.

Everything is deterministic. The same seed and flags produce the same sessions, the same tool calls, and the same failures, so a replay of an imported trace hits the recorded baseline.

| File | Use it for |
|---|---|
| `stack.py` | Server, database, and worker lifecycle |
| `seed.py` | One-command end-to-end stack with data |
| `traces.py` | Generate JSONL traces to import |
| `agent.py` | Dummy agent, runs as a worker task or records directly |
| `fixtures.py` | Register the agent, importer, and evaluators |
| `evaluators.py` | Evaluator plugin, one entrypoint per result shape |
| `importer.py` | JSONL importer plugin with failure modes |
| `overview.py` | Read-only dashboard of a running server |
| `load.py` | Concurrent load with latency percentiles |
| `dbstats.py` | Postgres and query statistics, lock sampling |
| `resilience.py` | Worker crash, zombie, fleet-death, abandonment scenarios |
| `simulation.py` | Shared simulation core, do not invoke directly |

Requires `uv sync --extra server` and Docker for Postgres. Add `--extra cli` when you shell out to `kitaru`.

## Isolate your stack

`devtools/.run/server.json` holds exactly one server record. A second `stack.py up` overwrites it, and `stack.py down` and `status` then target the wrong server. Another agent or a background session may already own it.

Always pass an explicit `--db-name` that no one else would pick. Never assume the recorded stack is yours: check `stack.py status` and `git worktree list` before acting on it.

`seed.py --keep` starts a server without recording it at all, so `stack.py down` cannot stop it. Keep the printed pid and port, and kill it yourself.

## Getting a stack

```bash
uv run python devtools/stack.py up --db-name kitaru_<yourtask> --fresh
uv run python devtools/stack.py workers --count 2
```

Jobs (imports, session runs, experiments) settle only while a worker runs. A job stuck pending means no worker, not a bug.

`--auth local` prints an API key export, the default is `none`. `--env KEY=VALUE` passes server settings. `--docker` runs from `docker-compose.yml` instead.

For a full stack with data in one command:

```bash
uv run python devtools/seed.py --sessions 50 --db-name kitaru_<yourtask> --keep
```

That imports generated traces, records live sessions, builds a cohort, runs an experiment with a model override and a baseline history tool policy, evaluates the results, and prints a pass/fail check summary. Treat a `[FAIL]` line as the signal, not the prose above it. `--base-url` targets a running server instead of starting one, and needs `KITARU_API_KEY` exported when that server authenticates.

## Resetting instead of reseeding

Seeding is slow, snapshots are not. Seed once, then snapshot, then restore whenever a test trashed the data:

```bash
uv run python devtools/stack.py db snapshot kitaru_snap_<what>
uv run python devtools/stack.py db restore kitaru_snap_<what>
```

Both default to the running stack's database and take about a second. They terminate active connections, and the server's pool reconnects on the next request. A snapshot is a database, not a file, so name it `kitaru_snap_*` to keep `db list` and `db clean --prefix` useful.

## Exact command forms

`--importer` and `--agent` take `NAME@VERSION`. A bare name is rejected:

```bash
uv run kitaru session import traces.jsonl --importer 'dummy-jsonl@1' --agent 'dummy-agent@1' --wait
```

`dummy-jsonl` is a script plugin registered by `fixtures.py` and works offline. The built-in `kitaru/kitaru-jsonl@1` installs its package from PyPI at task time and only accepts well-formed input. Use `dummy-jsonl` unless you are specifically testing the built-in.

`traces.py --malformed N` caps at the number of canned broken lines. Asking for more is clamped, not an error.

`dbstats.py --db-name` defaults to the running stack's database. `dbstats.py enable` restarts the Postgres container to preload `pg_stat_statements`, which interrupts every other stack on that container.

## Injecting failures

Failures are decided from a hash of the inputs, so a given input either always fails or never does. Do not retry expecting a different outcome, change the inputs or the rate.

- Agent, via environment (bake in with `fixtures.py --agent-env KEY=VALUE --display-version NAME`): `DUMMY_AGENT_FAILURE_RATE` fails sessions through the simulated model, `DUMMY_AGENT_CRASH_RATE` kills the process before finalization and leaves the session in progress, `DUMMY_AGENT_CRASH_BEFORE_SESSION_RATE` kills it before the session exists, `DUMMY_AGENT_SLEEP_SECONDS` delays every task.
- Evaluators, via `EvaluatorConfig.params`: `failure_rate` raises for a fraction of sessions, `sleep_ms` delays every evaluation.
- Importer `dummy-jsonl`, via import params: `fail_line_rate` turns valid lines into import failures, `sleep_ms_per_line` slows parsing, `crash` fails the whole task.
- `traces.py --failure-rate` produces sessions that completed with a failed status, `--malformed` produces lines the importer must reject.

## Replay coverage

`agent.py` honors `ReplayOverride.model` (string or mapping), `ReplayOverride.model_params`, and `ReplayOverride.prompt`, plus tool policies `passthrough`, `static` (exact and subset case matching), and `history` (all scopes, with baseline occurrence tracking) with every `on_miss` behavior. The `llm` policy is rejected on purpose.

A prompt override changes tool inputs by reseeding the per-session RNG, so whether it forces a history miss depends on the baseline. Verify the miss actually happened before concluding an `on_miss` path works. No generated session calls the same tool twice with identical inputs, so occurrence identity needs hand-built baseline nodes.

## Inspecting and measuring

```bash
uv run python devtools/overview.py            # agents, sessions, jobs, evaluations, cohorts, experiments
uv run python devtools/load.py --duration 30 --concurrency 10 --write-rate 0.1
uv run python devtools/dbstats.py report      # sizes, scans, index diagnostics, top queries
uv run python devtools/dbstats.py sample --seconds 30   # lock waits during a workload
uv run python devtools/resilience.py          # worker recovery scenarios
```

`overview.py` takes section names to print a subset. `load.py --write-rate` needs registered fixtures. `resilience.py --db-name BASE` gives each scenario a stable `BASE_<scenario>` database and keeps it, otherwise databases are random and only failed ones survive. Logs land under `devtools/.run/`, which is gitignored.

Read the numbers before reporting them. Latency that grows with concurrency while throughput stays flat is saturation, not regression, and a single-process server saturates on CPU well before Postgres does.

## Clean up

Leaving stacks and databases behind breaks the next agent. Before you finish:

- Stop the servers and workers you started, by pid when `stack.py down` does not know about them.
- Drop the databases you created, and only those. Never drop one you did not create.
- `stack.py db list` shows every `kitaru*` database, `db clean --prefix` removes a family.
