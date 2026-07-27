# Plugin implementation notes

Decisions taken while implementing the plugins plan
(`design/plugins_plan.md`) that the plan did not specify, plus issues
that came up and how they were resolved. Scope: registry and blobs,
scoring decoupling, server-side imports, and the job-system hot path,
verified live with a local server, a pool worker, registered scorer and
importer plugins, and a replay driven through the API.

## Orchestration

- Four parallel exploration passes mapped the job system, scoring and
  replay flow, the new-resource conventions, and sessions plus the
  example layout before any code changed.
- Implementation followed the plan's sequencing: registry and blobs,
  scoring decoupling (server side, then worker and harness), imports
  (server side in parallel with the scoring worker slice, then the
  harness), hot path. Each slice was implemented against a spec
  distilled from the exploration maps, then integrated and reviewed
  here. Parallel slices ran with disjoint file ownership and separate
  test database names.
- The test suite is not safe to run twice concurrently against the
  shared Postgres container, a doubled run produced 6 failures and 32
  errors that vanished on a solo rerun (1257 passed).

## Decisions

### Registry and blobs

- `BlobStorage` takes `store(sha256, content) -> BlobLocation` and
  `load(blob) -> bytes`, with `BlobLocation` a frozen value carrying
  `data` or `uri`. The `Blob` entity enforces exactly-one-of data/uri,
  so content placement resolves before entity construction. Storage
  keys on sha256, keeping the seam content-addressed for the later
  object storage adapter. Rows and bytes stay behind separate
  interfaces (`BlobRepository`, `BlobStorage`) even though both hit
  the blob table today.
- Upload dedupe is a sha256 pre-lookup plus a conflict catch with
  re-lookup, so concurrent identical uploads both get 200 with the
  surviving row. New blobs return 201, dedupe hits 200, the worker
  upsert precedent.
- One `PluginService` and one shared plugin repository, kind fixed per
  router via a module constant. Version allocation lives in the
  repository (`UPDATE ... RETURNING` on `latest_version`), the fake
  mirrors it, the unique constraint is the backstop.
- Only the `inline` format is accepted, `archive` and `git` stay
  deferred per the plan, so `PluginFormat` has a single member for now.
- Scorer DTOs expose no provider, metadata, or kind. The domain rejects
  provider and metadata on scorers, importers carry both.
- No PATCH endpoints on scorers or importers, versions are append-only
  and the plugin row has nothing worth mutating yet.
- Oversize blob uploads map to 422 (`BlobTooLarge` is a domain
  validation error), not 413.
- `MAX_BLOB_SIZE_BYTES` server setting, default 100 MB.

### Scoring decoupling

- The fan-out uniqueness constraint uses a dedicated `scorer_name`
  column mirroring the config snapshot's name. Expression uniqueness
  over JSONB fights the ORM and autogenerate, the plan allowed the
  column.
- Score children copy the parent replay's execution target. Source-arm
  children carry the replay's `agent_version_id` for routing and env,
  registry children carry none.
- Run-scoped claims match score jobs whose parent belongs to the run,
  otherwise `Runner.run_experiment_run` deadlocks with replays parked
  in `scoring` and nobody claiming their children. A `parent_job_id`
  claim scope exists for the same reason on standalone replays.
- The agent-filtered claim matches jobs with no agent binding (registry
  score jobs) in addition to the filtered agents. A worker serving
  agent A must never claim source-arm score jobs of agent B, but
  registry code is agent-agnostic by construction.
- A failing score child fails the parent replay and cancels its
  remaining non-terminal siblings. The plan only required the parent
  failure, leaving siblings running after the verdict is already
  failed wastes worker time.
- Score jobs report their value via a score-only PATCH while running,
  the worker still finalizes status from the harness exit code. This
  keeps the worker's exit-code contract uniform across kinds.
- Replays no longer complete from worker-submitted scores at all. The
  server owns verdict arithmetic (`ScoringPolicy` evaluation moved
  into the domain), the worker only moves replays to `scoring` after
  result-session verification.

### Imports

- `resolve_plugin_version` is kind-agnostic in a shared
  `plugin_resolution` helper, the scorer resolution delegates to it.
- Import params ride the existing `inputs` column, the session-run
  precedent.
- Import stats mirror the score-value contract: a stats-only PATCH
  records them while the job runs, completion requires them
  (`JobMissingStats`), retry clears them. Failure samples are bounded
  to 20 entries in the domain.
- `POST /v1/imports` returns the job (`JobResponse`, kind import),
  there is no separate import resource. Listing rides `GET /v1/jobs`
  with the kind filter.
- The import harness lives at `kitaru/imports.py` and runs as
  `python -m kitaru.imports`, settling the plan's open question on
  harness module names together with `kitaru.score`.
- `ParsedSession` (the plan's other open question) carries the session
  fields plus a NESTED `ParsedNode` tree. The harness owns node ids,
  parent links, sequence numbers, and batching, importer code only
  parses. Importers yield `ParseFailure(line, external_id, error)` for
  recoverable per-line problems instead of raising, a raise is a
  harness-level failure that fails the job.
- The importer-author contract lives in `kitaru/importing.py`, split
  from the `kitaru/imports.py` entrypoint. Running the entrypoint as
  `python -m` loads a second module copy, so isinstance checks against
  models the plugin imported from the entrypoint module failed. The
  scoring.py/score.py split had the same shape, now both harnesses
  follow it, with shared plugin loading in `kitaru/plugin_loader.py`.
- Importer params pass through as `parse(payload, **params)`, matching
  the scorer call shape.
- A node ingest failure counts the session as failed, not created. The
  session row exists but its data did not land.
- Two plan gaps closed for imported sessions: the importer spec carries
  the plugin's `provider` (sessions with origin imported require one,
  and import creation rejects providers outside the session enum), and
  `POST /v1/imports` requires an `agent_id` stored in a new nullable
  job column (sessions bind to an agent, the old CLI design took
  --agent, the plan's request body omitted it).

### Harness and worker

- The score harness lives at `kitaru/score.py` and runs as
  `python -m kitaru.score`. It fetches the job spec, builds the
  `SessionView`, loads the scorer (registry arm from the file at
  `KITARU_JOB_PLUGIN_PATH`, source arm through the existing source ref
  loader), validates the float, PATCHes the score, and exits 0. The
  worker owns status finalization from the exit code.
- `KITARU_JOB_PLUGIN_PATH` joins the env contract for the plugin code
  path, the plan only named the payload variable. It is stripped from
  inherited env like the other contract vars.
- Registry scorers run under `uv run --with <dep>` only when the plugin
  file declares PEP 723 dependencies, dependency-free plugins run on
  the worker's interpreter directly. The plan said `uv run`
  unconditionally, but outside a project directory `uv run --with` has
  no kitaru to offer the harness. Pool workers that execute plugins
  with dependencies must run from a kitaru project checkout for now.
- The source arm runs the harness on the worker's interpreter with the
  agent env's working dir, env vars, and secrets. The plan's "run env"
  does not name an interpreter, and today's in-process scoring already
  assumed the worker environment can import the agent codebase, so the
  guarantee is unchanged.
- The worker blob cache verifies the sha256 on both read and write and
  writes atomically. Code blobs are kept indefinitely.
- `evaluate_scoring_policy` and `ScoringResult` left the client
  package, verdict arithmetic is server-only now. `SessionView` stays
  the scorer-facing type.

### Hot path

- The claim response pairs each job with its assembled spec
  (`ClaimedJobResponse {job, spec}`). Batch assembly does one IN-query
  per referent table in two rounds, since secrets, plugins, and blobs
  are only reachable through run specs and plugin versions. Single-job
  `GET /v1/jobs/{id}/spec` shares the same builder over a one-job
  batch.
- Blob referents load through `get_hashes(ids) -> dict[id, sha256]`
  rather than a `get_many` of entities. A blob entity load pulls the
  bytea, and specs only need the hash.
- Immutable-referent caching covers agent versions, plugins, plugin
  versions, and blob hashes in a bounded module-level FIFO map. Agent
  versions qualify because run specs freeze once a job references the
  version, which is the precondition for entering the cache. Sessions,
  replay configs, and secrets are mutable and stay uncached.
- A job whose spec referents are gone is failed server-side during the
  claim (with parent settlement and run finalization), and dropped from
  the response instead of poisoning the batch.
- The batched worker heartbeat lives at
  `POST /v1/workers/{worker_id}/heartbeat` and returns the submitted
  ids the worker should abandon: unreached by the guarded UPDATE
  (terminal, canceled, foreign, unknown) or attached to a canceling
  run. The per-job heartbeat endpoint is gone, the runner owns one
  heartbeat task per worker.
- `execution_target` is stamped NOT NULL at creation and the pool
  claim scope is a pure `status = 'pending' AND execution_target =
  'pool'` walk over the pending partial index, the run join is gone.

## Issues and deviations

- The live e2e pass after the scoring decoupling surfaced two server
  bugs the unit surfaces missed. `ExperimentRunProgress` lacked a
  `scoring` bucket, so run reads 500ed the moment a replay entered
  scoring and the run driver's stop condition broke. And sibling score
  jobs finishing in overlapping requests each read the other as
  non-terminal, so neither aggregated and the replay hung in `scoring`
  forever at worker concurrency 2. Fixed with the missing field and a
  `SELECT ... FOR UPDATE` on the parent replay row in the aggregation
  path, the waiter re-reads the children after the holder commits.
- Baseline `just check` fails before any change: 423 ty diagnostics,
  all in `examples/` (v1 examples not migrated to the v2 API) and
  `scripts/`, none in `src/` or `tests/`. Migrating the examples is out
  of scope, the bar held for this pass is zero diagnostics in `src/`
  and `tests/`.

## Review pass

A four-angle cleanup review (reuse, simplification, efficiency,
altitude) over the full diff produced 13 applied fixes, mostly
dedupe between the two harnesses and their per-kind surfaces, a
`_settle_score` helper over the three aggregation call sites, the
plugin kind bound at `PluginService` construction, and dropped
re-hashing on blob cache hits. Deliberately skipped findings:

- The uuid7 implementation exists twice, hand-rolled in the base
  package and via uuid-utils in the server package. The base package
  cannot depend on the server extra, sharing the hand-rolled form
  would change the server's id generation for no functional gain.
- The claim-time referent caches are module-level maps inside the job
  service rather than caching repository decorators. The service still
  only calls the repository Protocols, the cache memoizes immutable
  rows by unique id, and three decorator classes plus DI wiring would
  add more machinery than they remove.
- Per-distinct-run lookups in the batched heartbeat and per-row child
  cancellation stay as they are, both are bounded by worker
  concurrency or scorer count and the bulk forms would bypass the
  domain state machine.
- Registry scorer resolution at fan-out and policy validation stays
  per-scorer (two queries each), bounded by policy size and off the
  claim hot path.

## Validation

Beyond the test suite (1580 tests) and `scripts/run_e2e.sh` (all steps
pass on the squashed migration from an empty database, including the
registry scorer arm and the import plus re-import flow), a live
verification ran the target scenario end to end: dedicated database
`kitaru_verify`, server on port 8402, and an uncommitted driver
(`verify_plugins.py`) that

- created an agent with a runnable version wrapping
  `adapter_example.main` and recorded one session through the adapter,
- registered a scorer plugin (`verify-brevity`, uploaded as a blob,
  version 1) and the example importer
  (`importer_example/importer.py`, provider otlp),
- started one pool worker inside the driver process
  (`Runner(...).run_worker(...)`, concurrency 2, no agent filter),
- imported `importer_example/trace.jsonl` through an import job the
  worker claimed, landing stats created=3 skipped=0 failed=1 and three
  `origin=imported` sessions,
- created a single replay through the API with a two-scorer policy,
  one source ref (`adapter_example.scorers:answer_quality`) and one
  registry reference (`verify-brevity`). The worker ran the agent,
  handed the replay to `scoring`, executed all four score children
  (two result, two baseline, the registry pair through the materialized
  plugin blob), and the server settled the replay to completed with
  both scorer names in `scores`, `passed=true`, baselines merged onto
  the original session, and the result session carrying both scores.

The aggregation row lock held at worker concurrency 2, no replay
parked in `scoring`. All steps passed on the first run.

After the review-pass cleanups the same driver ran again with the
server in Docker (`docker compose up -d --build`, port 8000, worker
and agent on the host), all steps passed. One trap hit on the way:
`docker compose up` returns at "Started", the app inside is still
migrating, and a request racing the startup dies with a read error
since Docker's port proxy accepts connections before the app listens.
Wait on `/health` before driving the server. A stale `kitaru` database
in the compose volume would also silently miss the plugin tables,
since the squashed revision keeps its id, drop and recreate it when
in doubt.
