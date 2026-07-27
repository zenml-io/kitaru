# Worker implementation notes

Decisions taken while implementing `design/v2/worker.md` and `design/v2/job.md`
that the spec did not fully pin down, plus unclarities encountered and how they
were resolved.

## Orchestration

- Mapping pass over the current tree first, then implementation waves:
  api_models and DB, server services and API, client resources, the
  `kitaru/job` and `kitaru/worker` packages in parallel, examples, then
  integration (tests, openapi, migration squash).

## Unclarities and decisions

### Replay as a separate resource

The v2 docs remove the `scoring` job status (uniform completion, fan-out
inside the completion request, verdict settles server-side afterwards). A
completed job cannot carry a pending verdict, so the verdict moves off the
job entirely:

- New `replay` table and domain entity: id, owner_id, job_id (unique FK),
  experiment_run_id, replay_config_id, input_session_id, passed, score,
  scores, diff, error. Created together with its job by `POST /v1/replays`
  and by experiment-run fan-out.
- `POST /v1/replays` returns the new `ReplayResponse` instead of a
  `JobResponse`. The diff endpoint moves to `GET /v1/replays/{id}/diff`.
- The job table drops `replay_config_id`, `passed`, `score`, `scores`,
  `diff`, `stats`. Score and import outcomes land in one opaque
  `result` JSONB column, per job.md.
- A child score job failing marks the replay row failed (error set,
  verdict null) and cancels non-terminal siblings. The replay job itself
  stays completed, its process did succeed.
- Run summaries read pass rate and scores from replay rows.
- `ReplayCreateRequest` and the diff DTOs move to `api_models/v1/replays.py`.
  The override, tool policy, and scorer config models stay in `jobs.py`,
  the job spec references them.

### Session-job link

job.md stores the link on the session row. `session.job_id` becomes the
authoritative link with a unique index (one session per job) and the
create-time check that the job is running. `job.result_session_id` is kept
as a denormalized copy written in the same transaction, so job lists and
spec resolution read it without a join. Deviation from a literal "only on
the session row" reading, chosen for read cost.

### WorkerScope

- worker.md types `WorkerScope` as `FrozenModel`. No such base exists in
  `api_models`, it is a `RequestModel` with `frozen=True` model config.
- `WorkerCreateRequest` becomes `(name, scope, metadata)`. worker.md lists
  only name and scope, the existing metadata field is kept since nothing
  in v2 contradicts it. The worker row stores the scope verbatim,
  replacing `agent_ids`.
- The no-live-worker warning on run and session-run creation now matches
  workers whose scope pins the agent version (or is unconstrained).

### Other decisions

- Baseline score jobs (score_baselines on runs) keep their current
  behavior. v2 does not mention them, they fan out as children of the
  replay job and are claimable through the same pinned scopes.
- `client.jobs.claim_standalone` and `POST /v1/jobs/{id}/claim` are
  removed per worker.md, a job-pinned scope covers the case.
- Retry semantics stay job-level: retrying re-queues a failed job. A
  settled replay verdict is not re-scorable, nothing in v2 specs
  re-scoring.
- Top-level `kitaru/__init__.py` drops the executable exports (`Runner`,
  `JobRunner`, `WorkerHeartbeat`, `SessionView`, `load_scorer`,
  `job_id`, `job_inputs`). The v2 docs name exact import paths
  (`kitaru.job`, `kitaru.job.scorer`, `kitaru.job.importer`,
  `kitaru.worker`), so the root package no longer re-exports them.
- `JobSpecResponse.input_session_id` (top-level) is kept even though the
  v2 worker only reads `scorer.input_session_id`, removing it adds churn
  without benefit.
- `tool_lookup` stays on `/v1/jobs/{id}/tool_lookup`. The adapter only
  knows its job id, the server resolves the policy through the replay row.
- `pydantic-settings` moves from the server extra to the main dependency
  list, `WorkerConfig` is client-side and builds on `BaseSettings`.

### Job package

- job.md lists only the public contract per module. Scorer resolution,
  plugin and payload loading, node ingest, and the stats builder stay
  private inside `scorer.py` and `importer.py`, tests drive them through
  `run()`.
- `ParsedSession` keeps the old model's defaults and validators (status
  defaults to completed, in_progress rejected), the spec snippet omits
  defaults.
- The `__main__` process-entry tests (unknown kind, missing env) live in
  their own `tests/test_job_main.py`, the scaffolding is shared by both
  kinds and fits neither kind's test file.

### Worker package

- Pinned-scope stop condition: worker.md words it as "job and its
  children terminal", but the client exposes no children listing. The
  worker checks that the pinned job reads terminal after an empty claim.
  Children are created before the parent's completion is observable and
  an empty claim means none are pending, so the only uncovered case is
  children held by another worker, which the one-off flow does not have.
- `pydantic-settings` 2.14 only auto-decodes JSON for nested list env
  values. `WorkerConfig` carries a before-validator splitting un-bracketed
  `KITARU_WORKER_SCOPE__AGENT_VERSION_IDS` and `..__KINDS` values on
  commas, the wire model stays untouched.
- The constants table is split across the modules that use the values as
  defaults instead of one constants module, matching the spec's layout
  which names no such module.
- Worker tests live in `tests/worker/` with shared fakes in `fakes.py`
  (not a nested conftest, `ty` resolves the bare `conftest` module name
  against the top-level one).
- The default worker name helper lives in `worker.py`, config.py is
  specced as behavior-free.

### Server

- The job subclass for the replay kind is named `ReplayJob`, the new
  resource entity owns the `Replay` name.
- `JobUpdateRequest.result` is typed `JsonValue`, not `Any`, a non-finite
  result would otherwise produce invalid JSON on responses.
- Score completion rejects null, bools, non-numbers, and values outside
  0..1 with a 409. Import completion requires a result, shape not
  validated.
- A replay job that itself fails, times out, or is canceled leaves the
  replay row unsettled (passed and error stay null), the error lives on
  the job. Only child score outcomes settle the verdict.
- Canceling a run cancels non-terminal fan-out children of every job,
  canceled children do not settle the verdict.
- `SessionResponse` does not expose `job_id`, the link is readable
  through `job.result_session_id`. `ReplayResponse.result_session_id` is
  served from the job row.
- Retrying a failed replay job deletes its children so the next
  completion fans out again, a settled replay row is not reset.

## Integration

- The e2e driver replaces `Runner` calls with pinned-scope workers:
  `WorkerScope(experiment_run_id=...)` drains a run,
  `WorkerScope(job_id=...)` executes one job, and the session-run step
  uses a version-and-kind-pinned pool worker stopped by a watcher task
  once the job goes terminal. Verdict assertions moved to the replay
  resource, job-level assertions stayed on jobs.
- The changelog rewrites the affected `[Unreleased]` bullets in place
  instead of adding Removed entries, `kitaru.Runner` and the standalone
  claim endpoint were introduced in the same unreleased block.

## Open points

- `replay.diff` is stored at settlement but `ReplayResponse` carries no
  `diff` field, so the stored summary is not readable over the wire. The
  full diff is served by `GET /v1/replays/{id}/diff`. Either surface the
  field or drop the column.
- The job-pinned stop check reads only the pinned job. Two workers
  sharing a job scope could let one stop while the other still holds a
  child. Closing this needs a `parent_job_id` filter on the jobs list.
- `just check` fails typecheck on ~420 pre-existing diagnostics, all in
  `examples/` and four scripts referencing the pre-v2 product API
  (`kitaru.flow`, `kitaru.checkpoint`, adapters). Present since the
  branch start, porting or pruning the examples is its own task. The
  link check could not run locally, `lychee` is not installed.
