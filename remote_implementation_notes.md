# Remote execution implementation notes

Decisions taken while implementing the server-triggered execution plan
(`~/.claude/plans/tender-nibbling-flurry.md`) that the plan did not
specify, plus issues that came up and how they were resolved. The plan
was written as a design-doc update, this pass implements it in code:
polling workers fully, the on-demand executor as DB and response model
preparation plus an interface only.

## Orchestration

- Implementation waves: worker vertical and execution-target fields in
  parallel worktrees, then the replay-to-job generalization (domain and
  DB first, then services, API, and client), then the runner split, then
  integration (migration squash, openapi, e2e).
- Each wave ships its own Alembic revision to keep its postgres tests
  runnable. They are squashed into a single revision at the end, same as
  the previous implementation pass. The squash drops and recreates the
  `replay` table as `job` instead of renaming in place, acceptable
  pre-release.

## Decisions

### Scope

- The plan's deliverable was a design-doc edit. The goal supersedes it:
  implement the architecture in code. `design/design.md` is updated to
  match at the end (gitignored, not committed).
- On-demand execution: `JobExecutor` interface, `execution_target` and
  `executor_handle` columns, and response fields exist. No executor
  implementation, no reconciler, no minted credentials. Creating work
  with `execution_target=on_demand` is accepted when the run spec has an
  image (409 otherwise) and the job stays pending, since no executor is
  configured to launch it.

### Entity and table shape

- Domain: a `Job` base model carries the shared lifecycle (status,
  attempt, worker, claim and heartbeat state machine). `Replay` and
  `SessionRun` subclass it with their kind-specific fields, hydrated
  from the one `job` table by `kind`.
- `job.replay_config_id` and `job.original_session_id` become nullable,
  session_run jobs have neither. `job.inputs` is set for session_run
  jobs only.
- `job.worker_id` becomes a uuid FK to `worker` with `ondelete=SET
  NULL`, so workers stay deletable after having run jobs. A claimed job
  whose worker row is deleted recovers through the normal staleness
  requeue.
- `execution_target` is stamped on `experiment_run` and on standalone
  and session_run jobs, per the plan. Run-created replay jobs read the
  run's target, the claim query joins for it.

### Workers and claiming

- Every claimant registers as a worker first, including
  `run_experiment_run` and single-job runs, since `job.worker_id` is an
  FK. `POST /v1/workers` upserts by name and returns 200.
- Worker liveness is `last_seen_at` within
  `WORKER_LIVENESS_TIMEOUT_SECONDS` (new server setting, default 60).
  Worker responses carry a computed `live` flag.
- The plan's "warn or 409" when a pool-target run or session run has no
  live worker serving the agent: no 409 (the create-then-start-worker
  flow must keep working) and no response field. The server logs a
  warning.
- One claim endpoint `POST /v1/jobs/claim` with `{worker_id, max_jobs,
  agent_ids?, experiment_run_id?}`. The old
  `POST /v1/experiment-runs/{id}/claim` is removed. Unfiltered claims
  return only pool-target jobs. Every claim bumps the worker's
  `last_seen_at`.
- Standalone job lifecycle endpoints (`claim`, `release`, `retry`) move
  under `/v1/jobs/{id}/...` unchanged.

### API surface

- `GET /v1/experiment-runs/{id}/replays` is renamed to `/{id}/jobs`,
  matching the `JobResponse` it returns.
- `POST /v1/replays` stays as the semantic standalone-replay creation
  endpoint and returns the replay-kind `JobResponse`.
- `POST /v1/session-runs` creates a session_run job. Body
  `{agent_id or agent_version_id, inputs, name, execution_target}`,
  default version is the latest runnable.
- One `JobSpecResponse` with a `kind` field. Replay kind keeps the old
  spec shape, session_run kind carries `inputs`, `run`, and
  `secret_env` with the replay-only fields null.
- Server settings `REPLAY_HEARTBEAT_TIMEOUT_SECONDS` and
  `REPLAY_MAX_ATTEMPTS` are renamed to `JOB_*`.

### Worker vertical

- `DuplicateWorkerName(ConflictError)` exists alongside `WorkerNotFound`,
  the register upsert and constraint translation need it. Register is
  try-create, on duplicate fetch-by-name and `refresh` (replaces
  agent_ids and metadata, bumps `last_seen_at`).
- Liveness rule lives on the domain (`Worker.is_live(timeout_seconds)`),
  the REST mapping derives the `live` DTO field from the service's
  configured timeout.
- The `agent_id` worker filter matches workers serving the agent or
  serving all agents (empty `agent_ids`), for the claim path. It is
  repository-level only, the REST list filters by name.
- `agent_ids` stored as stringified UUIDs in JSONB, the
  `secondary_parent_ids` precedent.
- `touch` renews `updated` too, unavoidable with the client-side
  `onupdate` hook, pinned by a contract test so the fake cannot drift.

### Execution targets

- `ExperimentRun.execution_target` and
  `RunSpec.default_execution_target` default to `pool` in the domain
  models, keeping existing constructions valid.
- `MissingRunImage(ConflictError)` lives in `domain/agent_version.py`
  next to `AgentVersionNotRunnable`, raised from `start_run` when the
  resolved target is on_demand and the run spec has no image.
- The migration backfills `run_default_execution_target='pool'` for
  rows with a run command so `to_domain` can assert non-null alongside
  the other run spec columns.
- `start_run` takes `execution_target` as a plain keyword parameter, no
  command model exists for run creation.
- The `JobExecutor` seam ships `ExecutorLaunchRequest(image, env)` and
  `ExecutorJobStatus` (running, succeeded, failed, unknown).

### Runner and env contract

- `KITARU_REPLAY_ID` becomes `KITARU_JOB_ID` everywhere, including the
  session create request field (`replay_id` to `job_id`). The public
  accessors `kitaru.replay_id()` and `kitaru.replay_inputs()` are
  renamed to `job_id()` and `job_inputs()`, no aliases, pre-release.
- Session linking by kind: a session created with a replay-kind `job_id`
  is stored with `origin=replay` as before. A session_run-kind `job_id`
  links `result_session_id` on the job and the session stays
  `origin=recorded`.
- `Runner.run_session` now executes one session_run job by id. The old
  client-side live run (resolve spec locally, `KITARU_SESSION_ID_FILE`
  handoff) is removed, superseded by server-visible session_run jobs.
