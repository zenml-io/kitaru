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

### Replay-to-job rename

- The rename landed as its own behavior-preserving commit before the
  kind discriminator, keeping the tree green at every step.
- Replay-kind concepts keep their names inside the renamed modules:
  `ReplayCreateRequest`, `ReplayOverride`, `ReplayConfig`,
  `replay_diff.py` with its DTOs and `replay_id` diff field, tool
  policy and scoring DTOs, `SessionOrigin.REPLAY`, and the run summary
  keys.
- The routers and client resources split along the endpoint layout:
  `/v1/jobs` owns the lifecycle, `/v1/replays` keeps only standalone
  replay creation.
- `Runner.run_replay` became `run_job`, it drives the generic job
  lifecycle.
- `AgentVersionFrozen` and the `InUse` errors now say "jobs", they name
  rows of the renamed table.

### Job kinds and global claim

- `JobKindMismatch(ConflictError)` guards tool lookup and diff on
  session runs, message names the required kind.
- The base `Job.standalone` returns True and `Replay` overrides it with
  the run link, keeping the state machine and service checks
  kind-agnostic.
- `create_replay` stamps `run_spec.default_execution_target` on
  standalone replays, otherwise unfiltered pool claims could never see
  them. Replay creation has no explicit target parameter and therefore
  no image check.
- Claim scoping uses correlated EXISTS subqueries (experiment_run for
  the pool fallback, agent_version for the agent filter) so
  `FOR UPDATE SKIP LOCKED` locks only job rows.
- The migration adds `kind` with a temporary server default of
  'replay', and drops and re-adds `worker_id` as uuid since old values
  are names.
- The no-live-worker warning lives in a shared
  `application/services/worker_liveness.py` helper used by run creation
  and session-run creation, backed by a repository-level `seen_after`
  worker filter.
- `client.jobs.claim` is the global claim, the per-job standalone claim
  became `client.jobs.claim_standalone`.
- `claim_jobs` bumps the worker's `last_seen_at` on every call
  including empty claims, the standalone claim bumps after a successful
  claim.

### Runner and env contract

- `KITARU_REPLAY_ID` becomes `KITARU_JOB_ID` everywhere, including the
  session create request field (`replay_id` to `job_id`). The public
  accessors `kitaru.replay_id()` and `kitaru.replay_inputs()` are
  renamed to `job_id()` and `job_inputs()`, no aliases, pre-release.
- Session linking by kind: a session created with a replay-kind `job_id`
  is stored with `origin=replay` as before. A session_run-kind `job_id`
  links `result_session_id` on the job and the session stays
  `origin=recorded`.
- `Runner.run_session` is removed rather than repointed: `run_job`
  claims and executes any standalone job, replay or session run, so a
  separate session method added nothing. The old client-side live run
  (resolve spec locally, `KITARU_SESSION_ID_FILE` handoff,
  `KITARU_OVERRIDE`) is removed from runner and adapter, superseded by
  server-visible session_run jobs. `RunnerError` went with it, nothing
  raised it anymore.

### Runner split

- `JobRunner(api_url, api_key, heartbeat_interval)` executes one
  already-claimed job via `execute(client, job_id)`. `Runner` composes
  it and owns the scopings (`run_job`, `run_experiment_run`,
  `run_worker`) over one shared claim loop whose stop predicate is
  checked after empty claims, so a stopping worker drains in-flight
  claims first.
- The `worker_id` constructor parameter became `worker_name`,
  registration is by name. The default hostname-pid name is sanitized
  to the server's `Name` charset, macOS hostnames contain dots and
  every default-named runner failed to register (regression test).
- `run_worker(agent_ids, stop)` registers with its agent filter, other
  modes register with empty agent_ids.
- The adapter installs its tool interceptor only when the spec carries
  a tool policy, session_run jobs execute tools directly.
- The e2e driver's live-run step now creates a session run via
  `client.session_runs.create` and executes it with `run_job`.
