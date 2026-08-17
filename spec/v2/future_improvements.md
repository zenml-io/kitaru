# Future improvements

Deferred by choice. Each entry names the improvement, the trigger that makes it worth doing, and where it hooks in.

## Liveness checks for worker and task tokens

Resolving a worker or task token verifies the JWT and the account being active, never the worker or task row. Worker operations that dereference the worker (claim, heartbeat, get) fail on a deleted worker, but blob download and job get do not, so a deleted worker's token keeps read access until it expires. Task token writes are fenced by the attempt and transition legality, and session create checks the task is running, but reads (`get_task`, `get_spec`, granted sessions and blobs) never re-check the task row, so a superseded or finished attempt keeps read access until expiry, concurrently with the new attempt's token. Load the rows at token resolution, rejecting a deleted worker and a superseded attempt, at the cost of one read per request. Trigger: treating worker deletion as revocation, or running untrusted plugin code where a leaked task token's read window matters. Hooks: `AuthService._resolve_worker_token`, `AuthService._resolve_task_token`.

## Idempotency keys on job-creating POSTs

Replays inside a run dedup naturally on unique (experiment_run_id, baseline_session_id), and imports dedup per session on unique (imported_from, external_id). Standalone `POST /v1/replays`, `POST /v1/evaluations`, and `POST /v1/session-runs` have no natural key, so a client retry after a timeout creates duplicate jobs. The client already sends an `Idempotency-Key` header held stable across retry attempts, add the server-side dedup table keyed on it when duplicate jobs start showing up in practice.

## Task retry

Add `POST /v1/tasks/{id}/retry`, re-queueing a terminal failed, timed_out, abandoned, or canceled task. The sketch: move the task back to pending without resetting attempt (the fencing token stays monotonic), clear the claim and outcome fields, unlink but keep the failed attempt's session (`session.task_id` to null, freeing the one-session-per-task slot), delete the task's evaluation rows, and re-open the settled job (back to running, replay to its pre-settled status) so settlement re-runs when the retried task drains. Retrying an agent task additionally deletes the appended result evaluator tasks so the append re-runs against the new result session. Trigger: transient failures worth re-running without recreating the pipeline. Hooks: `TaskService`, `JobService.advance_job`, `replay_pipeline`, the session repository unlink.

## Run-scoped workers

No worker can scope itself to one experiment run today. It can scope by task-kind or agent-version claims, label selectors, and a job pin. The mechanism is already in place: stamp an `experiment_run` label on every task of run-owned replays at fan-out, and a run worker sets a required selector on it. A `WorkerConfig.experiment_run_id` convenience can expand into that selector plus a run-terminal stop condition read via `client.experiment_runs.get`, keeping the wire scope generic labels. Trigger: dedicated worker pools per run, or CLI flows that launch a worker for one run and want it to exit on its own. Hooks: `replay_pipeline` task creation, `WorkerConfig`, the Worker stop check.

## Declared task dependencies

Tasks are appended by pipeline subscribers reacting to sibling completions, so a job's full shape becomes visible only as it executes. A declared `depends_on` (single task id) would let creators lay out the whole task set at creation: a `blocked` initial status, unblocking on parent completion, structural cancel when the parent goes terminal without completing. Needs a deferred-input mechanism for values that exist only after the parent runs (the result evaluator's `input_session_id`), which is the reason appending won initially. Trigger: pipelines whose task graph must be inspectable before execution, or a kind whose sequencing outgrows reaction-time appends. Hooks: the task model and claim query, `JobService.advance_job`, `replay_pipeline`.

## Smarter history matching via tool_definition

History tool configs match recorded calls by cache_key, an exact hash over tool name and inputs. Matching on the tool definition would let semantically equivalent calls with differing inputs hit. Trigger: replays with low history hit rates on tools whose inputs carry noise (timestamps, ids). Hooks: `compute_tool_cache_key`, `tool_lookup`.

## Multi-turn conversation replay

Replays run one session end to end from its recorded inputs. Multi-turn replay would re-drive a conversation turn by turn, injecting recorded user turns. Trigger: agents whose sessions are conversations rather than single tasks. Hooks: replay spec details, the adapter contract, session node ingest.

## Per-node evaluations

Evaluations attach to sessions as a whole. Per-node evaluations would let evaluators rate individual LLM or tool calls. Trigger: diagnosing which step of a session degrades. Hooks: the `evaluation` table, `EvaluationResult`, the evaluator contract.

## Score optimization direction

Scores carry no direction, so a reader comparing baseline and replay cannot tell whether a higher value is better. Add a maximize/minimize flag per evaluation, letting comparison views render improved and regressed instead of changed. Trigger: automated baseline/replay comparison verdicts. Hooks: `EvaluationResult`, the `evaluation` table, `EvaluationResponse`.

## Deployed and LLM-judge evaluator kinds

Evaluators are registry plugins executed in a task process. A deployed evaluator would call an external endpoint, an LLM-judge evaluator would prompt a model directly from a config instead of code. Trigger: teams that want evaluation without shipping code. Hooks: `EvaluatorConfig`, `EvaluationHandler`, the evaluation flow.

## Push-based run progress

Run progress is polled through `GET /v1/experiment-runs/{id}`. A push channel (SSE or websocket) would feed a UI without polling. Trigger: a UI rendering live run progress. Hooks: `ExperimentRunService`, the progress aggregation.

## Node payload size limits and blob offload

Session node inputs and outputs are stored inline as JSONB with no size bound. Cap the payload size at ingest and offload oversized payloads to blobs. Trigger: sessions with large tool outputs bloating the node table. Hooks: `SessionNodeService.ingest_nodes`, the blob store.

## Surface no-live-worker to the job creator

Creating a job whose tasks no live worker's scope can claim succeeds silently, the tasks sit pending with no visible reason. Add a caller-visible signal, a response field or a dedicated check endpoint the CLI polls, backed by a liveness-and-scope query over the worker table matching selectors against the tasks' labels. Trigger: users repeatedly confused by pending jobs whose worker pool is missing or misscoped. Hooks: the job-creating services, the worker repository.

## Blob offload to object storage

Blobs are content-addressed bytes in the database. Large plugin payloads or import files may outgrow that. Add a nullable `uri` column and a storage adapter serving blob content from object storage (S3, GCS), keeping the sha256 dedup and the upload path unchanged, the migration is additive. Trigger: blob volume or size making database storage impractical. Hooks: `BlobService`, the blob repository.

## Per-plugin secret references

Evaluator and importer tasks run without a run spec, so `secret_env` is empty and plugin credentials come from the worker's inherited environment. Add secret references on the plugin version or the evaluation and import requests, resolved into `secret_env` at spec build like run spec secrets. Trigger: shared workers where per-plugin credentials differ or must stay out of the worker environment. Hooks: the plugin registry, the spec builders.

## Deduplicate concurrent blob materialization

Two concurrently claimed tasks referencing the same plugin or payload blob can both miss the cache and download the same content. The atomic rename keeps the cache correct, the second download is just wasted bandwidth. Add a per-sha in-flight lock in the materialization helper so one download serves both. Trigger: high-concurrency workers on tasks sharing plugins or payloads. Hook: the blob materialization helper in `handlers/base.py`.

## Comma-separated worker scope env lists

`KITARU_WORKER_SCOPE__CLAIMS` takes a JSON list of claim objects, since pydantic-settings only auto-decodes JSON for nested list env values. Add a before-validator on `WorkerConfig` splitting un-bracketed values on commas. Trigger: deployment ergonomics complaints about quoting JSON in env vars. Hook: `WorkerConfig`.

## Time-gate the claim-time staleness sweep

The sweep runs on every claim. Its no-op cost is one probe of the staleness partial index, which only holds in-flight tasks, so this is cheap at current scale. If claim volume grows enough that the probe shows up in profiles, add a per-process time gate: an in-memory last-swept timestamp, sweep at most once per few seconds per server replica. Staleness detection is heartbeat-timeout-granular, so a gate below that resolution changes nothing about detection latency.

## Freeze an agent version once its code is pinned

An agent version's run spec and capabilities are editable for the version's whole life, so two sessions carrying one `agent_version_id` can have been produced by different code and every comparison keyed on that id is only as trustworthy as the user's discipline. The freeze this wants is not the one that was removed: keying it on task existence rejected edits without pinning anything, since a run spec names a command and a working directory whose contents move underneath it. Pin the code first (a commit sha, an image digest, a content hash of the packaged agent, recorded on the version at creation and reported on the session), then reject run spec and capability edits on any version carrying that identity, and let versions without one stay editable. Trigger: replay comparisons and cost or quality deltas being read as evidence, where a silently edited version is a wrong answer rather than an inconvenience. Hooks: `AgentVersion`, `AgentVersionService.update_version`, the run spec model, session attribution.

## Composite foreign key for the session agent version check

Session create validates that `agent_version_id` belongs to `agent_id` with one primary-key read of the agent version. On the agent-task path that row is loaded anyway to infer `agent_id`, so the read is extra only when a task-less adapter sends both ids itself. A composite foreign key from `session(agent_id, agent_version_id)` to a `UNIQUE (agent_id, id)` on `agent_version` moves that case into the database at no query cost, and the default `MATCH SIMPLE` semantics skip the constraint when the version is null, matching the optional field. The repository's savepoint-and-translate path (`_add` with a constraint mapping) turns the violation into a 422 naming both ids, so the error stays as precise as the service check. The version-versus-task comparison on the agent-task path stays in the service either way, a constraint cannot express it. Trigger: task-less session recording becoming the hot write path and the extra read showing up in profiles. Hooks: `SessionService.create_session`, `SessionORM`, the `agent_version` unique constraint, a migration.
