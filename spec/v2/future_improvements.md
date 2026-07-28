# Future improvements

Deferred by choice. Each entry names the improvement, the trigger that makes it worth doing, and where it hooks in.

## Worker auth via registration-issued tokens

Workers authenticate with a plain account API key today, so any authenticated caller can write job transitions and read decrypted `secret_env` from `GET /v1/jobs/{id}/spec`. Issue a worker-scoped token at registration, require it on claim, heartbeat, and job updates, and restrict `get_spec` to the claiming worker. Trigger: running untrusted plugin code in shared deployments, or any hardening pass before exposing the API beyond a trusted team. Needs answers for token rotation on re-registration upserts, expiry, and worker-row deletion.

## Idempotency keys on job-creating POSTs

Replays inside a run dedup naturally on unique (experiment_run_id, input_session_id), and imports dedup per session on unique (provider, external_id). Standalone `POST /v1/replays`, `POST /v1/scores`, and `POST /v1/session-runs` have no natural key, so a client retry after a timeout creates a duplicate job. The client already sends an `Idempotency-Key` header held stable across retry attempts, add the server-side dedup table keyed on it when duplicate jobs start showing up in practice.

## Job retry

Add `POST /v1/jobs/{id}/retry`, re-queueing a terminal failed, timed_out, abandoned, or canceled job. The sketch: move the job back to pending without resetting attempt (the fencing token stays monotonic), clear the claim and outcome fields, unlink but keep the failed attempt's session (`session.job_id` to null, freeing the one-session-per-job slot), and for replay jobs delete the score children and reset the replay row to pending so fan-out and settlement re-run. Trigger: transient failures worth re-running without recreating the job. Hooks: `JobService`, the jobs router, the session repository unlink.

## Fenced job cancellation

`PATCH /v1/jobs/{id}` with status canceled is the user-facing cancel and carries no attempt, so it is the one unfenced transition: a stale worker confirming a cancel after its job was requeued and re-claimed cancels the new attempt. Split the two callers: keep the worker's cancel confirmation on the fenced PATCH, requiring the attempt, and move the user cancel to `POST /v1/jobs/{id}/cancel`, which needs no attempt and only ever enters canceling. Trigger: requeued jobs turning up canceled by workers that lost them. Hooks: `JobService.update_job`, the jobs router.

## Smarter history matching via tool_definition

History tool configs match recorded calls by cache_key, an exact hash over tool name and inputs. Matching on the tool definition would let semantically equivalent calls with differing inputs hit. Trigger: replays with low history hit rates on tools whose inputs carry noise (timestamps, ids). Hooks: `compute_tool_cache_key`, `tool_lookup`.

## Multi-turn conversation replay

Replays run one session end to end from its recorded inputs. Multi-turn replay would re-drive a conversation turn by turn, injecting recorded user turns. Trigger: agents whose sessions are conversations rather than single tasks. Hooks: replay spec details, the adapter contract, session node ingest.

## Per-node scores

Scores attach to sessions as a whole. Per-node scores would let scorers rate individual LLM or tool calls. Trigger: diagnosing which step of a session degrades. Hooks: `session_node` table, `ScoreResult`, the scorer contract.

## Deployed and LLM-judge scorer kinds

Scorers are source refs or registry plugins executed in a job process. A deployed scorer would call an external endpoint, an LLM-judge scorer would prompt a model directly from a config instead of code. Trigger: teams that want scoring without shipping code. Hooks: `ScorerConfig` union, `ScoreHandler`, the score flow.

## Push-based run progress

Run progress is polled through `GET /v1/experiment-runs/{id}`. A push channel (SSE or websocket) would feed a UI without polling. Trigger: a UI rendering live run progress. Hooks: `ExperimentRunService`, the progress aggregation.

## Node payload size limits and blob offload

Session node inputs and outputs are stored inline as JSONB with no size bound. Cap the payload size at ingest and offload oversized payloads to blobs. Trigger: sessions with large tool outputs bloating the node table. Hooks: `SessionNodeService.ingest_nodes`, the blob store.

## Surface no-live-worker to the job creator

Creating a job that no live worker's scope can claim succeeds silently, the job sits pending with no visible reason. Add a caller-visible signal, a response field or a dedicated check endpoint the CLI polls, backed by a liveness-and-scope query over the worker table. Trigger: users repeatedly confused by pending jobs whose worker pool is missing or misscoped. Hooks: the job-creating services, the worker repository.

## Blob offload to object storage

Blobs are content-addressed bytes in the database. Large plugin payloads or import files may outgrow that. Add a nullable `uri` column and a storage adapter serving blob content from object storage (S3, GCS), keeping the sha256 dedup and the upload path unchanged, the migration is additive. Trigger: blob volume or size making database storage impractical. Hooks: `BlobService`, the blob repository.

## Per-plugin secret references

Registry scorer and importer jobs run without a run spec, so `secret_env` is empty and plugin credentials come from the worker's inherited environment. Add secret references on the plugin version or the score and import requests, resolved into `secret_env` at spec build like run spec secrets. Trigger: shared workers where per-plugin credentials differ or must stay out of the worker environment. Hooks: the plugin registry, the spec builders.

## Deduplicate concurrent blob materialization

Two concurrently claimed jobs referencing the same plugin or payload blob can both miss the cache and download the same content. The atomic rename keeps the cache correct, the second download is just wasted bandwidth. Add a per-sha in-flight lock in the materialization helper so one download serves both. Trigger: high-concurrency workers on jobs sharing plugins or payloads. Hook: the blob materialization helper in `handlers/base.py`.

## Comma-separated worker scope env lists

`KITARU_WORKER_SCOPE__AGENT_VERSION_IDS` and `KITARU_WORKER_SCOPE__KINDS` take JSON lists, since pydantic-settings only auto-decodes JSON for nested list env values. Add a before-validator on `WorkerConfig` splitting un-bracketed values on commas. Trigger: deployment ergonomics complaints about quoting JSON in env vars. Hook: `WorkerConfig`.

## Time-gate the claim-time staleness sweep

The sweep runs on every claim. Its no-op cost is one probe of the staleness partial index, which only holds in-flight jobs, so this is cheap at current scale. If claim volume grows enough that the probe shows up in profiles, add a per-process time gate: an in-memory last-swept timestamp, sweep at most once per few seconds per server replica. Staleness detection is heartbeat-timeout-granular, so a gate below that resolution changes nothing about detection latency.
