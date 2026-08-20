# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Added experiment export projects for Harbor 0.20 and Verifiers 0.3, with explicit content, environment, and source policies; protected runtime values; deterministic provenance; target-native agent and evaluator execution; and a PrimeRL 0.8 training-source handoff.
- Added user-facing methods to `KitaruClient` and `KitaruSyncClient`: get agents and experiments by name or id, list sessions and session nodes, replay a session or start an experiment run and wait for the result. The one-method-per-endpoint API client stays reachable as `client.api`.
- Added server-side idempotency keys: a retried POST request with the same `Idempotency-Key` (stamped by both the Python and TypeScript clients) replays the first committed response instead of re-executing, marked with an `Idempotent-Replayed: true` header. The same key with a different request body is rejected with 422. Keys are retained for `KITARU_SERVER_IDEMPOTENCY_KEY_RETENTION_SECONDS` (default 900 seconds).

### Fixed

- Repeated identical tool calls with baseline-scoped history replay their distinct recorded results in baseline order instead of all resolving to the newest match. Replayed calls past the last recorded occurrence follow the configured `on_miss` behavior.
- The TypeScript adapters (Mastra and Vercel AI SDK) now apply the same baseline occurrence ordering: repeated identical tool calls send a zero-based `occurrence` with each history lookup and advance it only on a found result.

### Changed

- Unified all runnable examples under a single `examples/` tree: the Python adapter examples moved from `examples/integrations/` to `examples/python/`, and the TypeScript examples moved from `v2_examples/` to `examples/typescript/`. The `examples/v2/mcp` configuration example was removed in favor of the MCP setup documentation.

## [0.22.1]

### Changed

- Updated the bundled frontend to `kitaru-ui-v0.2.1`.
- Included all nine stable Python plugin distributions in `plugins/default-requirements.txt`, while keeping adapter distributions out of the server plugin catalog.

## [0.22.0]

### Changed

- Released Kitaru 0.22 with `kitaru-ui-v0.2.0`, nine stable `0.1.0` Python plugin distributions, and the stable `0.1.1` TypeScript packages.
- Raised the default worker concurrency from 1 to 10, so `kitaru worker start` runs up to ten tasks in parallel without a flag. Set `--concurrency` or `KITARU_WORKER_CONCURRENCY` to restore the previous single-slot behavior.

## [0.22.0rc10]

### Added

- Restored the SDK reference docs generator (`scripts/generate_sdk_docs.py`) for the v2 SDK: a reviewed `PUBLIC_API` allowlist over `kitaru.client` (including the resource method groups such as `client.sessions`), `kitaru.task`, and the developer-facing `kitaru.api_models.v1` types, re-enabling `just generate-docs` and the `sdkdocs.kitaru.ai` build and deploy pipeline.
- Added a v2 CLI reference generator (`scripts/generate_cli_docs.py`) that renders `sdkdocs.kitaru.ai` CLI pages from the `kitaru schema` contract, replacing the stale v1 CLI reference.

### Changed

- Prepared the tenth Kitaru 0.22 release candidate with `kitaru-ui-v0.2.0-rc.6` and the Langfuse importer at `0.1.0rc3`.

## [0.22.0rc9]

### Added

- Added the Logfire records-query importer as a default plugin at `0.1.0rc0`.

### Changed

- Prepared the ninth Kitaru 0.22 release candidate with `kitaru-ui-v0.2.0-rc.5` and the Langfuse importer at `0.1.0rc2`.

## [0.22.0rc8]

### Changed

- Prepared the eighth Kitaru 0.22 release candidate with `kitaru-ui-v0.2.0-rc.4`.
- Evaluation aggregates now report `mean`, `min`, and `max` scores instead of `average`.
- Completed-session analytics now include the framework, adapter version, LLM call count, and tool call count.
- Local server image conflicts now show the current and expected image tags and clarify that `kitaru login --local --upgrade` preserves the local database.

## [0.22.0rc7]

### Changed

- Prepared the seventh Kitaru 0.22 release candidate with `kitaru-ui-v0.2.0-rc.4`.

### Fixed

- The server info endpoint reports the persisted server id instead of the configured one, which was unset on servers that generate their id at startup.
- `kitaru login --local` no longer prints a Kitaru Cloud link, and takes its analytics opt-in and analytics debug settings from the environment instead of forcing the opt-in off.
- `kitaru login --local` no longer dead-ends when Docker resources outlive the CLI ownership state. The conflict names the resources it found, and `kitaru logout --volumes` deletes them.

## [0.22.0rc6]

### Changed

- Prepared the sixth Kitaru 0.22 release candidate with the local prerelease image-tag fix and `kitaru-ui-v0.2.0-rc.3`.

## [0.22.0rc5]

### Changed

- Prepared the fifth Kitaru 0.22 release candidate with `kitaru-ui-v0.2.0-rc.3` and the current `develop` source.
- Removed stale documentation for the retired OpenTelemetry plugin.

## [0.22.0rc4]

### Changed

- Prepared the fourth Kitaru 0.22 release candidate with TypeScript publishing fixes and simplified deployable release publishing.

## [0.22.0rc3]

### Changed

- Updated the bundled Kitaru UI to `kitaru-ui-v0.2.0-rc.2`.

## [0.22.0rc2]

### Fixed

- Fixed Helm rendering when the database TLS certificate settings are left empty.

## [0.22.0rc1]

### Changed

- Prepared the second Kitaru 0.22 release candidate with the frontend from RC0 and the current `develop` source.

## [0.22.0rc0]

### Added

- Prepared the first Kitaru 0.22 release candidate with a selected frontend release and independently versioned plugin packages.
## [Unreleased]

### Changed

- All API routes moved from the `/v1` prefix to `/api/v1`.
- The public `zenml-io/kitaru-template` repository is now the sole canonical returns walkthrough; the duplicate in-tree example was removed.
- Replaced the worker scope `kinds` field with explicit claims that can pin agent task claims to a specific agent version, so agent-specific workers on one server no longer claim each other's tasks. The `--kinds` worker CLI flag is replaced by the repeatable `--claim` flag.
- Added public installation, adapter, replay-boundary, and runnable-example documentation for the TypeScript SDK, Mastra adapter, and Vercel AI SDK adapter.

### Fixed

- Worker plugin subprocesses now resolve their dependencies outside the worker's current project, preventing a task from resynchronizing or mutating the host project environment.
- Fetching a pending device authorization by id now succeeds when the request carries the device's user code, so the device verification page can render before an account approves it.
- Fixed TypeScript replay and recording edge cases, aligned credential redaction and provider diagnostics across adapters, and made the adapter examples reproducible, rerunnable, and part of CI.
- TypeScript cloud examples now reuse CLI logins without exporting a token, verify dedicated-worker access before creating remote resources, isolate exact-job recovery state, and stop when a mutation or cancellation cannot be journaled safely.

### Added
- CLI and MCP investigation creation return the dashboard review page as a `review` link, resolved from the server-stated dashboard URL or, for servers hosting the bundled UI, from the URL the client logged into. If link resolution fails after creation, both preserve the created investigation and return a warning.
- Standalone replay CLI create, list, and get commands; cohort baseline selection in the CLI and MCP; and native MCP reads for tags and workers plus capability-gated tag lifecycle operations.
- Vercel AI SDK 7 `ToolLoopAgent` support for typed, non-streaming agent generation with Kitaru recording and safe replay boundaries.
- The server now serves the bundled Kitaru UI at its root URL with SPA fallback. Setting `KITARU_SERVER_EXTERNAL_UI=true` makes it redirect to the configured dashboard URL instead of serving files, and `GET /api/v1/info` reports the served UI version.
- Every filterable entity list filters by `id`, including `in` over a list of ids.
- TypeScript SDK packages for the core client and replay runtime, with Mastra and Vercel AI SDK adapters and runnable examples.
- TypeScript release packaging for the core, Mastra, and Vercel AI SDK packages, with lockstep release-candidate versions, clean-consumer tarball checks, npm publishing, and GitHub release artifacts.
- Session lists filter by `experiment_run_id`, returning the sessions produced as the results of the run's replays. Baseline sessions do not match.
- The PydanticAI adapter records estimated LLM costs from its bundled pricing catalog and accepts a custom cost calculator for private models or negotiated rates.
- Service accounts, managed via `/api/v1/service-accounts`. A service account is created active without credentials and cannot be an admin, and admins update its metadata and active state. Account writes moved from `/api/v1/accounts` to `/api/v1/users` and `/api/v1/service-accounts`, leaving `/api/v1/accounts` read-only over both kinds with a new `is_service_account` filter and a `/api/v1/accounts/me` endpoint returning the calling account. Like all account management, service accounts require the `local` auth scheme and are unavailable when a control plane owns accounts.
- `kitaru session import --join-on /json/pointer` groups provider traces into sessions by a scalar selected with an RFC 6901 JSON Pointer. The CLI passes the pointer through importer parameters, and Braintrust, Langfuse, and LangSmith importers support the same `join_on` parameter.
- A Kitaru v2 LangGraph adapter, shipped as the independently versioned `kitaru-langgraph` distribution with the `kitaru_langgraph` import package, for synchronous and asynchronous invocation recording across compiled LangGraph runnables, LangChain agents, and Deep Agents. Factory-built agents support live model-request overrides and capability-gated static or recorded-history tool-result substitution; streaming, batch invocation, native checkpoint reconstruction, and worker-managed interrupt resume remain unsupported.
- A non-streaming OpenAI Agents adapter, shipped as the independently versioned `kitaru-openai-agents` distribution with the `kitaru_openai_agents` import package, that preserves the native `RunResult` while recording each run as a Kitaru session with root and observed activity nodes.
- Bare `kitaru` and `kitaru --help` now report Kitaru agent-skill discovery and the canonical installation action, while `kitaru doctor` checks skill availability without treating absent skills as unhealthy.
- The PydanticAI recording and replay adapter now ships as the independently versioned `kitaru-pydantic-ai` distribution with the `kitaru_pydantic_ai` import package.
- Ten versioned offline deterministic evaluator plugins distributed with the existing basic evaluators in `kitaru-evaluator`, covering session integrity, output contracts, trajectory signals, tool and LLM diagnostics, timing, resource budgets, tool and model policies, and workflow conformance. They run only through explicitly started evaluation Jobs.
- The server persists a generated server id on first startup, seeded from `KITARU_SERVER_SERVER_ID` when set. Analytics events carry that id together with the auth scheme, runtime environment, operating system, Python version, and, when available, the enrolled workspace id, the acting account's service account flag, and its control plane user id. The default account is identified at startup, and identify traits carry an `account_origin` (`bootstrap`, `api`, or `control_plane`).
- Evaluators and importers carry an optional `logo_url`, set at creation, changed through the update endpoints, and returned in responses. Default plugin definitions can supply one.
- Server-side analytics now cover agent, agent version, investigation, and annotation creation. A created account is identified with its email, service account flag, and source, and an account mirrored from a control plane user is aliased to that user.
- The `kitaru/` plugin name prefix is reserved for built-in default plugins, which are registered at server startup without an owner. Creating a plugin whose name starts with `kitaru/` via the API is rejected.
- Sessions carry a per-agent sequential `number`, assigned at creation and returned in session responses. Numbers may skip when a create fails after its number was allocated.
- CLI investigation and annotation commands, plus MCP commands for review workflows, evaluator registration, and starting bounded evaluations or experiment runs. The new operations use existing API and SDK resources, require exact IDs and evaluator versions, and return submitted workflows without polling.
- Investigations and annotations, managed via `/api/v1/investigations` and `/api/v1/annotations`. An investigation collects a set of sessions, each optionally carrying a curated view, a question to answer, and a list of highlights, each a selector plus a description. Annotations attach directly to a session or to an investigation session, targeting the whole session or a part of it through a selector.
- Stage 4 CLI hardening adds client/server compatibility diagnostics, structured interrupt handling, and isolated wheel/sdist artifact smoke coverage.
- CLI commands to create, inspect, update, and delete experiments with exact evaluator versions, inline replay configuration, sparse updates, and explicit destructive-delete confirmation.
- CLI commands to create, inspect, update, and delete cohorts and immutable cohort versions, including exact version references and explicit destructive-delete confirmation.
- CLI commands to import and inspect sessions and nodes, run bounded session evaluations with optional job waiting, and inspect stored evaluations.
- A client configuration file, `config.json` in the Kitaru config directory, storing the server URL, the client installation id, and CLI preferences. Client construction resolves the server URL from `KITARU_API_URL`, falling back to the stored server URL, and authenticates through the credential store when the environment carries no credential.
- The server sweeps stale tasks on a background interval, so tasks held by dead workers are requeued or abandoned even when no worker is polling for new tasks. Configured through `KITARU_SERVER_TASK_SWEEP_INTERVAL_SECONDS`, 0 disables the loop.
- Agents and agent versions, managed via `/api/v1/agents` and `/api/v1/agent-versions`, where a version carries a run spec and its attached secrets.
- Sessions and session nodes, managed via `/api/v1/sessions`, recording an agent run and its nested LLM calls, tool calls, and sub-agent calls.
- Blobs, managed via `/api/v1/blobs`, content-addressed storage for plugin payloads and task inputs.
- Plugins for evaluators and importers, managed via `/api/v1/evaluators` and `/api/v1/importers`, each versioned and backed by a script or package source.
- Cohorts and cohort versions, managed via `/api/v1/cohorts` and `/api/v1/cohort-versions`, where a cohort names a group of sessions and each immutable version snapshots the membership used as experiment baselines. A new version applies a membership delta to a baseline version chosen via `baseline_id`, defaulting to the latest version.
- Tags, managed via `/api/v1/tags`, linking to sessions, cohorts, cohort versions, agent versions, experiments, and experiment runs.
- Experiments and experiment runs, managed via `/api/v1/experiments` and `/api/v1/experiment-runs`, comparing agent versions against a cohort version.
- Replays, managed via `/api/v1/replays`, re-running a session against a replay config with a per-tool lookup, passthrough, or override policy.
- Evaluations, managed via `/api/v1/evaluations`, scoring a session or task output. An evaluation result can carry an optional `passed` pass or fail verdict alongside its score, value, and explanation. The flag is independent of the score and is named `passed` because `pass` is a Python keyword.
- Jobs and tasks, managed via `/api/v1/jobs` and `/api/v1/tasks`, running agent runs, evaluations, and imports through workers that claim tasks and send heartbeats. Every job carries a server-stamped `kind` naming the workflow that created it, usable in job list filters. The new `kitaru.worker` and `kitaru.task` packages run the worker process and give task code access to task-scoped accessors.
- `GET /api/v1/info` reports the server id, version, auth scheme, server URL, dashboard URL, and control plane API URL, and is readable via `client.info.get()`. The endpoint is unauthenticated, because a client has to read it before it knows which credential to present. The URLs come from `KITARU_SERVER_SERVER_URL` and `KITARU_SERVER_DASHBOARD_URL` and are null when unset.
- Device authentication for headless clients, following the OAuth 2.0 device authorization grant. `POST /api/v1/device_authorization` issues a short user code and a device code, a signed-in account confirms the user code at the verification URI, and the client exchanges its device code for a token at `POST /api/v1/login`. Authorized devices are listed, locked, and revoked via `/api/v1/devices`, and revoking one immediately invalidates every token issued for it. Configured through `KITARU_SERVER_DASHBOARD_URL`, `KITARU_SERVER_DEVICE_AUTH_TIMEOUT_SECONDS`, `KITARU_SERVER_DEVICE_AUTH_POLLING_INTERVAL_SECONDS`, `KITARU_SERVER_MAX_FAILED_DEVICE_AUTH_ATTEMPTS`, `KITARU_SERVER_DEVICE_EXPIRATION_MINUTES`, and `KITARU_SERVER_TRUSTED_DEVICE_EXPIRATION_MINUTES`.
- A client credential store at `~/.config/kitaru/credentials.json`, holding API keys, device authorizations, and cached tokens per server URL. The file is written atomically with owner-only permissions, and `KITARU_CREDENTIALS_PATH` moves it while `KITARU_DISABLE_CREDENTIALS_CACHE` keeps it in memory.
- The API client can authenticate from the credential store via `KitaruAPIClient.from_credentials(...)`, renewing its token before it expires and once more after an HTTP 401. Concurrent callers share a single renewal.
- The API client now retries requests that fail with a transport error or a retryable status code (408, 429, 502, 503, 504) with exponential backoff, configurable via the `retries` constructor parameter. Every request carries an `Idempotency-Key` header that stays the same across retries of the same request, and the connection pool size is configurable via the `pool_size` constructor parameter.
- Filter expressions on list endpoints, a JSON-encoded `filter` query parameter combining `and`, `or`, and `not` over per-field conditions (`eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, `is_null`, `startswith`, `endswith`, `contains`) on an allowlisted set of fields per resource, including tag filtering on sessions, cohorts, experiments, and experiment runs. The expression replaces the previous per-field filter query parameters. List queries run under a statement timeout configured through `KITARU_SERVER_LIST_QUERY_TIMEOUT_SECONDS` (default 10, 0 disables it), and a timed-out list query returns HTTP 503.
- Cohorts filter on `agent_id` and experiment runs on `agent_version_id`.
- Evaluations filter on `agent_id`, `cohort_id`, and `experiment_run_id`, experiment runs on `cohort_id` and `agent_id`, and replays on `result_session_id`. The evaluation run filter scopes by producing task rather than by session, so a run's baseline and result evaluations reach the same result set, and `result_session_id` supports `is_null` to select the replays that have not produced a result. Fields that resolve through a relation accept `eq` and `in`; negation is expressed by wrapping the condition in `not`.
- List endpoints now paginate with an opaque cursor instead of page numbers. Requests take `cursor`, `size`, and `sort` (`created:asc` or `created:desc`), and responses return `items` and `next_cursor` instead of a total count. A cursor is invalidated by changing filters or sort mid-pagination. Client resources can also iterate every item across pages with `iter()`.
- The API client now identifies itself with an `X-Kitaru-Client` header (`kitaru-python/<version>`) on every request, and the server uses it as the source for analytics events. Server-side analytics can be disabled with `KITARU_SERVER_ANALYTICS_OPT_IN=false`.
- Secrets holding string key-value pairs, managed via `/api/v1/secrets`. Values are encrypted at rest with AES-GCM using the new required `KITARU_SERVER_SECRET_ENCRYPTION_KEY` setting, and reads return them only when explicitly requested, never in list responses.
- Tags and tag links, managed via `/api/v1/tags`. A tag links to sessions, cohorts, experiments, and experiment runs through `POST /api/v1/tags/{id}/links` and `DELETE /api/v1/tags/{id}/links/{resource_type}/{resource_id}`, and deleting a tag cascades its links.
- Worker and task tokens. `POST /api/v1/workers` returns a worker-scoped bearer token alongside the worker, and the worker uses it for task claims, heartbeats, job polling, and blob downloads, renewing it by re-registering with its API key. Each claimed task additionally carries a task-scoped token bound to the task's attempt. It fences status updates, authorizes the sessions, session nodes, evaluations, and blobs the task produces, and is the only credential passed into the task process (`KITARU_API_TOKEN`), so the account API key never reaches task code. Resources produced by a task are owned by the account that owns the task's job. Configured through `KITARU_SERVER_WORKER_TOKEN_LIFETIME_SECONDS` and `KITARU_SERVER_TASK_TOKEN_EXPIRY_SLACK_SECONDS`.
- Worker registration via `POST /api/v1/workers`, upserting by name so a rescheduled worker keeps its id while its scope, runtime, and metadata refresh. Workers are listed, read, and deleted via `/api/v1/workers`, and a worker's `live` field reflects whether it was seen within `KITARU_SERVER_WORKER_LIVENESS_TIMEOUT_SECONDS` (default 60).
- The `control_plane` auth scheme, in which a control plane owns every account on the server. `POST /api/v1/login` takes a control plane credential in the `Authorization` header, mirrors the control plane user into a local account, and returns a Kitaru bearer token. Control plane API keys also authenticate requests directly, while local API keys, local accounts, and the default-account bootstrap are disabled. Configured through `KITARU_SERVER_AUTH_SCHEME=control_plane`, `KITARU_SERVER_CONTROL_PLANE_API_URL`, and `KITARU_SERVER_SERVER_ID`.
- Accounts and API keys with username/password login. `POST /api/v1/login` authenticates with a username and password and returns a bearer token, and API keys authenticate requests directly as bearer tokens with revocation taking effect immediately. Keys are managed via `/api/v1/api-keys`. The server bootstraps a default account configurable through `KITARU_SERVER_DEFAULT_ACCOUNT_NAME` and `KITARU_SERVER_DEFAULT_ACCOUNT_PASSWORD`, and the new `local` auth scheme enables password login.
- Accounts carry a `metadata` dictionary, set through `PATCH /api/v1/accounts/{id}`. An account can only write its own metadata, and unlike active state and password it is writable under every auth scheme. A PATCH carrying `metadata` replaces the map whole.
- `PATCH /api/v1/accounts/{id}` rejects a password write for any account other than the caller's own. Changing your own password requires the current one in `old_password`. Active state is no longer writable through PATCH.
- Account activation tokens. An account created without a password starts inactive and its create response carries a one-time `activation_token`. `POST /api/v1/accounts/{id}/deactivate` deactivates an account and mints a fresh token the same way, and rejects the calling account. `POST /api/v1/accounts/{id}/activate` takes that token plus a new password, activates the account, and spends the token. The activate route is unauthenticated, because a pending account cannot log in until it holds a password. Both tokens arrive on an `AccountActivationTokenResponse`, so the plain `AccountResponse` never carries token material.
- The `api-key` grant type at `POST /api/v1/login` exchanges a local API key in the `Authorization` header for a session token, readable via `client.auth.exchange_api_key(...)`. The key is checked once, at the exchange, so deactivating it leaves the tokens already issued from it valid until they expire.
- Local stacks can now use S3, GCS, or Azure Blob/ADLS artifact storage while keeping flow execution local, with existing-connector reuse and ambient provider credentials when explicit credentials are not supplied. (#593)
- Flow and execution deletion is now available through the SDK, `kitaru flow delete` and `kitaru executions delete` CLI commands, and MCP tools.
- OpenTelemetry instrumentation for the API server, enabled by setting `KITARU_SERVER_OTEL_EXPORTER_OTLP_ENDPOINT` (or the standard `OTEL_*` environment variables) with the `otel` extra installed.

### Changed
- Experiments are now scoped to an agent. Creating an experiment requires an `agent_id`, experiments filter on `agent_id`, and starting a run rejects a cohort version or agent version that belongs to a different agent.
- The worker now stops promptly when asked. A stop request or the configured lifetime ends every wait in the claim loop, including error backoff and the wait for a free slot, and a stopping worker never claims again. A second SIGINT or SIGTERM during a drain cancels the held tasks instead of being ignored, and the new `--drain-timeout` option bounds the drain before canceling. The worker also survives transient network failures during claiming and job polling instead of exiting, and its heartbeat loop restarts after an unexpected error.
- A task process is now killed together with every descendant on all outcomes, including success, so a background process spawned by a task can no longer outlive it or hang the worker by holding the task's output pipes open. Worker-built task commands run without a shell, and process spawning and killing are platform-specific, preparing task execution on Windows.
- Starting an experiment run now writes its replays, jobs, and tasks in three batched inserts instead of row by row, so run creation stays fast for large cohorts.
- A control plane API key configured on the client is now exchanged for a session token instead of being sent on every request. The server had to authorize each of those requests against the control plane, which is a rate-limited round trip per call. Local API keys are unaffected and still authenticate directly.
- A control plane request no longer writes to the mirrored account unless the control plane reports a name, email, or active state the account does not already have.
- The CSRF token is now required only when the session token arrives in the auth cookie. A token sent in the `Authorization` header no longer needs the `X-CSRF-Token` header, because a browser never attaches that header on its own. The auth cookie also honors the new `KITARU_SERVER_AUTH_COOKIE_DOMAIN` and `KITARU_SERVER_AUTH_COOKIE_SECURE` settings, so the secure attribute is no longer derived from the request scheme behind a TLS-terminating proxy.
- The checkpoint table in `kitaru executions diff` text output now includes replay-minus-original duration deltas and per-role artifact comparison states (`unchanged`/`changed`/`unavailable`) alongside token and cost deltas, without printing artifact hashes or values. (#520)

### Fixed
- Passwordless user creation through `client.users.create(...)` now returns `UserActivationTokenResponse`, preserving the one-time activation token from the server response.
- Experiment run job listings now return only the jobs backing the selected run's replays instead of every job in the database. (#749)
- A Mastra replay no longer executes tools live after a tool-policy decision fails. Mastra swallowed the exception the policy hook raised, so a replay that could not find a recorded result ran the real tool and still recorded the session as completed. Replays now stop at the first policy failure, and the TypeScript core refuses every later tool call in the run rather than relying on each adapter to notice.
- A Mastra replay now rejects tools it cannot intercept, and no longer reads or writes live Mastra memory threads, so recording state and replay state stay separate.
- The Vercel AI adapter no longer fails a running agent because a model response or tool payload was too large to record. Recording bounds oversized values instead of raising, and the shared recorder used by the TypeScript core applies the same bound to tool results captured during replay.
- The TypeScript adapters record the model identity the same way the Python adapters do: `requested_model` holds the requested identifier, `model` holds the identifier the provider served, `model_provider` holds the bare provider family, and the raw provider string is kept as a `provider_id` attribute. Both adapters accept a `costCalculator` and record the resulting cost with an explicit status attribute, so TypeScript sessions no longer report zero cost.
- A `system_prompt` replay override now takes effect when the caller passes a message array containing a system message. Both TypeScript adapters previously left the original system message in place, so the replay silently ran the prompt it was meant to replace.
- Replay model replacement, model-parameter overrides, and replay tool-policy support are now validated once in the TypeScript core rather than separately in each adapter, so the two adapters can no longer accept or reject the same replay specification differently.
- TypeScript tool cache keys now escape the delete character the way Python does, so a tool input containing it resolves against recorded history instead of missing every lookup.
- A TypeScript replay no longer resolves a tool call against recorded history when the call's arguments could not be recorded faithfully. Recording bounds long strings and large collections, replaces credential-named values, and cannot reproduce objects such as `URL`, and each of those turns two different calls into the same recorded value and so the same cache key, which made a replay return another call's result. Such a call now follows the policy's `on_miss` setting instead. This narrows what replays from history: a tool whose arguments are that large, or that carry a key named like a credential, no longer matches a recorded result.
- A TypeScript cost calculator that returns a value the server cannot store, such as a currency-formatted string, is now reported as an unavailable cost instead of failing the run it was recording.
- Replaying with a `system_prompt` override no longer drops the other keys of a structured input, and a replay of a replay no longer fails to start. The TypeScript core is pinned to the server's input rule by a shared fixture, the way cache keys already were.
- The TypeScript client reports the server's error message instead of the bare HTTP status text, matching the Python SDK. Recorded nodes carry `started_at`, and a failed recording no longer leaves queued steps pending.
- Default importers, the PydanticAI adapter, and deterministic evaluators now use the `model_provider` session-node field introduced by the consolidated v2 specification.
- Importer output selectors now ignore reasoning, thinking, Gemini thought, Anthropic redacted-thinking, and tool-call parts, leaving the selector unset when a model response has no visible text.
- Importer JSON Pointer grouping now rejects invalid array indices instead of accepting Python-specific negative, signed, whitespace-padded, leading-zero, or underscored forms.
- Langfuse imports now treat scrubbed or redacted session identifiers as missing and fall back to trace identifiers instead of merging unrelated traces.
- Stack creation now verifies discovered service connectors before reusing them for local cloud storage or Modal stacks, failing fast with an actionable error instead of letting runs fail later; `--no-verify` skips the check.
- `--credentials aws-profile:NAME` is now rejected when connected to a remote ZenML server, because the server cannot resolve AWS profiles that only exist on the local machine; portable `aws-access-keys`/`aws-session-token` credentials remain supported.
- Claude Agent SDK failures no longer copy raw `ResultMessage.result` content into Kitaru errors or durable failure records; allowlisted diagnostics remain available in durable records and live terminal events.
- Live LLM integration runs now retain tested SHA, workflow run ID, and run-attempt provenance for release evidence.
- Detailed execution graphs now resolve downstream parent call IDs to the visible successful checkpoint after a retry, instead of retaining the hidden failed attempt ID. (#564)
- Reporting now preserves physical retry identity, marks unavailable secret-list key metadata explicitly, accounts for unknown billing conservatively, and documents the distinct workload-token and incurred-cost diff bases. (#566)
- Execution diffs now reject blank selectors and explicit comparisons without recorded direct replay lineage, while deduplicating repeated selectors and aliases in first-occurrence order. (#565)
- `kitaru executions diff` now shows useful checkpoint comparison rows in its default text output, and diff serialization includes redacted evidence that replay output overrides were applied without exposing override values. (#563)

## [0.21.0] - 2026-07-14

### Added
- `FlowHandle.wait(timeout=...)` can now stop waiting after a positive, finite number of seconds without changing the remote execution. It raises the new typed `KitaruTimeoutError` with the execution ID, configured and elapsed timeout, and last observed status. (#523)
- New `kitaru.list_secrets()` SDK function and matching `kitaru_secrets_list` MCP tool return secret names and metadata only, never secret values, so an agent can discover which secrets exist without being able to read them. (#527)
- New `kitaru_executions_resume` and `kitaru_executions_abort_wait` MCP tools bring the MCP surface in line with the SDK and CLI, letting an agent resume a paused execution or abort a pending `kitaru.wait()` without shelling out. (#521)

### Changed
- `kitaru executions get` now shows every checkpoint call's current ID and status in human-readable output. Failed executions also include a copyable replay command using the first eligible failed call ID. (#538)

### Fixed
- Cohort diff replay discovery now scans each flow once using ZenML's native replay linkage and reuses successful artifact hashes across rows, avoiding repeated flow scans and duplicate successful artifact loads; unrelated executions consume the 10,000-execution scan bound, and a warning appears only when older executions remain. (#525)
- Checkpoint execution diffs now calculate token and cost changes from canonical LLM usage records, including explicit zero usage, reused work, and unavailable provider-call cost. (#518)
- Retried checkpoint invocations are now exposed as one checkpoint call with attempt history ordered by retry version. (#530)
- Missing stack integration dependencies now fail before deploy, run, or replay with a concise Kitaru explanation followed by ZenML's exact integration and whole-stack installation guidance. (#506)
- Document the `openai`, `anthropic`, and `llm` provider extras on the installation page, and add the `kitaru[openai]` install step before the quickstart's first LLM call so a base-package user does not crash on the first `kitaru.llm()` invocation. (#522)

## [0.20.2] - 2026-07-10

### Added
- LLM cost and token usage is now attributed per checkpoint, not just per execution. When a flow reaches a terminal state, Kitaru publishes flat `kitaru_llm_*_v1` metadata on each checkpoint that made model calls, so SDK, CLI, MCP, and dashboard clients can see which checkpoint incurred which cost. Per-checkpoint values sum exactly to the execution-level totals, and the execution-level `llm_usage_summary_v1` payload is unchanged. (#528)
- Cached, skipped, and replay-reused checkpoints report their token counts under the `reused_*` fields with zero incurred and zero display cost, so a replayed run shows what the work would have cost without billing it again. Retried checkpoints keep a separate record per attempt, so a retry that really called the provider twice is counted twice. (#528)

### Changed
- Terminal cost metadata writes are best-effort: a failed write is debug-logged, the remaining checkpoint writes are still attempted, and incomplete persistence is reported so aggregation can be retried. A metadata write failure never fails the flow run. (#528)

### Infrastructure
- The release workflow's GitHub Release asset reconcile step now skips signature and attestation assets, which are regenerated per dispatch and previously caused recovery re-dispatches to hard-fail on a non-reproducible asset mismatch. (#513)

## [0.20.1] - 2026-07-08

### Fixed
- Project create/use/delete operations now stop before changing server state unless Kitaru can verify a ZenML Pro/Cloud server. Read-only project inspection (`list`, `current`, and `show`) remains available on local/OSS servers for diagnostics. (#512)

## [0.20.0] - 2026-07-08

### Added
- Added explicit adapter checkpoint metadata to SDK/API inspection output: checkpoint calls now expose `checkpoint_origin`, `adapter`, `adapter_checkpoint_kind`, `replay_input_slots`, and `replay_output_slots`, so clients can distinguish adapter-generated checkpoints from hand-written checkpoints with the same display type.
- Added `kitaru stack create --type modal` and MCP `manage_stack(..., stack_type="modal")` support for Modal-backed stacks with remote artifact storage, remote image registry, optional `sandbox="modal"`, and Modal-specific component overrides.
- Added Modal stack cloud credential support for private S3/ECR, GCS/GAR/GCR, and Azure Blob/ADLS/ACR resources by linking provider service connectors to the artifact-store and container-registry components.
- Added Kitaru projects across SDK, CLI, and MCP: `KitaruClient.projects`, `KitaruClient.for_project_management()`, `kitaru project list/current/show/create/use/delete`, and MCP read/switch tools `kitaru_projects_list`, `kitaru_projects_current`, `kitaru_projects_show`, and `kitaru_projects_use`. `kitaru login --project ...` now reports `Project: ...` in text output without returning to the older `Active project` wording.
- Added Python 3.14 as a supported and tested runtime.
- Added opt-in remote-stack release smoke covering an operator-provided Kubernetes stack and a local-runner stack with remote artifact storage, with sanitized structured evidence and deterministic contract tests.

### Changed
- PydanticAI tool-checkpoint replay input overrides now rerun the tool body with edited `tool_args`. Users still pass the public `input` override field; shorthand tool arguments and explicit `{"tool_args": ...}` input-slot overrides are both supported.
- **Breaking:** Replay planning now uses recorded replay input slots and real step inputs before falling back to older type-based guesses, so a hand-written `type="tool_call"` checkpoint is no longer treated as a PydanticAI tool checkpoint unless it actually exposes replayable tool arguments. Input overrides against older recordings without replay input-slot metadata now fail loudly instead of silently doing nothing.
- Bumped the minimum ZenML dependency, server image tag, and Helm subchart version to `0.96.1`.

### Fixed
- Modal stack creation now reuses matching server-side service connectors for artifact stores and container registries when explicit cloud credentials are not provided, avoiding remote-server failures caused by local-only credential inputs such as AWS SSO profiles.
- Replay now preserves recorded flow parameters when submitting a replay, so overriding one flow argument no longer lets defaulted arguments such as `model=None` silently replace recorded values.
- `FlowHandle.wait()` now distinguishes paused executions with pending wait input from paused executions that need `kitaru executions resume`, and `kitaru executions resume` accepts `--exec-id` while preserving clearer wait-condition resume diagnostics.
- PydanticAI edited tool-argument replay now reruns the tool's own argument validator, so JSON override values are coerced back into richer Python types such as `date` before the tool body runs.
- Checkpoint metadata now keeps Kitaru's reserved `boundary`, `type`, and `flow_result_candidate` keys authoritative when user metadata contains the same names.
- Plain user checkpoint input overrides can again mix recorded artifact input names with literal parameter overrides, while adapter-declared replay input slots still reject unknown keys.
- `FlowHandle.wait()` / `.get()` now preserve explicit `None` flow returns instead of falling back to discarded terminal checkpoint outputs.
- Flow result extraction now resolves a single eligible terminal checkpoint instead of raising ambiguity when replay or adapter runs record several terminal checkpoints, and honors the `flow_result_candidate` marker and saved flow-return artifacts before terminal-step heuristics.
- Execution deep links and compare links now resolve to the correct Kitaru UI route when connected to a Pro workspace, instead of landing on the workspace projects page via a stale `/flows/...` URL.
- Fixed replay LLM usage accounting so replay executions write terminal usage rollups, preserve incurred/executed records for live replay-tail calls, and classify explicitly skipped replay checkpoints as reused. (#490)
- Fixed `kitaru stack use` and `kitaru status` when `ZENML_ACTIVE_STACK_ID` points to an unresolvable stack: stack activation no longer fails while re-reading the active stack after activation, and diagnostics now tell users to unset, update, or remove the environment variable.
- Fixed flow submissions with an explicit stack so a successful run is not reported as failed only because Kitaru could not restore a stale previous active stack ID afterward.

## [0.19.0] - 2026-06-30

### Added
- Added experimental Google ADK adapter support with `KitaruADKRunner`, `KitaruADKModel`, and `KitaruADKTool`, plus docs, direct and persisted-workflow integration examples, isolated no-dev contract/live smoke paths, explicit-wrapper `calls` mode, and tool-confirmation resume helpers.
- Added public replay-mode detection helpers: `kitaru.is_replay()`, `kitaru.get_replay_runtime_context()`, and `kitaru.ReplayRuntimeContext`, so side-effectful checkpoints can guard behavior during replay.
- Added named LLM cost/token metric shortcuts for execution statistics across SDK, CLI, and MCP.
- Added `kitaru.diff(original, *executions)` for per-checkpoint structural comparison between an original execution and its replays (auto-discovers replays via `original_exec_id` when omitted).
- Added unified multi-execution replay through `flow.replay([...], *, at=..., ...)`, `KitaruClient().executions.replay([...], ...)`, and multi-ID `kitaru executions replay`. Parents missing the `at` checkpoint are skipped in collect mode and recorded in `ReplaySubmission.skipped`.
- Added `kitaru.diff_matrix(exec_ids)` to diff many originals against their auto-discovered replays.
- Added CLI commands `kitaru executions diff` and `kitaru executions diff-matrix`, plus MCP tools `kitaru_executions_diff`, `kitaru_executions_diff_matrix`, and unified `kitaru_executions_replay` with explicit execution IDs.
- Added client-side cohort selection via `kitaru.cohort(...).resolve()`, `KitaruClient().executions.cohort(...)`, CLI `kitaru executions cohort` (dry-run selection), and MCP `kitaru_executions_cohort`; replay now takes explicit execution IDs rather than selecting cohorts inside the replay command.
- Added a LangGraph replay and fork adapter (`kitaru.adapters.langgraph.replay`) that reconstructs a recorded LangGraph run as a directed graph from a captured trace and forks it for replay. Public surface includes `KitaruReplayAgent`, `KitaruAdapter`, `import_langgraph_trace` / `import_trace`, and graph `edit` helpers, with Langfuse and JSONL trace import sources.
- `kitaru executions replay` now prints UI compare URLs (and includes `compare_url` in JSON output) for the original execution vs the new replay.
- Reshaped the replay overrides demo as a prod walkthrough: slim `demo.py` dispatcher, `replay_scenarios/` modules, `publish-input` re-publish override, and narrative `README.md` (removed synthetic `record_replay_observation` tail and `inject-output`).
- `kitaru.diff()` now sets `ExecutionDiff.urls` to a single UI compare link listing the original and all compared replays (auto-discovered or explicitly passed). Compare URLs prefer deployment version metadata when present.
- Added `KITARU_UI_URL` to override the dashboard base URL used for compare and execution deep links when the Kitaru frontend is hosted separately from the API server.

### Changed
- **Breaking:** Redesigned the replay API. `flow.replay(...)`, `KitaruClient().executions.replay(...)`, CLI `kitaru executions replay`, and MCP `kitaru_executions_replay` now take the checkpoint selector as `at=` (previously `from_=`) with separate override groups `flow_overrides`, `checkpoint_overrides`, and `invocation_overrides` (previously a single `overrides` map), and return a shared `ReplaySubmission` result model (previously a `FlowHandle`). Code written against the earlier replay signature needs updating.
- Clarified Google ADK MCP docs around the safe subset: Kitaru can checkpoint a replay-safe ADK `BaseTool`-like object wrapped with `KitaruADKTool`, but it does not restore ADK MCP processes, sessions, or hidden server state.
- Refreshed the Google ADK dependency note: `google-adk` still stays out of the normal local/dev project environment, even though a 2026-06-29 direct resolver probe with `zenml[server]` now succeeds, until the full local server path is certified with the newer FastAPI/Starlette stack.

### Fixed
- Kitaru terminal logs now rewrite ZenML's named pipeline completion message to ``Flow `...` completed successfully.``, so ``Pipeline `...` completed successfully.`` no longer leaks into flow output.
- Replay checkpoint overrides now fan out across repeated adapter-generated model and tool calls, while suffixed selectors still target exactly one recorded call.
- Replay output overrides on terminal checkpoints now fail with a clearer error explaining that output replacement requires a downstream consumer.
- MCP replay now lets omitted `on_error` use the shared SDK default (`fail` for one parent, `collect` for batches), and `kitaru.diff()` now scans up to 10,000 same-flow executions when auto-discovering replays before warning that older replays may require explicit IDs.
- Unknown checkpoint override targets now give repeated-call guidance when a likely model/tool family exists, pointing users to family-level `checkpoint_overrides` or one-call `invocation_overrides`.
- LangGraph replay import now raises a catchable Kitaru error instead of `SystemExit` when Langfuse rows never arrive, and live forks support node callables that accept `(state, config)`.
- `KitaruClient.executions.replay()` and the pipeline fallback replay path now wait for completion and run terminal LLM usage aggregation before returning, so replay executions expose `llm_usage_summary` for compare/outcomes views.
- Cohort selection now hydrates list summaries when checking replay anchors, so `executions cohort` matches originals that only expose checkpoints on `executions get`.
- `kitaru.diff()` and `kitaru executions diff` now emit one multi-execution compare URL when auto-discovering replays, not one pairwise URL per replay.
- Replay planning now re-executes the full live tail after `at` for linear adapter call sequences that lack explicit DAG upstream edges (for example PydanticAI `calls` checkpoints after `lookup_policy_tool`).
- `FlowHandle.wait()` / `.get()` now return a flow's persisted output instead of raising an ambiguous-result error when an adapter produced several non-result model/tool checkpoints (the common `checkpoint_strategy="calls"` shape) or the flow has an explicit return value. Previously such flows raised an ambiguous-terminal error even though they completed successfully; the returned value is now linked via execution metadata and read back.
- Updated LLM usage summaries so Kitaru normally writes them when executions finish, while `FlowHandle.wait()` and `.get()` can populate missing summaries for older executions or executions where the finish-time summary was not written.

### Security
- Raised the `pydantic-ai-slim` lower bound and lockfile version to clear CVE-2026-48782.

## [0.18.0] - 2026-06-25

### Added
- Added adapter-owned cost calculator inputs for Claude Agent SDK, Gemini Interactions, and Pydantic AI, so adapter users can record estimated LLM costs with their own calculator hooks.
- Added default `genai-prices` estimated-cost support for OpenAI Agents, LangGraph, Claude Agent SDK, Gemini Interactions, and Pydantic AI adapter usage records when Kitaru has reliable provider/model/token data.
- Added estimated-cost recording for direct `kitaru.llm()` calls when provider usage includes token counts and `genai-prices` has pricing for the model, with config and environment controls to leave estimation on automatic or opt out. Cost estimation is best effort: a pricing-lookup miss never turns a successful LLM call into a failure.
- Added `--page` and `--size` pagination options to `kitaru executions statistics`.

### Changed
- Recorded Claude Agent SDK `total_cost_usd` as estimated cost metadata instead of provider-reported actual cost, with user calculators and then `genai-prices` as fallbacks when the SDK does not report a cost.

### Fixed
- Fixed direct `kitaru.llm()` OpenAI calls so public `max_tokens` is sent as OpenAI's `max_completion_tokens` for newer reasoning/GPT-5-style models, while older OpenAI, OpenRouter, and Ollama calls keep using `max_tokens`.

## [0.17.1] - 2026-06-22

### Added
- Added a LangGraph runner convenience API so fresh `invoke`, `ainvoke`, `stream`, and `astream` calls can pass raw graph input with `thread_id=...` instead of manually building `LangGraphRunRequest.start(...)`, while keeping request objects as the resume and advanced path. (#455)

### Changed
- Repositioned user-facing messaging (README, PyPI description, CLI `--help`, docs welcome page) around recording, replaying, and improving agents in production; durable execution is now described as the underlying mechanism rather than the headline.

### Fixed
- Fixed LangGraph calls-mode checkpoints so tool calls survive model-call materialization and LangGraph routes to tools correctly. (#458)

### Security
- Bumped audited dependency locks (`langsmith`, `msgpack`, `pydantic-settings`) to clear advisories flagged against the earlier pinned versions. (#455)

## [0.17.0] - 2026-06-19

### Added
- Added sandbox stack component support and the public `kitaru.run_sandbox_command(...)` SDK helper. Local stacks now get a default local sandbox, stack creation accepts explicit sandbox flavors through CLI/YAML/MCP paths, and `examples/features/sandbox/active_stack_sandbox_command.py` shows a tracked flow checkpoint running a command through the sandbox on the current stack. (#423)
- Added a PydanticAI sandbox command toolset via `sandbox_command_toolset(...)`, plus docs, example coverage, and `examples/integrations/pydantic_ai_agent/pydantic_ai_sandbox_toolset.py`, so PydanticAI agents can call `run_sandbox_command` through Kitaru's shared sandbox helper. (#429)
- Added an OpenAI Agents SDK sandbox command tool via `sandbox_command_tool(...)`, plus docs and `examples/integrations/openai_agents_agent/openai_agents_sandbox_tool.py`, so OpenAI agents can call a local `FunctionTool` that runs commands through the sandbox on the current stack. (#430)
- Added caller-owned Gemini custom function execution through Kitaru's sandbox helper, with explicit registered sandbox commands, a dry-run example showcase, and docs that keep Antigravity / Google-owned tool internals outside Kitaru's replay promise. This requires the Gemini extra's current Google GenAI SDK 2.x range (`google-genai>=2.8.0,<3`) for the Interactions step and function-result schema. (#433)
- Added a LangGraph/LangChain sandbox command tool via `create_sandbox_command_tool(...)`, plus a sandbox strategy in the LangGraph example, so agents can run shell commands through the sandbox on the current stack. (#434)
- Added a Claude Agent SDK sandbox MCP helper (`create_kitaru_sandbox_mcp_server`) and runnable example at `examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_sandbox_tool.py`, letting Claude call the sandbox on the current stack through a Kitaru-owned MCP tool while Claude-owned `Bash` stays disabled. (#432)

### Changed
- Bumped the ZenML dependency floor and aligned runtime surfaces to `zenml>=0.95.1`, which provides the sandbox stack component and sandbox session APIs used by this release.
- Reworked the prospect scout example (`examples/end_to_end/prospect_scout/`) into a genuinely agentic sweep: the qualifier is now a real PydanticAI agent that calls a `search_web` tool and decides its own searches (instead of being handed pre-fetched snippets), classifies prospects against a `LineOfBusiness` enum, and is built lazily inside its checkpoint so remote runs can inject keys via a secret. The README is reorganized around the durability, agent-observability, type-safety, and human-in-the-loop "aha moments", and a regression test asserts the agent actually invokes its search tool. (#446, #451)

### Fixed
- Rejected sandbox config overrides when stack creation did not also select a sandbox flavor, avoiding the confusing case where remote stack creation accepted sandbox-looking settings and then created no sandbox.
- Fixed the LangGraph sandbox demo so its real-model run deterministically executes the documented demo command, and redacted static sandbox tool env values from wrapped provider error messages. (#434)
- Kept the Claude Agent SDK sandbox MCP helper's default output limits and serialized JSON tool results within Claude Code's MCP result-size ceiling, so Kitaru's stdout/stderr truncation flags match what Claude actually receives. (#432)
- Fixed the news scout example (`examples/end_to_end/news_scout/`) to build its agent lazily inside the flow body instead of at module import, so remote-stack runs no longer crash at import when the provider key is only applied to the environment at run time. (#446)

## [0.16.0] - 2026-06-15

### Added
- Added count-based execution statistics through `KitaruClient().executions.statistics(...)`, `kitaru executions statistics`, and the `kitaru_executions_statistics` MCP tool, with grouping by status, flow, stack, tag, time bucket, and execution metadata. (#378)
- Added LLM token tracking: tracked LLM calls (`kitaru.llm()` and the framework adapters) record input/output/total token counts as execution metadata, split into freshly-incurred vs. replay-reused, feeding the new execution statistics metrics. (#378)
- Added LLM cost tracking where a trustworthy cost source exists: the Claude Agent SDK adapter records provider-reported USD cost automatically, and the LangGraph and OpenAI Agents adapters accept a cost calculator. Calls with no cost source (such as plain `kitaru.llm()`) are tallied under a records-without-cost count rather than reported as free. (#378)
- Added a local chatbot driver for `examples/chatbot/`, giving the durable chatbot example a direct command-line path for trying the conversation loop without the browser UI. (#408)
- Added the prospect scout example (`examples/end_to_end/prospect_scout/`): a durable prospect-research sweep with one checkpoint per company, enum-typed PydanticAI qualification, a `kitaru.wait()` shortlist approval gate, per-prospect outreach drafts, and optional Exa-backed web search.

### Fixed
- Fixed chatbot history persistence so resumed or continued local chatbot sessions keep their conversation history available to the driver and UI artifacts. (#410)
- Fixed `kitaru executions retry` and `kitaru executions resume` so failed or paused executions are reopened correctly before continuation is submitted, preventing local no-ops and server-token failures. (#442)
- `KitaruClient.executions.list()` now pushes flow and status filters to the server instead of scanning all project runs client-side. (#440)
- Redact credential values from LLM provider error messages raised by `kitaru.llm()`. (#439)
- Fixed a LangGraph adapter crash when running graphs without a LangGraph checkpointer under Kitaru's default durability policy. (#409)
- Fixed duplicate PydanticAI stream events so watched `KitaruAgent` runs no longer emit repeated stream updates. (#428)
- Fixed streaming wait tools so human-in-the-loop tool calls behave correctly during streamed adapter runs. (#431)
- Fixed agent instruction drift so adapter-managed agents keep their configured instructions across turns. (#435)
- Fixed the chatbot example image build so its PydanticAI dependencies resolve correctly. (#427)

## [0.15.0] - 2026-06-04

### Added
- Added the experimental Gemini Interactions adapter (`kitaru.adapters.gemini`) with an Interactions-first design: one stable Gemini response maps to one Kitaru checkpoint, non-stable background statuses fail instead of being cached as success, raw provider payload capture is opt-in by default, and the public API includes an Antigravity managed-agent preset plus explicit `cache_identity` disambiguation while keeping Google-owned sandbox/tool internals outside Kitaru's replay promise.
- Added OpenAI Agents live streaming via `KitaruRunner.run_stream(...)` and `run_stream_sync(...)`, publishing best-effort `openai_agents.stream.*` events while preserving `OpenAIRunResult` as the durable checkpoint output, plus a provider-key-gated runnable example at `examples/integrations/openai_agents_agent/openai_agents_streaming.py`.
- Added LangGraph graph-call streaming via `KitaruGraphRunner.stream(...)` and `astream(...)`, publishing best-effort `langgraph.stream.*` events while preserving `LangGraphRunResult` as the durable checkpoint output, plus a provider-neutral runnable example at `examples/integrations/langgraph_agent/langgraph_streaming.py`.
- Added Claude Agent SDK live streaming via `KitaruClaudeRunner.run_stream(...)` and `run_stream_sync(...)`, publishing best-effort `claude_agent_sdk.stream.*` events while preserving `ClaudeRunResult` as the durable checkpoint output.
- Added Gemini Interactions streaming surfaces (`run_stream(...)` and `run_stream_sync(...)`) plus exported stream event constants, conservative live-event privacy defaults, streamed poll support when the installed Google SDK exposes it, and example/smoke coverage for the no-network streaming path.
- Updated the Gemini Interactions example so `--stream` prints clipped Gemini text chunks by default for manual testing, with `--hide-text-deltas` available when event-label-only output is preferred; SDK live-event text remains hidden unless `GeminiInteractionCapturePolicy(include_stream_text_deltas=True)` is set.
- Added PydanticAI live stream events for `KitaruAgent` runs: the adapter now emits `pydantic_ai.stream.started`, `.event`, `.completed`, and `.failed` updates for watched PydanticAI streams, plus a provider-key-gated runnable example at `examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py`.
- Added SDK event watching: `KitaruClient().executions.events(...)` now consumes server-backed live execution events with kind/checkpoint/correlation filters, SSE cursor reconnects, and clear feature-unavailable errors for local database mode or disabled server streaming.
- Added checkpoint live-event publishing: `kitaru.progress(...)` and `kitaru.events.publish(...)` can now emit best-effort progress/custom events from inside running checkpoints, and Kitaru automatically publishes checkpoint started/completed/failed lifecycle events when the checkpoint body executes.

### Changed
- Moved adapter docs into a first-class Adapters docs section, with redirects from the old guide URLs to the new canonical adapter pages.

### Fixed
- Fixed PydanticAI adapter compatibility with newer Pydantic AI releases that forward `retries=` from `run_sync()` into `run()`, while preserving Kitaru's legacy `output_retries=` keyword.

## [0.14.0] - 2026-06-02

### Added
- Added PydanticAI `checkpoint_strategy="calls" | "turn"` as the preferred public spelling for adapter checkpoint placement. `"calls"` remains the default and maps to the existing per-model/tool/MCP checkpoint behavior; `"turn"` maps to the existing one-checkpoint-per-agent-run behavior. Existing `granular_checkpoints=True | False` code remains supported. (#374)
- Added a durable chatbot example at `examples/chatbot/` that models an entire conversation as a single PydanticAI agent with one human-in-the-loop tool, using `kitaru.wait()` to release compute between turns so a session can sleep for minutes or days and resume exactly where it left off. (#376)

### Changed
- Standardized adapter docs and examples around the shared `checkpoint_strategy` concept while keeping framework-specific boundary names such as PydanticAI `"turn"`, OpenAI Agents `"runner_call"`, LangGraph `"graph_call"`, and Claude Agent SDK `"invocation"`. (#374)
- Bumped the minimum ZenML dependency, server image, and Helm subchart versions to `0.94.6` so Kitaru tracks the latest upstream ZenML release. (#382)

## [0.13.1] - 2026-05-21

### Added
- **Agent Harness Platform** — a chapter-by-chapter flagship example at `examples/end_to_end/agent_harness_platform/` and a dedicated docs section at [`/docs/agent-harness-platform/`](https://kitaru.ai/docs/agent-harness-platform/). A platform-engineer's starter kit for building internal agent platforms on Kitaru + PydanticAI: six runnable stages take a 30-line durable agent → `DockerSandbox` → skills as markdown → credential proxy with mitmproxy + auth injection → typed-union `exec_service` dispatcher → HITL via `kitaru.wait()`. Includes a per-stage `Profile` gating model, an `agent_harness_platform/` library, mocks + Dockerfiles, and layer-A smoke tests in `tests/test_agent_harness_platform_example.py`. (#288)

### Changed
- `docs/content/docs/getting-started/examples.mdx` reorganized into three categories — Agent Harness Platform tour / Other end-to-end / Feature-focused. The previous goal-keyed table is replaced. (#288)
- `docs/content/docs/guides/news-scout.mdx` removed; the `news_scout` example itself stays runnable in the repo and is now listed under "Other end-to-end examples" on the docs site. The guides section is reserved for Kitaru-feature how-tos. (#288)

### Fixed
- Fixed PydanticAI adapter compatibility with `pydantic-ai-slim>=1.95`, where upstream renamed built-in tools to native tools. The adapter no longer fails at import time on `AgentBuiltinTool` or crashes by forwarding `builtin_tools=None` into PydanticAI's deprecation shim. (#370)

## [0.13.0] - 2026-05-20

### Added
- Added OpenAI Agents adapter context passthrough: `KitaruRunner.run(...)` and `run_sync(...)` now accept a `context=` argument that is forwarded to the OpenAI Agents SDK and included in runner/tool checkpoint cache keys, with an explicit `context_cache_identity=` projection hook for stable production contexts. Context-derived cache identity also covers tool calls resumed from interrupted `RunState` so approved tools after a HITL resume cannot reuse stale same-args/different-context cache entries. (#345)
- Added OpenAI Agents tool-input guardrail observability in `checkpoint_strategy="calls"`: model-requested tool calls a guardrail blocks before the tool body runs are now recorded as `tool_call` events with guardrail metadata, without creating a tool checkpoint or persisting rejected arguments. `OpenAICapturePolicy.save_input=False` redacts guardrail rejection text and unexpected exception details, and `save_interruption_payloads=False` omits raw interruption argument previews. (#345)
- Built wheels now include the `kitaru/py.typed` PEP 561 marker so downstream type checkers pick up Kitaru's public type information. (#343)

### Changed
- Bumped the minimum ZenML dependency, server image, and Helm subchart versions to `0.94.4` so Kitaru tracks the latest upstream ZenML release. (#344)
- `kitaru logout` now resets persisted store state and clears credentials before attempting best-effort local-daemon shutdown, so a failure to stop the daemon no longer leaves the CLI pointed at a broken remote connection. (#343)
- `kitaru secrets list` now uses a stable backend page size before applying CLI pagination, producing deterministic ordering across runs. (#343)

### Fixed
- Fixed adapter-created granular checkpoints being treated as flow-return candidates, so `flow.run(...).wait()` / `.get()` can return the user's final checkpoint result when adapters also produced model/tool checkpoints. (#355)
- Fixed Kitaru-owned request constructors to reject checkpoint output handles with guidance to call `.load()` instead of surfacing generic Pydantic string validation errors. (#353)
- Fixed PydanticAI direct sync tool-body `kp.wait_for_input(...)` calls under ZenML `0.94.4` with explicit `allow_sync_tool_body_waits=True` opt-in, keeping `tool_checkpoint_config_by_name={"tool": False}` as checkpoint-only configuration. (#351)
- Fixed Kitaru flow return compatibility with ZenML `0.94.4` dynamic-pipeline output validation by persisting plain flow returns as internal artifacts while preserving user-facing Python return values and avoiding marker-shaped user dictionaries being mistaken for hidden tuple metadata. (#344)
- Fixed adapter result identity after checkpoint load for OpenAI Agents, Claude Agent SDK, and LangGraph runners: results restored from a synthetic checkpoint are now rebuilt as the canonical local result class, so `isinstance(result, OpenAIRunResult)` (and the Claude/LangGraph equivalents) no longer fails when the loaded payload originally came from an alternate import path. (#354)
- Replaced local-server cleanup's PID-only `SIGKILL` fallback with a "warn and continue" path so a recycled PID cannot cause Kitaru to kill an unrelated process during `kitaru clean global/all`. Inspection failures now surface as `unknown (inspection failed: ...)` instead of being silently treated as "no local server". (#343)
- Restored the caller process environment exactly after `kitaru login` startup attempts, even when local-daemon deployment or connection fails partway through. (#343)
- Removed stale references to the deprecated native memory surface from the docs site, agent-native guides, and comparison pages. (#342)

## [0.12.0] - 2026-05-17

### Added
- Added LangGraph `checkpoint_strategy="calls"` support via `KitaruLangGraphMiddleware`, creating true sync LangChain model/tool call checkpoints while keeping `graph_call` as the default coarse mode. The guide now explicitly documents that callbacks/event streams are trace-only, LangGraph checkpointers remain LangGraph-owned, and async calls mode is metadata-only.
- Added a local LangGraph adapter example (`examples/integrations/langgraph_agent/`) plus a new LangGraph adapter guide (`/adapters/langgraph/`) covering the adapter boundary: Kitaru owns graph-call or middleware-wrapped call checkpoints, LangGraph owns thread/checkpointer semantics, and Deep Agents filesystem/sandbox behavior remains pass-through. Updated the examples indexes and smoke test to include deterministic LangGraph examples with no API keys required.
- Claude Agent SDK adapter (`kitaru.adapters.claude_agent_sdk`) for invocation-level durability: wrap a Claude SDK query in one Kitaru checkpoint, capture the session ID, final result, usage/cost, messages/transcript artifacts when available, and a redacted run manifest. Includes a guide, integration example, and smoke-test coverage while explicitly documenting that Claude-internal Bash, MCP, custom tool, and workspace side effects are not granular replay boundaries.
- Added `kitaru.current_execution_id()` as the public way to read the active Kitaru execution ID inside a running flow or checkpoint.

### Fixed
- LangGraph adapter event logs and run summaries are now saved as real role-first Kitaru context artifacts inside checkpoint scope, with best-effort event persistence by default and hardened config/context redaction for unusual values.
- PydanticAI granular checkpoints now store model messages and tool arguments as structural checkpoint inputs and use the returned checkpoint output as the canonical response/result artifact, avoiding duplicate manual artifacts in new runs.
- OpenAI Agents `checkpoint_strategy="calls"` now stores model inputs and function-tool arguments as structural checkpoint inputs, and adapter-generated artifact names now put the human-readable role first across PydanticAI, OpenAI Agents, and Claude Agent SDK captures.

## [0.11.0] - 2026-05-12

### Fixed
- PydanticAI adapter run surfaces now accept and forward upstream `conversation_id` and `output_retries` kwargs and include them in turn-checkpoint cache keys, while temporarily capping `pydantic-ai-slim` to the supported 1.89–1.92 line.
- Fixed PydanticAI MCP tool calls hanging after a successful request when an explicitly lifecycle-managed MCP server was already open. Kitaru now keeps already-running MCP calls on the active event loop inside explicit flows, and fails fast when auto-flow would otherwise move a pre-opened MCP server across event loops; auto-connected MCP servers still use granular MCP checkpoints by default.
- Added a compatibility shim for the `pydantic_ai.mcp` import path used by current PydanticAI releases when Kitaru is installed with the MCP SDK version still compatible with ZenML server dependencies.

### Removed
- Removed the native memory surface from Kitaru: `kitaru.memory`, `KitaruClient.memories`, the `kitaru memory` CLI group, MCP `kitaru_memory_*` tools, and the corresponding memory docs/examples. Use your own storage for durable application state and pass values into flows explicitly.

### Security
- Bumped transitive dependencies flagged by `pip-audit`: `gitpython` 3.1.47 → 3.1.49 (CVE-2026-44244), `mako` 1.3.11 → 1.3.12 (CVE-2026-44307), `python-multipart` 0.0.26 → 0.0.27 (CVE-2026-42561), and `urllib3` 2.6.3 → 2.7.0. Lockfile-only change; no API surface affected.

## [0.10.0] - 2026-05-08

### Changed
- `kitaru.wait()` and adapter wait paths are flow-scope only. Waits created from checkpoint-contained tool bodies must move to flow scope, or those waiting tools must be opted out of granular tool checkpoints. (#280)
- `KitaruAgent` now defaults to `granular_checkpoints=True`, so model, tool, and MCP calls are persisted as separate adapter checkpoints by default. Pass `granular_checkpoints=False` to keep the previous one-checkpoint-per-agent-run turn mode. (#280)
- PydanticAI adapter checkpoint configs now accept `cache`, and granular model checkpoint cache keys ignore PydanticAI-generated per-run message metadata so identical logical prompts can cache across runs. (#280)
- Streamlined the `openai_research_bot` end-to-end example for readability, and refreshed the OpenAI Agents adapter guide and examples index to match. (#308)

### Fixed
- PydanticAI flow-scope trackers now allocate unique artifact namespaces to avoid cross-run artifact-name collisions. (#280)
- Cached granular PydanticAI model responses now preserve model event/tool-call ordering for parallel tool calls. (#280)
- OpenAI Agents adapter parallel tool-call events now keep assistant-emitted order in event logs and summaries, even when tools start or finish out of order. (#280)
- PydanticAI adapter parallel tool-call events now keep assistant-emitted order in event logs, summaries, and fan-in metadata, even when tools start or finish out of order. (#280)
- PydanticAI adapter observability artifact names now use shorter event-local suffixes inside readable tracker namespaces, avoiding collisions across flow-scope and checkpoint-scope trackers. (#280)

## [0.9.0] - 2026-05-05

### Added
- OpenAI Agents SDK adapter (`kitaru.adapters.openai_agents`) — wrap an `Agent`/`Runner` with `KitaruRunner` to make OpenAI Agents SDK runs durable, replayable, and observable under a Kitaru flow. Supports two tracking strategies via `checkpoint_strategy="runner_call"` (one checkpoint per `Runner.run`, recommended when you want a clean `.wait()` return value) or `checkpoint_strategy="calls"` (per-tool/per-model checkpoints for finer replay units, with per-checkpoint artifacts visible in the Kitaru UI / `KitaruClient`). The guide at `/adapters/openai-agents/` walks through the trade-offs. (#295)
- OpenAI Agents integration example (`examples/integrations/openai_agents_agent/`) and an end-to-end `openai_research_bot` example (planner/writer runner checkpoints, submitted search fan-out, and final report artifacts, with remote secret guidance and Kitaru UI artifacts). Both are exercised by the smoke test. (#295)
- Markdown exports for every docs page at `kitaru.ai/docs/<slug>.md`, plus a substantially expanded `/llms.txt` index — making the docs friendlier for LLMs and agents that consume them programmatically. (#303)

### Changed
- `flow.run(...).wait()` now raises a new dedicated `KitaruAmbiguousFlowResultError` (subclass of `KitaruRuntimeError`) when the flow has multiple terminal checkpoints with no single sink (common with the OpenAI Agents adapter's `checkpoint_strategy="calls"`). The error names the terminal checkpoints, points at the execution's artifacts in the Kitaru UI, and suggests `KitaruClient` retrieval and the `runner_call` strategy as alternatives. Catching this specific subclass lets callers handle the ambiguity case without accidentally swallowing real execution failures.

## [0.8.0] - 2026-05-04

### Added
- OSS-first auth management for service accounts and API keys via `KitaruClient.auth`, `kitaru auth service-accounts`, and `kitaru auth api-keys`. Raw API-key values are only returned on create/rotate so they can be stored immediately; list/show/update responses stay metadata-only. (#230)

### Changed
- Pydantic AI adapter now supports `pydantic-ai-slim>=1.86.0,<2`: per-run `capabilities` and `spec` are forwarded to Pydantic AI and included in turn-checkpoint cache keys to avoid stale cached turns. (#270)
- `examples/` is reorganized into `features/`, `integrations/`, and `end_to_end/` subdirectories. Existing example paths (e.g. `examples/basic_flow/...`) move under one of these categories — update any pinned references. (#242)

### Fixed
- Checkpoint output handles now display Kitaru guidance to call `.load()` instead of leaking raw ZenML artifact metadata when stringified in flow bodies. (#252)
- `kitaru executions replay` now resolves project-local modules correctly when invoked from a project directory, instead of falling back to the CLI bootstrap module via `__main__` and producing a misleading replay. (#218)
- Runtime log retrieval (`KitaruClient.executions.logs(...)`, `kitaru executions logs`) now tolerates server/client version skew on log payload schemas instead of erroring out. (#251)
- Active-stack resolution no longer silently falls back to a deleted or unavailable stack — flow submission, MCP, and `kitaru status` now surface a clear error when the configured active stack is gone. (#263)
- `KitaruAgent` auto-checkpointing of agents that use `@hitl_tool(schema=...)` no longer crashes with `PydanticSerializationError: Unable to serialize unknown type: <class 'type'>` under `pydantic-ai-slim>=1.86`, which now surfaces per-tool metadata through the `AgentRunResult` tree. (#292)

## [0.7.0] - 2026-04-24

### Added
- `kitaru build --image`, `kitaru deploy --image`, and MCP `kitaru_deployments_deploy(image=...)` now accept deploy-time image configuration (base image string or `ImageSettings`-style object), so saved deployment snapshots can carry remote-only package installs and secret-backed environment injection. (#221)

### Changed
- **Breaking:** Replay planning now uses graph reachability from replay roots, so replaying from a branch leaf only re-executes that branch's downstream path. Checkpoint override semantics are aligned accordingly: `checkpoint.<selector>` injects into direct consumers, and replay roots include those consumers. Scripts relying on the previous ordering/index-based frontier may see different execution paths when replaying parallel branches. (#228)
- Bumped the minimum ZenML version to `0.94.3`, picking up upstream artifact-store path validation alongside compatibility fixes to Kitaru's materializers and tests. (#232)
- Clearer error when a stack references an integration whose dependencies are not installed — flow resolution now points users to the exact extra they need to install (e.g. `kitaru[k8s]`, `kitaru[vertex]`) instead of a low-level ZenML import error. (#227)

### Fixed
- `kitaru executions` URL logging now prints the correct dashboard URL for each execution. (#223)

## [0.6.0] - 2026-04-23

### Added
- `kitaru auth token` for printing a short-lived bearer token for the active Kitaru server, suitable for shell command substitution. (#210)
- `kitaru flow deployments curl FLOW` for generating a copy-pasteable curl command that starts a deployment execution through the active Kitaru server without inlining real tokens. (#210)
- CLI commands for building, deploying, invoking, listing, tagging, logging, and deleting snapshot-backed flow deployments. (#210)
- MCP deployment tools for deploying, invoking, listing, inspecting, deleting, tagging, and untagging snapshot-backed flow deployments. (#210)
- Deployment model docs covering auto-versioning, reserved/default tag routing, serverless invocation, active Kitaru server authentication, and producer/consumer examples. (#210)
- Python SDK secret write helpers: `kitaru.create_secret(...)` and `kitaru.delete_secret(...)`. (#206)
- MCP secret creation tool `kitaru_secrets_create` for metadata-only secret creation from MCP clients. (#206)
- `kitaru.adapters.pydantic_ai.wait_for_input(...)` helper for pausing a PydanticAI tool call until a human supplies input, with the wait recorded under the adapter's metadata. (#216)
- `news_scout` example and accompanying guide: an agentic news monitor that demonstrates granular checkpoints, durable shared memory, and replay across executions. (#191)
- `compliance_review` example: a multi-stage Claude Agents SDK workflow illustrating single-turn, multi-domain, memory-backed, and conversational patterns under Kitaru. (#161)

### Changed
- `kitaru secrets set` now creates public secrets by default. Pass `--private` to create a private secret. Updating an existing secret still only updates values and leaves existing visibility unchanged. (#206)
- `kitaru.wait(...)` can now be called from inside `@checkpoint` bodies (previously flow-level only). The enclosing checkpoint suspends for the duration of the wait; on resume, the checkpoint re-runs from the top. (#216)
- Reframed the concept docs around the "platform-builder" primitive: new `harness-runtime-platform` concept page, rewritten `how-it-works` / `flows` / `checkpoints` explainers, and removal of the now-redundant `execution-model` page. (#208)

## [0.5.1] - 2026-04-17

### Added
- `ImageSettings.secret_environment_from` field for attaching ZenML secret references to a flow execution; Kitaru forwards the list through `Pipeline.with_options(secrets=[...])` so secret values never enter `DockerSettings.environment`, image build metadata, logs, or the frozen execution spec (#188)
- `kitaru info --all` now includes active stack/project provenance, showing whether the effective context came from environment variables, repo-local `.kitaru/config.yaml`, or global config. The same structured fields are available through JSON output, exported diagnostics files, and MCP `kitaru_info(all=True)` (#186)
- `KitaruMemoryArtifactUnavailableError` typed exception (subclass of `KitaruBackendError`) for memory entries whose backing artifact cannot be loaded from the current runtime (#189)
- `strict=False` parameter on `kitaru.memory.get(...)`, CLI `kitaru memory get --strict`, and MCP `kitaru_memory_get(strict=...)`. Lenient mode warns and returns `None` (Python) or returns a payload with `value_available: False` and nested `value_unavailable` diagnostics (CLI/MCP); strict mode raises `KitaruMemoryArtifactUnavailableError` (#189)

### Changed
- `kitaru.memory.get(...)` no longer raises `KitaruBackendError` by default when a memory entry's artifact value is unreachable from the current stack (for example, dev→prod stack switches where old artifact URIs point at a local filesystem path). The new default is to warn and return `None` so flows can fall through to their existing missing-key handling. Callers that depended on exception-based signaling should pass `strict=True` (#189)

## [0.5.0] - 2026-04-17

### Breaking Changes
- `kitaru.adapters.pydantic_ai.wrap(...)` is deprecated in favor of `KitaruAgent(...)`. A compatibility shim remains for one release (#156)
- Legacy adapter capture config names were renamed: `"metadata_only"` -> `"metadata"` and `"off"` -> `None` (#156)
- Legacy `tool_capture_config_by_name={"name": {"mode": "metadata_only"}}` now maps to `capture=CapturePolicy(tool_capture_overrides={"name": "metadata"})` (#156)

Migration snippet:

```python
from kitaru.adapters.pydantic_ai import CapturePolicy, KitaruAgent

wrapped = KitaruAgent(
    agent,
    capture=CapturePolicy(
        tool_capture="full",
        tool_capture_overrides={"name": "metadata"},
    ),
)
```

### Added
- `kitaru.get_secret()` and the public `Secret` model for exact, Kitaru-native secret reads in Python code without importing ZenML directly (#185)
- `@checkpoint(cache=...)` per-checkpoint cache overrides (`True`/`False`/`None`) with updated configuration docs (#184)
- `kitaru.adapters.pydantic_ai.wrap(...)` compatibility shim with deprecation warning to ease migration to `KitaruAgent(...)` (#156)
- Granular checkpoint mode now installs a run-level tracker at flow scope and persists `pydantic_ai_events` plus `pydantic_ai_run_summaries` even when no turn checkpoint is opened (#156)
- Restored end-to-end PydanticAI adapter integration coverage for turn mode, granular mode, and auto-flow execution (#156)

### Changed
- PydanticAI adapter auto-flow now re-enters the normal run path so turn checkpoints, tracking, and message-history capture apply outside explicit flows (#156)
- PydanticAI granular mode now defaults its per-call checkpoint configs on, rejects invalid config combinations eagerly, keeps HITL interception active when capture is disabled, and raises clear usage errors for unsupported deferred-tool schemas (#156)
- PydanticAI adapter docs, README examples, and migration guidance now match the shipped runtime: `runtime="inline"` only for adapter-managed checkpoints, explicit deprecation path for `wrap(...)`, and corrected capture-policy examples (#156)

### Fixed
- Execution-level cache no longer defaults to `True`, so `@checkpoint(cache=False)` is preserved through ZenML compilation when no flow-level cache is explicitly configured (#184)

## [0.4.1] - 2026-04-16

### Changed
- CLI list commands now default to paginated windows (`--page 1 --size 20`) for executions, memory, stacks, models, and secrets. `kitaru executions list` also shows compact `Started` and `Ended` columns, while JSON output keeps the existing `{command, items, count}` envelope shape. Paging past the end of a non-empty list now reports `no items on page N` across all five commands rather than a misleading "none found". `kitaru executions list --limit N` still works but no longer accepts any explicit `--page`/`--size`, so the two modes don't silently mix (#139)
- Clarified flow-body artifact loading semantics in the concepts and guides docs, including a dedicated section in the artifacts guide and tighter guidance in the `wait`/`input` and LLM-call pages (#143)
- Expanded the MCP setup docs with a venv/PATH caveat (the common failure mode where Claude Code inherits its launcher's PATH rather than a later-activated venv) and added `claude mcp add` with all three scope flags as an alternative to hand-editing `.mcp.json`

### Fixed
- Fixed SDK and CLI reference rendering in the generated docs, including docstring cleanups across `checkpoint`, `flow`, `logging`, `artifacts`, and `client` so the griffe/fumapy pipeline emits correctly formatted reference pages (#141)

## [0.4.0] - 2026-04-12

### Added
- **Durable agent memory** (`kitaru.memory`) — a new core primitive for durable, artifact-backed agent memory with typed scopes (`namespace` for cross-flow sharing, `flow` for per-flow state, `execution` for per-run state). Values persist through restarts, replays, and cross-execution workflows. Inside flows, reads and writes are captured via private non-cacheable synthetic steps so they remain replayable; outside flows, `kitaru.memory.configure(scope=..., scope_type=...)` unlocks the same API for seeding and inspection scripts (#82)
- **Memory compaction** — `kitaru memory compact`, `KitaruClient.memories.compact(...)`, and MCP `kitaru_memory_compact` summarize one or many memory values using an LLM and write the summary back as a new version. Supports single-key or multi-key compaction, current-value or full-history source modes, and records every operation in a per-scope audit log viewable via `kitaru memory compaction-log`
- **Memory purging** — `kitaru memory purge` deletes old versions of a single key while keeping the latest; `kitaru memory purge-scope` reclaims an entire scope (optionally including tombstoned keys) and records audit entries alongside compaction events. The internal compaction log is never itself purged
- Full `kitaru memory` CLI command group: `scopes`, `get`, `set`, `delete`, `history`, `purge`, `purge-scope`, `compact`, `compaction-log`, and `reindex`
- `KitaruClient.memories` typed namespace for `get/list/history/set/delete` plus maintenance operations (`purge`, `compact`, `reindex`) by explicit scope
- Nine MCP memory tools (`kitaru_memory_list/get/set/delete/history/purge/purge_scope/compact/compaction_log`) for agent-facing access from Claude, Cursor, and other MCP clients
- Automatic flow-membership indexing for new execution-scoped memory writes, plus `kitaru memory reindex` / `KitaruClient.memories.reindex(apply=...)` for dry-run-first backfilling of historical memory tags in existing projects
- Shared memory transport helpers (`kitaru._interface_memory`, `kitaru.inspection.serialize_memory_*`) so CLI, MCP, and SDK surfaces share one payload/validation layer
- Dedicated memory docs: concept page (`/concepts/memory`) and full guide (`/guides/memory`) covering typed scopes, in-flow vs outside-flow usage, durability semantics, and maintenance workflows
- Runnable memory example under `examples/features/memory/flow_with_memory.py` with narrated text output

### Changed
- `kitaru.memory.set/get/list/history/delete()` outside flows now require a configured scope via `kitaru.memory.configure(...)` and raise `KitaruStateError` with setup guidance when no scope has been configured. Inside flows, no configuration is needed — the execution scope is inferred automatically
- `memory.*` remains forbidden inside `@checkpoint` — the replay boundary is preserved by routing all memory operations through flow-scope synthetic steps
- Memory writes re-fetch the exact created artifact version by ID before returning typed metadata, so the client surface reports the concrete written version rather than guessing from "latest by name"

### Fixed
- Memory artifact version queries now use the correct `desc:version_number` sort order (was `version_number:desc`)

## [0.3.6] - 2026-04-11

### Added
- Copy-paste prompt examples in MCP server documentation for common workflows (status checks, flow execution, replay, artifact inspection)
- MCP extra mentioned earlier in the installation guide
- Troubleshooting guidance for MCP environment variable configuration

### Changed
- Improved anonymous telemetry metadata for opted-in users (richer flow lifecycle context, version stamping, deployment classification)

## [0.3.5] - 2026-04-11

### Added
- `kitaru analytics` CLI command group with `opt-in`, `opt-out`, and `status` subcommands for managing anonymous usage analytics preferences — persists to config file so the preference is respected by all surfaces including MCP servers

### Fixed
- Analytics events leaking from smoke test runs to Mixpanel (disabled via `ZENML_ANALYTICS_OPT_IN=false` export)
- MCP server ignoring user's analytics opt-out when launched via stdio transport (env vars stripped by MCP SDK; `kitaru analytics opt-out` persists preference to config file as the fix)
- `kitaru analytics` commands no longer eagerly bootstrap the ZenML store (added to `_DEFERRED_BOOTSTRAP_COMMANDS`)

## [0.3.4] - 2026-04-11

### Added
- `kitaru clean` command group with `project`, `global`, and `all` subcommands for resetting Kitaru state (with `--dry-run`, `--force`, `--yes` flags, auto-backup, model registry protection, and local server teardown)
- Enhanced `kitaru info` with new flags (`--all`, `--all-packages`, `--packages`, `--file`) and multi-section output including config provenance, connection source breakdown, system info, ZenML version, and package inventory
- `kitaru info --file` exports diagnostics to JSON or YAML (environment variable secrets are masked)
- Show actionable recovery hint (`kitaru executions retry <id>`) after flow failure in SDK errors and CLI follow-mode output (#120)

## [0.3.3] - 2026-04-08

### Added
- `ImageSettings` now supports `build_context_root`, `image_tag`, `target_repository`, and `user` fields for finer-grained container image configuration
- `ImageSettings.platform` field for specifying the target Docker build platform (e.g. `linux/amd64`)
- Anonymous usage analytics instrumentation across CLI, MCP, and SDK surfaces
- Pre-release smoke test script (`scripts/smoke-test.sh`) for end-to-end sanity checks

### Changed
- Replace runtime dashboard file patching with `ZENML_SERVER_DASHBOARD_FILES_PATH` environment variable, simplifying local server startup (#92)

### Fixed
- Suppress noisy config-change warnings that appeared during flow resume (#97)

## [0.3.2] - 2026-04-06

### Fixed
- Skip eager ZenML store bootstrap for commands that don't need a server connection (`--version`, `--help`, `login`, `logout`, `init`), preventing ~30 second startup delays when the stored config points to an unreachable server (#107)

### Changed
- Add Apple Silicon Docker guidance: `--platform linux/amd64` workaround for M-series Macs, troubleshooting for manifest mismatch errors, and startup timing notes (#106)
- Default Kitaru UI Docker build tag to latest release instead of requiring explicit version (#103)

## [0.3.1] - 2026-04-06

### Fixed
- Fix duplicate terminal handler accumulation after `importlib.reload()` by using marker-based detection instead of `isinstance` checks, preventing duplicated log output in long-running or reload-heavy environments

### Changed
- Bump minimum `pydantic-ai-slim` from `>=0.2.0` to `>=1.75.0` to align with upstream API changes (new method signatures, `tool_plain` decorator, `AgentSpec` support)
- Rewritten examples: realistic research-agent metaphor in basic flow, two-wait pattern (boolean gate + Pydantic schema) in wait/resume, parallel tool submission in coding agent, and consistent “Getting Started” READMEs across all example groups
- CLI command tracking now uses an allowlist of known multi-word commands to avoid leaking positional arguments (URLs, paths) into analytics
- Add PyPI classifiers and keywords for improved package discoverability

## [0.3.0] - 2026-03-24

### Added
- `@checkpoint(runtime="isolated")` parameter for running individual checkpoints in separate containers on remote orchestrators (Kubernetes, Vertex, SageMaker, AzureML); accepts `"inline"`, `"isolated"`, or `StepRuntime` enum values with early validation

### Changed
- Replace LiteLLM dependency with direct OpenAI and Anthropic SDK support
  - `openai` and `anthropic` are now optional extras: `pip install kitaru[openai]`, `pip install kitaru[anthropic]`, or `pip install kitaru[llm]` for both
  - `kitaru.llm()` public API is unchanged; lazy imports raise a clear `KitaruUsageError` with install guidance if the required SDK is not installed
  - Built-in runtime support now covers `openai/*`, `anthropic/*`, `ollama/*`, and `openrouter/*` models; other providers can be used directly inside `@checkpoint`
  - Ollama and OpenRouter use the OpenAI-compatible API (no new dependencies, reuse `kitaru[openai]`)
  - Model alias resolution, credential handling, and artifact/metadata persistence are unchanged
  - `cost_usd` metadata field is now omitted (direct provider SDKs do not include cost data)

### Removed
- `litellm` core dependency (removed due to [PyPI supply chain compromise](https://github.com/BerriAI/litellm/issues/24512) in versions 1.82.7–1.82.8)

## [0.2.1] - 2026-03-23

## [0.2.0] - 2026-03-20

### Added
- `docker/Dockerfile.server-dev` for local server + UI development without a published UI release

### Changed
- Switch ZenML dependency from pinned git commit to PyPI release (`zenml>=0.94.1`)
- Production server Docker image now layers on `zenmldocker/zenml-server` instead of rebuilding ZenML from source
- Kitaru UI is now bundled into the server image, replacing the ZenML dashboard
- Flow-execution image (`docker/Dockerfile.dev`) now installs ZenML from PyPI instead of git

### Removed
- `_FlowDefinition.deploy()` method; `.run(stack="...")` is now the single way to start a flow execution, whether local or remote
- `FlowInvocationResult.invocation` field and the `"invocation"` key in MCP run-tool payloads
- `kitaru run` CLI command and its live terminal renderer; flow execution is now started via Python (`my_flow.run(...)` / `my_flow.deploy(...)`) or MCP tools, while the CLI focuses on execution lifecycle management via `kitaru executions ...`
- `kitaru.terminal` module (run-only Rich Live renderer and helpers)
- Runtime submission observer plumbing (`_submission_observer`, `_notify_submission_observer`) from `kitaru.runtime` and `kitaru.flow`

### Added
- Unified config directory: Kitaru and ZenML now share a single config directory by default; the init hook sets `ZENML_CONFIG_PATH` to Kitaru's app dir so the database, credentials, and local stores live alongside Kitaru's own config; `KITARU_CONFIG_PATH` overrides the location for both; `kitaru status` now reports this unified directory
- `kitaru init` command to initialize a project root by creating a `.kitaru/` directory; this sets the source root for code packaging during remote execution and prevents ambiguous source-root heuristics; the command checks for both `.kitaru/` and legacy `.zen/` markers before initializing
- `kitaru executions input` now auto-detects the single pending wait condition, removing the need for `--wait`; use `--interactive` (`-i`) for guided review with JSON schema display, continue/abort/skip/quit actions, and multi-execution sweep mode; use `--abort` to abort a wait in non-interactive mode
- `KitaruClient.executions.pending_waits(exec_id)` returns all pending wait conditions for an execution
- `KitaruClient.executions.abort_wait(exec_id, wait=...)` aborts a pending wait condition
- MCP local lifecycle tools: `kitaru_start_local_server(port?, timeout?)` and `kitaru_stop_local_server()`
- Native Kitaru terminal logging: ZenML console output is now intercepted and rewritten to Kitaru vocabulary (pipeline→flow, step→checkpoint, run→execution) with colored lifecycle markers; ZenML-specific noise (Dashboard URLs, user/build info, component listings) is suppressed from the terminal while remaining available in stored logs via `kitaru executions logs`
- Shared source-alias module (`kitaru._source_aliases`) centralizing alias prefix constants and normalization helpers previously duplicated across 7+ files

### Changed
- **Breaking:** `kitaru executions input` no longer accepts `--wait`; the CLI auto-detects the single pending wait (use `--interactive` for multi-wait executions). MCP `kitaru_executions_input` still requires explicit `wait` for deterministic tool calls.
- Flows and checkpoints now register with plain names in ZenML (e.g. `my_flow`, `fetch_data`) instead of prefixed internal aliases (`__kitaru_pipeline_source_my_flow`, `__kitaru_checkpoint_source_fetch_data`); the internal source aliases remain for ZenML source loading but are no longer visible in the ZenML UI or API responses
- Moved Claude Code skills (kitaru-scoping, kitaru-authoring) to dedicated repository: [zenml-io/kitaru-skills](https://github.com/zenml-io/kitaru-skills)
- Config and stack helpers now raise Kitaru-specific exception subclasses instead of raw `ValueError` / `RuntimeError`, while preserving compatibility through inheritance
- `kitaru stack list --output json` and MCP `kitaru_stacks_list` now include `is_managed`, derived from the stack's `kitaru.managed` label
- `kitaru stack create --type kubernetes` and MCP `manage_stack(action="create", stack_type="kubernetes", ...)` are now backed by ZenML's one-shot stack provisioning flow: Kitaru validates provider-specific credentials, preflights the connector config, creates the cloud connector plus Kubernetes/orchestrator, artifact-store, and container-registry components transactionally, and returns the richer stack-create metadata (including service connectors and cloud resources) through both surfaces
- `kitaru stack create --type vertex` and MCP `manage_stack(action="create", stack_type="vertex", ...)` now ship the first cloud-managed runner flow beyond Kubernetes: Kitaru provisions a GCP connector plus Vertex orchestrator, GCS artifact store, and GCP container registry components transactionally and returns the richer stack-create metadata through both surfaces
- `kitaru stack create --type sagemaker` and MCP `manage_stack(action="create", stack_type="sagemaker", ...)` now provision an AWS connector plus SageMaker orchestrator, S3 artifact store, and ECR container registry transactionally; `kitaru stack show` / structured stack inspection now classify SageMaker stacks explicitly and surface the runner `execution_role`
- `kitaru stack create --type azureml` and MCP `manage_stack(action="create", stack_type="azureml", ...)` now provision an Azure connector plus AzureML orchestrator, Azure artifact store, and Azure container registry transactionally; `kitaru stack show` / structured stack inspection now classify AzureML stacks explicitly and surface the runner subscription, resource group, workspace, and location
- `kitaru stack create` now accepts `--file/-f` YAML input, letting stack definitions come from a config file while keeping explicit CLI flags authoritative when both are provided
- Stack creation internals now share one CLI/MCP validation layer across local, Kubernetes, Vertex, SageMaker, and AzureML flows, and `kitaru stack show` / structured stack inspection now classify managed-runner stacks explicitly and surface runner-specific metadata (`location` for Vertex, `execution_role` for SageMaker, and subscription/resource-group/workspace details for AzureML)
- `kitaru stack create` and MCP `manage_stack(action="create", ...)` now support advanced component defaults via repeatable `--extra` / structured `extra`, plus the convenience `--async` / `async_mode` flag for remote orchestrators; invalid advanced ZenML options are now rewritten into clear user-facing `KitaruUsageError` messages with suggestions and docs links when available
- Flow submissions now serialize temporary stack rebinding within a Python process, making per-run/decorator/runtime stack overrides safer when multiple executions are submitted concurrently
- Model aliases registered via `kitaru model register` are now automatically transported to submitted and replayed remote executions via `KITARU_MODEL_REGISTRY`; `kitaru.llm()` and `kitaru model list` now read the effective registry visible in the current environment, and frozen execution specs capture that transported snapshot for debugging
- `kitaru stack delete --recursive` now gives Kubernetes-managed stacks full cleanup parity by reporting container-registry deletion alongside the orchestrator and artifact store and by garbage-collecting unshared linked service connectors after a successful delete
- Examples are now grouped into topic-focused subdirectories under `examples/`, each with its own README, and can be run with `uv run examples/<path>.py`; the root README, docs site, and tester guide now point to a unified examples catalog
- Kitaru now treats `KITARU_*` environment variables as the public configuration surface for remote connection/bootstrap, translating the supported connection/debug vars into `ZENML_*` env vars before CLI/SDK startup
- Connection resolution now understands direct `ZENML_*` env vars as a compatibility layer below `KITARU_*`, while env-driven remote connections fail at first use unless an explicit project is set
- `kitaru status` now includes an Environment section showing active `KITARU_*` variables with token/API-key masking
- `kitaru login` now starts and connects to a local daemon server when you omit `SERVER`; remote login remains `kitaru login <server>`
- `kitaru login` CLI flags now distinguish local and remote modes: removed `--url` and `--cloud-api-url` / `--pro-api-url`, added local `--port`, and made `--timeout` shared across local startup and remote connection flows
- Local login now warns — instead of failing — when `KITARU_*` / `ZENML_*` auth environment overrides are active; remote login and `kitaru logout` still refuse to fight those environment variables
- `kitaru logout --output json` now includes `local_server_stopped`, and logout now also tears down any registered local daemon while disconnecting from remote state
- Kitaru now supports `KITARU_CONFIG_PATH` for relocating its config directory and `KITARU_DEFAULT_MODEL` for setting the default `kitaru.llm()` model without touching the alias registry
- The production Docker image now uses `KITARU_DEBUG` / `KITARU_ANALYTICS_OPT_IN` defaults and documents `KITARU_SERVER_URL` / `KITARU_AUTH_TOKEN` / `KITARU_PROJECT` for headless server connection setup
- `kitaru status` and `kitaru log-store show` now surface a mismatch warning when the Kitaru log-store preference differs from the active stack's ZenML stack log store
- Kitaru's global config file now lives in Kitaru's OS-aware app config directory (for example `~/.config/kitaru/config.yaml` on Linux or `~/Library/Application Support/kitaru/config.yaml` on macOS)
- CLI output (`kitaru status`, `kitaru info`) no longer exposes ZenML config paths or local stores path
- Project is no longer inferred from ZenML's active project; `ResolvedConnectionConfig.project` only reflects explicit overrides via `KITARU_PROJECT` env var or `kitaru.configure(project=...)`
- `kitaru info` shows "Project override" row only when an explicit override is set (instead of always showing "Active project")
- `kitaru` and `kitaru-mcp` now fail fast with a clear message on Python versions older than 3.11
- CLI and MCP startup no longer resolve the Kitaru package version eagerly at import time; missing metadata now falls back to `unknown`
- `kitaru login` no longer prints "Active project" in its success output
- `kitaru.configure()` now accepts a `project` parameter for internal/testing use

### Added
- Local stack lifecycle support across SDK, CLI, and MCP: `kitaru.create_stack()`, `kitaru.delete_stack()`, `kitaru stack create/delete`, and MCP `manage_stack`
- New local-stack semantics: `kitaru stack create <name>` auto-activates by default, `--no-activate` leaves the current stack unchanged, and forced active-stack deletion falls back to the default stack
- `kitaru stack show <name-or-id>` for inspecting one stack in Kitaru vocabulary, including translated runner/storage/image-registry component details in both text and JSON output
- Runtime log retrieval with Rich-based checkpoint-by-checkpoint progress display for execution inspection
- Runtime log retrieval lane: `KitaruClient.executions.logs(...)`, `kitaru executions logs` (with `--follow`, `--grouped`, `-v`/`-vv`, and JSONL output), and MCP `get_execution_logs`
- Runtime log retrieval docs updates across logging/log-store guides plus a new getting-started page for execution logs
- Production Docker image (`docker/Dockerfile`): multi-stage server image based on ZenML server architecture with all cloud plugins, published as `zenmldocker/kitaru` during releases
- Docker image build and push integrated into the release workflow (`release.yml`)
- `.dockerignore` to keep Docker build context clean
- Justfile recipes: `just server-image` and `just server-image-push` for local Docker builds
- Phase 16 replay support: replay planning (`src/kitaru/replay.py`), `KitaruClient.executions.replay(...)`, flow-object replay (`my_flow.replay(...)`), `kitaru executions replay`, and fully-enabled MCP replay tool responses
- Replay docs and examples: `/getting-started/replay-and-overrides`, updated execution/error/MCP docs, and `examples/features/replay/replay_with_overrides.py`
- Agent-native MCP server surface: optional `kitaru[mcp]` extra, `kitaru-mcp` console entry point, and Phase 19 MCP tools for execution/artifact/status/stack queries
- Claude Code authoring skill: `.claude-plugin/skills/kitaru-authoring/SKILL.md` (installable via plugin marketplace)
- Phase 19 example workflow: `examples/features/mcp/mcp_query_tools.py`
- MCP-focused tests: import guard coverage (`tests/test_mcp_import_guard.py`) and tool wrapper tests (`tests/mcp/test_server.py`)
- Agent integrations docs pages: `/agent-integrations/mcp-server` and `/agent-integrations/claude-code-skill`
- PydanticAI framework adapter: `kitaru.adapters.pydantic_ai.wrap(agent)` for checkpoint-scoped child-event tracking of model/tool activity
- Adapter capture policy controls: `tool_capture_config` + `tool_capture_config_by_name` with `full`, `metadata_only`, and `off` modes
- Adapter run-summary metadata (`pydantic_ai_run_summaries`) and event-stream-handler metadata (`pydantic_ai_event_stream_handlers`)
- Adapter stream transcript artifacts (`*_stream_transcript`) for streaming replay inspection
- Adapter HITL tool decorator: `kitaru.adapters.pydantic_ai.hitl_tool(...)` with flow-level wait translation
- Optional dependency extra: `pydantic-ai` (`pydantic-ai-slim`)
- Phase 17 runnable example: `examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py`
- Phase 17 integration/unit tests for adapter tracking, runtime scope suspension, HITL behavior, capture config, stream transcripts, and synthetic flow-scope run semantics
- Getting Started docs page for the PydanticAI adapter (`/getting-started/pydantic-ai-adapter`)
- Typed Kitaru exception hierarchy (`KitaruError`, `KitaruContextError`, `KitaruStateError`, `KitaruExecutionError`, `KitaruUserCodeError`, `KitaruDivergenceError`, `KitaruFeatureNotAvailableError`, and related types)
- Failure journaling in `KitaruClient`: structured execution-level failure details (`execution.failure`) and per-checkpoint retry attempt history (`checkpoint.attempts`)
- Phase 14 execution CLI commands: `kitaru executions get/list/retry/cancel`
- Getting Started error-handling docs page (`/getting-started/error-handling`)
- `kitaru.llm()` implementation with LiteLLM backend, context-aware flow/checkpoint behavior, prompt/response artifact capture, and automatic usage/cost/latency metadata logging
- Local model alias registry persisted in Kitaru's user config file, including default alias behavior and model-resolution helpers for `kitaru.llm()`
- Model registry CLI surface: `kitaru model register` and `kitaru model list`
- Phase 12 example workflow: `examples/features/llm/flow_with_llm.py`
- Getting Started LLM docs page (`/getting-started/llm-calls`)
- Secrets CLI surface: `kitaru secrets set/show/list/delete`
- `kitaru secrets set` create-or-update behavior with private-by-default secret creation
- Secret assignment parsing with env-var-style key validation (`--KEY=value`)
- `KitaruClient` execution management API with Kitaru domain models (`Execution`, `ExecutionStatus`, `CheckpointCall`, `ArtifactRef`)
- Execution management operations: `client.executions.get/list/latest/cancel/retry`
- Artifact browsing operations: `client.artifacts.list/get` and `artifact.load()`
- Phase 11 example workflow: `examples/features/execution_management/client_execution_management.py`
- Getting Started execution management docs page (`/getting-started/execution-management`)
- `kitaru.wait(...)` implementation with flow-only guardrails and checkpoint-context blocking
- Wait-input lifecycle APIs: `client.executions.input(...)` and `client.executions.resume(...)`
- Execution CLI wait/resume commands: `kitaru executions input` and `kitaru executions resume`
- Phase 15 wait/resume example workflow: `examples/features/execution_management/wait_and_resume.py`
- Getting Started wait/resume docs page (`/getting-started/wait-and-resume`)
- `kitaru.save()` for explicit named artifact persistence inside checkpoints
- `kitaru.load()` for cross-execution artifact loading inside checkpoints
- Artifact taxonomy validation for explicit `kitaru.save(..., type=...)` values (`prompt`, `response`, `context`, `input`, `output`, `blob`)
- Phase 8 example workflow: `examples/features/basic_flow/flow_with_artifacts.py`
- Global log-store configuration with `kitaru log-store set/show/reset`
- Active stack selection in SDK via `kitaru.list_stacks()`, `kitaru.current_stack()`, and `kitaru.use_stack()`
- Active stack CLI commands: `kitaru stack list/current/use`
- Runtime configuration API: `kitaru.configure(...)`
- Unified config models: `kitaru.KitaruConfig` and `kitaru.ImageSettings`
- Execution config precedence resolution across invocation/decorator/runtime/env/project/global/default layers
- Frozen execution spec persistence on each flow run (`kitaru_execution_spec` metadata)
- Phase 10 example workflow: `examples/features/basic_flow/flow_with_configuration.py`
- Getting Started configuration docs page (`/getting-started/configuration`)
- Persisted Kitaru user config (`config.yaml`) for log-store override state
- Environment override support for runtime log-store resolution

### Changed
- Runtime internals now include `_suspend_checkpoint_scope()` to support adapter-managed flow-level waits during checkpoint-local agent execution
- PydanticAI adapter event metadata now includes timing (`duration_ms`), explicit ordering/lineage fields (`sequence_index`, `turn_index`, `fan_out_from`, `fan_in_from`), and immutable wrapper dispatch semantics across function/MCP/generic toolsets
- Wrapped PydanticAI `run()` / `run_sync()` calls at flow scope now use a synthetic `llm_call` checkpoint boundary so adapter tracking remains available outside explicit checkpoints
- Kitaru global config persistence now uses field-preserving updates, so log-store and model-registry settings no longer clobber each other
- Updated README, CLAUDE guide, AGENTS guide, and docs pages to reflect shipped LLM/model-registry functionality and current implemented primitive status
- Updated the CLI/docs surface so generated command reference pages show real positional usage, `executions logs`/`executions replay` appear everywhere they should, and runtime logs are documented separately from structured metadata
- Agent-facing CLI commands now support a consistent `--output json` / `-o json` contract, with single-item commands emitting `{command, item}`, list commands emitting `{command, items, count}`, and structured JSON errors on stderr
- `kitaru executions logs --output json` now returns a JSON envelope for non-follow mode, while `--follow --output json` emits JSONL event objects (`log`, `waiting`, `terminal`, `interrupted`)
- Added a dedicated secrets + model registration walkthrough and clarified the current secret story: `kitaru.llm()` auto-resolves linked secrets, while non-LLM secret access remains a low-level pattern
- Updated quickstart, docs, and README wording to reflect shipped replay/log/MCP behavior, typed errors, and current Claude Code skill packaging

## [0.1.0] - 2026-03-06

### Added
- Initial project scaffolding with uv, ruff, ty, and CI
- CLI with cyclopts (`kitaru --version`, `kitaru --help`)
- Justfile for common development commands
- Link checking with lychee
- Typo checking with typos
