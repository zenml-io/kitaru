# Server

The server design: every endpoint, API model, domain model, and ORM model, with their connections.

## Layer map

A request flows through four layers:

```
routers (adapters/rest/routers)          FastAPI handlers, api_models in and out
  └─ mapping (adapters/rest/mapping)     api_models ↔ domain models, filters, and commands
      └─ services (application/services) orchestration, invariants, transactions
          └─ repositories (adapters/db/repositories)  domain ↔ ORM models
              └─ orm (adapters/db/orm)                SQLAlchemy tables
```

- `api_models/` is the wire contract shared by server and client. Neither imports the other, both import `api_models`.
- `application/models/` holds `FrozenModel` filter and command objects that services accept.
- `application/interfaces/` holds the repository protocols the services depend on.
- `domain/` holds the entities, value objects, enums, and domain errors.
- Exception mapping is global in `app.py`: `ForbiddenError` 403, `NotFoundError` 404, `ConflictError` 409, `PayloadTooLargeError` 413, `ValidationError` 422, `QueryTimeoutError` 503, `DomainError` 500, body always `{"detail": str}`.
- Auth is one dependency, `authorize`, yielding `AuthContext(account, csrf_token)`. Credentials are an API key (`KITKEY_` prefix) or a JWT, from bearer header or cookie. Health, login/logout, and account activation routes skip it. There is no separate worker auth.
- Ownership is provenance, not authorization: the server is a trusted-team deployment, `owner_id` records who created a resource, and no service filters or rejects by owner. Every authenticated account reads and writes every resource. The exceptions are `account.metadata` and `account.password`, which the accounts router rejects with a 403 unless the path id is the caller's own account.
- Pagination is uniform: `cursor` (opaque, from the previous response), `size` (ge=1, le=1000, default 20), and `sort` (`created:asc` or `created:desc`, default `created:desc`) on the `ListParams` base model, response `Page[T]` with `items` and `next_cursor` (null on the last page). The keyset rides the UUIDv7 id. Cursors embed the sort and a hash of the filter fields, changing either mid-pagination is a 422, changing `size` is allowed. Sortable fields are an allowlist per filter model (`sortable_fields` ClassVar, default `created`), a field beyond that needs a `(field, id)` composite index.
- Filtering is a `filter` query parameter holding a JSON-encoded expression tree: `and`/`or`/`not` nodes over `{field, op, value}` conditions with ops `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, `is_null`, `startswith`, `endswith`, `contains`. `eq`/`ne` reject a null value, null checks are `is_null` (negated via `not`). Filterable fields are an allowlist per filter model (`filterable_fields` ClassVar mapping field name to value type and allowed ops), the expression is capped at 5 nesting levels, 30 conditions, and 100 `in` items, and a violation is a 422. The expression is hashed into the cursor with the other filter fields and compiles to SQL through `adapters/db/filtering.py` against a per-repository binding mapping whose values are columns or predicate factories (the tag filters on sessions, cohorts, experiments, and experiment runs compile to EXISTS probes this way). The tree replaced the flat per-field filter params, including the probe-style session filters (`tag`, `cohort_version_id`, `has_evaluation`), which bind to EXISTS predicate factories. List queries run under a transaction-local statement timeout applied in `paginate()`, configured through `KITARU_SERVER_LIST_QUERY_TIMEOUT_SECONDS` (default 10, 0 disables it), and a timed-out list query is a 503. Internal-only filter dimensions (`owner_id`, `internal`, `stale_before`, `seen_after`, `job_ids`, `account_id`, path-scoped ids) stay flat model fields set by services.
- Commits happen before the response leaves: routers register through a custom `APIRoute` subclass that commits the request session after the handler returns and before the response is sent, so a 2xx means the write is committed and an immediate follow-up read sees it. An exception skips the commit and the session rolls back on close.
- PATCH semantics are uniform: an omitted field is unchanged, an explicitly null field clears, a 422 where clearing is invalid (e.g. `status`). Mapping functions build update commands from the request's `model_fields_set` only, and commands preserve their own `model_fields_set`, so services distinguish omitted from null without per-resource conventions. A dict-valued field (`metadata`, `values`, `env`) is replaced whole, never merged key by key, so a PATCH carrying a dict always states the final map. Key-level merging is a dedicated endpoint where it is wanted, `POST /v1/sessions/{id}/evaluations` being the one case.
- Paginated list endpoints take an `XListParams(ListParams)` query model from `api_models`, bound in routers via `Annotated[XListParams, Query()]` and converted to the application filter by `<x>_list_params_to_filter` in the mapping module. Lists without resource-specific params take `ListParams` directly.
- Client list methods take the same params model, defaulting to a fresh instance, and send `model_dump(mode="json", exclude_unset=True)`. No hand-rolled `if x is not None` param dicts in resources. Each resource also exposes `iter()` next to `list()`, an async generator following `next_cursor` to exhaustion. Endpoints nested under a resource path live on the parent resource with the path parameter as the first argument (`/v1/sessions/{id}/nodes` → `client.sessions.list_nodes`, `iter_nodes`, `ingest_nodes`), there is no separate client resource for them.
- The client is constructed directly or via `KitaruAPIClient.from_env()` (`KITARU_API_URL`, `KITARU_API_KEY`, a missing URL is a `RuntimeError`). Requests go through a retrying transport: transport errors and 408/429/502/503/504 retry with backoff, a request with a streaming body is sent exactly once, and every request carries an `Idempotency-Key` header held stable across attempts. The server does not dedup on it yet, see future_improvements.md.

## Endpoints

`/v1/imports` and `/v1/session-runs` are POST-only command endpoints creating jobs, they have no GET. `POST /v1/evaluations` creates jobs too, while its GETs read stored evaluation rows.

### health (`/health`, unauthenticated)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| GET | /health | - | `dict[str, str]`, 503 on DB probe failure | raw `SELECT 1` |
| GET | /health/live | - | `dict[str, str]` | - |

### auth (`/v1`, unauthenticated)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/login | form `username`, `password` | `TokenResponse`, sets auth cookie | `AuthService.login_with_password` |
| POST | /v1/logout | - | 204, deletes cookie | - |

### accounts (`/v1/accounts`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/accounts | `AccountCreateRequest` | `AccountResponse` or `AccountActivationTokenResponse` 201 | `AccountService.create_account` |
| GET | /v1/accounts | query filter | `Page[AccountResponse]` | `AccountService.list_accounts` |
| GET | /v1/accounts/{id} | - | `AccountResponse` | `AccountService.get_account` |
| PATCH | /v1/accounts/{id} | `AccountUpdateRequest` | `AccountResponse` | `AccountService.update_account` |
| POST | /v1/accounts/{id}/activate | `AccountActivateRequest` | `AccountResponse` | `AccountService.activate_account` |
| POST | /v1/accounts/{id}/deactivate | - | `AccountActivationTokenResponse` | `AccountService.deactivate_account` |

No DELETE for accounts. Active state is not writable through PATCH: an account leaves the active set through `POST /v1/accounts/{id}/deactivate` and returns through `POST /v1/accounts/{id}/activate`.

### agents (`/v1/agents`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/agents | `AgentCreateRequest` | `AgentResponse` 201 | `AgentService.create_agent` |
| GET | /v1/agents | query filter | `Page[AgentResponse]` | `AgentService.list_agents` |
| GET | /v1/agents/{id} | - | `AgentResponse` | `AgentService.get_agent` |
| PATCH | /v1/agents/{id} | `AgentUpdateRequest` | `AgentResponse` | `AgentService.update_agent` |
| DELETE | /v1/agents/{id} | - | 204 | `AgentService.delete_agent` |
| POST | /v1/agents/{id}/versions | `AgentVersionCreateRequest` | `AgentVersionResponse` 201 | `AgentVersionService.create_version` |
| GET | /v1/agents/{id}/versions | - | `Page[AgentVersionResponse]` | `AgentVersionService.list_versions` |

### agent-versions (`/v1/agent-versions`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| GET | /v1/agent-versions/{id} | - | `AgentVersionResponse` | `AgentVersionService.get_version` |
| PATCH | /v1/agent-versions/{id} | `AgentVersionUpdateRequest` | `AgentVersionResponse` | `AgentVersionService.update_version` |
| DELETE | /v1/agent-versions/{id} | - | 204 | `AgentVersionService.delete_version` |

### api-keys (`/v1/api-keys`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/api-keys | `ApiKeyCreateRequest` | `ApiKeyIssuedResponse` 201 | `ApiKeyService.create_api_key` |
| GET | /v1/api-keys | query filter | `Page[ApiKeyResponse]` | `ApiKeyService.list_api_keys` |
| GET | /v1/api-keys/{id} | - | `ApiKeyResponse` | `ApiKeyService.get_api_key` |
| PATCH | /v1/api-keys/{id} | `ApiKeyUpdateRequest` | `ApiKeyResponse` | `ApiKeyService.update_api_key` |
| DELETE | /v1/api-keys/{id} | - | 204 | `ApiKeyService.delete_api_key` |

### blobs (`/v1/blobs`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/blobs | multipart file | `BlobResponse`, 201 or 200 on dedup hit | `BlobService.upload_blob` |
| GET | /v1/blobs/{id} | - | `BlobResponse` | `BlobService.get_blob` |
| GET | /v1/blobs/{id}/content | - | raw bytes with the blob media type | `BlobService.download_blob` |
| DELETE | /v1/blobs/{id} | - | 204 | `BlobService.delete_blob` |

Uploads are capped by a server setting (max blob size, default 100 MiB), a larger file is a 413. The cap must stay at or below the worker's payload cache budget (`PAYLOAD_CACHE_MAX_BYTES`, see worker.md), otherwise a payload exists that no worker can ever cache. Deleting a blob restricts on the plugin versions and jobs referencing it, surfacing as a `BlobInUse` conflict, so only unreferenced blobs delete. The size check and the sha256 run streaming from the spooled upload before any materialization: an oversized upload is rejected at the cap holding one chunk, a dedup hit returns the stored row without loading the content, and only a new blob is materialized in memory, once, for the insert (bytea parameters cannot be streamed). Memory per upload is bounded by the cap. Dedup is race-safe: the create catches the sha256 unique violation from a concurrent identical upload and returns the stored row with a 200. Content downloads carry `Content-Disposition: attachment` and `X-Content-Type-Options: nosniff`, so the client-supplied media type is never rendered inline.

### cohorts (`/v1/cohorts`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/cohorts | `CohortCreateRequest` | `CohortResponse` 201 | `CohortService.create_cohort` |
| GET | /v1/cohorts | query filter | `Page[CohortResponse]` | `CohortService.list_cohorts` |
| GET | /v1/cohorts/{id} | - | `CohortResponse` | `CohortService.get_cohort` |
| PATCH | /v1/cohorts/{id} | `CohortUpdateRequest` | `CohortResponse` | `CohortService.update_cohort` |
| DELETE | /v1/cohorts/{id} | - | 204 | `CohortService.delete_cohort` |
| POST | /v1/cohorts/{id}/versions | `CohortVersionCreateRequest` | `CohortVersionResponse` 201 | `CohortVersionService.create_version` |
| GET | /v1/cohorts/{id}/versions | - | `Page[CohortVersionResponse]` | `CohortVersionService.list_versions` |

A cohort is a namespace, membership lives on its versions. A version's member list is immutable: changing membership means creating a new version, whose list is the latest version's list minus `remove_session_ids` plus `add_session_ids` appended. Removing a session not in the base list or adding one already present is a 422, and the first version starts from an empty list. Members are listed through the `cohort_version_id` session filter.

### cohort-versions (`/v1/cohort-versions`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| GET | /v1/cohort-versions/{id} | - | `CohortVersionResponse` | `CohortVersionService.get_version` |
| PATCH | /v1/cohort-versions/{id} | `CohortVersionUpdateRequest` | `CohortVersionResponse` | `CohortVersionService.update_version` |
| DELETE | /v1/cohort-versions/{id} | - | 204 | `CohortVersionService.delete_version` |

### evaluations (`/v1/evaluations`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/evaluations | `EvaluationBatchCreateRequest` | `JobResponse` 201 | `JobService.create_evaluations` |
| GET | /v1/evaluations | query filter | `Page[EvaluationResponse]` | `EvaluationService.list_evaluations` |
| GET | /v1/evaluations/{id} | - | `EvaluationResponse` | `EvaluationService.get_evaluation` |

The POST is the evaluation command: it creates one job holding one evaluator task per (input session, evaluator) pair and returns the job. The tasks carry `on_failure=continue`, so one failed scoring never cancels the rest, the job settles failed only after every pair has run. Creation is atomic, an unknown session id fails the whole request. The pair count per request is capped by a server setting (default 100), a larger request is a 422. The GETs read stored evaluation rows. Rows are written by the server when an evaluator task completes and by `POST /v1/sessions/{id}/evaluations`, never created directly here.

### evaluators (`/v1/evaluators`, `PluginService` bound to `PluginKind.EVALUATOR`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/evaluators | `EvaluatorCreateRequest` | `EvaluatorResponse` 201 | `PluginService.create_plugin` |
| GET | /v1/evaluators | query filter | `Page[EvaluatorResponse]` | `PluginService.list_plugins` |
| GET | /v1/evaluators/{id} | - | `EvaluatorResponse` | `PluginService.get_plugin` |
| PATCH | /v1/evaluators/{id} | `EvaluatorUpdateRequest` | `EvaluatorResponse` | `PluginService.update_plugin` |
| DELETE | /v1/evaluators/{id} | - | 204 | `PluginService.delete_plugin` |
| POST | /v1/evaluators/{id}/versions | `EvaluatorVersionCreateRequest` | `EvaluatorVersionResponse` 201 | `PluginService.create_version` |
| GET | /v1/evaluators/{id}/versions | - | `Page[EvaluatorVersionResponse]` | `PluginService.list_versions` |
| GET | /v1/evaluators/{id}/versions/{version} | - | `EvaluatorVersionResponse` | `PluginService.get_version` |
| PATCH | /v1/evaluators/{id}/versions/{version} | `EvaluatorVersionUpdateRequest` | `EvaluatorVersionResponse` | `PluginService.update_version` |

The importer and evaluator routers stay two thin declarative files (paths, tags, response models, status-code docstrings), but every handler body is a one-liner into shared kind-parametrized functions. The two mapping modules are one `mapping/plugins.py` whose functions take the target response class as a parameter, since the field-for-field mapping is identical modulo that class. The one semantic difference, importers carrying `provider` while evaluators have none, lives in that shared mapping keyed off the request type, not in branches. No router factory: FastAPI reads static type annotations for request bodies and response models, so the declarations stay per-resource and only the orchestration is shared.

### experiments (`/v1/experiments`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/experiments | `ExperimentCreateRequest` | `ExperimentResponse` 201 | `ExperimentService.create_experiment` |
| GET | /v1/experiments | query filter | `Page[ExperimentResponse]` | `ExperimentService.list_experiments` |
| GET | /v1/experiments/{id} | - | `ExperimentResponse` | `ExperimentService.get_experiment` |
| PATCH | /v1/experiments/{id} | `ExperimentUpdateRequest` | `ExperimentResponse` | `ExperimentService.update_experiment` |
| DELETE | /v1/experiments/{id} | - | 204 | `ExperimentService.delete_experiment` |
| POST | /v1/experiments/{id}/runs | `ExperimentRunCreateRequest` | `ExperimentRunResponse` 201 | `ExperimentService.start_run` |

### experiment-runs (`/v1/experiment-runs`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| GET | /v1/experiment-runs | query filter | `Page[ExperimentRunResponse]` | `ExperimentRunService.list_runs` |
| GET | /v1/experiment-runs/{id} | - | `ExperimentRunResponse` | `ExperimentRunService.get_run` |
| DELETE | /v1/experiment-runs/{id} | - | 204 | `ExperimentRunService.delete_run` |
| GET | /v1/experiment-runs/{id}/jobs | query filter | `Page[JobResponse]` | `ExperimentRunService.list_run_jobs` |
| POST | /v1/experiment-runs/{id}/cancel | - | `ExperimentRunResponse` | `ExperimentRunService.cancel_run` |

### importers (`/v1/importers`, `PluginService` bound to `PluginKind.IMPORTER`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/importers | `ImporterCreateRequest` | `ImporterResponse` 201 | `PluginService.create_plugin` |
| GET | /v1/importers | query filter | `Page[ImporterResponse]` | `PluginService.list_plugins` |
| GET | /v1/importers/{id} | - | `ImporterResponse` | `PluginService.get_plugin` |
| PATCH | /v1/importers/{id} | `ImporterUpdateRequest` | `ImporterResponse` | `PluginService.update_plugin` |
| DELETE | /v1/importers/{id} | - | 204 | `PluginService.delete_plugin` |
| POST | /v1/importers/{id}/versions | `ImporterVersionCreateRequest` | `ImporterVersionResponse` 201 | `PluginService.create_version` |
| GET | /v1/importers/{id}/versions | - | `Page[ImporterVersionResponse]` | `PluginService.list_versions` |
| GET | /v1/importers/{id}/versions/{version} | - | `ImporterVersionResponse` | `PluginService.get_version` |
| PATCH | /v1/importers/{id}/versions/{version} | `ImporterVersionUpdateRequest` | `ImporterVersionResponse` | `PluginService.update_version` |

### imports (`/v1/imports`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/imports | `ImportCreateRequest` | `JobResponse` 201 | `JobService.create_import` |

### jobs (`/v1/jobs`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| GET | /v1/jobs | query filter | `Page[JobResponse]` | `JobService.list_jobs` |
| GET | /v1/jobs/{id} | - | `JobResponse` | `JobService.get_job` |
| GET | /v1/jobs/{id}/tasks | query filter | `Page[TaskResponse]` | `JobService.list_job_tasks` |
| POST | /v1/jobs/{id}/cancel | - | `JobResponse` | `JobService.cancel_job` |
| DELETE | /v1/jobs/{id} | - | 204 | `JobService.delete_job` |

There is no job POST and no status field on any job write: jobs are created by the command endpoints (`/v1/replays`, `/v1/evaluations`, `/v1/imports`, `/v1/session-runs`, run fan-out), and job status is written only by the server's settlement logic reacting to task transitions. `POST /v1/jobs/{id}/cancel` is the user surface: it stamps `cancel_requested_at` on the job, moves the job's pending tasks to `canceled`, and stamps `cancel_requested_at` on its claimed and running tasks, leaving their terminal writes to the worker or the sweep. Deleting a job cascades its tasks and its replay row.

### tasks (`/v1/tasks`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| GET | /v1/tasks | query filter | `Page[TaskResponse]` | `TaskService.list_tasks` |
| POST | /v1/tasks/claim | `TaskClaimRequest` | `TaskClaimResponse` | `TaskService.claim_tasks` |
| GET | /v1/tasks/{id} | - | `TaskResponse` | `TaskService.get_task` |
| GET | /v1/tasks/{id}/spec | - | `TaskSpecResponse` | `TaskService.get_spec` |
| PATCH | /v1/tasks/{id} | `TaskUpdateRequest` | `TaskResponse` | `TaskService.update_task` |

`PATCH /v1/tasks/{id}` is the executor surface and every transition on it is fenced by the claim's attempt. There is no task creation endpoint: tasks are created server-side only, by the command services and the replay pipeline, and appending a task to a settled job is a conflict. There is no user-facing task write either, cancellation is job-level.

### replays (`/v1/replays`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/replays | `ReplayCreateRequest` | `ReplayResponse` 201 | `ReplayService.create_replay` |
| GET | /v1/replays | query filter | `Page[ReplayResponse]` | `ReplayService.list_replays` |
| GET | /v1/replays/{id} | - | `ReplayResponse` | `ReplayService.get_replay` |
| POST | /v1/replays/{id}/tool-lookup | `ToolLookupRequest` | `ToolLookupResponse` | `ReplayService.tool_lookup` |

### secrets (`/v1/secrets`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/secrets | `SecretCreateRequest` | `SecretResponse` 201 | `SecretService.create_secret` |
| GET | /v1/secrets | query filter | `Page[SecretResponse]` | `SecretService.list_secrets` |
| GET | /v1/secrets/{id} | query include_values | `SecretResponse` or `SecretWithValuesResponse` | `SecretService.get_secret` |
| PATCH | /v1/secrets/{id} | `SecretUpdateRequest` | `SecretResponse` | `SecretService.update_secret` |
| DELETE | /v1/secrets/{id} | - | 204 | `SecretService.delete_secret` |

### session-runs (`/v1/session-runs`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/session-runs | `SessionRunCreateRequest` | `JobResponse` 201 | `JobService.create_session_run` |

### sessions (`/v1/sessions`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/sessions | `SessionCreateRequest` | `SessionResponse` 201 | `SessionService.create_session` |
| GET | /v1/sessions | query filter | `Page[SessionResponse]` | `SessionService.list_sessions` |
| GET | /v1/sessions/{id} | - | `SessionResponse` | `SessionService.get_session` |
| PATCH | /v1/sessions/{id} | `SessionUpdateRequest` | `SessionResponse` | `SessionService.update_session` |
| DELETE | /v1/sessions/{id} | - | 204 | `SessionService.delete_session` |
| POST | /v1/sessions/{id}/nodes | `SessionNodeBatchRequest` | `list[SessionNodeResponse]` | `SessionNodeService.ingest_nodes` |
| GET | /v1/sessions/{id}/nodes | query include_payloads, cursor, size | `Page[SessionNodeResponse]`, ordered by index | `SessionNodeService.list_nodes` |
| POST | /v1/sessions/{id}/evaluations | `SessionEvaluationsRequest` | `list[EvaluationResponse]` | `EvaluationService.merge_evaluations` |

### tags (`/v1/tags`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/tags | `TagCreateRequest` | `TagResponse` 201 | `TagService.create_tag` |
| GET | /v1/tags | query filter | `Page[TagResponse]` | `TagService.list_tags` |
| PATCH | /v1/tags/{id} | `TagUpdateRequest` | `TagResponse` | `TagService.update_tag` |
| DELETE | /v1/tags/{id} | - | 204 | `TagService.delete_tag` |
| POST | /v1/tags/{id}/links | `TagLinkCreateRequest` | `TagLinkResponse` 201 | `TagService.create_tag_link` |
| DELETE | /v1/tags/{id}/links/{resource_type}/{resource_id} | - | 204 | `TagService.delete_tag_link` |

### workers (`/v1/workers`)

| Method | Path | Request | Response | Service |
|---|---|---|---|---|
| POST | /v1/workers | `WorkerCreateRequest` | `WorkerResponse` 200, upsert by name (atomic `INSERT ... ON CONFLICT (name) DO UPDATE`, a concurrent delete cannot race a fallback lookup) | `WorkerService.register_worker` |
| GET | /v1/workers | query filter | `Page[WorkerResponse]` | `WorkerService.list_workers` |
| GET | /v1/workers/{id} | - | `WorkerResponse` | `WorkerService.get_worker` |
| POST | /v1/workers/{id}/heartbeat | `WorkerHeartbeatRequest` | `WorkerHeartbeatResponse` | `TaskService.heartbeat_worker` |
| DELETE | /v1/workers/{id} | - | 204 | `WorkerService.delete_worker` |

## API models (`api_models/v1/`)

Conventions: requests extend `RequestModel` (`extra="forbid"`), responses extend `ResponseModel`, list responses use `Page[XResponse]`, paginated list query params extend `ListParams`, request datetimes are `AwareDatetime`. The bases set `protected_namespaces=()` so fields like `model_params` pass pydantic's `model_` namespace check.

Modules are named after the entity in the singular (`account.py`, `job.py`), matching `domain/`, `orm/`, and `application/models/`. Routers and client resources are instead named after the URL segment they serve (`/v1/accounts` → `accounts.py`), and mapping modules follow their router. `imports.py` keeps the plural since `import` is a reserved word.

### base.py

- `RequestModel(BaseModel)`, `ResponseModel(BaseModel)`, `ErrorBody(detail: str)`
- `ListParams(RequestModel)`: `cursor`, `size` (ge=1, le=1000, default 20), `sort` (`field:asc` or `field:desc`, default `created:desc`), the base for `XListParams` query models
- `DiscriminatedRequestModel(RequestModel)`: base for discriminated union members, its `model_post_init` marks the `type` discriminator as set so `exclude_unset` dumps keep it
- `TimestampedResponseModel(ResponseModel)`: `created`, `updated`. `OwnedResponseModel(TimestampedResponseModel)`: adds `owner_id`. Response models carrying these fields extend the mixins instead of redeclaring the fields.
- `Page(ResponseModel, Generic[ItemT])`: `items: list[ItemT]`, `next_cursor: str | None` (null on the last page)
- Aliases: `FiniteFloat` (float, no inf/nan), `JsonValue` (Any, recursively finite), `PlainSerializedSecretStr` (SecretStr dumped as plaintext in JSON mode)

### Enums

Enum member names are uppercase, the table lists the wire values.

| Enum | Values | Defined in |
|---|---|---|
| `EvaluationDataType` | float, bool, str, categorical | evaluation.py |
| `ExperimentRunStatus` | running, canceling, completed, failed, canceled | experiment_run.py |
| `JobStatus` | pending, running, completed, failed, canceled | job.py |
| `TaskKind` | agent, evaluator, importer | task.py |
| `TaskOnFailure` | abort, continue, ignore | task.py |
| `TaskStatus` | pending, claimed, running, completed, failed, timed_out, canceled, abandoned | task.py |
| `HistoryScope` | baseline, cohort_version, agent | replay_config.py |
| `ReplayStatus` | pending, evaluating, completed, failed, canceled | replay.py |
| `ToolPolicyOnMiss` | fail, passthrough, error_result | replay_config.py |
| `StaticMatchMode` | exact, subset | replay_config.py |
| `SessionOrigin` | imported, recorded, replay | session.py |
| `SessionStatus` | in_progress, completed, failed | session.py |
| `NodeType` | llm_call, tool_call, subagent_call, span | session_node.py |
| `NodeStatus` | in_progress, completed, failed | session_node.py |
| `TagResourceType` | session, cohort, experiment, experiment_run | tag.py |

Terminal values: `TaskStatus` completed, failed, timed_out, canceled, abandoned. `JobStatus` completed, failed, canceled. `ExperimentRunStatus` completed, failed, canceled. `ReplayStatus` completed, failed, canceled. Every other value is non-terminal, `ExperimentRunStatus.canceling` included.

Neither a task nor a job carries a canceling status. Cancellation is a request flag, `cancel_requested_at`, orthogonal to the status: a claimed or running task keeps its status until the worker or the sweep writes a terminal value, and a job keeps `running` until its tasks drain and settlement writes the terminal value. This keeps every task status a fact about the process and leaves exactly one writer per terminal transition. The full transition tables are at the end of this document. `ExperimentRunStatus` keeps `canceling` because a run has no executor, so the request and the settled outcome need distinct values there.

### account.py

- `AccountCreateRequest`: name, email?, password?
- `AccountActivateRequest`: activation_token, password
- `AccountUpdateRequest`: password?, old_password?, metadata?
- `AccountListParams`: filter?
- `AccountResponse`: id, name, email?, is_service_account, active, metadata, created, updated
- `AccountActivationTokenResponse`: `AccountResponse` plus activation_token, returned once when a token is minted

### agent.py

- `AgentCreateRequest`: name, description?
- `AgentUpdateRequest`: name?, description?
- `AgentListParams`: filter?
- `AgentResponse`: id, owner_id, name, description?, latest_version, created, updated

### agent_version.py

- `RunSpec` (RequestModel): command, working_dir?, env: dict[str, str], secret_ids: list[UUID], timeout_seconds: PositiveInt = 3600
- `AgentCapabilities` (RequestModel): tools, mcp_servers, skills: list[str]
- `AgentVersionCreateRequest`: display_version?, description?, run_spec: RunSpec?, capabilities: AgentCapabilities?
- `AgentVersionUpdateRequest`: display_version?, description?, run_spec?, capabilities?
- `AgentVersionResponse`: id, owner_id, agent_id, version: int, display_version?, description?, run_spec?, capabilities, created, updated

The version number is server-assigned, so the create request carries no version. `display_version` is the human-readable designator, a semver string, a git sha, or a branch name. Its shared rules live with the domain models.

### api_key.py

- `ApiKeyCreateRequest`: name
- `ApiKeyUpdateRequest`: active
- `ApiKeyListParams`: filter?
- `ApiKeyResponse`: id, owner_id, name, active, last_used?, created, updated
- `ApiKeyIssuedResponse(ApiKeyResponse)`: + key (plaintext, shown once)

### auth.py

- `TokenResponse`: access_token, token_type, expires_in, csrf_token?

### blob.py

- `BlobResponse`: id, sha256, size, media_type, created

### cohort.py

- `CohortCreateRequest`: name, description?, agent_id, metadata: dict[str, JsonValue]
- `CohortUpdateRequest`: name?, description?, metadata?
- `CohortListParams`: filter?
- `CohortResponse`: id, owner_id, name, description?, agent_id, metadata, latest_version, created, updated

### cohort_version.py

- `CohortVersionCreateRequest`: add_session_ids, remove_session_ids, display_version?
- `CohortVersionUpdateRequest`: display_version?
- `CohortVersionResponse`: id, owner_id, cohort_id, version: int, display_version?, session_count, created, updated

### evaluation.py

- `EvaluationResult` (RequestModel): name, score: FiniteFloat | bool?, value: str?, explanation?, passed: bool?. The name follows the `Name` rules. At least one of score and value must be set. The data type is derived, never supplied: float or bool from a lone score (bool checked before float), str from a lone value, categorical when both are set. A single positional constructor argument routes by type, bool and float to score, str to value. `passed` is an independent optional verdict, never derived from the score and never constrained by the data type. It is named `passed` rather than `pass` because `pass` is a Python keyword. One result maps to one evaluation row.
- `EvaluationBatchCreateRequest`: input_session_ids (min 1, unique), evaluators: list[EvaluatorConfig] (min 1). Creation returns `JobResponse`, one job holding one `on_failure=continue` evaluator task per (input session, evaluator) pair.
- `EvaluationListParams`: filter?
- `EvaluationResponse`: id, owner_id, evaluator_version_id?, evaluator_name?, evaluator_version?, session_id, task_id?, name, data_type, score?, value?, explanation?, passed?, created, updated. evaluator_name and evaluator_version are denormalized from the referenced evaluator version by the mapping, null on manual evaluations along with evaluator_version_id and task_id. score and value mirror the request channels: score carries the stored number, returned as a bool for bool rows, value carries the label or string.

### evaluator.py

- `EvaluatorCreateRequest`: name, description?, metadata: dict[str, JsonValue]
- `EvaluatorUpdateRequest`: description?, metadata?
- `EvaluatorListParams`: filter?
- `EvaluatorResponse`: id, owner_id, name, description?, metadata, latest_version, created, updated
- `EvaluatorVersionCreateRequest`: source: PluginSource, display_version?
- `EvaluatorVersionUpdateRequest`: display_version?
- `EvaluatorVersionResponse`: id, evaluator_id, version: int, display_version?, source, created, updated

### experiment.py

- `ExperimentCreateRequest`: name, description?, override: ReplayOverride?, tool_policy: ToolPolicy?, evaluators: list[EvaluatorConfig] (min 1)
- `ExperimentUpdateRequest`: all of the above optional
- `ExperimentListParams`: filter?
- `ExperimentResponse`: id, owner_id, name, description?, override?, tool_policy, evaluators, created, updated

### experiment_run.py

- `ExperimentRunProgress` (ResponseModel): pending, evaluating, completed, failed, canceled, total. Counts the run's replay rows by `ReplayStatus`, so the numbers track replays rather than the tasks inside their jobs.
- `ExperimentRunCreateRequest`: cohort_version_id, agent_version_id, evaluate_baselines: bool
- `ExperimentRunListParams`: filter?
- `ExperimentRunJobsListParams`: filter?
- `ExperimentRunResponse`: id, owner_id, experiment_id, number, status: ExperimentRunStatus, cohort_version_id, agent_version_id, evaluate_baselines, started_at?, ended_at?, error?, progress: ExperimentRunProgress, created, updated

There is no run summary. The run's output is its replays, and per-run statistics are computed by the reader from the replay listing and `GET /v1/evaluations`.

### importer.py

- `ImporterCreateRequest`: name, description?, provider?, metadata: dict[str, JsonValue]
- `ImporterUpdateRequest`: description?, metadata?
- `ImporterListParams`: filter?
- `ImporterResponse`: id, owner_id, name, description?, provider?, metadata, latest_version, created, updated
- `ImporterVersionCreateRequest`: source: PluginSource, display_version?
- `ImporterVersionUpdateRequest`: display_version?
- `ImporterVersionResponse`: id, importer_id, version: int, display_version?, source, created, updated

### imports.py

- `ImportCreateRequest`: importer (name), agent_id, agent_version_id?, version?, payload_blob_id, params: dict[str, JsonValue]. Importer and version resolve at creation to the plugin version id stored on the task, an omitted version resolves to latest. An agent_version_id must belong to agent_id and is stamped on every session the import creates, an omitted one leaves those sessions without a version. Creation returns `JobResponse`, there is no ImportResponse.
- `ImportFailure`: line, external_id?, error
- `ImportStats`: created, skipped, failed, failures: list[ImportFailure] (max 20)

### job.py

- `JobResponse`: id, owner_id, status, cancel_requested_at?, started_at?, ended_at?, error?, created, updated. A job is a generic group of tasks: no kind, no domain references, no result. Its status is written only by settlement (see the application layer), its error is the first counted task failure's error, and its output lives on whatever domain resource owns it (the replay row, the evaluation rows, the linked sessions).
- `JobListParams`: filter?
- `JobTasksListParams`: filter?

### task.py

Task lifecycle, claim, and spec models.

- `TaskResponse`: id, job_id, kind, status, on_failure, attempt, labels, agent_version_id?, plugin_version_id?, payload_blob_id?, input_session_id?, agent_id?, worker_id?, result_session_id?, claimed_at?, heartbeat_at?, cancel_requested_at?, started_at?, ended_at?, error?, result, created, updated. result is validated only at completion (evaluator and importer kinds require one), on a non-completed task it is diagnostic output (partial import stats, for example), and readers gate on status: evaluation recording reads results from completed tasks only. A result larger than a server setting (default 1 MiB, matching the worker's `MAX_RESULT_BYTES`) is a 413, so the cap holds for every writer and not only for workers.
- `TaskUpdateRequest`: status?, attempt?, error?, result?. The executor surface, and every transition on it (running, completed, failed, timed_out, canceled) requires attempt to match the task's current attempt, a mismatch is a 409. There is no unfenced status write and no user-facing task write, cancellation is job-level.
- `cancel_requested_at` is set by job cancellation (user cancel, run cancellation) and by `on_failure=abort` propagation, never cleared. It is what the heartbeat reads to build `cancel_task_ids`, and what the sweep reads to decide whether a stale task settles to canceled instead of requeueing.
- `on_failure`: what this task's hard failure (failed, timed_out, abandoned) does to the rest of its job. `abort` stamps `cancel_requested_at` on non-terminal siblings and counts toward the job outcome, `continue` counts without touching siblings, `ignore` neither cancels nor counts. Set at task creation, default abort.
- `labels`: plain dict[str, str] written at task creation, matched by worker scope selectors. The one convention of the built-in creators: agent tasks carry `agent_version`.
- `TaskListParams`: filter?
- `LabelSelector` (frozen): key, values (non-empty), required: bool = False. A required selector matches tasks carrying the key with a value in `values`, a non-required selector additionally matches every task lacking the key.
- `WorkerScope` (frozen): kinds?, selectors?, job_id?. The fields combine as a conjunction. Validator: lists non-empty when set, selector keys unique.
- `TaskClaimRequest`: worker_id, max_tasks (1..100). The scope comes from the worker row, not the request.
- `TaskRunSpec`: command, working_dir?, env (copied from the version's run spec)
- `ScriptPluginSpec`: type="script", entrypoint, blob_id, sha256
- `PackagePluginSpec`: type="package", entrypoint, requirement
- `PluginSpec` = discriminated union of the two on `type`
- `PayloadSpec`: blob_id, sha256
- Spec details, discriminated on `kind`:
  - `AgentTaskDetails`: kind="agent", inputs
  - `EvaluationTaskDetails`: kind="evaluator", evaluator_name, params, plugin: PluginSpec, input_session_id
  - `ImportTaskDetails`: kind="importer", plugin: PluginSpec, payload: PayloadSpec, provider, agent_id, params
- `TaskSpecResponse`: task_id, kind, timeout_seconds, run: TaskRunSpec?, env, secret_env, details (the union above, kind mirrors the top-level field). `env` is the creator-set process environment extras, shipped verbatim: the replay pipeline sets `KITARU_REPLAY_ID` on its agent tasks, session runs set `KITARU_SESSION_NAME`, and the contract variable names are rejected at task creation. secret_env merges the run spec's secrets in secret_ids order, a later secret overrides an earlier one on key collision.

`timeout_seconds` sits on the spec rather than inside `run` because it is the one run field every kind has. The server populates it for all three kinds: from the agent version's run spec for agent tasks, and from a server setting for evaluator and importer tasks (defaults 300 and 600). Evaluator and importer tasks carry no `run` at all, since the worker builds their command from `kitaru.task`. One field, always set, so the worker reads a single source and keeps no per-kind timeout constants of its own.
- `TaskWithSpec`: task: TaskResponse, spec: TaskSpecResponse
- `TaskClaimResponse`: tasks: list[TaskWithSpec]

### plugin.py

Shared by the evaluator and importer resources:

- `ScriptPluginSource`: type="script", blob_id, entrypoint (attribute in the file)
- `PackagePluginSource`: type="package", requirement (pinned PEP 508), entrypoint (`module:attribute`)
- `PluginSource` = discriminated union of the two on `type`, members extend `DiscriminatedRequestModel`

### replay.py

- `ReplayCreateRequest`: baseline_session_id, agent_version_id?, override?, tool_policy?, evaluators (min 1), evaluate_baselines: bool = false. An omitted agent_version_id resolves to the baseline session's recorded agent version, rejected when the session carries none. The resolved version must have a run spec either way. With `evaluate_baselines`, the replay's job additionally scores the baseline session (see the replay pipeline).
- `ReplayListParams`: filter?
- `ReplayResponse`: id, job_id, experiment_run_id?, baseline_session_id, result_session_id?, override?, tool_policy, evaluators, evaluate_baselines, status: ReplayStatus, error?, created, updated
- `ToolLookupRequest`: tool_name, cache_key (64 chars)
- `ToolLookupResponse`: found, result

There is no verdict and no summary on a replay: its output is the result session plus that session's evaluation rows, compared by the reader via `GET /v1/evaluations` on the baseline and result sessions.

### replay_config.py

The replay configuration value objects, mirroring `domain/replay_config.py`. Imported by experiment.py, replay.py, and evaluation.py:

- `ReplayOverride`: model (str or old-to-new map)?, system_prompt?, prompt?, model_params?
- `EvaluatorConfig`: evaluator (name), version?, params. Resolved at creation to an evaluator version id, the stored form carries it, an omitted version resolves to latest. There is no per-config name, an evaluation's identity is the evaluator name and version plus the names the evaluator emits.
- `StaticCase`: match?, match_mode: StaticMatchMode, result
- `PassthroughConfig` | `HistoryConfig(scope, on_miss)` | `StaticConfig(cases, on_miss)` | `LLMConfig(model, instructions?)`
- `ToolConfig` = discriminated union of the four on `type` (values passthrough, history, static, llm)
- `ToolPolicy`: default: ToolConfig, tools: dict[str, ToolConfig]
- The `ToolConfig` union members extend `DiscriminatedRequestModel` (base.py) so their `type` discriminator survives `exclude_unset` dumps

### secret.py

- `SecretCreateRequest`: name, type?, values: dict[str, SecretStr]
- `SecretUpdateRequest`: type?, values?
- `SecretListParams`: filter?
- `SecretResponse`: id, owner_id, name, type?, created, updated
- `SecretWithValuesResponse(SecretResponse)`: + values

### session.py

- `TokenUsage` (RequestModel): input_tokens?, output_tokens?, cached_input_tokens?, reasoning_tokens?
- `SessionCreateRequest`: agent_id?, agent_version_id?, origin, status?, name?, system_prompt?, inputs, outputs, error?, started_at?, ended_at?, external_id?, metadata, imported_from?, framework?, adapter_version?, task_id?. Both ids are optional on the wire because a task_id naming an agent or import task supplies them, see the session create rules below.
- `SessionUpdateRequest`: status?, outputs, error?, ended_at?, name?, system_prompt?, metadata?
- `SessionEvaluationsRequest`: evaluations: list[EvaluationResult] (min 1)
- `SessionListParams`: filter?
- `SessionResponse`: id, owner_id, agent_id, agent_version_id?, task_id?, origin, status, name?, system_prompt?, inputs, outputs, error?, started_at?, ended_at?, external_id?, metadata, imported_from?, framework?, adapter_version?, cost: Decimal?, tokens: TokenUsage?, llm_call_count, tool_call_count, created, updated. The session carries no evaluations inline, they are read via `GET /v1/evaluations?session_id=...`.
- `imported_from` is a free-form string naming the source system the session was imported from

### session_node.py

- `SessionNodeCreateRequest`: index, parent_index?, secondary_parent_indexes, external_id?, trace_id?, node_type, name, status, error?, started_at?, ended_at?, input_text?, output_text?, system_prompt?, reasoning?, inputs, outputs, requested_model?, model?, provider?, tokens: TokenUsage?, cost?, model_params?, tool_name?, subagent_id?, attributes, metadata. The text fields are human-readable projections for display while `inputs` and `outputs` preserve complete payloads. `reasoning` contains visible text only and remains null for redacted, encrypted, or unavailable reasoning. No client-generated ids: `(session, index)` is the wire identity, batches upsert on it, and the server mints the row id.
- `SessionNodeBatchRequest`: nodes (parent before child, `parent_index < index`). An index already stored is replaced whole, not merged, so a batch always states the node's full content.
- `SessionNodeResponse`: the request fields plus id, session_id, parent_id?, secondary_parent_ids, cache_key?. inputs/outputs/attributes only populated with `include_payloads`.

### session_run.py

- `SessionRunCreateRequest`: agent_version_id, inputs, name?. Creation returns `JobResponse`.

### tag.py

- `TagCreateRequest`: name
- `TagUpdateRequest`: name
- `TagListParams`: filter?
- `TagResponse`: id, owner_id, name, created, updated
- `TagLinkCreateRequest`: resource_type: TagResourceType, resource_id
- `TagLinkResponse`: id, tag_id, resource_type, resource_id, created, updated

### worker.py

- `WorkerRuntime`: platform: str (kubernetes, docker, bare, ...), hostname?, os?, arch?, python_version?, kitaru_version?, namespace?, pod?. Detected by the worker at registration, see worker.md.
- `WorkerCreateRequest`: name, scope: WorkerScope, runtime: WorkerRuntime, metadata
- `WorkerListParams`: filter?
- `WorkerHeartbeatRequest`: task_ids
- `WorkerHeartbeatResponse`: cancel_task_ids: list[UUID]
- `WorkerResponse`: id, owner_id, name, scope: WorkerScope, runtime: WorkerRuntime, last_seen_at, live, metadata, created, updated

### Cross-file imports

replay_config.py is the shared hub for replay configuration: experiment.py and replay.py import `ReplayOverride`, `EvaluatorConfig`, `ToolPolicy` from it, and evaluation.py imports `EvaluatorConfig`. task.py imports nothing from replay_config.py, the spec details carry no replay concepts. worker.py imports `WorkerScope` from task.py. importer.py and evaluator.py import `PluginSource` from plugin.py. session.py imports `EvaluationResult` from evaluation.py. session_node.py imports `TokenUsage` from session.py.

## Application layer

### Services

| Service | Responsibility |
|---|---|
| `AccountService` | Account CRUD, credentials |
| `AgentService` | Agent CRUD |
| `AgentVersionService` | Version CRUD with server-assigned version numbers |
| `ApiKeyService` | Key issue, list, deactivate |
| `BlobService` | Content-addressed upload, metadata reads, download |
| `CohortService` | Cohort namespace CRUD |
| `CohortVersionService` | Version creation with membership deltas and server-assigned version numbers, reads, display version updates, delete |
| `EvaluationService` | Evaluation reads, manual evaluation upserts |
| `ExperimentService` | Experiment CRUD, run launch with replay fan-out |
| `ExperimentRunService` | Run reads, cancel, progress aggregation |
| `PluginService` | Plugin and version registry with server-assigned version numbers, one instance per `PluginKind` |
| `ReplayService` | `create_replay`, `get_replay`, `list_replays`, `tool_lookup` |
| `SecretService` | Secret CRUD |
| `SessionService` | Session lifecycle, task link check on create |
| `SessionNodeService` | Node batch upsert on (session, index) with parent_index resolution, cache_key derivation, rollups via atomic SQL increments. An upsert replaces the whole row, so a resent node with fewer fields clears the omitted ones, which is what the delta-based rollups already assume. The batch's existing rows load in one bulk fetch, not per-row gets. Ingest requires an in-progress session, except origin=imported sessions, created terminal with nodes ingested afterward |
| `TagService` | Tag CRUD, resource links |
| `JobService` | Job reads, cancel, delete, settlement, job and task composition for the command endpoints |
| `TaskService` | Task claim, heartbeat, spec building, transitions with event dispatch, staleness sweep |
| `WorkerService` | `register_worker` (upsert by name), reads, delete |

Shared helper modules: `agent_version_resolution.resolve_agent_version`, `plugin_resolution.resolve_plugin/resolve_plugin_version`, `evaluator_resolution.resolve_evaluator_config/validate_evaluators`, `run_finalization.finalize_run_if_drained`, `replay_pipeline.create_replay_pipeline/append_result_evaluations/settle_replay`, `evaluation_recording.record_task_evaluations`, and the event registry in `application/events.py`.

`resolve_agent_version` rejects versions without a run spec for task-creating callers (replays, session runs, run fan-out) with a validation error, so the failure surfaces as a 422 at the POST instead of at claim time. `validate_evaluators` resolves every config of an evaluator list and rejects duplicate resolved version ids, so one evaluator version appears at most once per list.

Full-collection reads (`ExperimentService._resolve_members`, `ExperimentService._resolve_cohort_session_ids`) page through `paginate_all` (`server/utils.py`), which drives a page-by-page query callable until exhaustion. No hand-rolled while-loops with per-module page-size constants.

`finalize_run_if_drained` decides drained with one count of the run's non-settled replay rows on the (experiment_run_id, status) index. Baseline scoring lives inside the replay jobs, so a settled replay set means the run has no live tasks and no separate baseline accounting exists.

The terminal status of a drained run:

| Condition | Status |
|---|---|
| the run was `canceling` when it drained | canceled |
| any replay ended failed or canceled | failed |
| otherwise, every replay completed | completed |

Cancellation wins over everything. A failed replay is always a pipeline failure, there is no verdict that could count as a result, so it fails the run.

`TaskService` methods: `claim_tasks`, `heartbeat_worker`, `get_task`, `list_tasks`, `get_spec`, `update_task`, plus private spec builders (`_build_spec` dispatching to `_agent_spec`, `_evaluation_spec`, `_import_spec`), `_check_result_session`, and `_apply_status`. Every task status transition, no matter the writer, goes through `_apply_status`: `update_task`, job cancellation, run cancellation, and the staleness sweep all call it, and no task status is written outside it.

`JobService` methods: `get_job`, `list_jobs`, `list_job_tasks`, `cancel_job`, `delete_job`, `create_session_run`, `create_import` (each creating a job with one task of the matching kind), `create_evaluations` (one job with one `continue` evaluator task per pair), plus `create_job` and `add_task` used by those and by `replay_pipeline`, and `advance_job` called from `_apply_status`. Task creation is server-internal, no endpoint exists, and `add_task` raises a conflict on a settled job, checked under the job row lock.

### Events

Task transitions drive the pipelines through an in-process event registry (`application/events.py`): frozen event objects, a dict from event type to handlers, registration at app composition, synchronous dispatch on the request session so subscribers commit or roll back with the transition. The events are `TaskTerminal(task, previous_status)`, `JobSettled(job)`, and `ReplaySettled(replay)`. The substrate imports no subscriber.

`_apply_status` runs one ordered sequence inside the transition's transaction:

1. Apply the task transition.
2. Dispatch `TaskTerminal` when the new status is terminal.
3. `JobService.advance_job`: on a counted hard failure of an `abort` task, stamp `cancel_requested_at` on all non-terminal siblings and move pending ones to canceled (each through `_apply_status`), then lock the job row FOR UPDATE and settle when every task is terminal. Tasks appended by `TaskTerminal` subscribers land before this check, so a job never settles past its subscribers.
4. Dispatch `JobSettled` when the job settled.

Settlement outcome precedence: failed on any counted hard failure (failed, timed_out, or abandoned with `on_failure != ignore`, the error from the first), else canceled on any canceled task, else completed. The job moves pending → running when its first task is claimed, stamping started_at, and settlement stamps ended_at.

Registered subscribers, each owning one aggregate: `evaluation_recording.record_task_evaluations` (`TaskTerminal`, a completed evaluator task's result becomes evaluation rows), `replay_pipeline` (`TaskTerminal`, a completed agent task appends the replay's result evaluator tasks and moves the replay to evaluating, and `JobSettled`, mapping the outcome onto the replay row and emitting `ReplaySettled`), `run_finalization` (`ReplaySettled`, running the drained check).

### Application models (`application/models/`, all FrozenModel)

Filters: `AccountFilter`, `AgentFilter`, `AgentVersionFilter`, `ApiKeyFilter`, `CohortFilter`, `CohortVersionFilter`, `EvaluationFilter`, `ExperimentFilter`, `ExperimentRunFilter`, `ExperimentRunJobsFilter`, `JobFilter`, `TaskFilter` (plus `JobTasksFilter` narrowing it for the nested jobs listing), `PluginFilter` (kind required, narrowed by `EvaluatorFilter` and `ImporterFilter`), `PluginVersionFilter`, `ReplayFilter`, `SecretFilter`, `SessionFilter`, `TagFilter`, `WorkerFilter`. Filters extend `ListFilter` (`server/base.py`), which carries `cursor`, `size`, `sort`, the optional `expression` filter tree validated against the `filterable_fields` allowlist, the `sortable_fields` allowlist, and the filter hash the cursors embed. Wire-facing filter dimensions live in `filterable_fields`, not flat fields.

Filters are built from the `XListParams` wire models in the mapping layer. The remaining flat filter fields are either path-scoped ids (`AgentVersionFilter.agent_id`, `CohortVersionFilter.cohort_id`, `PluginVersionFilter.plugin_id`, `SessionNodeFilter.session_id`), or internal, set by services (`SecretFilter.owner_id` and `internal`, `ApiKeyFilter.owner_id`, `DeviceFilter.account_id`, `TaskFilter.job_id` and `stale_before`, `JobFilter.job_ids`, `WorkerFilter.seen_after`).

Commands: `AccountUpdate`, `AgentUpdate`, `AgentVersionUpdate`, `CohortCreate`, `CohortUpdate`, `CohortVersionCreate`, `CohortVersionUpdate`, `ExperimentCreate`, `ExperimentUpdate`, `SessionRunCreate`, `ImportCreate`, `EvaluationBatchCreate`, `TaskUpdate`, `PluginUpdate`, `ReplayCreate`, `SecretUpdate`, `SessionCreate`, `SessionUpdate`, `TagUpdate`, `SessionNodeUpsert` (index-referenced like the wire model, no id or cache_key, both server-derived).

`AuthContext`: account: Account, csrf_token?.

## Domain models (`server/domain/`)

Bases: `DomainModel` (`extra="forbid"`, `validate_assignment=True`) for mutable entities, `FrozenModel` (frozen) for value objects. `FrozenModel` lives in the top-level `src/kitaru/base.py` so `api_models` value objects (`WorkerScope`) use the same base. Errors derive from `DomainError` with `NotFoundError`, `ConflictError`, `PayloadTooLargeError`, `ValidationError` branches, mapped globally to 404/409/413/422. Ids are `uuid7()` defaults. `Name` is a validated str alias (max 255, charset `[A-Za-z0-9_.-]`, no leading or trailing separator). It constrains resource names and evaluation names alike, so a qualified display form like `accuracy@3:relevance` stays parseable even though nothing server-side depends on one. `VersionName` widens the `Name` charset with `+` and `/`, so semver build metadata and branch-style labels pass, and every display_version field uses it.

Every versioned resource numbers its versions the same way: `version` is a server-assigned positive int, counted per parent, and `display_version` is an optional `VersionName` carrying whatever the user calls that version. Nothing resolves a version by `display_version`, so it stays non-unique. It is set at creation and editable afterward on `AgentVersion`, `PluginVersion`, and `CohortVersion`, which is what makes a plugin version a mutable entity rather than a write-once row. The code or member list a version points at stays immutable either way, only the label moves.

### Entities

| Entity | Fields beyond id/owner_id/created/updated | Methods |
|---|---|---|
| `Account` | is_service_account, name, email?, password_hash?, activation_token_hash?, active, metadata: dict | update_active, update_activation_token_hash, update_metadata, update_password_hash |
| `Agent` | name, description?, latest_version | update_name, update_description |
| `AgentVersion` | agent_id, version: int, display_version?, description?, run_spec: RunSpec?, capabilities: AgentCapabilities | update_display_version, update_description, update_run_spec, update_capabilities |
| `ApiKey` | name, key_hash, active, last_used? | update_active, mark_used |
| `Blob` | sha256, size, media_type, data, no updated | - |
| `Cohort` | name, description?, agent_id, metadata: dict, latest_version | check_members, update_name, update_description, update_metadata |
| `CohortVersion` | cohort_id, version: int, display_version?, session_count | update_display_version |
| `Evaluation` | evaluator_version_id?, session_id, task_id?, name, data_type, score?, value?, explanation?, passed? | - |
| `Experiment` | name, description?, replay_config_id | update_name, update_description, update_replay_config_id(frozen) |
| `ExperimentRun` | experiment_id, number, status, cohort_version_id, agent_version_id, evaluate_baselines, started_at?, ended_at?, error? | start, cancel, finalize |
| `Plugin` | kind: PluginKind, name, description?, provider?, metadata: dict, latest_version | update_description, update_metadata, validator: evaluators carry no provider |
| `PluginVersion` | plugin_id, version: int, display_version?, source: PluginSource | update_display_version |
| `ReplayConfig` | override?, tool_policy, evaluators | check_standalone (rejects cohort_version history scope) |
| `Replay` | job_id, experiment_run_id?, replay_config_id, baseline_session_id, evaluate_baselines, status: ReplayStatus, error? | settled property, start_evaluating, complete, fail(error), cancel |
| `Secret` | name, internal, type?, values: dict[str, SecretStr] | update_type, update_values |
| `Session` | agent_id, agent_version_id?, task_id?, origin, status, name?, system_prompt?, inputs, outputs, error?, started_at?, ended_at?, external_id?, metadata, imported_from?, framework?, adapter_version?, cost?, tokens?, llm_call_count, tool_call_count | update_name, update_system_prompt, update_metadata, check_node_ingest, finish |
| `SessionNode` | session_id, parent_id?, secondary_parent_ids, index, external_id?, trace_id?, node_type, name, status, error?, started_at?, ended_at?, input_text?, output_text?, system_prompt?, reasoning?, inputs, outputs, requested_model?, model?, provider?, tokens?, cost?, model_params?, tool_name?, cache_key?, subagent_id?, attributes, metadata | - |
| `Tag` | name | update_name |
| `TagLink` | tag_id, resource_type, resource_id, no owner_id | - |
| `Worker` | name, scope: WorkerScope, runtime: WorkerRuntime, last_seen_at, metadata | refresh, is_live |

### Job and task hierarchy (`domain/job.py`, `domain/task.py`)

`Job(DomainModel)`: id, owner_id, status, cancel_requested_at?, started_at?, ended_at?, error?, created?, updated?. Methods: start, settle, request_cancel. A job is a generic group of tasks and knows nothing about what its tasks compute: no kind, no domain references, no result. Its status is written only by settlement.

`Task(DomainModel)` base fields: id, job_id, status, attempt, on_failure, labels, env, worker_id?, result_session_id?, claimed_at?, heartbeat_at?, cancel_requested_at?, started_at?, ended_at?, error?, result, created?, updated?. Methods: claim, start, requeue, check_result, complete, fail, time_out, cancel, request_cancel, abandon, link_result_session, is_stale, with_staleness. Abstract `kind` property. `claim` increments `attempt`, making it the fencing token for status updates. `labels` is matched by worker scope selectors, `env` is creator-set process environment extras, both written at creation, and the contract variable names are rejected in `env`.

`request_cancel` is the propagation path: it moves a pending task straight to canceled, and on a claimed or running task it only stamps `cancel_requested_at`, leaving the status alone. `cancel` is the executor path, writing the terminal `canceled` once the process is actually gone. The two are separate methods because they have different writers and different fencing, and collapsing them is what previously required a canceling status.

| Subclass | Kind | Extra fields | Rules |
|---|---|---|---|
| `AgentTask` | agent | agent_version_id, inputs | requires an agent version with a run spec, result session required for completion |
| `EvaluationTask` | evaluator | plugin_version_id, input_session_id, params | no agent version, result must be a non-empty list of `EvaluationResult` with unique names |
| `ImportTask` | importer | plugin_version_id, payload_blob_id, agent_id, agent_version_id?, params | the optional agent version is stamped on created sessions and never executed, result must be non-null |

Spec value objects (FrozenModel): `ScriptPluginSpec(entrypoint, blob_id, sha256)` and `PackagePluginSpec(entrypoint, requirement)` with `PluginSpec` as their union, `PayloadSpec(blob_id, sha256)`, per-kind details mirroring the wire details (`AgentTaskDetails`, `EvaluationTaskDetails`, `ImportTaskDetails`), `TaskSpec(task_id, kind, run_spec: TaskRunSpec?, env, secret_env, details)`, `WorkerScope` and `LabelSelector` (the `api_models` models reused directly). The spec value objects share their names with the wire models.

### Replay config module (`domain/replay_config.py`)

`ReplayOverride`, `EvaluatorConfig` (with the resolved evaluator_version_id), `StaticCase`, the four tool configs and `ToolConfig` union, `ToolPolicy`, `ReplayConfig` entity, `effective_inputs(inputs, override)`.

### Plugin source module (`domain/plugin.py`)

`ScriptPluginSource(blob_id, entrypoint)` and `PackagePluginSource(requirement, entrypoint)` are the two plugin code sources, `PluginSource` their union. A script source is one uploaded file with the entrypoint naming an attribute in it. A package source is an installable distribution with the entrypoint as a `module:attribute` reference, validated through `parse_source_ref(ref) -> tuple[str, str]` in the top-level `src/kitaru/source_refs.py` (pure, stdlib-only, raises `ValueError`, exactly one colon so `mod:attr:extra` is rejected), wrapped into the domain validation error. The format definition exists once, the task-side entrypoint loader parses it through the same helper (see task.md). The spec builders copy the source into the matching `PluginSpec` variant, joining the blob to fill the script sha256.

Requirement validation, via `packaging.requirements.Requirement` (a direct server dependency): parses as PEP 508, max 255 characters, no URL, no marker, exactly one `==` specifier without `*` in the version. Extras pass, `===` does not. The exact pin makes a registry version an immutable code reference, matching the sha256 immutability of script sources. The server never checks the package index at registration, a bad requirement fails at task time with the uv error in the stderr tail.

### Value objects elsewhere

`RunSpec`, `AgentCapabilities` (agent_version.py), `TokenUsage`, `SessionRollups` (session.py), `ExperimentRunProgress` (experiment_run.py), `WorkerRuntime` (worker.py).

## ORM models (`adapters/db/orm/`)

24 tables. Tables use `UUIDPrimaryKeyMixin` (uuid7 pk `id`) and `TimestampMixin` (`created`, `updated`), except the `agent_version_secret` and `cohort_version_session` link tables, which use composite primary keys and keep the timestamps. `tag_link` keeps its uuid pk since the id is exposed through `TagLinkResponse`. No SQLAlchemy relationships, joins are explicit in repositories. Enums are stored as short varchar values, JSON is always JSONB. Nullable JSONB columns set `none_as_null`, so Python `None` stores as SQL NULL and `IS NULL` matches, never the JSON null literal. `metadata` columns map from a `metadata_` attribute, the bare name collides with the declarative base.

Repository `get_many` methods load id lists through `_load_by_ids` on the base SQL repository, which returns rows keyed by id with missing ids omitted. Repository-specific conversion (`to_domain`, decryption, hydration) wraps around it at the call site.

| Table | ORM class | Domain model | Columns beyond id/created/updated |
|---|---|---|---|
| account | `AccountORM` | `Account` | is_service_account, name, email?, password_hash?, activation_token_hash?, active, metadata JSONB. Unique (name, is_service_account). |
| agent | `AgentORM` | `Agent` | owner_id FK account, name unique, description?, latest_version |
| agent_version | `AgentVersionORM` | `AgentVersion` | owner_id, agent_id FK, version, display_version?, description?, run_command?, run_working_dir?, run_env JSONB?, run_timeout_seconds?, capabilities JSONB. Unique (agent_id, version). The version number comes from an `UPDATE ... RETURNING` bump of agent.latest_version in the same transaction as the insert, matching plugin_version. RunSpec is flattened into run_* columns, secret_ids live in the link table. |
| agent_version_secret | `AgentVersionSecretORM` | none (repository-managed) | Composite pk (agent_version_id FK CASCADE, secret_id FK), index with unique (agent_version_id, index) preserving secret order. |
| api_key | `ApiKeyORM` | `ApiKey` | owner_id, name unique, key_hash, active, last_used? |
| blob | `BlobORM` | `Blob` | owner_id, sha256 unique, size, media_type, data (bytea) |
| cohort | `CohortORM` | `Cohort` | owner_id, name unique, description?, agent_id FK, metadata JSONB, latest_version |
| cohort_version | `CohortVersionORM` | `CohortVersion` | owner_id, cohort_id FK CASCADE, version, display_version?, session_count (denormalized). Unique (cohort_id, version). The version number comes from an `UPDATE ... RETURNING` bump of cohort.latest_version in the same transaction as the insert, matching plugin_version. |
| cohort_version_session | `CohortVersionSessionORM` | none (repository-managed) | Composite pk (cohort_version_id FK CASCADE, session_id FK), index with unique (cohort_version_id, index). |
| evaluation | `EvaluationORM` | `Evaluation` | owner_id, evaluator_version_id FK plugin_version? (null for manual evaluations), session_id FK CASCADE, task_id FK CASCADE?, name, data_type, numerical_value double precision?, string_value?, explanation?, passed boolean?. A CHECK ties data_type to the populated value columns: float and bool in numerical_value (bool as 0/1), str in string_value, categorical in both, the value in string_value and the score in numerical_value (bool scores as 0/1). Unique (task_id, name), partial unique (session_id, name) where task_id is null (the manual upsert key). Indexes session_id and evaluator_version_id. |
| experiment | `ExperimentORM` | `Experiment` | owner_id, name unique, description?, replay_config_id FK |
| experiment_run | `ExperimentRunORM` | `ExperimentRun` | owner_id, experiment_id FK, number, status, cohort_version_id FK, agent_version_id FK, evaluate_baselines, started_at?, ended_at?, error?. Unique (experiment_id, number). |
| job | `JobORM` | `Job` | owner_id, status, cancel_requested_at?, started_at?, ended_at?, error?. Index (status). |
| plugin | `PluginORM` | `Plugin` | owner_id, kind, name, description?, provider?, metadata JSONB, latest_version. Unique (kind, name), index (kind, provider). |
| plugin_version | `PluginVersionORM` | `PluginVersion` | plugin_id FK CASCADE, version, display_version?, type, blob_id FK? (script), requirement? (package), entrypoint. Unique (plugin_id, version). The source union is flattened, exactly one of blob_id and requirement is set, enforced in the domain. The version number comes from an `UPDATE ... RETURNING` bump of plugin.latest_version in the same transaction as the insert, so a rejected insert leaves no gap and the unique constraint is the backstop. |
| replay | `ReplayORM` | `Replay` | owner_id, job_id FK CASCADE unique, experiment_run_id FK CASCADE?, replay_config_id FK, baseline_session_id FK, status, error?. Unique (experiment_run_id, baseline_session_id), one replay per baseline per run. Indexes on (experiment_run_id, status) and baseline_session_id. |
| replay_config | `ReplayConfigORM` | `ReplayConfig` | owner_id, override JSONB?, tool_policy JSONB, evaluators JSONB |
| secret | `SecretORM` | `Secret` | owner_id, name unique, internal, type?, values_encrypted (text, AES-GCM over JSON) |
| session | `SessionORM` | `Session` | owner_id, agent_id FK, agent_version_id FK?, task_id FK SET NULL indexed, origin, status, name?, system_prompt?, inputs/outputs JSONB?, error?, started_at?, ended_at?, external_id?, metadata JSONB, imported_from?, framework?, adapter_version?, cost numeric?, input/output/cached_input/reasoning_tokens bigint?, llm_call_count, tool_call_count. Unique (imported_from, external_id). Indexes (agent_id, started_at) and (status). The has_evaluation filter is an EXISTS probe against the evaluation session_id index, the cohort_version_id filter an EXISTS probe against the cohort_version_session pk. One session per agent task is enforced in the service, import tasks link many. |
| session_node | `SessionNodeORM` | `SessionNode` | session_id FK CASCADE, parent_id self-FK CASCADE?, secondary_parent_ids JSONB, index, external_id?, trace_id?, node_type, name, status, error?, started_at?, ended_at?, input_text?, output_text?, system_prompt?, reasoning?, inputs/outputs JSONB?, requested_model?, model?, provider?, token columns, cost?, model_params JSONB?, tool_name?, cache_key char(64)?, attributes JSONB, metadata JSONB, subagent_id?. Unique (session_id, index), (session_id, external_id). Partial index on cache_key where cache_key is not null (tool_lookup across cohort_version and agent history scopes). Row ids are server-minted uuid7, ingest resolves parent_index against stored and in-batch rows. |
| tag | `TagORM` | `Tag` | owner_id, name unique |
| tag_link | `TagLinkORM` | `TagLink` | tag_id FK CASCADE, resource_type, resource_id (no FK, polymorphic). Own uuid pk, unique (tag_id, resource_type, resource_id), index (resource_type, resource_id). |
| task | `TaskORM` | `Task` subclasses | see below |
| worker | `WorkerORM` | `Worker` | owner_id, name unique, scope JSONB, runtime JSONB, last_seen_at, metadata JSONB |

### task table

Single-table polymorphism over the three `Task` subclasses, discriminated by `kind`. Columns: kind, job_id FK CASCADE, agent_version_id FK? (AgentTask, optional on ImportTask), agent_id FK? (ImportTask), plugin_version_id FK? (EvaluationTask, ImportTask), payload_blob_id FK? (ImportTask), input_session_id FK? (EvaluationTask), result_session_id FK?, status, attempt, on_failure, labels JSONB, env JSONB, worker_id FK SET NULL?, inputs JSONB? (AgentTask inputs, EvaluationTask and ImportTask params), claimed_at?, heartbeat_at?, cancel_requested_at?, started_at?, ended_at?, error?, result JSONB?.

Constraints and indexes:

- unique (job_id, input_session_id, plugin_version_id): one evaluator task per evaluator version per input session within a job. A replay job scoring both sides holds two tasks per evaluator version, distinguished by input session.
- one replay per baseline per run lives on the replay table, unique (experiment_run_id, baseline_session_id) there
- index (job_id, status) (settlement and task listing), index input_session_id
- partial index on id where status = 'pending' (claim query)
- partial GIN index on labels where status = 'pending' (selector conditions)
- partial expression index on coalesce(heartbeat_at, claimed_at) where status in ('claimed', 'running') (staleness query, which covers cancel-requested tasks too since they keep their claimed or running status)

Claim query (`claim_pending`): scope conditions + status = pending, ordered by id, `FOR UPDATE SKIP LOCKED`. Job pin is `job_id = X`, kind filter is `kind IN (...)`. A required selector is `labels->>key IN (values)`, a non-required one is `NOT labels ? key OR labels->>key IN (values)`, terms ANDed. An unpinned scope with no selectors adds no condition and claims any pending task. Staleness (`requeue_stale` and effective-status reads) uses the coalesce expression against the heartbeat timeout.

### JSON columns and what is stored in them

| Column | Content |
|---|---|
| agent_version.run_env | plain `dict[str, str]` |
| agent_version.capabilities | `AgentCapabilities` dump |
| task.inputs | untyped (agent task inputs, evaluator and importer params) |
| task.result | list of `EvaluationResult` dumps (evaluator), `ImportStats` dump (importer) |
| task.labels | plain `dict[str, str]` |
| task.env | plain `dict[str, str]` |
| account.metadata | plain dict |
| plugin.metadata | plain dict |
| replay_config.override | `ReplayOverride` dump |
| replay_config.tool_policy | `ToolPolicy` dump |
| replay_config.evaluators | list of `EvaluatorConfig` dumps |
| session.inputs/outputs | untyped payloads |
| session.metadata | plain dict |
| session_node.secondary_parent_ids | list of stringified UUIDs |
| session_node.inputs/outputs | untyped payloads |
| session_node.model_params | plain dict |
| session_node.attributes/metadata | plain dicts |
| worker.scope | `WorkerScope` dump |
| worker.runtime | `WorkerRuntime` dump |

Only seven JSON columns round-trip through a model: capabilities, task.result, the three replay_config columns, worker.scope, and worker.runtime.

Flattened value objects (queryable scalar columns instead of JSON): `TokenUsage` on session and session_node, `RunSpec` on agent_version.

### Cascades

CASCADE: agent_version_secret.agent_version_id, cohort_version.cohort_id, cohort_version_session.cohort_version_id, evaluation.session_id, evaluation.task_id, task.job_id, plugin_version.plugin_id, replay.job_id, replay.experiment_run_id, session_node.session_id, session_node.parent_id, tag_link.tag_id. SET NULL: task.worker_id, session.task_id. Everything else restricts and surfaces as `*InUse` conflict errors, evaluation.evaluator_version_id included, so an evaluator with stored evaluations does not delete.

Jobs have no FK pointing upward, so nothing cascades onto them: deleting a job cascades its tasks and its replay row, and `ExperimentRunService.delete_run` deletes the run's jobs first, collected through `replay.job_id`, before the run row's cascade removes any remaining replay rows.

## Connections

### Per resource, wire to table

| Wire model | Mapping module | Domain model | Repository | Table |
|---|---|---|---|---|
| `AccountResponse` | mapping/accounts.py | `Account` | `SQLAccountRepository` | account |
| `AgentResponse` | mapping/agents.py | `Agent` | `SQLAgentRepository` | agent |
| `AgentVersionResponse` | mapping/agent_versions.py | `AgentVersion` | `SQLAgentVersionRepository` | agent_version (+ agent_version_secret) |
| `ApiKeyResponse` | mapping/api_keys.py | `ApiKey` | `SQLApiKeyRepository` | api_key |
| `BlobResponse` | mapping/blobs.py | `Blob` | `SQLBlobRepository` | blob |
| `CohortResponse` | mapping/cohorts.py | `Cohort` | `SQLCohortRepository` | cohort |
| `CohortVersionResponse` | mapping/cohort_versions.py | `CohortVersion` | `SQLCohortVersionRepository` | cohort_version (+ cohort_version_session) |
| `EvaluationResponse` | mapping/evaluations.py | `Evaluation` | `SQLEvaluationRepository` | evaluation |
| `EvaluatorResponse`, `EvaluatorVersionResponse` | mapping/plugins.py (parametrized by response class) | `Plugin`, `PluginVersion` | `SQLPluginRepository` | plugin, plugin_version |
| `ExperimentResponse` | mapping/experiments.py | `Experiment` + `ReplayConfig` | `SQLExperimentRepository` | experiment (+ replay_config) |
| `ExperimentRunResponse` | mapping/experiment_runs.py | `ExperimentRun` + `ExperimentRunProgress` | `SQLExperimentRunRepository` | experiment_run |
| `ImporterResponse`, `ImporterVersionResponse` | mapping/plugins.py (parametrized by response class) | `Plugin`, `PluginVersion` | `SQLPluginRepository` | plugin, plugin_version |
| `JobResponse` | mapping/jobs.py | `Job` | `SQLJobRepository` | job |
| `ReplayResponse` | mapping/replays.py | `Replay` | `SQLReplayRepository` | replay |
| `SecretResponse` | mapping/secrets.py | `Secret` | `SQLSecretRepository` | secret |
| `SessionResponse` | mapping/sessions.py | `Session` | `SQLSessionRepository` | session |
| `SessionNodeResponse` | mapping/session_nodes.py | `SessionNode` | `SQLSessionNodeRepository` | session_node |
| `TagResponse`, `TagLinkResponse` | mapping/tags.py | `Tag`, `TagLink` | `SQLTagRepository` | tag, tag_link |
| `TaskResponse`, `TaskSpecResponse` | mapping/tasks.py | `Task` subclasses, `TaskSpec` | `SQLTaskRepository` | task |
| `WorkerResponse` | mapping/workers.py | `Worker` | `SQLWorkerRepository` | worker |

The experiment response inlines the replay config: `Experiment` stores `replay_config_id`, the service loads the `ReplayConfig` and the mapping merges override, tool_policy, and evaluators into `ExperimentResponse`. `ReplayResponse` does the same, and additionally serves `result_session_id` from its job's agent task. `EvaluationResponse` denormalizes evaluator_name and evaluator_version from the joined plugin and plugin_version rows.

### Entity reference graph

```mermaid
erDiagram
    account ||--o{ agent : owner
    agent ||--o{ agent_version : versions
    agent_version }o--o{ secret : "agent_version_secret"
    agent ||--o{ session : sessions
    agent_version |o--o{ session : "recorded with"
    session ||--o{ session_node : nodes
    agent ||--o{ cohort : cohorts
    cohort ||--o{ cohort_version : versions
    cohort_version }o--o{ session : "cohort_version_session (ordered)"
    cohort_version ||--o{ experiment_run : runs
    replay_config ||--o{ experiment : config
    experiment ||--o{ experiment_run : runs
    job ||--o{ task : tasks
    job ||--o| replay : "1:1 via replay.job_id"
    replay_config ||--o{ replay : config
    experiment_run |o--o{ replay : run
    agent_version |o--o{ task : "runs on"
    session |o--o{ task : "input_session_id"
    session |o--|| task : "task_id / result_session_id (1:1)"
    session ||--o{ evaluation : evaluations
    task |o--o{ evaluation : writes
    plugin_version |o--o{ evaluation : evaluator
    plugin ||--o{ plugin_version : versions
    plugin_version |o--o{ task : "evaluator/importer code"
    blob |o--o{ plugin_version : "script code"
    blob |o--o{ task : "import payload"
    worker |o--o{ task : claims
    tag }o--o{ session : tag_link
    tag }o--o{ cohort : tag_link
    tag }o--o{ experiment : tag_link
    tag }o--o{ experiment_run : tag_link
```

### Job, task, and replay flow connections

- `POST /v1/replays` creates a `ReplayConfig` row, a `Replay` row, a `Job`, and the job's initial tasks in one transaction (`replay_pipeline.create_replay_pipeline`): one agent task carrying the baseline session's inputs, `KITARU_REPLAY_ID` in its env extras, and the `agent_version` label, plus, with `evaluate_baselines`, one baseline evaluator task per evaluator version that has not already scored the baseline session, each with `input_session_id = baseline_session_id` and claimable immediately, baseline scoring does not wait for the agent task. Run fan-out (`ExperimentService.start_run`) does the same per session of the run's cohort version, passing the run's `evaluate_baselines` flag into each replay, rejecting an empty version with a validation error so every run starts with at least one job.
- The already-scored check for baselines reads the task table for completed evaluator tasks by (input_session_id, plugin_version_id). It is unlocked, so two concurrent runs sharing a session can both score the same baseline, accepted as waste, the duplicate rows stay distinguishable by task_id and job_id.
- Completing the agent task appends one result evaluator task per evaluator in the config's evaluator list (`replay_pipeline.append_result_evaluations` on `TaskTerminal`, inside the completion request), each with `input_session_id` set to the agent task's `result_session_id`, and moves the replay to `evaluating`. Appended tasks are complete at creation, no deferred references exist, because the result session exists before the append runs. The terminal transition, the append, job advancement, settlement, and run finalization commit in one transaction: a failure in any step rolls back the whole transition and the worker's PATCH fails, nothing is half-applied.
- Completing an evaluator task writes one evaluation row per `EvaluationResult` in its result list, in the completion transaction: evaluator_version_id from the task's plugin_version_id, session_id from input_session_id, task_id from the task. This holds uniformly for standalone, result, and baseline tasks, `evaluation_recording.record_task_evaluations` is the single writer.
- Every task of a replay job carries `on_failure=abort`, so the first hard failure, agent task or any evaluator task, baselines included, cancels the rest and the job settles failed when the tasks drain. A baseline failure failing the replay is deliberate: the comparison the replay exists for cannot be produced. Settlement keys off terminal status rather than off completion, so a task that times out or is abandoned by the sweep settles the job the same way an explicit failure does.
- `JobSettled` maps the outcome onto the `Replay` row (`replay_pipeline.settle_replay`): completed → completed, failed → failed with the job's error, canceled → canceled. `ReplaySettled` then triggers `finalize_run_if_drained`. The replay status tracks the pipeline: `pending` from creation, `evaluating` when the agent task completes, terminal at job settlement.
- `POST /v1/evaluations` creates one job holding one evaluator task per (input session, evaluator) pair, resolving the evaluator versions at creation. Those tasks carry `on_failure=continue`, so every pair runs regardless of sibling failures and the job outcome reports whether all of them scored. `POST /v1/imports` and `POST /v1/session-runs` create a job with one importer or agent task, session runs put the session name into the task env extras as `KITARU_SESSION_NAME`. Manual evaluations through `POST /v1/sessions/{id}/evaluations` are `INSERT ... ON CONFLICT` upserts on the (session_id, name) partial unique key, so a resent name overwrites its value, data type, explanation, and pass flag. The rollup updates on node ingest are atomic SQL increments. The increments are delta-based: each upserted node contributes new minus old against the stored row for cost and the token columns, and 0 or 1 for the call counts, summed per batch into one atomic UPDATE on the session row, so a replacement corrects itself and a retried identical batch has delta zero.
- Sessions link to tasks at create time (`SessionCreateRequest.task_id`, task must be running). Agent tasks link exactly one session and get `result_session_id` written in the same transaction, import tasks link every session they create, listable via the `task_id` session filter.
- The task owns the session's agent and agent version. A session naming an agent or import task takes `agent_version_id` from that task and, for an import task, `agent_id` too, so the adapter records neither. A request that carries a value disagreeing with the task is a 422 (`SessionAgentVersionMismatch`, `SessionAgentMismatch`), which includes sending a version for an import task that carries none. Without a task the request carries them, and naming only a version infers the agent from it. Whenever a version is in play, resolved or given, it must belong to the session's agent, checked by one primary-key read of `agent_version.agent_id` and rejected with `AgentVersionAgentMismatch`. On the agent-task path that read is the same one the agent inference needs, so it costs nothing extra. A request left with no agent at all is a 422 (`SessionAgentRequired`). Enforcing the pair in the database instead is future_improvements.md.
- The claim path is `POST /v1/tasks/claim`. The scope is read from the caller's worker row, stored at registration, and interpreted by `_scope_conditions` in the task repository. A claim refreshes worker.last_seen_at, so an idle worker polling an empty queue stays live.
- `heartbeat_worker` updates worker.last_seen_at, stamps heartbeat_at only on reported tasks whose worker_id matches the caller, and returns the rest in cancel_task_ids (cancel-requested, reassigned, or no longer owned). The staleness sweep runs at claim time, before the claim query, capped to a bounded row count per claim (a server setting, default 100, overflow rolls to the next claim), selecting with `FOR UPDATE SKIP LOCKED` so concurrent claims never block on each other's sweep. It applies to tasks whose coalesce(heartbeat_at, claimed_at) is older than the heartbeat timeout (a server setting, default 60 seconds, kept a comfortable multiple of the worker heartbeat interval so one dropped heartbeat never requeues a live task): a stale task with `cancel_requested_at` set settles to canceled, otherwise it is requeued while attempt is under the retry cap (a server setting, default 3) and abandoned at the cap. A requeue unlinks the stale attempt's result session (`session.task_id` and the task's `result_session_id` to null), freeing the one-session-per-task slot so the next attempt can link its own. The sweep routes its writes through the same `_apply_status` dispatch as `update_task`, so an abandoned or canceled last task settles its job, its replay, and its run without any worker transition arriving. `abandoned` is written only by the sweep, `timed_out` only by the worker's process timeout. With no worker polling, nothing sweeps, stale rows surface through effective-status reads until the next claim.
- `ExperimentRunService.cancel_run` writes the run's `canceling` status and propagates in the same transaction, canceling each non-settled replay's job the way `cancel_job` does: claimed and running tasks get `cancel_requested_at` stamped in one bulk UPDATE and keep their status, pending tasks move to `canceled` through `_apply_status` one by one, so drained jobs settle, their replay rows cancel, and the run can finalize. Claimed and running tasks then reach a terminal status the usual way, through their worker's next heartbeat or through the sweep, so run cancellation adds no second terminal writer. The run settles to `canceled` when it drains.
- Run progress counts, run job listing, and run finalization read the run's replay rows and their 1:1 jobs through `replay.job_id`, never the task table.
- `tool_lookup` resolves the replay by id, looks up the tool's config in the tool policy from the replay config (rejecting tools not under a history config), and searches recorded tool-call nodes by cache_key within the config's history scope (baseline, the run's cohort version, or agent). Config and scope resolution is server-side: the adapter sends only the tool name and cache_key, never a scope, so the policy is interpreted in one place. The cache_key is `compute_tool_cache_key(tool_name, inputs)` in the top-level `src/kitaru/cache_keys.py` (pure, stdlib-only, sha256 hex over the tool name and canonical JSON inputs): node ingest derives it for recorded tool-call nodes, the replaying adapter derives it for the lookup, so the format is defined once. The key is null when the inputs are absent or cannot be canonicalized, since a key over those would match unrelated calls of the same tool: an ingested node then stores a null cache_key, which no lookup can match, and the adapter skips the lookup and goes straight to the config's on_miss behavior. `ToolLookupRequest.cache_key` stays required and 64 characters, so a null key can never reach the search. The adapter receives the replay id through `KITARU_REPLAY_ID`, set by the replay pipeline in the agent task's env extras, and fetches the override and tool policy from `GET /v1/replays/{id}`.

## Task status transitions

Rows are the current status, columns the target, cells the writer allowed to make the move. Empty cells are illegal and answered with a 409. Every `PATCH /v1/tasks/{id}` transition is fenced by the claim's attempt, so "worker" always means the worker holding the current attempt.

| from \ to | pending | claimed | running | completed | failed | timed_out | canceled | abandoned |
|---|---|---|---|---|---|---|---|---|
| **pending** | | server (claim) | | | | | server (job cancel, run cancel, abort propagation) | |
| **claimed** | sweep (requeue) | | worker | | worker | | sweep (cancel-requested) | sweep (at retry cap) |
| **running** | sweep (requeue) | | | worker | worker | worker | worker, sweep (cancel-requested) | sweep (at retry cap) |
| **completed** | | | | | | | | |
| **failed** | | | | | | | | |
| **timed_out** | | | | | | | | |
| **canceled** | | | | | | | | |
| **abandoned** | | | | | | | | |

Reading the table:

- Terminal statuses have no outgoing transitions. Re-running terminal work is a future improvement (see the task retry entry in future_improvements.md).
- `claimed` reaches `completed` only through `running`, because the worker always writes `running` before spawning the process.
- A stale task reaches `pending` (requeue) or `abandoned` (retry cap) depending on attempt, and reaches `canceled` when `cancel_requested_at` is set. The sweep is the only writer of `abandoned`, the worker the only writer of `timed_out`.
- `cancel_requested_at` is orthogonal to this table. It never changes a status by itself, it only changes which branch the sweep takes and what the heartbeat returns. A cancel-requested running task whose process exits 0 first is still reported `completed` by its worker and the transition is accepted, because discarding finished work is worse than honoring a cancel late.
- The pending → canceled transition is server-written and safe unfenced because a pending task has no worker.

## Job status transitions

Every job transition is server-written inside the task transition dispatch, jobs have no external writer.

| from \ to | running | completed | failed | canceled |
|---|---|---|---|---|
| **pending** | server (first task claim) | | | server (settlement) |
| **running** | | server (settlement) | server (settlement) | server (settlement) |

- pending → canceled happens when a job is canceled before any task was claimed: the pending tasks move to canceled one by one and the drained job settles.
- pending → completed and pending → failed are impossible, a task must be claimed before it can complete or fail, and the claim moves the job to running.

## Invariants

Which invariants have a database constraint behind them and which are service-level checks. A service-level check is a read followed by a write, so it holds only under the transaction isolation and locking noted beside it.

| Invariant | Enforced by |
|---|---|
| One blob per sha256 | DB, unique (sha256), the create catches the violation and returns the stored row |
| One agent version per (agent_id, version) | DB, unique, with the version number from an `UPDATE ... RETURNING` bump in the same transaction |
| One plugin per (kind, name), one version per (plugin_id, version) | DB, unique, with the version number from an `UPDATE ... RETURNING` bump in the same transaction |
| One cohort version per (cohort_id, version) | DB, unique, with the version number from an `UPDATE ... RETURNING` bump in the same transaction |
| Cohort version membership is immutable | Service, member links are written only at version creation and no endpoint updates them |
| One replay per baseline per run | DB, unique (experiment_run_id, baseline_session_id) on replay |
| One evaluator task per evaluator version per input session per job | DB, unique (job_id, input_session_id, plugin_version_id) |
| One evaluation per (task, name) | DB, unique (task_id, name), with a partial unique (session_id, name) where task_id is null as the manual upsert key |
| One session per (imported_from, external_id) | DB, unique, and the dedup import re-runs rely on |
| One replay row per job | DB, unique job_id on replay |
| Attempt fencing on executor transitions | DB, the conditional UPDATE matches on attempt and the affected row count decides 409 |
| Claim hands a task to exactly one worker | DB, `FOR UPDATE SKIP LOCKED` |
| Job settlement runs once | Service, serialized by locking the job row FOR UPDATE in `advance_job`, the second completer re-reads the task set after the first commits |
| Job status has one writer | Service, settlement inside the task transition dispatch, no endpoint carries a job status field |
| Task creation is server-internal and appends only to unsettled jobs | Service, no endpoint exists and `add_task` checks under the job row lock |
| Exactly one result session per agent task | Service, a check-then-act on session create. Not constrained, so a stale worker whose task was requeued can create a second session while the new attempt is running |
| A session create names a running task | Service, a status read on the task row. Not fenced by attempt, same exposure as above |
| Terminal transitions are applied once | Service, `_apply_status` is the single dispatch point. The transition itself is not idempotent, so an automatic client retry of a committed agent task completion re-enters the result-evaluator append and hits the evaluator-task unique constraint |
| Evaluator and importer tasks complete with a result | Service, validated at the transition |
| Run finalization waits for its replays | Service, one count inside the settling transaction |
| An evaluator version appears once per evaluator list | Validator at resolution, `validate_evaluators` |
| A registry plugin requirement is an exact pin | Validator via `packaging.requirements.Requirement` |

The result-session, session-create, and terminal-transition service rows are the known gaps. They are listed here rather than left implicit because each one reads as atomic at the call site and is not.
