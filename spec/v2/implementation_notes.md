# v2 implementation notes

Decisions made during implementation that the spec left open, and issues that
required solving beyond what the documents describe.

## Foundations

- `FrozenModel` moved from `server/base.py` to the top-level
  `src/kitaru/base.py` as the spec requires. `server/base.py` re-exports it,
  so existing server imports keep working.
- `JsonValue` is specified as "Any, recursively finite". Implemented as an
  annotated `Any` with a `BeforeValidator` that walks dicts, lists, and
  tuples and rejects non-finite floats.
- `compute_tool_cache_key` hashes the tool name and the canonical JSON dump
  of the inputs (sorted keys, compact separators) with a NUL byte between
  the two, so a tool name that is a prefix of another cannot collide with
  shifted input bytes. It returns None for absent inputs and for inputs
  that json refuses to dump canonically (unserializable values, non-finite
  floats), so a node ingested without usable inputs keeps a null cache_key
  and the replaying adapter skips the lookup instead of matching on a key
  every unrecorded call would share.
- `packaging` added to the server extra for plugin requirement validation.

## API models

- `AgentCapabilities.tools` and `.mcp_servers` are untyped in the spec table.
  Typed as `list[str]` matching `skills`.
- `RunSpec.command` and `TaskRunSpec.command` are typed `str`, not
  `list[str]`, since worker.md runs the command via `sh -c`.
- `StaticCase.match` is `JsonValue | None` and `StaticCase.result` is
  `JsonValue`, the spec leaves both untyped.
- `EvaluationResult`'s positional routing is a single optional positional
  parameter routed by type in `__init__`. `model_validate` bypasses
  `__init__`, so wire parsing is unaffected.
- `ImportFailure` and `ImportStats` extend `ResponseModel`, they are read
  back from task results rather than supplied by clients.
- The evaluation name rule is implemented locally in
  `api_models/v1/evaluation.py`, since `api_models` cannot import the server
  domain's `Name` type.
- `job.py` imports `TaskKind` and `TaskStatus` from `task.py` for
  `JobTasksListParams`, an import the spec's cross-file list omits.

## Agents and agent versions

- Nothing freezes an agent version. The run spec and capabilities stay
  editable for the version's whole life, tasks referencing it included,
  since the server records no identity for the code a run spec executes and
  a freeze keyed on task existence would protect an identity that is not
  actually pinned. See future_improvements.md.
- An explicit `capabilities: null` on a version PATCH resets to empty
  `AgentCapabilities` since the domain field is not nullable.
- An explicit `name: null` on an agent PATCH is a 422, names cannot clear.
- Referencing a nonexistent secret id in a run spec surfaces as an FK
  violation, no dedicated validation was specified.
- `list_versions` on an unknown agent returns an empty page, matching other
  filter-scoped list endpoints.

## Tags and workers

- `TagResourceType`, `WorkerScope`, and `WorkerRuntime` are reused from
  `api_models` directly in the domain, per the spec's "reused directly" note
  for `WorkerScope`.
- Worker registration has no duplicate-name error, the upsert absorbs it.
- `live` derives from `last_seen_at` against a new
  `KITARU_SERVER_WORKER_LIVENESS_TIMEOUT_SECONDS` setting (default 60),
  computed at the router layer to keep the mapping pure.
- The upsert needs `populate_existing` on the RETURNING select and the
  `DO UPDATE SET` clause must key off real column names, plus an explicit
  `updated` assignment since the ORM onupdate hook does not fire for it.

## Blobs and plugins

- `PayloadTooLargeError` added to the domain error taxonomy, mapped to 413.
- Blob dedup avoids loading the stored row's data column on a hit via a
  column-limited select, and the create inserts directly, catching the
  unique violation to return the stored row with a 200.
- `PluginService.create_version` checks blob existence through the blob
  repository so the fake surfaces `BlobNotFound`, the DB FK is the backstop.
- The client multipart upload always sends a filename (default "blob"),
  httpx only encodes a file part when a filename is present.
- `MAX_BLOB_SIZE_BYTES` setting (default 100 MiB) caps uploads.
- Plugin version `display_version` updates use plain None-means-unchanged,
  full set-vs-omit semantics were kept for the plugin PATCH only.

## Sessions and session nodes

- `task_id` is stored without an FK or the running-task check, the tasks
  wave adds both.
- PATCH status semantics: explicit null is a 422, terminal back to
  in_progress is a 409, terminal to terminal is allowed.
- `has_evaluation` filtering raised a placeholder validation error until the
  evaluations wave completed the EXISTS probe.
- Node upserts preserve the stored row id on replace, ids are server-minted
  on insert.
- Parent resolution bulk-fetches every index referenced by the batch,
  including parents outside the batch.
- A tool_call node without a tool name gets no cache_key.
- Rollup deltas are summed per batch in Python and applied in one atomic
  UPDATE, which stamps `updated` by hand since the Core statement bypasses
  the ORM onupdate hook.
- The ingest response always populates payloads, list responses defer the
  heavy columns unless `include_payloads` is set.
- Node listing paginates over a fixed index:asc keyset via a
  `SessionNodeListParams` model (cursor, size, include_payloads) added to
  `api_models`, and a `paginate_by_index` helper next to `paginate`.
- Date-bound filters are inclusive on both ends.
- Agent attribution resolves in one private `SessionService._resolve_agent`
  covering both the task and the task-less path, so the invariant has one
  home. The task branch matches `AgentTask | ImportTask`, an evaluator task
  named as `task_id` attributes nothing and the command's own ids stand.
- An import task whose `agent_version_id` is null rejects a command that
  names one, the task's value wins whether or not it is set.
- The version-belongs-to-agent probe is a narrow
  `AgentVersionRepository.get_agent_id` rather than `get`, which would cost
  a second query for the secret links and build a domain object that is
  thrown away.
- `SessionCreateRequest.agent_id` had to become optional for the inference,
  so its required-ness moved from pydantic to `SessionAgentRequired` in the
  service. The response model keeps it required, a stored session always
  has one.

## Experiments and replay config

- `effective_inputs` gates on `override.prompt`: dict inputs get their
  `prompt` (and `system_prompt` when set) keys replaced, non-dict inputs are
  replaced wholesale by the override prompt. Model and model_params
  overrides never touch inputs. The spec leaves the input shape undefined.
- An omitted `tool_policy` defaults to a passthrough-only policy.
- Updating any of override, tool_policy, or evaluators builds a new
  replay_config row (unchanged fields carried over) and deletes the old row
  in the same transaction. Explicit null clears override only, tool_policy
  and evaluators cannot clear.
- The domain `EvaluatorConfig` stores the resolved version int and
  `evaluator_version_id` alongside the name, so responses echo what was
  resolved without a join.
- `PluginRepository.get_by_name(kind, name)` was added for evaluator
  resolution, and the application layer uses an `EvaluatorConfigInput`
  model since it cannot import wire DTOs.
- No freeze check on `update_replay_config_id` while runs do not exist.

## Cohorts

- `Cohort.check_members` validates list shape only, the service does the
  existence and agent-match checks via a new `SessionRepository.get_many`.
- A minimal `CohortSessionsListParams` (cursor, size) was added, the list
  order is fixed by cohort position.
- `paginate_join_by_index` was added for paginating sessions by the link
  table's index column.
- Deleting a session in a cohort surfaces `SessionInUse` via the FK
  restriction. Agent deletion while cohorts reference it stays a raw
  integrity error for now.

## Evaluations

- Manual evaluation upserts have no session-status gate, any status
  accepts them.
- A duplicate name within one request is a 422.
- The manual upsert preserves row id, owner, and created via
  `INSERT ... ON CONFLICT DO UPDATE`.
- Evaluation names get their own `validate_evaluation_name` in
  `domain/names.py` since resource names reserve the dot character.
- `EvaluationWithEvaluator` named tuple carries the denormalized evaluator
  name and version from the repository join.
- Deleting an evaluator with stored evaluations surfaces `PluginInUse`.
- `evaluation.task_id` was created without an FK, the tasks wave adds it.

## Jobs and tasks core

- The agent task label convention writes
  `{"agent_version": str(agent_version_id)}`, the spec names the key but
  not the value.
- `request_cancel` on a pending task moves it straight to canceled without
  stamping `cancel_requested_at`, no in-flight process ever saw a request.
- Canceling an already settled job is a 409 (`JobAlreadySettled`).
- Unknown input session ids in `POST /v1/evaluations` fail the whole
  request as a 422 naming the id.
- Task reads report the effective status a sweep would write without
  mutating the row, the sweep stays the single status writer.
- The events substrate (`application/events.py` plus `api/composition.py`)
  ships with no registered subscribers in this wave.
  `build_event_dispatcher` is the extension point the pipeline subscribers
  hook into.
- Session-task linking errors: `TaskNotFound` 404, `TaskNotRunning` 409,
  `TaskResultSessionAlreadyLinked` 409. Import tasks link many sessions.
- New server settings: task heartbeat timeout 60s, retry limit 3, sweep
  batch 100, evaluator timeout 300s, importer timeout 600s, result cap
  1 MiB, evaluation pair cap 100.
- `create_evaluations` inserts tasks directly instead of `add_task`, the
  job was created in the same call and cannot have settled.

## Integration

- The nine per-wave migrations were squashed into one revision,
  `007_add_v2_resources`. Autogenerate dropped the `use_alter` FK from
  `session.task_id` inside `create_table`, it now lives in a separate
  `create_foreign_key` after the task table exists, with the matching
  drop first in the downgrade.
- `just check` passes fully except the `links` recipe, the lychee binary
  is not installed in this environment. No hand-written Markdown links
  changed.
- One conftest module: worker tests originally shipped their own
  `tests/worker/conftest.py`, which broke bare `from conftest import`
  resolution in subset runs. Shared fakes stay in the root conftest only.

## Replays, pipeline, and experiment runs

- `effective_inputs` is applied once, when the pipeline builds the agent
  task's inputs. The adapter still fetches the override from the replay for
  model and prompt handling at run time.
- Tool lookup with agent scope restricts to sessions with origin recorded
  or imported, so a replay's own result sessions never serve history. All
  scopes pick the newest match by id, UUIDv7 ids sort by creation time.
- Run numbers come from `max(number) + 1` under an exclusive lock of the
  experiment row, the spec defines no counter column on experiment. The
  unique (experiment_id, number) constraint is the backstop.
- Experiments reject config changes once runs exist, a boolean passed into
  the domain mutator with the service running the existence probe. Agent
  versions carry no equivalent freeze, see the agent versions section.
- `ExperimentInUse` and `ReplayConfigInUse` were added, deleting an
  experiment with runs and deleting a referenced config now map to 409
  instead of raw integrity errors. Config rows deleted on experiment
  update survive when replays reference them.
- Run progress is one grouped count per query over the
  (experiment_run_id, status) index.
- `_resolve_cohort_session_ids` returns sessions rather than ids, the
  id-only shape forced a redundant refetch.

## Worker package

- A `worker` extra on pyproject carries pydantic-settings.
- Constants live next to their primary consumer instead of one constants
  module, which also avoids a config/heartbeat/worker import cycle.
- A 409 on any non-completion transition is handled uniformly (log and
  return), the spec only special-cases the completion 409.
- The client-side job settled check compares against the terminal
  `JobStatus` values directly, the domain's settled property is server
  code the client cannot import.
- Raw `httpx.TransportError` escaping the client's retries is treated like
  a non-409 API error everywhere the runner and heartbeat write.

## Task package

- `ImportedNode` mirrors `SessionNodeCreateRequest` and adds `children`. Provider importers use nested single-parent trees. The portable Kitaru JSONL importer uses flat indexed nodes, including secondary parent indexes.
- `session_request` takes the task id as an explicit parameter, the flow
  owns it.
- API failures during import are sampled as `ImportFailure` rows with the
  item's 1-based stream ordinal as the line.
- A bad process kind exits 2 through argparse, runtime failures exit 1.
- `tests/task` shares fixtures through `task_fixtures.py` instead of a
  second conftest module. The root conftest registers itself under the
  bare `conftest` name so subset runs resolve shared fakes consistently,
  and the worker env fixture lives there gated by test path.
