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
  shifted input bytes.
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

- The spec lists `update_run_spec(frozen)` and `update_capabilities(frozen)`
  without defining what freezes a version. No freeze check is implemented
  while nothing references versions. Revisit when tasks and replays land.
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
