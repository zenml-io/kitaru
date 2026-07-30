# Consolidation notes

Notes from consolidating `alex/codex-spec-fleshing-out` into the claude base
branch per `consolidation.md`. Records what was done and issues found.

## Plan

1. Apply consolidation.md decisions 1-13 (decisions 6, 11, 13 need no change).
2. Consolidate tests, port alex-only coverage (excluding integration tests per
   decision 11).
3. Squash migrations 001-007 into a single 001_initial.
4. Regenerate openapi/openapi.json, run checks and tests.
5. Squash all commits into one on top of `v2`, message "Implement v2 spec".

## Work log

- Applied the two migration 007 edits directly (decision 3: `subagent_id`
  becomes `sa.String(length=255)`, decision 7: `ck_plugin_version_type_blob_id_requirement`
  CHECK constraint on `plugin_version`), so the parallel agents never touch the
  same file.
- Decisions 1, 2, 3, 9, 10 (session node fields and validator, subagent_id str,
  worker config constraints, task `__all__`) delegated to one agent. Decisions
  4, 5, 6 (client pagination, streaming body, no health resource) to a second.
  Decisions 7, 8, 12, 13 (plugin CHECK constraint ORM, generic plugin
  resolution, plugin router restructure, liveness no-op) to a third. A fourth
  agent maps alex's test files to existing coverage on this branch.
- Confirmed no code outside `migrations/versions/` references revision ids, so
  the migration squash is self-contained. Tests create schema via
  `Base.metadata.create_all`, migrations run only at server startup, so the
  squashed migration gets verified by diffing `alembic upgrade head` against
  the ORM metadata schema on a fresh Postgres.
- Migration squash done. Migrations 002-006 fold into the new `001_initial`
  with 004's `external_id` column inlined into the account table and 005's
  final FK names (`fk_api_key_owner_id`, `fk_secret_owner_id`) used directly,
  then 007 carried over unchanged (including the decision 3 and 7 edits).
  Verified by running the old seven-revision chain and the new single revision
  against two scratch Postgres databases and diffing
  `pg_dump --schema-only` output: identical, 25 tables each.
- Client decisions done. `iterate_pages` ported, 24 inline cursor loops
  converted across 17 resource files (consolidation.md counted 23, actual
  count is 24), streaming `content` parameter added to `request()`, alex's
  redundant idempotency setdefault not ported. All 230 client tests pass.
- Issue found: `CohortSessionsListParams` and `SessionNodeListParams` declare
  their own `cursor` field instead of inheriting `ListParams`, so they violate
  the `iterate_pages` bound under ty. Alex's branch inherits `ListParams` in
  both spots. Fix pending, queued behind the session-node agent that is
  editing `session_node.py`.
- Session node decisions done. `SessionNodeResponse` echoes `parent_index` and
  `secondary_parent_indexes`, derived at mapping time from a new
  `get_index_by_id(session_id)` repository and service method so the list path
  resolves parents outside the current page. Alex's batch validator replaces
  the primary-parent-only check. `subagent_id` is `str` at every listed site.
  Worker config constraints and task `__all__` lists ported.
- Plugin decisions done. `ck_plugin_version_type_blob_id_requirement` CHECK
  constraint added to the ORM (migration already had it), generic
  `plugin_resolution.py` added with `evaluator_resolution.py` rewritten on top
  of it, and the nine shared plugin route operations moved from
  `mapping/plugins.py` to `routers/plugins.py` with `mapping/plugins.py`
  reduced to pure conversion. No wire-behavior change.
- Resolved the `ListParams` issue without changing wire behavior: new
  `CursorParams` base (cursor plus size) in `api_models/v1/base.py`,
  `ListParams` extends it with `sort`, `CohortSessionsListParams` and
  `SessionNodeListParams` now inherit `CursorParams` instead of duplicating
  fields, and `iterate_pages` bounds on `CursorParams`. Alex's alternative
  (inheriting `ListParams`) would have added an unsupported `sort` query param
  to those two endpoints. `ty` is clean across `src/kitaru`.
- Regenerated `openapi/openapi.json`. Diff is exactly the expected wire
  change: `subagent_id` loses the uuid format and the two parent index fields
  join `SessionNodeResponse` and its required list.
- Pre-existing issue: `just check` was never green on this branch because the
  `links` recipe needs `lychee` (installed it via brew) and because
  `examples/README.md` and `examples/features/basic_flow/README.md` still link
  to v1 test files that the v2 rewrite deleted. Replaced the dead Test-column
  links with the "—" placeholder the tables already use. The example docs are
  still v1-era overall and need a broader pass outside this consolidation.
- Pre-existing issue: `just example-coverage-audit` is broken on this branch,
  `scripts/audit-example-coverage.py` no longer exists. Left as is.

## Test consolidation

A full comparison of alex's 21 non-integration test files (about 137 test
functions) against this branch's suite found the bulk already covered, usually
at finer granularity. Roughly 20 functions were worth porting and are being
added: wire-model discriminator and client resource-registration checks,
isolated mapping-layer unit tests, EventDispatcher ordering, a route manifest
smoke test, repository-level scope tests for `find_latest_by_cache_key_in_*`
plus coverage for the new `get_index_by_id`, and assorted task and worker
gaps (importer failure branches and failure cap, plugin loader ordering and
cleanup, `parse_source_ref` formats, main dispatch and flow failure, config
kwarg-over-env, process exit-beats-cancel, claim clamp, job-pinned
repolling).

All ports landed. New test files: `tests/api_models/test_base.py`,
`tests/server/test_rest_mapping.py`, `tests/server/test_events.py`,
`tests/server/test_route_manifest.py` (asserts the full 66-path route set
with equality so added and removed routes both fail it). The rest went into
existing files. Notable adaptations: the mapping tests call
`run_spec_to_domain` and `capabilities_to_domain` directly since this branch
has no combined create-values helper, script plugin entrypoints here are bare
attribute names so alex's `"plugin:Importer"` fixture became `"score"`, and
the registration-order test targets `EventDispatcher`.

## Final state

`just check` passes in full (format, lint, ty, typos, yaml, actions lint,
links) and `just test` passes with 1933 tests, Postgres-parametrized tests
included. All branch history squashed into a single commit on top of `v2`
with message "Implement v2 spec". Not pushed.

Two small source fixes ride along with the ports, both completing existing
validation in the task package: `call_evaluator` rejects non-EvaluationResult
items with EvaluationError instead of a raw AttributeError, and `call_parser`
rejects unknown item types with SessionImportError.

Alex tests deliberately NOT ported, needing design decisions outside
consolidation.md:

- Blob metadata reads load full content here. Alex splits repository `get`
  from `get_content` so metadata-only reads skip the payload. Current
  `BlobService.get_blob` always loads content. Left as is, worth a decision.
- Alex scopes the parent-index lookup during node listing to only referenced
  parent ids. Our decision 1 implementation resolves the whole session via
  `get_index_by_id`, so alex's scoped-lookup test contradicts the chosen
  shape. Left as is.
- `ReplayService.create_replay` does not validate the baseline session
  status. Alex rejects in-progress baselines. Possible missing validation,
  not added since consolidation.md decides nothing here.
- Tag resource type and worker runtime conversions are inlined in the mapping
  modules, so alex's isolated converter unit test has no target. Covered by
  API round-trip tests.
- Behavioral difference noted: the argparse-based task `main()` exits 2 on
  invalid arguments where alex's hand-rolled parser returned 1.
- `BlobCache` here creates its root eagerly in the constructor while alex only
  creates it on `put()`, so alex's cache-miss-does-not-create-root test does
  not apply. Left as is.
