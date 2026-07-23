# Implementation notes

Decisions taken during the implementation of `design/design.md` that the
design doc did not specify, plus issues that came up and how they were
resolved. Ordered roughly by implementation phase.

## Orchestration

- Implementation order: tags and agents/agent-versions first (parallel),
  then sessions/session-nodes, then cohorts, then replay-configs/replays,
  then experiments/experiment-runs, then the runner and the e2e user code.
- Each vertical initially shipped its own Alembic revision to keep its
  postgres tests runnable. The revisions are squashed into a single one
  before the PR per the one-migration-per-PR rule.

## Decisions

### Tags

- `tag_link.resource_type` is stored as `varchar(64)`, not a Postgres ENUM
  type. The `StrEnum` in the domain and API layers enforces the value set
  and adding a resource type stays a code change.
- The `TagResourceType` enum exists twice (domain and `api_models`) with
  conversion in the REST mapping, since the domain layer imports nothing
  from other layers.
- Attach returns a `TagLinkResponse` with 201. Detach via a nonexistent
  tag id returns 404 `TagLinkNotFound` rather than `TagNotFound`.
- Link creation does not validate that the referenced resource exists.
  The polymorphic reference has no FK and most target tables did not
  exist when tags landed.
- No `owner_id` index on `tag`, matching the design doc which specifies
  one for `secret` but not for `tag`.

## Issues

### Pre-existing repo state

- `just check` typecheck reports 422 pre-existing diagnostics, all in
  `examples/` referencing the deleted v1 API. Identical on the base
  branch, zero diagnostics in `src/` or `tests/`. Rewriting the examples
  against the v2 SDK is out of scope here.
- The `actionlint` and `lychee` binaries are not installed on this
  machine, so those two `just check` recipes cannot run locally.

### Tags

- Alembic autogenerate emitted `sqlmodel.sql.sqltypes.AutoString` without
  the matching import, added by hand (revision 003 needed the same).
- Attaching a link races a concurrent tag delete: the service checks the
  tag exists before insert, but a delete between check and flush surfaces
  the raw FK IntegrityError. Closing it needs named-FK constraint
  translation support in `schema_utils`.
