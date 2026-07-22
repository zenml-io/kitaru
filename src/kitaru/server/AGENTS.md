# Server persistence rules

Rules for everything under `src/kitaru/server/` that touches the database.

## One transaction per use case

- The REST session dependency (`get_session` in
  `adapters/rest/dependencies.py`) commits after the route handler succeeds.
  Any exception skips the commit and pending writes roll back when the session
  closes.
- Repositories never call `commit()`. Write methods end with `flush()` so the
  SQL runs and constraint violations surface inside the repository method.
- Wrap constraint-risky writes in `session.begin_nested()` (a savepoint) and
  translate `IntegrityError` into the matching domain error. The savepoint
  keeps the surrounding transaction usable after the failure.
- A use case that must commit before the request ends, for example to release
  row locks ahead of slow work, opens an explicitly scoped transaction on its
  own session. Never commit the request session mid-request, since commit is
  transaction-wide and would also commit unrelated pending writes.

## Generated values

- Ids and timestamps are client-side defaults (`default_factory`, `onupdate`).
  Flush sets them on the row, so repositories return `to_domain()` without
  `refresh()`.
- `TimestampMixin.updated` renews through its client-side `onupdate` hook on
  every UPDATE. Never set `updated` by hand.
- A column the database itself generates (`server_default`, trigger) must be
  fetched in the flush via `eager_defaults` RETURNING. A post-flush attribute
  access on an expired column raises `MissingGreenlet` on async sessions.
