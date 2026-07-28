# DB adapter rules

This file covers repository, ORM, and transaction mechanics.

- ORM classes live one module per resource under `orm/`, extend `Base` plus
  `UUIDPrimaryKeyMixin` and `TimestampMixin`, and are exported from
  `orm/__init__.py`.
- Columns are declared with `Mapped[...]` annotations. Use a bare annotation
  when the column needs no arguments and `mapped_column(...)` only when it
  does. Nullability follows the annotation (`Mapped[str | None]` is nullable),
  never pass `nullable=` explicitly.
- ORM classes are named with an `ORM` suffix (`AccountORM`, `SecretORM`).
  Never use a `Schema` or `Table` suffix. "Schema" refers to API models in
  the FastAPI ecosystem and to the database schema in the DDL sense, so it
  stays out of class and module names here.
- Table names are singular (`__tablename__ = "agent"`, not `"agents"`).
- Enforce uniqueness with a named `UniqueConstraint` in `__table_args__`, not
  with `mapped_column(unique=True)`. A `UniqueConstraint` is backed by its own
  index, so do not also index the same column separately.
- Declare every other index as a named `Index` in `__table_args__`. Never pass
  `index=True` to `mapped_column`. It bypasses `index_name` and leaves no
  module-level constant for the migration and the repository to refer to.
- Sortable fields are declared via the `sortable_fields` ClassVar on the
  filter model (`server/base.py`), defaulting to `created`, which rides the
  UUIDv7 primary key and needs no index of its own. A field added beyond that
  default needs a matching `(field, id)` composite `Index` in
  `__table_args__`, an Alembic revision, and `paginate()` support.
- Declare foreign keys as a named `ForeignKeyConstraint` in `__table_args__`,
  never with `mapped_column(ForeignKey(...))`. An inline foreign key gets an
  auto-generated name that `violated_constraint` can never match.
- Never hand-write index or constraint names. Generate them with `index_name`,
  `unique_constraint_name`, and `foreign_key_name` from
  `orm/orm_utils.py`, and store the result as the module-level constant
  the repository compares against, so the ORM class and the
  `violated_constraint` check share one source of truth.
- String columns that back a name or other bounded identifier declare an
  explicit length via `String(...)`.
- `from_domain` passes the id and never timestamps. `to_domain` passes both
  timestamps. Immutable resources whose domain model has no `updated` field
  pass only `created`. Nothing outside a repository touches ORM models.
- Repositories live one module per resource under `repositories/`, implement
  the application-layer Protocol, and take the session in the constructor.
- Repositories never call `commit()`. Write methods end with `flush()` so the
  SQL runs and constraint violations surface inside the repository method.
  The request session commits at the REST boundary (`get_session` in
  `adapters/rest/dependencies.py`) after the route handler succeeds. Any
  exception skips the commit and pending writes roll back when the session
  closes.
- `query` methods build a filtered, unordered `Select` and pass it to the
  shared `paginate()` helper along with the filter and the id column.
  `paginate()` decodes the incoming cursor, applies the keyset
  `WHERE`/`ORDER BY` for the requested sort direction, fetches one row beyond
  the requested size to detect a next page, and returns the matching rows plus
  the next cursor.
- Unique-column lookups use `.one_or_none()`, never `.first()`, so a would-be
  invariant violation surfaces instead of being hidden.
- Translate `IntegrityError` by constraint name via
  `errors.violated_constraint`, comparing against a module-level constant.
  Never assume which constraint fired. Re-raise when the name does not match a
  known constraint.
- Constraint-risky writes run inside `session.begin_nested()` (a savepoint).
  The savepoint keeps the surrounding transaction usable after the translated
  failure.
- `get(..., exclusive=True)` on a repository emits `SELECT ... FOR UPDATE`.
  Queue-style claim methods lock internally with `FOR UPDATE SKIP LOCKED`
  instead of exposing a flag.
- Ids and timestamps are client-side defaults (`default`, `onupdate`).
  Flush sets them on the row, so repositories return `to_domain()` without
  `refresh()`.
- `TimestampMixin.updated` renews through its client-side `onupdate` hook on
  every UPDATE. Never set `updated` by hand.
- A column the database itself generates (`server_default`, trigger) must be
  fetched in the flush via `eager_defaults` RETURNING. A post-flush attribute
  access on an expired column raises `MissingGreenlet` on async sessions.
- Every schema change ships a matching Alembic revision under
  `database/migrations/versions/`.
