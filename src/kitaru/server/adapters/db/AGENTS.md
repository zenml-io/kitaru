# DB adapter rules

Transaction rules live in `src/kitaru/server/AGENTS.md`. This file covers
repository and schema mechanics.

- Schemas live one module per resource under `schemas/`, extend
  `UUIDPrimaryKeyMixin` and `TimestampMixin` with `table=True`, and are
  exported from `schemas/__init__.py`.
- Table names are singular (`__tablename__ = "agent"`, not `"agents"`).
- Enforce uniqueness with a named `UniqueConstraint` in `__table_args__`, not
  with `Field(unique=True)`. A `UniqueConstraint` is backed by its own index, so
  do not also index the same column separately.
- Declare every other index as a named `Index` in `__table_args__`. Never pass
  `index=True` on a `Field`. It bypasses `index_name` and leaves no module-level
  constant for the migration and the repository to refer to.
- Never hand-write index or constraint names. Generate them with `index_name`
  and `unique_constraint_name` from `schemas/schema_utils.py`, and store the
  result as the module-level constant the repository compares against, so the
  schema and the `violated_constraint` check share one source of truth.
- String columns that back a name or other bounded identifier declare an
  explicit `max_length`.
- `from_domain` passes the id and never timestamps. `to_domain` passes both
  timestamps. Nothing outside a repository touches ORM models.
- Repositories live one module per resource under `repositories/`, implement
  the application-layer Protocol, and take the session in the constructor.
- Translate `IntegrityError` by constraint name via
  `errors.violated_constraint`, comparing against a module-level constant.
  Never assume which constraint fired. Re-raise when the name does not match a
  known constraint.
- Constraint-risky writes run inside `session.begin_nested()` so the
  translated error leaves the surrounding transaction usable (see the
  transaction rules).
- Every schema change ships a matching Alembic revision under
  `database/migrations/versions/`.
