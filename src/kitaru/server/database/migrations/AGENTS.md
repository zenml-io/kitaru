# Migration rules

Rules for the Alembic migration scripts under `versions/`.

## One migration per PR

- A PR contains at most one new migration script. Squash schema changes into
  a single revision before opening the PR.

## Naming

- Revision ids are an increasing number plus a short name, for example
  `001_initial`, `002_add_agent_labels`. The file name matches the revision
  id.

## ALWAYS use autogenerate

Never hand-write a migration from scratch. Generate it against the schema
state of `develop`:

1. Create a separate baseline worktree from current `origin/develop` under the repository's `.worktrees/` directory. Keep the active feature checkout on its branch.
2. From the baseline worktree, migrate a uniquely named disposable database to its head revision. Do not reuse another task's database.
3. From the feature checkout, point Alembic at that same disposable database and run `alembic revision --autogenerate -m "<short name>"`. The database must still contain the baseline schema when autogenerate compares it with the feature branch's ORM metadata.
4. Review and modify the generated script as needed for backfills or operations autogenerate cannot detect. Validate it with the migration checks.
5. Remove the baseline worktree and drop only the disposable database created for this task.

Index and constraint names in the generated script are inherited from the
ORM classes, which build them with the `orm_utils` helpers. Do not rename
them by hand, since the repository string-matches the ORM-side constant.
