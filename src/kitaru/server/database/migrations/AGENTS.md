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

1. Create the database state from `develop`: check out `develop` and migrate
   a database to its head revision.
2. Switch back to your branch and run
   `alembic revision --autogenerate -m "<short name>"`.
3. Modify the generated script if necessary, for example for data
   backfills or operations autogenerate cannot detect.

Index and constraint names in the generated script are inherited from the
ORM classes, which build them with the `orm_utils` helpers. Do not rename
them by hand, since the repository string-matches the ORM-side constant.
