# Cohort versions implementation notes

Implements the spec/v2/client_server.md changes from commit 563a38c0 (cohort
versions). task.md and worker.md did not change in that commit, so the whole
delta is the cohort-versions rework in client_server.md.

## Scope

- Cohort becomes a namespace: name, description?, agent_id, metadata,
  latest_version. No session_ids on create, no session_count, no
  /cohorts/{id}/sessions endpoint.
- New CohortVersion resource: server-assigned version number per cohort,
  optional display_version, immutable ordered member list built from the
  latest version's list minus remove_session_ids plus add_session_ids.
- Endpoints: POST/GET /v1/cohorts/{id}/versions, GET/PATCH/DELETE
  /v1/cohort-versions/{id}.
- Sessions gain a cohort_version_id list filter (EXISTS probe against
  cohort_version_session).
- ExperimentRun.cohort_id becomes cohort_version_id everywhere.
- HistoryScope value cohort becomes cohort_version, check_standalone and
  tool_lookup scope resolution follow.
- New VersionName type used by every display_version field (AgentVersion,
  PluginVersion, CohortVersion).

## Decisions

- Pattern source: plugin/plugin_version. Cohort mirrors Plugin (metadata,
  latest_version), CohortVersion mirrors PluginVersion (version bump via
  UPDATE ... RETURNING on the parent row in the insert transaction).
- VersionName separators: the spec describes the Name charset as
  [A-Za-z0-9_.-] and says VersionName widens it with + and /. The code's
  default Name separators are {-, _}, so VersionName gets its own separator
  set {-, _, ., +, /} (dot included, otherwise semver would not pass, which
  the spec calls out as a requirement). Since / is in RESERVED_SEPARATORS, a
  dedicated validate_version_name bypasses the reserved check the way
  validate_account_name does.
- Migrations: the repo consolidated everything into 001_initial.py in
  563a38c0 (002-006 deleted), so the schema change edits 001 in place rather
  than adding a new migration.

## Work log

- Created this file, surveyed the diff and current code, planned the phases:
  contracts (api_models + domain), application layer + client, ORM/repos +
  REST, tests, then integration (openapi regen, just fix/check/test).
- Launched phase 1 subagents: api_models rework and domain rework.
- Survey findings for later phases: CohortSessionsFilter dies with the
  /cohorts/{id}/sessions endpoint, experiment fan-out switches to the
  cohort_version_id session filter per the spec (members are listed through
  the session filter, fan-out order is no longer the stored member order).
  session_node find_latest_by_cache_key_in_cohort becomes a cohort-version
  scoped lookup fed from run.cohort_version_id in replay_service.

- api_models done: cohort.py reworked, cohort_version.py added,
  SessionListParams.cohort_version_id, experiment_run rename, HistoryScope
  rename. No CohortVersionListParams, the versions listing takes plain
  ListParams like agent/evaluator version listings.
- domain done: VersionName in names.py (AgentVersion's local display_version
  validator and InvalidDisplayVersion were removed in favor of it),
  cohort.py reworked, cohort_version.py added with pure
  apply_membership_delta helper, experiment_run and replay_config renames,
  SessionInUse wording.
- client done: cohorts resource reworked (create_version, list_versions,
  iter_versions), new cohort_versions resource, api_client wiring. Sessions
  resource needed no change, it forwards SessionListParams whole.
- Launched the application layer agent (models, interfaces, services).
- Application layer done: CohortVersionRepository protocol and
  CohortVersionService added, CohortService trimmed to namespace CRUD,
  experiment fan-out reads members via SessionFilter(cohort_version_id=...),
  replay tool lookup renamed to find_latest_by_cache_key_in_cohort_version.
  CohortVersion.version defaults to 0 pre-assignment (AgentVersion
  precedent), CohortVersionIdNotFound added for by-id lookups.
- Launched phase 3: DB layer (ORM, 001 migration in place, SQL repos) and
  REST layer (mapping, routers, dependencies, app) in parallel.

- REST layer done: cohort mapping/router rework, new cohort_versions
  mapping/router (/v1/cohort-versions, tag cohort-versions), dependencies
  and app wiring, experiment/replay docstring updates. composition.py
  needed no change (no cohort wiring there).

- DB layer done: cohort/cohort_version/cohort_version_session ORM, 001
  migration edited in place and schema-diff verified against a scratch
  Postgres, SQL repositories including the latest_version bump, session
  filter EXISTS probe, session_node lookup rename. CohortVersionInUse added
  (experiment_run.cohort_version_id restricts).
- Regenerated openapi/openapi.json from the app.
- Launched the conftest fakes agent, parallel test agents follow it.
- conftest done: FakeCohortVersionRepository wired to the session fake for
  the cohort_version_id filter and the SessionInUse guard, ReplayServices
  gained cohort_versions, create_experiment_run takes cohort_version_id.
  Known fake limitation: deleting a cohort in the fake does not cascade its
  versions, unlike the DB.
- Launched three parallel test agents (cohort server tests, experiment and
  replay tests, session and client tests) and the sandbox driver agent.

- Experiment and replay tests migrated and green (120 tests). Behavior
  note: run fan-out order is no longer the stored member order, members are
  read through the cohort_version_id session filter whose default sort is
  created descending. The spec only promises one replay per member session,
  the order assertion was dropped from the test.
- Sandbox run_e2e.py updated: namespace create plus first version with
  add_session_ids, cohort_version_id on runs, cohort_version history scope,
  final-state tables adjusted. Stays untracked.

- Cohort server tests done (204 tests): reworked cohort
  repository/service/api tests, four new cohort version test files
  mirroring the agent version layout, VersionName cases in test_names,
  route manifest updated for the five new routes and the removed sessions
  route.
- Session and client tests migrated and green (165 tests): renamed
  session_node lookup coverage, new cohort_version_id session filter tests
  at repository and API level, SessionInUse guard moved to version
  membership, client cohort/cohort_versions resource tests reworked.

- Integration: updated the CHANGELOG cohort entries to the version model,
  regenerated openapi/openapi.json, just fix and just check pass, full
  suite green (2025 passed). Amended into the v2 spec commit, not pushed.
- scripts/check_openapi.sh assumes a bare `python` on PATH and fails
  locally under uv-only setups, worked around by regenerating and diffing
  directly. Left the script untouched.

## Issues

- domain rework removed MAX_DISPLAY_VERSION_LENGTH from
  domain/agent_version.py (superseded by VersionName) while
  adapters/db/orm/agent_version.py still imported it. Fixed by hand, the
  column is String(255) now, matching plugin_version and cohort_version.
- The cohort_version_repository interface and cohort_version_service delete
  docstrings missed CohortVersionInUse, added by hand.
