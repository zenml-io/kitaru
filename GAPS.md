# Docs v2 restructure — gaps, unverified claims, and open questions

Working notes for the `docs/replay-positioning` branch ahead of the ~Aug 18
launch. First pass checked Aug 3 against `origin/v2-spec-consolidated`
(spec + server) and `origin/feat/kitaru-v2-cli-620` (CLI, adapter,
plugins); **re-verified Aug 4** after major movement on both branches plus
`origin/feat/kitaru-mcp-v2-624` (MCP), `origin/v2-importer` (bundled
plugins), and `origin/examples/build-canonical-example` (canonical CLI
journey); **re-verified twice Aug 5** (deltas below). As of Aug 5
afternoon the union is nearly closed: **`cli-620` now contains the CLI,
bundled plugins, the adapter, AND the MCP server** (merged 13:36, with
the MCP surface narrowed in the process). What is *not* yet in any
merged line: the `feature/client-config` branch (client config file,
`KITARU_TASK_TOKEN` → `KITARU_API_TOKEN` rename). The docs assume the
merged union ships. Re-verify against the actual release branch before
publish.

## Aug 5 overnight deltas (docs updated Aug 5)

- **Session selectors everywhere** — `cohort create` now snapshots a
  selection into version 1 directly (`--session`/`--sessions-file`/
  `--tag`/`--cohort`/`--filter`/`--all` + `--display-version`);
  `session evaluate` gained `--agent`/`--cohort`/`--filter`;
  `session list` gained `--tag`/`--cohort`. Cohorts/regression-suite/
  evaluators pages updated to the one-command forms.
- **`session import --tag` is repeatable**; tags are applied by the CLI
  after the import job settles (still requires `--wait`; receipt gains
  `tagged_session_count`). Docs wording already matched.
- **Adapter `agent_id` is now optional** — task-bound (replay) runs
  infer the agent from the task's agent version; explicit id still the
  way for production recording. Adapter page updated.
- **API key rotation (#662)** — `POST /v1/api-keys/{id}/rotate`, client
  `api_keys.rotate(...)` with `retain_period_minutes` grace window.
  Documented in `deploy/authentication.md` (still client-only, no CLI
  noun).
- **`KITARU_REPLAY_ID` unchanged for users** — it moved to the agent
  task details server-side but is still set in the task subprocess env;
  no doc changes needed.
- **Default-plugin seeding regressed** (`aa766dae`, Aug 4): server
  startup auto-seeding (`ensure_default_plugins`) and the
  `kitaru==<version>` package pin + air-gap skip
  (`filter_uninstalled_requirements`) were **removed**. Seeding is now a
  manual dev script — `scripts/seed_default_plugins.py`, which registers
  the six plugins as ordinary script-source versions via the public API
  (the canonical example runs it explicitly; nothing runs it
  automatically, and the script isn't shipped in the installed package).
  Docs softened to "seeded into the server as part of setting it up"
  with a `TODO(v2-launch)` in `import-your-traces.md`; the air-gap claim
  was dropped from `deploy/workers.md`. **Confirm the final shipped
  mechanism before publish** — a pip-installed user currently has no way
  to seed.

## Aug 5 afternoon deltas (docs updated Aug 5)

- **MCP merged into `cli-620`** and its surface narrowed
  (`f0292be9`): still seven tools, but `kitaru_workflow_start` was
  replaced by the narrower `kitaru_session_import` (import from an
  already-uploaded blob) — starting replays/evaluations/experiment runs
  stays with the CLI/client. Connection resolution simplified:
  `--server` > `KITARU_MCP_SERVER` > `KITARU_API_URL`, hard failure
  without one; `--context`, the CLI-context fallback, and `--retries`
  are gone. `agent-native/mcp-server.md` updated. The team maintains
  its own detailed MCP page on `cli-620`'s (otherwise stale) docs tree
  — expect a merge collision there plus `guides/configuration.md` /
  `guides/projects.md`; reconcile at merge time.
- **Permission layer + admin flag (#663)** — accounts gained
  `is_admin`; creating/deactivating accounts and granting admin are
  admin-only (everything else stays open to any authenticated account);
  bootstrap `default` account is an admin; accounts can't change their
  own flag; service accounts can't be admins. `deploy/authentication.md`
  updated.
- **Adapter authenticates with the task token** (`5a761c7a`): `api_key`
  falls back to `KITARU_TASK_TOKEN` then `KITARU_API_KEY`. Adapter page
  updated.
- **Canonical example is complete and CI-integrated** (`4ed1a745`),
  with a team-written `docs/book/getting-started/examples.md` on that
  branch. A matching Examples page was added to this branch (toc +
  redirect removed) — reconcile the two versions at merge.
- **Client config landed (#666, merged ~14:49 and propagated to
  `v2-importer` and `cli-620`; docs swept same day)**:
  - `KITARU_TASK_TOKEN` → **`KITARU_API_TOKEN`** everywhere; docs
    renamed (workers concept, authentication, MCP, adapter pages).
  - `KitaruAPIClient.from_env()` is **gone** — precedence folded into
    the constructor: URL = `KITARU_API_URL` > stored server URL (else
    error); credential = `KITARU_API_TOKEN` > `KITARU_API_KEY` > stored
    login credential > anonymous. All nine doc snippets now construct
    `KitaruAPIClient()` directly.
  - The adapter (`5b65b0cc`, `v2-importer`) **dropped `api_url` /
    `api_key` params** — it uses the client's resolution. Note the copy
    of the adapter on `cli-620` still has the old params until the next
    merge from `v2-importer`; docs follow the newer surface.
  - The team's own MCP page on `cli-620` still says
    "`KITARU_TASK_TOKEN` is deliberately ignored" — stale on their
    side; flag at merge time.

## Resolved since Aug 3 (docs updated Aug 4)

- ~~Langfuse importer packaging~~ — **resolved, mechanism in flux**: the
  separate `kitaru-importer-langfuse` PyPI package is gone. `langfuse`,
  `braintrust`, and `otlp` importers plus `cost`/`latency`/
  `tool-call-patterns` evaluators are bundled in the `kitaru` package.
  Docs use `--importer langfuse@latest` with no code to write. **But
  see the Aug 5 seeding note below** — startup auto-seeding was removed
  again on Aug 4.
- ~~Published server image~~ — **resolved**: `release.yml` publishes
  `zenmldocker/kitaru-server:<version>` (+`latest`) and a
  `zenmldocker/kitaru` client image. `deploy/docker.md` now documents the
  server image.
- ~~Helm~~ — **resolved**: first-party chart at `helm/` (server-only,
  external Postgres, migration hook Job, ingress/HTTPRoute, HPA), pushed
  as an OCI artifact to ECR Public. `deploy/helm.md` restored. **But see
  open item 2 below on the OCI path.**
- ~~CLI verbs for the loop~~ — **resolved**: the CLI now covers the whole
  journey: `session import/list/get/nodes/evaluate` (with `--tag`/`--all`
  selection), `cohort create/version create/...`, `experiment create`,
  `experiment run start --wait` (CI-gate semantics: `remote_failed` maps
  to a nonzero exit), `evaluation list/get`, plus registration, workers,
  jobs, `login/status/info/doctor/schema`. Docs now show CLI-first flows
  with the Python client for what only it can do.
- ~~MCP server~~ — **resolved and merged into `cli-620`** (#624 +
  Aug 5 merge): `kitaru-mcp` console script, `kitaru[mcp]` extra (deps:
  `mcp>=2,<3`), seven tools gated by capability mode
  (`read-only`/`standard`/`destructive`). `agent-native/mcp-server.md`
  rewritten around it; surface changes from the merge are in the Aug 5
  afternoon deltas.

## Claims that need verification before publish

1. **Branch union** — mostly closed: as of Aug 5 13:36, `cli-620`
   carries the CLI, bundled plugins, adapter, and MCP server together
   (`23e7bed5` "Merge MCP into CLI" + `f0292be9`). Remaining spot-check
   on the release branch: `kitaru session import --tag`, `kitaru-mcp`,
   `--importer langfuse@latest`, `experiment run start --wait`,
   `cohort create --tag`. Watch `feature/client-config` (below) — it
   renames a documented env var.
2. **Helm chart OCI path** — release workflow pushes to ECR Public alias
   `zenml` (⇒ `oci://public.ecr.aws/zenml/kitaru`) but the in-repo
   `helm/README.md` says `oci://public.ecr.aws/kitaru/kitaru`. One is
   wrong. `deploy/helm.md` uses the workflow's path with a
   `TODO(v2-launch)` marker — verify with a real `helm install` and fix
   whichever side is stale. Also: `helm/Chart.yaml` description still
   says "Durable execution for AI agents" — banned framing; fix in the
   chart repo-side.
3. **`kitaru login --local`** — the command exists, but the canonical
   example calls it work-in-progress and connects via `KITARU_API_URL`
   only. Docs (installation, README) now use the env-var path for local
   and `kitaru login <url>` for shared servers. Re-check login UX before
   launch.
4. **`kitaru evaluator test` / `importer test` exact flags** — commands
   exist with `--entrypoint` (+ `--payload` for importers); still not run
   end to end by us. The canonical example uses the same shapes.
5. **PyPI extras at launch** — docs use `kitaru[cli,pydantic-ai]`
   (+ `worker`, `server`, `mcp`, `otel`). `cli-620` has no `mcp` extra
   (it lives on the MCP branch); confirm the merged release publishes all
   six.
6. **`run_sync` on `KitaruAgent`** — inherited from PydanticAI's
   `WrapperAgent`; the team's examples use `await agent.run(...)`.
   Smoke-test `run_sync` before it stays in the quickstart/README.
7. **Worker vs API key under `auth none`** — worker code reads
   `KITARU_API_KEY` as required env in one path while the local compose
   flow (auth `none`) runs without keys in the canonical example. Confirm
   a keyless local worker works before the quickstart's "just
   `kitaru worker start`" stands.

## Scoped down pending MVP confirmation (documented as roadmap, not shipped)

- **`llm` ("simulate") tool policy** — accepted/stored by the API, but
  the PydanticAI adapter raises on it. Documented with a warning hint in
  `guides/tool-policies.md`.
- **Adapters other than PydanticAI** — only PydanticAI is ported;
  `adapters/README.md` keeps its `TODO(v2-launch)` on the v2.0 adapter
  list (open question below).
- **Dashboard / UI** — still no v2 dashboard in code. Docs mention none.
- **CLI verbs still absent** — no `secret`, `api-key`, `account`,
  `replay` (single-session), or `tag` nouns. Single-session replay and
  key/account management are Python-client-only; TODO markers sit in
  `deploy/authentication.md` and `deploy/secrets.md`.
- **Evaluation `source` field and pairwise verdicts** — unchanged: the
  brief's settled model (source CODE/LLM/HUMAN, `group_id`) is not in the
  code; provenance is derived from `evaluator_version_id`/`task_id`
  null-ness. Docs follow the code.
- **Calibration / shadow mode / agentic triage / GitHub connect** — no
  implementation; calibration stays documented as a workflow.

## Flagged open questions (not resolved in the docs — need decisions)

a. **Canonical URLs** — README and docs keep `docs.zenml.io/kitaru` and
   `kitaru.ai` links as-is per instruction. The launch URL decision will
   require a link sweep.
b. **Exact adapter list shipping in v2.0** — only PydanticAI is ported.
   Update `adapters/README.md` and the README when decided.
c. **Pricing / managed offering wording** — the `control_plane` auth
   scheme, a private `kitaru-pro-server` ECR image + cloud.zenml.io
   Helm overlay (`server.pro.*` values), and now ZenML Pro CORS support
   (#664, Aug 5) all exist in code, all undocumented here. The managed
   offering is clearly being built — decide whether launch copy should
   mention it.
d. **`wait()` / HITL** — unchanged: zero v2 mention in code; docs treat
   HITL as ZenML's. Confirm the posture.

## Editorial notes

- The team is building a **canonical example** on
  `examples/build-canonical-example`: `examples/document_processing/`
  (full CLI journey: import → evaluate → cohort → experiment) and a
  newer, shorter `examples/canonical_example/` (bundled plugins +
  tag-based evaluation, stops before cohorts). It is mid-refactor — two
  overlapping dirs, one walkthrough. `getting-started/examples.md` was
  restored Aug 5 pointing at `canonical_example/`; reconcile
  wording (their README teaches the same journey the docs do; note they
  use "Score the imported baselines" phrasing — banned vocabulary — in
  the example README, worth flagging to the team). As of Aug 5 the
  walkthrough is complete ("canonical returns workflow") and its README
  ends with "Open http://localhost:8000 to compare each imported
  session with its replay" — but no server-served UI exists in any
  branch's code; flag to the team as either aspirational or a missing
  piece.
- There is still no standalone `kitaru replay` verb by design: at
  population scale replay is `experiment run start`; single-session
  replay is the client's `replays.create`. Docs are consistent with
  this; keep it that way.
- `GETTING_STARTED.md` at the repo root still documents the deleted v1
  CLI (`kitaru init`, stacks, executions); refresh or delete before
  launch.
- Old v1 doc content (durable experiments, `kitaru.Score`,
  `@kitaru.scorer`, `kitaru.diff`, verdicts, `client.executions.*`) was
  removed wholesale on Aug 3 — none of it exists in v2.
- All removed doc paths have redirects in `docs/book/.gitbook.yaml`
  (the `deploy/helm` redirect was dropped Aug 4 when the page returned).
  The hosted diagram PNGs referenced by deleted concept pages are
  orphaned; a v2 architecture diagram for `concepts/under-the-hood.md`
  would be worth commissioning.
