# Docs v2 restructure — gaps, unverified claims, and open questions

Working notes for the `docs/replay-positioning` branch ahead of the ~Aug 18
launch. First pass checked Aug 3 against `origin/v2-spec-consolidated`
(spec + server) and `origin/feat/kitaru-v2-cli-620` (CLI, adapter,
plugins); **re-verified Aug 4** after major movement on both branches plus
`origin/feat/kitaru-mcp-v2-624` (MCP), `origin/v2-importer` (bundled
plugins), and `origin/examples/build-canonical-example` (canonical CLI
journey). The branches still diverge — **no single branch contains
everything the docs describe** (notably: the MCP server sits on its own
branch that predates bundled plugins and the newest CLI). The docs assume
the merged union ships. Re-verify against the actual release branch before
publish.

## Resolved since Aug 3 (docs updated Aug 4)

- ~~Langfuse importer packaging~~ — **resolved**: the separate
  `kitaru-importer-langfuse` PyPI package is gone. `langfuse`,
  `braintrust`, and `otlp` importers plus `cost`/`latency`/
  `tool-call-patterns` evaluators are bundled in the `kitaru` package and
  auto-seeded by the server, pinned to `kitaru==<server version>`. Docs
  now use `--importer langfuse@latest` with no registration step.
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
- ~~MCP server~~ — **resolved on its branch** (#624): `kitaru-mcp`
  console script, `kitaru[mcp]` extra (deps: `mcp>=2,<3`), seven tools
  gated by capability mode (`read-only`/`standard`/`destructive`).
  `agent-native/mcp-server.md` rewritten around it.

## Claims that need verification before publish

1. **Branch union** — docs describe bundled plugins + newest CLI + MCP +
   Helm/auth together; that combination exists on no branch today
   (MCP branch forked from `72b07eb9`, before bundled plugins and the
   tag-based evaluation CLI). Confirm the release branch merges all of
   it, then spot-check: `kitaru session import --tag`, `kitaru-mcp`,
   `--importer langfuse@latest`, `experiment run start --wait`.
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
   scheme and a private `kitaru-pro-server` ECR image + cloud.zenml.io
   Helm overlay (`server.pro.*` values) now exist in code, all
   undocumented here. The managed offering is clearly being built —
   decide whether launch copy should mention it.
d. **`wait()` / HITL** — unchanged: zero v2 mention in code; docs treat
   HITL as ZenML's. Confirm the posture.

## Editorial notes

- The team is building a **canonical example** on
  `examples/build-canonical-example`: `examples/document_processing/`
  (full CLI journey: import → evaluate → cohort → experiment) and a
  newer, shorter `examples/canonical_example/` (bundled plugins +
  tag-based evaluation, stops before cohorts). It is mid-refactor — two
  overlapping dirs, one walkthrough. When it merges, link it from the
  quickstart / restore `getting-started/examples.md`, and reconcile
  wording (their README teaches the same journey the docs do; note they
  use "Score the imported baselines" phrasing — banned vocabulary — in
  the example README, worth flagging to the team).
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
