# Docs v2 restructure — gaps, unverified claims, and open questions

Working notes for the `docs/vision-restructure` branch ahead of the ~Aug 18
launch. Everything here was checked against `origin/v2-spec-consolidated`
(spec + server) and `origin/feat/kitaru-v2-cli-620` (CLI, adapter, plugins)
as of Aug 3. **The two branches have diverged** — the CLI, PydanticAI
adapter, and importer plugins exist only on the CLI branch; the newest
server/auth work only on the spec branch. The docs assume the merged union
ships. Re-verify against the actual release branch before publish.

## Claims that need verification before publish

1. **Langfuse importer packaging** — docs register it as
   `--package "kitaru-importer-langfuse==0.21.0"`. The plugin exists as a
   repo workspace member (`plugins/langfuse`); whether it publishes to PyPI
   under that name/version is unconfirmed. `TODO(v2-launch)` marker in
   `getting-started/import-your-traces.md`.
2. **Published server image** — `deploy/docker.md` documents Compose
   building from source; there is no confirmed published v2 server image
   name/tag. `TODO(v2-launch)` marker in place.
3. **`kitaru evaluator test` / `importer test` exact flags** — commands
   exist; the documented invocations use `--entrypoint` (+ `--payload` for
   importers) but were not run end to end.
4. **PyPI extras at launch** — docs use `kitaru[cli,pydantic-ai]`
   (+ `worker`, `server`), matching the v2 branch `pyproject.toml`
   (version 0.21.0). Confirm the release publishes with these extras.
5. **`run_sync` on `KitaruAgent`** — inherited from PydanticAI's
   `WrapperAgent`; the v2 example uses `await agent.run(...)`. Smoke-test
   `run_sync` before it stays in the quickstart/README.
6. **Compose file location/name** — docs say `docker compose up -d` from
   the repo root (`docker-compose.yml` exists on the CLI branch, dev
   Dockerfile, auth scheme `none`). Confirm it ships in the release tree.

## Scoped down pending MVP confirmation (documented as roadmap, not shipped)

- **`llm` ("simulate") tool policy** — accepted/stored by the API, but the
  PydanticAI adapter raises on it. Documented with a warning hint in
  `guides/tool-policies.md` and `adapters/pydantic-ai.md`.
- **Adapters other than PydanticAI** — the five v1 adapter pages are
  deleted; `adapters/README.md` says "more on the way" with a
  `TODO(v2-launch)` to confirm the v2.0 adapter list (open question below).
- **MCP server** — no `kitaru[mcp]` extra or MCP tools exist on the v2
  branches. `agent-native/mcp-server.md` now teaches CLI + Python client as
  the assistant surfaces, with a TODO to restore MCP when it ships.
- **Dashboard / UI** — no v2 dashboard exists in code. Docs mention no
  dashboard; the README's v1 dashboard screenshot was removed. If a v2 UI
  lands before launch, re-add a visual.
- **Helm** — the repo's chart is the v1 ZenML-based chart. `deploy/helm.md`
  deleted; `deploy/README.md` says "Helm chart on the roadmap".
- **CLI verbs for the loop** — the v2 CLI covers login/context/config,
  agent/importer/evaluator registration, workers, and jobs. There are **no**
  `session`, `replay`, `cohort`, `experiment`, `import`, `secret`,
  `api-key`, or `account` nouns. All loop snippets use the (implemented)
  async Python client; TODO markers sit where a CLI verb is clearly coming
  (`regression-suite.md` CI gate, `deploy/authentication.md`,
  `deploy/secrets.md`).
- **Evaluation `source` field and pairwise verdicts** — the brief's
  settled data model (source CODE/LLM/HUMAN, `group_id` pairwise) is **not
  in the code**; rows distinguish provenance only via
  `evaluator_version_id`/`task_id` null-ness, and data types are
  `float/bool/str/categorical` (derived), not NUMERIC/CATEGORICAL/BOOLEAN/
  TEXT. Docs follow the code. If the settled model lands before launch,
  `concepts/evaluators.md` needs a pass.
- **Calibration / shadow mode / agentic triage / GitHub connect** — no
  implementation. Calibration is documented as a *workflow* (compare
  evaluator rows against human rows) rather than a feature; the rest are
  not documented.
- **Tool-policy names** — the brief's `recorded/block/live:sandbox/simulate`
  do not exist in code; the shipped names are
  `history/static/passthrough/llm` with `on_miss: fail|passthrough|
  error_result`. Docs use the shipped names and explain the safety
  postures in `guides/tool-policies.md`.

## Flagged open questions (not resolved in the docs — need decisions)

a. **Canonical URLs** — README and docs keep `docs.zenml.io/kitaru` and
   `kitaru.ai` links as-is per instruction. The launch URL decision
   (cloud.zenml.io vs zenml.io/product/kitaru vs kitaru.ai) will require a
   link sweep (README nav, `docs/book/AGENTS.md` link rules, sdkdocs links).
b. **Exact adapter list shipping in v2.0** — only PydanticAI is ported.
   Decide whether any v1 adapters make the launch; update
   `adapters/README.md` table and README accordingly.
c. **Pricing / managed offering wording** — no managed offering is
   mentioned anywhere in the rewritten docs (the `control-plane` auth
   scheme exists in code but is undocumented). Needs a decision if launch
   copy should hint at it.
d. **`wait()` / HITL** — given zero v2 mention in code, the docs treat HITL
   as fully ZenML's ("ZenML runs agents durably; Kitaru replays and
   improves them") and mention `wait()` nowhere. Confirm this is the
   intended posture.

## Editorial notes

- The old branch content (durable experiments, `kitaru.Score`,
  `@kitaru.scorer`, `kitaru.diff`, `imported_mode=`, verdicts/protections,
  `client.executions.*`) documented the **v1 codebase's** API and was
  removed wholesale — none of it exists in the v2 rewrite.
- `examples/` in the repo (except `integrations/pydantic_ai_v2`) still
  target v1 and are dead against v2 src; `getting-started/examples.md` was
  removed until a v2 example set exists.
- `GETTING_STARTED.md` at the repo root is v1 and is no longer linked from
  the README; refresh or delete it before launch.
- All removed doc paths have redirects in `docs/book/.gitbook.yaml`. The
  hosted diagram PNGs referenced by the deleted concept pages
  (assets.kitaru.ai) are now orphaned; a v2 architecture diagram for
  `concepts/under-the-hood.md` would be worth commissioning.
