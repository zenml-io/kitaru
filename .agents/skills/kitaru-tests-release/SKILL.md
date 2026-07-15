---
name: kitaru-tests-release
description: Use for Kitaru tests, CI, releases.
---

# Kitaru Tests, CI, and Release Workflow

Use this when adding, moving, or debugging tests beyond the basic commands and
safety rules in `tests/AGENTS.md`, or when changing CI/release behavior.

## `primed_zenml`

`primed_zenml` eagerly initializes the ZenML store. Use it only when the test:

- actually runs a flow
- uses `KitaruClient` against real local state
- spawns threads or code paths that touch the ZenML runtime lazily

Do not add `primed_zenml` to lightweight unit tests, parser tests, serializer
tests, or simple CLI rendering tests.

## Unit and Contract Tests

- Build small stubs or `SimpleNamespace` objects instead of booting full runtime state.
- Assert on one behavior or contract at a time.
- Include regression coverage for bug fixes.

## CLI Tests

- Always call the Cyclopts app with an explicit arg list, such as `app(["--help"])`.
- Successful invocations raise `SystemExit(0)`; assert on that explicitly.
- Use `capsys` and assert on stable plain-text substrings.
- Prefer lightweight stubs/mocks for execution objects instead of full backend setup.

Keep CLI tests focused on argument parsing, command dispatch, and rendered
output, not unrelated runtime bootstrapping.

## Example-Driven Tests

`tests/test_phase*.py` tests are close to executable documentation. They prove
that examples in `examples/` still work end to end.

When adding or updating one:

- import and call the example entrypoint directly
- use `monkeypatch` to provide fake credentials or mock-response env vars
- request `primed_zenml` when the flow genuinely executes
- assert on persisted execution state, metadata, checkpoints, or artifacts, not
  only a return value

Good pattern: `tests/test_phase12_llm_example.py` registers a model alias,
injects fake API credentials, runs the example flow, and inspects recorded
metadata in ZenML.

## Live Provider Tests

Live provider tests live under `tests/live/` and are off by default. They are
for trusted manual or scheduled runs, not normal PR CI.

- Mark every live provider test with `@pytest.mark.live_llm` plus the provider
  marker: `live_openai`, `live_anthropic`, or `live_gemini`.
- `live_llm` by itself is invalid because it bypasses the deterministic
  provider-call guard but is not selected by provider workflows.
- Put slower or higher-cost checks under `@pytest.mark.provider_extended`.
- Skip cleanly when the required key is absent.
- The shared guard skips `live_openai` without `OPENAI_API_KEY`,
  `live_anthropic` without `ANTHROPIC_API_KEY`, and `live_gemini` without
  `GEMINI_API_KEY` or `GOOGLE_API_KEY`.
- Keep prompts tiny and bounded.
- Set max-turns or equivalent limits explicitly.
- Do not add live provider tests to fork PR workflows.
- Do not bypass the provider guard in deterministic tests; fake the provider or
  patch Kitaru's local call point instead.

## MCP Tests

- Keep MCP-only fixtures in `tests/mcp/conftest.py`.
- Use mocked `KitaruClient` namespaces unless the test truly needs deeper integration.
- Verify both delegation and serialized payload shape.
- Cover file/module loading behavior with `tmp_path` and
  `monkeypatch.syspath_prepend(...)` rather than real project files.

## Parallel Safety and Fixtures

- no hidden dependence on cwd unless the test sets it up itself
- no shared mutable module-level state
- no assumptions about another test having already created config, stores, or env vars
- no reliance on wall-clock ordering between tests

Put fixtures in `tests/conftest.py` when many files need them, in
`tests/mcp/conftest.py` when only MCP tests need them, and locally when only one
file needs them.

## Bug Fix Workflow

Every bug fix should come with a regression test that would have caught the
original problem:

1. Write or update the test so it captures the broken behavior.
2. Make the code change.
3. Rerun the targeted test.
4. Rerun the broader relevant suite.

If you change code after running tests, run the tests again. Do not assume the
earlier green run still counts.

## Feature Completion Checklist

When adding a new CLI command, MCP tool, or SDK feature:

- Add a non-destructive invocation to `scripts/smoke-test.sh`, such as
  `kitaru <command> --dry-run` or `kitaru <command> --help`.
- When adding, removing, renaming, or publicly documenting an example under
  `examples/`, update `examples/example-coverage.yaml` and run
  `just example-coverage-audit`.
- Check whether analytics coverage is needed. Add the event to
  `AnalyticsEvent` and wire it into the appropriate surface.
- If the CLI command is multi-word, add it to `_MULTI_TOKEN_COMMANDS` in `cli.py`.

## Python CI

`.github/workflows/ci.yml` runs on push/PR to `develop`. PR jobs run lint,
format check, YAML check, typos, type check, dependency audit, link check, base
tests on Python 3.11/3.12/3.13, and additional test lanes with `kitaru[mcp]`
installed on Python 3.11/3.12.

Push jobs also run Docker server smoke and wheel-packaging checks because those
paths may need trusted UI release credentials.

When changing Kitaru UI bundling, frontend smoke testing, Docker dashboard
packaging, or release UI selection, read `FRONTEND-TESTING.md` first.

## Docs CI

`.github/workflows/docs.yml` runs on manual dispatch, `main` pushes, and PRs
touching docs/reference-related inputs such as `docs/**`, docs generation
scripts, SDK source, `CHANGELOG.md`, `pyproject.toml`, `uv.lock`, or Wrangler
config.

It regenerates CLI/SDK reference docs and builds the FumaDocs static export on
all runs. It deploys `sdkdocs.kitaru.ai` and the `kitaru.ai/docs` redirect
worker only on `main` push or manual dispatch. PRs build only and do not create
preview Workers. Hand-written docs publish separately through GitBook Git Sync.
Marketing site deployment is handled from `zenml-io-v2`.

## Other Workflows

- `release.yml`: release automation for version bump, PyPI publish, Docker
  image publish, and GitHub Release. Do not add live provider calls here.
- `llm-integration.yml`: trusted weekly/manual live OpenAI/Anthropic provider
  checks. It has only `schedule` and `workflow_dispatch` triggers, runs paid
  tests outside PR CI, can target an exact ref/SHA, uploads logs/results only,
  and sends compact Discord failure alerts via `DISCORD_WEBHOOK_SRE`. Exact
  validation evidence is the tested SHA plus run ID and attempt in
  `llm-integration.provenance.json`. Run it again after environment-policy
  changes. Before removing required reviewers, cancel stale waiting runs. Never
  create a paid failure solely to test notifications.
- `spellcheck.yml`: typo/spell checking on `develop` pushes and non-draft PRs.
- `image-optimiser.yml`: PR-only compression for changed JPG/JPEG/PNG/WebP
  files in same-repo non-draft PRs.
- `zizmor.yml`: GitHub Actions security analysis for workflow/dependabot changes,
  plus weekly and manual runs.

## Branching and Releases

- Default branch is `develop`.
- All PRs target `develop`.
- `main` tracks the latest released version only; do not push directly.
- Releases are cut via the Release workflow: `workflow_dispatch` on `develop`
  or `v*` tag push.
- Release branches (`release/X.Y.Z`) and tags (`vX.Y.Z`) are created automatically.
- Version is maintained in `pyproject.toml` and bumped by the release workflow.
  Never hardcode it; use `importlib.metadata.version("kitaru")`.
- Update `CHANGELOG.md` under `[Unreleased]` when making user-facing changes.

Before release dispatch, check `.github/workflows/llm-integration.yml`. A weekly
green run on `develop` is a canary, not exact release evidence. If OpenAI or
Anthropic adapter/example behavior changed, trigger a manual
`llm-integration.yml` run for the exact release ref or SHA and require it to
pass, or record an explicit waiver in the release conversation. Gemini remains
local release-smoke evidence or waiver for v1.

## Release Smoke

Before releasing, run release-grade smoke with structured output:

```bash
./scripts/smoke-test.sh --release --json-out smoke-results.json
```

Add repeatable `--required-provider-area <area>` flags for changed
provider-backed behavior: `openai`, `anthropic`, `gemini-model`,
`gemini-antigravity`, `google-adk`, or `research-bot`.

For normal runtime releases, also opt into remote-stack smoke with
operator-provided private config: `--remote-stack-smoke` or
`KITARU_REMOTE_SMOKE=1`. Docs-only or no-runtime releases may record an explicit
waiver instead.

Bare `./scripts/smoke-test.sh` remains useful for local development, but it is
not enough release evidence for provider- or remote-stack-relevant changes.
Use `-s` to skip reinstall and `-k` to keep the server running afterward.
