# Test Suite Guidelines

This file applies to everything under `tests/`. It supplements the repo-root
`AGENTS.md`; when there is no conflict, follow both documents.

## What Lives Here

- `tests/test_*.py`: unit, contract, and integration tests for the SDK and CLI
- `tests/test_phase*.py`: example-driven end-to-end tests for runnable flows in `examples/`
- `tests/mcp/test_*.py`: MCP-specific tests for the optional `kitaru[mcp]` surface
- `tests/live/test_*.py`: paid/external provider checks, always marked
  `live_llm` plus a provider-specific marker and excluded from default pytest runs
- `tests/conftest.py`: shared isolation harness and provider-call guard
- `tests/mcp/conftest.py`: MCP-only fixtures and sample objects

When adding a new test file, mirror the source area it protects where possible.
For example, `src/kitaru/runtime.py` maps naturally to `tests/test_runtime.py`.

## Running Tests

Run tests from the repo root:

- `just test`: whole default suite
- `just test tests/test_runtime.py`: one file
- `just test tests/test_runtime.py::test_name`: one test
- `just check`: formatting, lint, type, typo, YAML, actionlint, and link checks

Default pytest uses `-vv -n auto -m 'not live_llm'`, so tests must be safe under
parallel execution. If a failure looks timing- or isolation-related, rerun it
serially with `just test -n 0 tests/...`.

Use `uv sync` for the base suite. Tests that execute real local Kitaru/ZenML
flows often assume `uv sync --extra local`; MCP tests assume `uv sync --extra mcp`.

## Shared Isolation Rules

`tests/conftest.py` protects local state before tests run. It clears Kitaru and
ZenML environment variables, redirects config/home lookups into `tmp_path`,
resets global clients/config singletons, and blocks accidental provider calls
from unmarked deterministic tests.

When writing new tests:

- use `tmp_path` for filesystem work; do not write to fixed paths
- use `monkeypatch` for env vars and process-global state
- prefer test-local fixtures or helpers over hidden module-level state
- do not rely on execution order
- do not bypass the provider guard in deterministic tests; fake the provider or
  patch Kitaru's local call point instead

## Live Provider Tests

Live provider tests live under `tests/live/` and are off by default. They are
for trusted manual or scheduled runs, not normal PR CI.

- Mark every live provider test with `@pytest.mark.live_llm` plus exactly the
  provider marker it needs, such as `live_openai`, `live_anthropic`, or
  `live_gemini`.
- Use `@pytest.mark.provider_extended` for slower or higher-cost checks.
- Skip cleanly when required credentials are absent.
- Keep prompts tiny and set max-turns or equivalent limits explicitly.
- Do not add live provider tests to fork PR workflows.

## More Test Guidance

For fixture placement, `primed_zenml`, CLI-test patterns, example-driven tests,
MCP tests, and bug-fix regression-test workflow, load the
`kitaru-tests-release` skill.
