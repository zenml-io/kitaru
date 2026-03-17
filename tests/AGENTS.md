# Test Suite Guidelines

This file applies to everything under `tests/`. It supplements the repo-root `AGENTS.md` with test-specific guidance. When there is no conflict, follow both documents.

## What Lives Here

The test suite mixes a few different layers of coverage:

- `tests/test_*.py`: unit, contract, and integration tests for the Python SDK and CLI
- `tests/test_phase*.py`: example-driven end-to-end tests that exercise the runnable flows in `examples/`
- `tests/mcp/test_*.py`: MCP-specific tests for the optional `kitaru[mcp]` surface
- `tests/conftest.py`: the shared isolation harness for Kitaru + ZenML state
- `tests/mcp/conftest.py`: MCP-only fixtures and sample objects

When adding a new test file, mirror the source area it protects where possible. Examples:

- `src/kitaru/runtime.py` -> `tests/test_runtime.py`
- `src/kitaru/_cli/_stacks.py` -> `tests/test_cli.py` or a focused CLI test file if it grows too large
- `src/kitaru/mcp/server.py` -> `tests/mcp/test_server.py`

## Running Tests

Use the repo-level commands from the project root:

- `just test`: run the whole suite
- `just test tests/test_runtime.py`: run one file
- `just test tests/test_runtime.py::test_name`: run one test
- `just check`: run the required formatting, lint, type, typo, YAML, actionlint, and link checks

Important repo defaults:

- `pytest` is configured with `-vv -n auto`, so tests should be safe under `xdist`
- `pythonpath = ["scripts"]`, so tests may import helpers from `scripts/` without ad hoc path hacks

Useful debugging pattern:

- If a failure looks timing- or isolation-related, rerun it serially with `just test -n 0 tests/...`

Extra installation notes:

- base suite: `uv sync`
- tests that execute real local Kitaru/ZenML flows often assume `uv sync --extra local`
- MCP tests assume `uv sync --extra mcp`

## Shared Isolation Rules

`tests/conftest.py` is the heart of the safety story. It does three important things before tests run:

1. clears Kitaru- and ZenML-related environment variables that could leak in from a developer machine
2. redirects config and home-directory lookups into `tmp_path`
3. resets global Kitaru and ZenML client/config singletons between tests

That means the suite is designed to avoid touching a real local config directory or real user state. Keep it that way.

When writing new tests:

- use `tmp_path` for filesystem work; do not write to fixed paths
- use `monkeypatch` for env vars and process-global state
- prefer test-local fixtures or helpers over hidden module-level state
- do not rely on execution order

## `primed_zenml` Fixture

`primed_zenml` eagerly initializes the ZenML store. Think of it like starting the engine before a road test: useful for tests that genuinely drive the system, but unnecessary overhead for tests that only inspect one component on the workbench.

Use `primed_zenml` only when the test:

- actually runs a flow
- uses `KitaruClient` against real local state
- spawns threads or code paths that touch the ZenML runtime lazily

Do not add `primed_zenml` to lightweight unit tests, parser tests, serializer tests, or simple CLI rendering tests. Keeping those tests cheap is what makes `-n auto` practical.

## Patterns To Copy

### Unit and contract tests

These should stay lightweight and explicit:

- build small stubs or `SimpleNamespace` objects rather than booting full runtime state
- assert on one behavior or contract at a time
- include regression coverage for bug fixes

This is the pattern used heavily in `tests/test_cli.py`, `tests/test_checkpoint.py`, and similar files.

### CLI tests

CLI tests in this repo follow a few important rules:

- always call the Cyclopts app with an explicit arg list, for example `app(["--help"])`
- successful invocations raise `SystemExit(0)`; assert on that explicitly
- use `capsys` and assert on stable plain-text substrings
- prefer lightweight stubs/mocks for execution objects rather than full backend setup

The story here is simple: keep CLI tests focused on argument parsing, command dispatch, and rendered output, not on unrelated runtime bootstrapping.

### Example-driven `test_phase*.py` tests

These are closer to executable documentation than ordinary unit tests. Their job is to prove that the examples in `examples/` still work end to end.

When adding or updating one:

- import and call the example entrypoint directly
- use `monkeypatch` to provide fake credentials or mock-response env vars
- request `primed_zenml` when the flow genuinely executes
- assert on persisted execution state, metadata, checkpoints, or artifacts, not just a return value

Good example: `tests/test_phase12_llm_example.py` registers a model alias, injects fake API credentials, runs the example flow, and then inspects the recorded metadata in ZenML.

### MCP tests

MCP tests live under `tests/mcp/` for a reason:

- keep MCP-only fixtures in `tests/mcp/conftest.py`
- use mocked `KitaruClient` namespaces unless the test truly needs deeper integration
- verify both delegation and serialized payload shape
- cover file/module loading behavior with `tmp_path` and `monkeypatch.syspath_prepend(...)` rather than real project files

## Parallel-Safety Expectations

Because the suite defaults to `-n auto`, tests should behave like good neighbors in a shared apartment:

- no hidden dependence on cwd unless the test sets it up itself
- no shared mutable module-level state
- no assumptions about another test having already created config, stores, or env vars
- no reliance on wall-clock ordering between tests

If a test is flaky under parallelism, fix the isolation problem instead of silently depending on single-process execution.

## Choosing Where Fixtures Live

Use this rule of thumb:

- if many test files across `tests/` need it, put it in `tests/conftest.py`
- if only MCP tests need it, put it in `tests/mcp/conftest.py`
- if only one file needs it, keep it local to that file unless duplication becomes painful

Keep shared fixtures small and intention-revealing. A fixture should make setup easier to understand, not hide the whole story.

## When Fixing Bugs

Every bug fix should come with a regression test that would have caught the original problem. The best pattern is:

1. write or update the test so it captures the broken behavior
2. make the code change
3. rerun the targeted test
4. rerun the broader relevant suite

If you change code after running tests, run the tests again. Do not assume the earlier green run still counts.
