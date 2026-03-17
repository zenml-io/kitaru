# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running tests

```bash
just test                             # Run all tests (uses pytest-xdist: -n auto)
just test tests/test_foo.py           # Run a single test file
just test tests/test_foo.py::test_bar # Run a single test
just test -x                          # Stop on first failure
uv run pytest tests/test_foo.py -k "test_bar" --no-header  # Filter by name
```

Tests run with `pytest-xdist` (`-n auto`) by default via `pyproject.toml` addopts. MCP tests (`tests/mcp/`) require the `kitaru[mcp]` extra and run in a separate CI lane.

## Test isolation model

Every test gets an isolated ZenML + Kitaru environment automatically via the `isolated_zenml_global_config` autouse fixture in `conftest.py`. This fixture:

- Creates a fresh `tmp_path/.zenml/` config directory and redirects `ENV_ZENML_CONFIG_PATH` to it
- Strips all `KITARU_*` and `ZENML_*` environment variables
- Resets ZenML singletons (`GlobalConfiguration._reset_instance()`, `Client._reset_instance()`)
- Resets Kitaru's runtime configuration (`_reset_runtime_configuration()`)
- Redirects `Path.home()` and `click.get_app_dir()` into `tmp_path` so file-based config never touches real user state

This isolation is why xdist parallelism works — each test gets its own mini-ZenML universe.

## Key fixtures

| Fixture | Scope | When to use |
|---|---|---|
| `isolated_zenml_global_config` | autouse, every test | Automatic — never request manually |
| `primed_zenml` | per-test, opt-in | Only for tests that run actual flows, use `KitaruClient` against real state, or spawn threads touching ZenML runtime. Eagerly initializes ZenML's store to avoid lazy-init races. **Do not use in lightweight unit tests** — it slows down xdist. |

## Test categories and patterns

### Unit tests (the majority)

Most test files are pure unit tests that mock ZenML internals. They follow these patterns:

- **Stub factories** — Helper functions like `_execution_stub()`, `_stack_create_result_stub()`, `_DummyRun`, `_FakeStep` etc. build lightweight `SimpleNamespace` or dataclass objects that look like ZenML responses. Prefer `SimpleNamespace` over full model construction.
- **Runtime scope setup** — Tests for primitives (`llm`, `log`, `save`, `load`, `wait`) use `_flow_scope()` and `_checkpoint_scope()` context managers from `kitaru.runtime` to simulate being inside a flow/checkpoint.
- **Patch targets** — Patches go on the module that imports the name, e.g. `patch("kitaru.llm._execute_llm_call")`, not on the original definition site.

### CLI tests (`test_cli.py`)

CLI commands (cyclopts) raise `SystemExit(0)` on success. Every CLI test must:

```python
with pytest.raises(SystemExit) as exc_info:
    app(["executions", "get", "kr-123"])
assert exc_info.value.code == 0
```

- Always pass an explicit arg list: `app(["--help"])`, never bare `app()` (reads `sys.argv`)
- Use `capsys` to capture output — assert on plain-text substrings (the non-TTY code path keeps this stable)
- Create `Console()` lazily inside helpers, not at module level (pytest replaces streams after import)

### Integration / example tests (`test_phase*`)

These files import and run real example flows from the `examples/` directory. They:

- Require the `primed_zenml` fixture
- Actually execute flows against an isolated ZenML store
- Some use threading for async scenarios (e.g. `test_phase15_wait_example.py` starts a flow in a background thread, then drives input/resume from the main thread)
- Guard on optional features: `pytest.skip()` when the installed ZenML build lacks needed functionality (e.g. wait support)

### MCP tests (`tests/mcp/`)

- Separate `conftest.py` with `mock_kitaru_client`, `sample_execution`, `sample_artifact` fixtures
- Run in a dedicated CI lane with `kitaru[mcp]` installed
- Import from `kitaru.mcp.server` — tests verify tool functions return correct dict shapes

### Import boundary tests (`test_mcp_import_guard.py`)

Test that the base SDK imports cleanly without optional extras (MCP, PydanticAI). Use `monkeypatch.setitem(sys.modules, "mcp", None)` to simulate missing packages.

### Contract tests (`test_dockerfile_contract.py`, `test_bootstrap.py`)

Verify structural invariants: Dockerfile build-arg alignment with pyproject.toml pins, console script entrypoint wiring, version metadata handling. These read files directly with `Path` rather than importing SDK code.

### Adapter tests (`test_pydantic_ai_adapter.py`)

- Guarded by `pytest.importorskip("pydantic_ai")` at module level
- Uses `pytestmark = [pytest.mark.anyio]` for async test support
- Tests can silently skip if pydantic_ai isn't installed — verify locally when the dependency is available

## Common patterns to follow

### Testing error boundaries

Use `pytest.raises` with `match=` regex for specific error messages:

```python
with pytest.raises(KitaruContextError, match=r"inside a @flow"):
    llm("hello")
```

### Parametrized tests

Used extensively for exhaustive coverage of enum variants, scope combinations, etc.:

```python
@pytest.mark.parametrize("scope_name", ["execution", "checkpoint"])
def test_parse_scope_uuid_rejects_invalid_uuid(scope_name: str) -> None:
    ...
```

### Test class grouping

Related tests for the same function/component are grouped in classes (no `self` state — just namespace organization):

```python
class TestBuildPipelineRegistrationName:
    def test_normal_name(self) -> None: ...
    def test_name_with_special_chars(self) -> None: ...
```

## Gotchas

- `pythonpath = ["scripts"]` in pytest config means `scripts/` modules are importable. Any module-level `sys.exit()` in scripts will crash pytest collection.
- Environment variables are stripped at **module load time** in `conftest.py` (before fixtures run), not just inside fixtures. This prevents leakage from the host shell into test discovery.
- The `_SRC_PATH` insertion in `conftest.py` ensures `import kitaru` resolves to the local `src/kitaru/` even when running from the tests directory.
- `primed_zenml` tests are slower and shouldn't be mixed into unit test files — keep them in `test_phase*` files.
