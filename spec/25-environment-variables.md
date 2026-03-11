# Spec 25 — Environment Variable Configuration

## Goal

Make Kitaru fully configurable via environment variables so a Docker image
or CI job can be bootstrapped **without** interactive commands (`kitaru login`,
`kitaru stack use`, etc.).  Users should never need to know about `ZENML_*`
env vars — `KITARU_*` vars are the public surface.

## Design decisions

1. **`KITARU_*` fully wraps `ZENML_*`** — the translation layer directly
   mutates `os.environ` to set the corresponding `ZENML_*` var so ZenML's
   Client picks it up. This is intentional and documented: user code that
   reads `os.environ["ZENML_STORE_URL"]` will see the translated value.
2. **`KITARU_*` takes precedence** — if both `KITARU_SERVER_URL` and
   `ZENML_STORE_URL` are set, `KITARU_SERVER_URL` wins. A
   `warnings.warn()` is emitted so the conflict is visible but filterable.
3. **Model aliases stay in config file** — too complex for env vars.
4. **`KITARU_DEFAULT_MODEL`** is added for the common case of setting a
   default model without aliases.
5. **Partial connection config fails fast** — if `KITARU_SERVER_URL` is set
   without `KITARU_AUTH_TOKEN`, error at translation time (but check
   `ZENML_STORE_API_KEY` as fallback before erroring).
6. **`KITARU_PROJECT` is required when `KITARU_SERVER_URL` is set** — but
   this validation happens at first use (config resolution), not at
   translation time, so benign commands like `kitaru --version` still work.
7. **`configure()` > env** — `kitaru.configure(server_url=...)` always
   takes precedence over env vars. The env→os.environ translation is for
   ZenML bootstrap, not for overriding Kitaru's own resolution chain.
8. **Soft deprecation of `ZENML_*` in Kitaru docs** — Kitaru docs only
   mention `KITARU_*` vars. `ZENML_*` vars still work (ZenML reads them
   natively) but are not documented in Kitaru's own docs. No runtime
   deprecation warnings for using `ZENML_*` directly.

## Env var inventory

### Connection (fix existing — make them actually work)

| Env var | Translates to | Notes |
|---------|--------------|-------|
| `KITARU_SERVER_URL` | `ZENML_STORE_URL` | Full server URL |
| `KITARU_AUTH_TOKEN` | `ZENML_STORE_API_KEY` | API key or token |
| `KITARU_PROJECT` | `ZENML_ACTIVE_PROJECT_ID` | Project name or UUID |

When any of these are set, CLI `login`/`logout` should refuse (same guard
as the existing `ZENML_STORE_*` check).

**Partial config validation (at translation time):**
- If `KITARU_SERVER_URL` is set but neither `KITARU_AUTH_TOKEN` nor
  `ZENML_STORE_API_KEY` is set → error immediately.
- If `KITARU_AUTH_TOKEN` is set but neither `KITARU_SERVER_URL` nor
  `ZENML_STORE_URL` is set → error immediately.

**Project requirement (at first use, not translation time):**
- When `KITARU_SERVER_URL` (or `ZENML_STORE_URL`) is set and
  `KITARU_PROJECT` (or `ZENML_ACTIVE_PROJECT_ID`) is not set → error
  when connection config is actually resolved (e.g. `KitaruClient` init,
  `flow.run()`). This keeps `kitaru --version` and other benign commands
  working even with partial env config.

### Execution (already working — no changes needed)

| Env var | Default | Notes |
|---------|---------|-------|
| `KITARU_STACK` | active stack | Override execution stack |
| `KITARU_CACHE` | `true` | Enable/disable checkpoint caching |
| `KITARU_RETRIES` | `0` | Max retries for failed checkpoints |
| `KITARU_IMAGE` | (none) | Docker image settings (JSON string) |

### Observability (already working — no changes needed)

| Env var | Default | Notes |
|---------|---------|-------|
| `KITARU_LOG_STORE_BACKEND` | `artifact-store` | `artifact-store` or `external` |
| `KITARU_LOG_STORE_ENDPOINT` | (none) | Required when backend=external |
| `KITARU_LOG_STORE_API_KEY` | (none) | Required when backend=external |

### LLM

| Env var | Status | Notes |
|---------|--------|-------|
| `KITARU_DEFAULT_MODEL` | **New** | Default model for `kitaru.llm()` when no `model=` given. Resolution: try alias lookup first; if no matching alias, pass through to LiteLLM as raw model string. |
| `KITARU_LLM_MOCK_RESPONSE` | Exists | Mock LLM responses for testing |

### Housekeeping

| Env var | Status | Translates to | Notes |
|---------|--------|--------------|-------|
| `KITARU_CONFIG_PATH` | **New** | (Kitaru-only) | Override config directory (default: platform `click.get_app_dir`). Directory is auto-created on first write, not at startup. |
| `KITARU_DEBUG` | **New** | `ZENML_DEBUG` | Debug mode |
| `KITARU_ANALYTICS_OPT_IN` | **New** | `ZENML_ANALYTICS_OPT_IN` | Analytics opt-in/out |

### Deliberately not added

| Skipped | Reason |
|---------|--------|
| `KITARU_MODEL_ALIASES` | Config file is the right place for structured alias definitions |
| `KITARU_SECRETS_*` | Secret resources require API calls, not env config |
| `KITARU_ACTIVE_STACK_ID` | `KITARU_STACK` (by name) already exists |
| `KITARU_LOGGING_FORMAT` | Too niche; use `ZENML_LOGGING_FORMAT` directly |
| `KITARU_LOGGING_VERBOSITY` | Same — use `ZENML_LOGGING_VERBOSITY` |

## Translation layer

### Where it runs

The KITARU→ZENML env var translation runs in `_apply_env_translations()`
called from two places:

1. **`_kitaru_bootstrap.py` entrypoints** (`cli_main`, `mcp_main`) —
   guarantees translation before ZenML imports initialize their Client/config.
2. **`kitaru/__init__.py` module-level** — so `import kitaru` in
   programmatic usage also applies translations.

The translation mutates `os.environ` directly. This is deliberate: ZenML
reads env vars from `os.environ`, and threading config through every call
site is not feasible without forking ZenML.

### Translation semantics

```python
import os
import warnings

_ENV_TRANSLATIONS: tuple[tuple[str, str], ...] = (
    ("KITARU_SERVER_URL", "ZENML_STORE_URL"),
    ("KITARU_AUTH_TOKEN", "ZENML_STORE_API_KEY"),
    ("KITARU_PROJECT", "ZENML_ACTIVE_PROJECT_ID"),
    ("KITARU_DEBUG", "ZENML_DEBUG"),
    ("KITARU_ANALYTICS_OPT_IN", "ZENML_ANALYTICS_OPT_IN"),
)

def _apply_env_translations() -> None:
    for kitaru_var, zenml_var in _ENV_TRANSLATIONS:
        kitaru_val = os.environ.get(kitaru_var)
        if kitaru_val is None:
            continue

        zenml_val = os.environ.get(zenml_var)
        if zenml_val is not None and zenml_val != kitaru_val:
            warnings.warn(
                f"Both {kitaru_var} and {zenml_var} are set with "
                f"different values; using {kitaru_var}.",
                stacklevel=2,
            )

        os.environ[zenml_var] = kitaru_val

    # Partial connection validation: fail fast if server URL is set
    # without any auth token available.
    server_url = os.environ.get("KITARU_SERVER_URL")
    auth_token = (
        os.environ.get("KITARU_AUTH_TOKEN")
        or os.environ.get("ZENML_STORE_API_KEY")
    )
    if server_url and not auth_token:
        raise RuntimeError(
            "KITARU_SERVER_URL is set but no auth token is available. "
            "Set KITARU_AUTH_TOKEN (or ZENML_STORE_API_KEY)."
        )
    if os.environ.get("KITARU_AUTH_TOKEN") and not (
        server_url or os.environ.get("ZENML_STORE_URL")
    ):
        raise RuntimeError(
            "KITARU_AUTH_TOKEN is set but no server URL is available. "
            "Set KITARU_SERVER_URL (or ZENML_STORE_URL)."
        )
```

### Idempotency

The translation is idempotent. In multi-process setups (e.g. Celery
workers inheriting env from the parent), re-running the translation
produces the same result. The conflict warning only fires when values
actually differ, so duplicate imports in child processes do not produce
spurious warnings.

### Kitaru-only env vars (no ZENML translation)

- `KITARU_CONFIG_PATH` — consumed only by `_kitaru_config_dir()`
- `KITARU_DEFAULT_MODEL` — consumed only by `kitaru.llm()` / model resolution
- `KITARU_LLM_MOCK_RESPONSE` — consumed only by `kitaru.llm()`
- `KITARU_STACK` / `KITARU_CACHE` / `KITARU_RETRIES` / `KITARU_IMAGE` — consumed by `resolve_execution_config()`
- `KITARU_LOG_STORE_*` — consumed by `resolve_log_store()`

These do not translate to ZENML_ equivalents; they are read directly by
Kitaru code.

## Precedence chain (complete)

For connection config, lowest → highest:

1. ZenML persisted global config (from `kitaru login`)
2. `ZENML_STORE_*` env vars (read natively by ZenML)
3. `KITARU_*` env vars (translated to `ZENML_*` by Kitaru, overwriting)
4. `kitaru.configure(...)` runtime overrides
5. Explicit arguments to `KitaruClient(...)` or `flow.run(...)`

For execution config (unchanged from current):

1. Active ZenML stack
2. `pyproject.toml` `[tool.kitaru]`
3. `KITARU_*` env vars
4. `kitaru.configure(...)` runtime overrides
5. `@flow(...)` decorator defaults
6. Invocation-time overrides (`my_flow.run(...)`)

## Login guard update

The CLI login/logout guard (`AUTH_ENV_VARS` in `cli.py`) must be extended:

```python
AUTH_ENV_VARS = (
    "ZENML_STORE_URL",
    "ZENML_STORE_API_KEY",
    "ZENML_STORE_USERNAME",
    "ZENML_STORE_PASSWORD",
    "KITARU_SERVER_URL",
    "KITARU_AUTH_TOKEN",
)
```

## Docker image update

Replace `ZENML_*` defaults with `KITARU_*` equivalents. Keep `ZENML_*`
vars that have no Kitaru equivalent (e.g. `ZENML_CONTAINER`,
`ZENML_CONFIG_PATH`).

```dockerfile
ENV \
  PYTHONUNBUFFERED=1 \
  PYTHONFAULTHANDLER=1 \
  PYTHONHASHSEED=random \
  VIRTUAL_ENV=$VIRTUAL_ENV \
  ZENML_CONTAINER=1 \
  ZENML_CONFIG_PATH=/zenml/.zenconfig \
  KITARU_DEBUG=false \
  KITARU_ANALYTICS_OPT_IN=true
```

Comment out connection vars as documentation:

```dockerfile
# Set these to configure the server connection without `kitaru login`:
# KITARU_SERVER_URL=https://...
# KITARU_AUTH_TOKEN=...
# KITARU_PROJECT=...
```

## Config path change

```python
KITARU_CONFIG_PATH_ENV = "KITARU_CONFIG_PATH"

def _kitaru_config_dir() -> Path:
    custom = os.environ.get(KITARU_CONFIG_PATH_ENV)
    if custom:
        return Path(custom)
    return Path(click.get_app_dir("kitaru"))
```

The custom directory is **not** created eagerly. It is auto-created (with
`mkdir -p` semantics) only on first write — matching the current lazy
behavior for the default path.

## Default model env var

`KITARU_DEFAULT_MODEL` env var is consumed in the `kitaru.llm()` model
resolution path when no explicit `model=` argument is given:

1. Check `KITARU_DEFAULT_MODEL` env var
2. Fall back to config file `default_alias`
3. Fall back to error ("no model specified")

**Resolution of the value itself:**

1. Try alias lookup in the config file's model registry
2. If no matching alias found, pass through to LiteLLM as a raw model
   string (e.g. `openai/gpt-4o`, `gpt-4o`, `claude-sonnet-4-20250514`)

This is the pragmatic approach: alias names are checked first, but
unrecognized values are forwarded to LiteLLM rather than erroring. LiteLLM
decides if the model string is valid.

## Status command enhancement

Extend `kitaru status` output to include an env var section showing which
`KITARU_*` vars are currently active:

```
Environment
  KITARU_SERVER_URL   https://my-server.example.com
  KITARU_AUTH_TOKEN   kat_abc***
  KITARU_PROJECT      my-project
  KITARU_STACK        my-remote-stack
  KITARU_DEBUG        false
```

**Secret masking:** Tokens and API keys (`KITARU_AUTH_TOKEN`,
`KITARU_LOG_STORE_API_KEY`) show the first 6-8 characters followed by
`***`. Other values are shown in full.

Only env vars that are actually set are shown. If no `KITARU_*` env vars
are active, the section is omitted.

## Full headless Docker recipe (after implementation)

```bash
# Connection
export KITARU_SERVER_URL=https://my-server.example.com
export KITARU_AUTH_TOKEN=kat_abc123...
export KITARU_PROJECT=my-project

# Execution
export KITARU_STACK=my-remote-stack
export KITARU_CACHE=true
export KITARU_RETRIES=2

# LLM
export OPENAI_API_KEY=sk-...
export KITARU_DEFAULT_MODEL=openai/gpt-4o

# Observability
export KITARU_LOG_STORE_BACKEND=external
export KITARU_LOG_STORE_ENDPOINT=https://logs.example.com
export KITARU_LOG_STORE_API_KEY=...

# Housekeeping
export KITARU_CONFIG_PATH=/app/.kitaru
export KITARU_DEBUG=false
export KITARU_ANALYTICS_OPT_IN=false
```

## Testing

### Translation layer tests

- Verify each KITARU→ZENML mapping sets `os.environ` correctly.
- Verify KITARU_ takes precedence when both are set (ZENML_ is overwritten).
- Verify `warnings.warn()` fires when both are set with different values.
- Verify no warning when both are set with the **same** value.
- Verify idempotency: calling `_apply_env_translations()` twice produces
  no duplicate warnings.
- Verify partial config: `KITARU_SERVER_URL` without any auth token → error.
- Verify partial config: `KITARU_AUTH_TOKEN` without any server URL → error.
- Verify cross-namespace fallback: `KITARU_SERVER_URL` + `ZENML_STORE_API_KEY`
  (no `KITARU_AUTH_TOKEN`) → no error (ZENML fallback satisfies auth).

### Config path tests

- Verify `KITARU_CONFIG_PATH` overrides `_kitaru_config_dir()`.
- Verify custom dir is NOT created on read (returns path even if absent).
- Verify custom dir IS created on first write (`_write_kitaru_global_config`).
- Verify default path (no env var) still uses `click.get_app_dir("kitaru")`.

### Default model tests

- Verify `KITARU_DEFAULT_MODEL` is used when no `model=` argument given.
- Verify `KITARU_DEFAULT_MODEL` with a registered alias name resolves
  the alias.
- Verify `KITARU_DEFAULT_MODEL` with an unrecognized string passes through
  to LiteLLM as raw model string.
- Verify `kitaru.configure(model=...)` or explicit `model=` arg takes
  precedence over `KITARU_DEFAULT_MODEL`.

### Login guard tests

- Verify `kitaru login` refuses when `KITARU_SERVER_URL` is set.
- Verify `kitaru login` refuses when `KITARU_AUTH_TOKEN` is set.
- Verify `kitaru logout` refuses when `KITARU_SERVER_URL` is set.

### Project requirement tests

- Verify `resolve_connection_config()` errors when server URL is set
  without project.
- Verify the error does NOT fire for `kitaru --version` (validation is
  lazy, not at import/translation time).

### Status command tests

- Verify `kitaru status` shows env var section when KITARU_ vars are set.
- Verify secret values are masked (first 6-8 chars + `***`).
- Verify env var section is omitted when no KITARU_ vars are set.

### Test cleanup

Tests continue managing env vars directly via conftest fixtures (manual
cleanup). No new `_reset_env_translations()` helper. The `conftest.py`
autouse fixture that clears `KITARU_*` and `ZENML_*` env vars provides
sufficient isolation.

## Documentation changes

- **`docs/content/docs/getting-started/configuration.mdx`**: Rewrite to
  use `KITARU_*` vars as the primary surface. Remove or demote `ZENML_*`
  references to a "ZenML compatibility" footnote.
- **`docs/content/docs/cli/login.mdx`**: Add note about env-based auth
  as an alternative to `kitaru login`.
- **New page or section**: "Headless / Docker / CI setup" with the full
  env var recipe.
- **Existing pages**: Update any remaining references from `ZENML_*` to
  `KITARU_*` equivalents.
