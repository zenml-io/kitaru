---
description: Kitaru config directory, execution defaults, environment variables, and precedence
icon: gear
---

# Configuration

This page is the reference for how Kitaru is configured: where settings live on
disk, how to set execution defaults, the `KITARU_*` environment variables, and
how all of these are resolved by precedence. Configuration is also what keeps
runs reproducible — at flow start Kitaru freezes a fully resolved execution spec,
so a [replay](replay-and-overrides.md) reruns under the same config it recorded.

## Config directory

Kitaru stores all of its state — settings, database, credentials, and local
stores — in a single directory. By default this is your platform's standard
application config directory:

```text
Linux:   ~/.config/kitaru/
macOS:   ~/Library/Application Support/kitaru/
Windows: %APPDATA%\\kitaru\\
```

Inside that directory you will find:

```text
kitaru.yaml    # Log-store, model aliases, and other Kitaru settings
config.yaml    # Internal runtime configuration (managed automatically)
local_stores/  # SQLite database and local artifact storage
```

You can see the active config directory by running `kitaru status` or
`kitaru info`.

If you want to relocate that directory, set:

```bash
export KITARU_CONFIG_PATH=/app/.kitaru
```

This moves everything — settings, database, and credentials — to the new
location. Kitaru reads that path immediately, but it only creates the
directory on first write.

## Configure runtime defaults in Python

Use `kitaru.configure(...)` to set process-local defaults:

```python
import kitaru

kitaru.configure(
    stack="local",
    cache=False,
    retries=1,
    llm_estimated_costs="auto",
    image=kitaru.ImageSettings(
        base_image="python:3.12-slim",
        environment={"LOG_LEVEL": "INFO"},
        secret_environment_from=["openai-creds"],
    ),
)
```

These defaults apply to flows started in the current Python process.

## Flow-level and invocation-level overrides

Flow defaults can override configured runtime defaults, and invocation-time
arguments can override both.

```python
from kitaru import flow

@flow(cache=True, retries=2, stack="gpu-cluster")
def my_flow(topic: str) -> str:
    ...

handle = my_flow.run("kitaru", retries=3, stack="prod-cluster")
```

In this example, resolved retries are `3`, and the execution runs on
`prod-cluster` for that one submission.

The important distinction is:

- `kitaru.configure(stack="...")` sets a process-local default
- `@flow(stack="...")` sets a default for one flow definition
- `my_flow.run(stack="...")` overrides both for one execution
- none of those change the persisted active stack selected by `kitaru stack use ...`

## Checkpoint cache overrides

You can set cache behavior at checkpoint granularity:

```python
from kitaru import checkpoint, flow

@checkpoint(cache=False)
def always_fresh(topic: str) -> str:
    ...

@flow
def my_flow(topic: str) -> str:
    return always_fresh(topic)
```

- `@checkpoint(cache=False)` disables cache for that checkpoint.
- `@checkpoint(cache=True)` enables cache for that checkpoint.
- Omitting `cache` defers to the execution-level setting (or the orchestrator default).

For mixed cache behavior in one flow, leave execution-level cache unset and
configure `cache` only on the checkpoints that need different behavior. If you
set cache at execution level (`KITARU_CACHE`, `kitaru.configure(cache=...)`,
`@flow(cache=...)`, or `my_flow.run(cache=...)`), that value is treated as an
execution-wide override for the run and replaces any checkpoint-level setting.

## Environment variables

Kitaru's public env-var surface uses `KITARU_*` names.

### Connection

The v2 client and native MCP server use two independent environment variables for non-interactive setup:

- `KITARU_API_URL` selects the API target.
- `KITARU_API_KEY` supplies a credential when the target is not anonymous.

For normal interactive use, prefer `kitaru login SERVER` and a persisted context. For CI, Docker, and other headless environments, set the API URL and, when required, an API key:

```bash
export KITARU_API_URL=https://my-server.example.com
export KITARU_API_KEY=kat_abc123...
```

The API key is optional so public servers can be used anonymously. Target resolution and credential resolution are independent: an explicit target can still use a stored credential for the same normalized URL, and `KITARU_API_KEY` overrides a stored credential. The native MCP server also accepts `--server` or `--context`, fixes the selected connection for the process lifetime, and intentionally ignores `KITARU_TASK_TOKEN`.

### Execution

Execution settings:

- `KITARU_STACK`
- `KITARU_CACHE`
- `KITARU_RETRIES`
- `KITARU_IMAGE` (JSON object or plain image string; the JSON form accepts every
  `ImageSettings` field, including `secret_environment_from` for secret
  references resolved at runtime)

The same image shape is used at deploy time:

- CLI: `kitaru deploy ... --image ...` / `kitaru build ... --image ...`
- SDK: `flow.deploy(image=...)`

Both accept either a base image string or an object matching `kitaru.ImageSettings`. On deployments, that image config becomes part of the
saved deployment snapshot.

### LLM

- `KITARU_DEFAULT_MODEL` sets the default model for `kitaru.llm()` when no
  explicit `model=` argument is provided.
- `KITARU_LLM_ESTIMATED_COSTS` controls automatic `genai-prices` estimated-cost tracking for direct `kitaru.llm()` calls and framework adapters. Use `auto` (default) or `off`.
- `KITARU_LLM_MOCK_RESPONSE` remains available for tests and demos.
- `OLLAMA_HOST` sets the Ollama server address for `ollama/*` models
  (default: `http://localhost:11434`).

`KITARU_DEFAULT_MODEL` first tries local alias lookup. If the value is not a
registered alias, Kitaru passes it straight through as a raw provider/model
string. This lookup happens before Kitaru falls back to the locally configured
default alias.

Automatic estimated costs are calculated with [`genai-prices`](https://github.com/pydantic/genai-prices) after the provider or adapter call succeeds. Kitaru only estimates when it has reliable provider, model, and token data. If pricing fails, the model response and token counts are still recorded. See [Execution Management → LLM usage and cost metadata](execution-management.md#llm-usage-and-cost-metadata) for the actual-vs-estimated cost fields.

If you want the full secret-backed setup path for `kitaru.llm()`, see
[Secrets + Model Registration](secrets-and-model-registration.md).

### Housekeeping

- `KITARU_CONFIG_PATH`
- `KITARU_DEBUG`
- `KITARU_ANALYTICS_OPT_IN`

### `kitaru status`

`kitaru status` shows an **Environment** section when `KITARU_*` vars are
active. Secret values such as `KITARU_API_KEY` and
`KITARU_LOG_STORE_API_KEY` are masked.

## Precedence (highest to lowest)

1. Invocation-time overrides (`my_flow.run(..., stack="prod")`)
2. Flow decorator defaults (`@flow(stack="prod")`)
3. `kitaru.configure(...)`
4. Environment variables (`KITARU_STACK`, `KITARU_API_URL`, etc.)
5. Project config (`pyproject.toml` under `[tool.kitaru]`)
6. Persisted active project/stack fallback plus global user config (for example connection defaults)
7. Built-in defaults (`retries=0`; cache is left unset so per-checkpoint cache settings and orchestrator defaults apply)

In practice, `stack` starts unset in the built-in defaults and is then resolved
from the persisted active stack when one is available.

For connection-specific resolution, Kitaru uses this lower-to-higher order:

1. Persisted connection and active project (`kitaru login`, `kitaru project use`)
2. Direct `ZENML_*` env vars for compatibility
3. `KITARU_*` env vars
4. Runtime overrides from `kitaru.configure(...)`

## Project config in pyproject.toml

```toml
[tool.kitaru]
stack = "prod"
cache = false
retries = 2
llm_estimated_costs = "auto"

[tool.kitaru.image]
base_image = "python:3.12-slim"
secret_environment_from = ["openai-creds"]

[tool.kitaru.image.environment]
LOG_LEVEL = "INFO"
```

{% hint style="warning" %}
`environment` is for non-secret values only. Put credentials in
`secret_environment_from` instead — those references are resolved at runtime
via the ZenML pipeline secret mechanism and never appear in image metadata,
log output, or the frozen execution spec. See
[Containerization → Secret-backed environment variables](containerization.md#secret-backed-environment-variables).
{% endhint %}

## Automatic package injection

Kitaru automatically adds itself to the container image requirements for remote
execution when Kitaru is assembling the image from requirements. You usually do
not need to add `kitaru` to your `requirements` list manually.

If you specify your own requirements, Kitaru appends itself without duplicating:

```python
kitaru.configure(
    image=kitaru.ImageSettings(
        requirements=["httpx", "pydantic"],
    ),
)
# Remote container installs: httpx, pydantic, kitaru
```

If you already include `kitaru` (with or without a version pin), it is not added
again:

```python
kitaru.configure(
    image=kitaru.ImageSettings(
        requirements=["kitaru>=0.2.0", "httpx"],
    ),
)
# Remote container installs: kitaru>=0.2.0, httpx (no duplicate)
```

If you specify a custom `base_image` or `dockerfile`, you control the image
contents yourself. In that case, Kitaru does **not** auto-inject the SDK, so
your image must already include `kitaru`.

### Replicating your local environment

During development, you can replicate your entire local Python environment into
the remote container. This installs all locally installed packages (via
`pip freeze`) so the remote container matches your dev setup exactly:

```python
from kitaru import flow

@flow(image={"replicate_local_python_environment": True})
def my_flow(topic: str) -> str:
    ...
```

### System packages

If your flow needs system-level dependencies (e.g., `git`, `ffmpeg`), use
`apt_packages`:

```python
from kitaru import flow
import kitaru

@flow(
    image=kitaru.ImageSettings(
        apt_packages=["git", "ffmpeg"],
        requirements=["httpx"],
    ),
)
def my_flow(topic: str) -> str:
    ...
```

## Project selection

Project selection is first-class Kitaru state. For interactive work, use the CLI:

```bash
kitaru project list
kitaru project use production
kitaru project current
```

That persists `production` as the active project for later CLI and SDK calls.
You can also choose a project while connecting to a remote server:

```bash
kitaru login https://kitaru.example.com --project production
```

For env-driven remote bootstrap, set `KITARU_PROJECT` alongside the server URL
and auth token:

```bash
export KITARU_API_URL=https://kitaru.example.com
export KITARU_API_KEY=kat_...
export KITARU_PROJECT=production
```

This rule prevents a headless job from accidentally using a stale persisted
project from some previous interactive session. The concrete story is: if a CI
container starts with `KITARU_API_URL` and `KITARU_API_KEY`, Kitaru expects
the container to also say `KITARU_PROJECT`. It does not silently borrow whatever
project a developer used last week.

When several sources name a project, higher-priority sources win:

1. Process-local `kitaru.configure(project=...)`
2. Public `KITARU_PROJECT`
3. Compatibility `ZENML_ACTIVE_PROJECT_ID`
4. Persisted active project from `kitaru login --project` or
   `kitaru project use ...`

Bare `kitaru login` is the local-server path: it does not use
`KITARU_API_URL` or `KITARU_API_KEY`, and it warns instead of failing if
those remote auth env vars are already set in your shell.

See [Projects](projects.md) for the full CLI and SDK project workflow.

## Headless / Docker / CI recipe

```bash
# Connection
export KITARU_API_URL=https://my-server.example.com
export KITARU_API_KEY=kat_abc123...
export KITARU_PROJECT=my-project

# Execution
export KITARU_STACK=my-remote-stack
export KITARU_CACHE=true
export KITARU_RETRIES=2

# LLM
export OPENAI_API_KEY=sk-...
export KITARU_DEFAULT_MODEL=openai/gpt-5-nano

# Remote secret references — resolved at runtime, never written into image env
export KITARU_IMAGE='{"secret_environment_from": ["openai-creds"]}'

# Observability
export KITARU_LOG_STORE_BACKEND=datadog
export KITARU_LOG_STORE_ENDPOINT=https://http-intake.logs.datadoghq.com
export KITARU_LOG_STORE_API_KEY=...

# Housekeeping
export KITARU_CONFIG_PATH=/app/.kitaru
export KITARU_DEBUG=false
export KITARU_ANALYTICS_OPT_IN=false
```

## Compatibility note

Direct `ZENML_*` env vars still work because the underlying runtime understands
them, but Kitaru docs use `KITARU_*` as the public interface.

## Frozen execution spec

At flow start, Kitaru computes a fully resolved execution config and persists it
with the execution metadata (`kitaru_execution_spec`). This keeps execution
behavior stable even if config changes later.

Deployments follow the same principle: `deploy(..., image=...)` (or CLI/MCP
image equivalents) is resolved and saved in the deployment snapshot. If you need
a different image later, create a new deployment version.

## Example in this repository

```bash
uv sync --extra local
uv run python examples/features/basic_flow/flow_with_configuration.py
uv run pytest tests/test_phase10_configuration_example.py
```

For the broader catalog, see [Examples](../getting-started/examples.md).

## Related pages

- [Stacks](../stacks/README.md)
- [Examples](../getting-started/examples.md)
- [Secrets + Model Registration](secrets-and-model-registration.md)
- [Tracked LLM Calls](llm-calls.md)
- [Flows](../concepts/flows.md)
- [Python configuration reference](https://sdkdocs.kitaru.ai)
