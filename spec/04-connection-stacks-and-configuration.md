# 4. Connection, Stacks, and Configuration

## Unified configuration

Kitaru uses a **single unified configuration object** that gathers all settings — connection, stack selection, image/environment, execution behavior — into one coherent structure. This config object inherits from multiple scopes (global, project, runtime) and is the single source of truth for how an execution runs.

The way to think about configuration is in two temporal phases:

- **Pre-execution settings** — things configured before a run starts: the active stack, image definition, global log settings, cache behavior, connection credentials. These are static for the duration of an execution.
- **Runtime overrides** — things specified at invocation time or in decorators: per-flow stack choice, per-checkpoint retries, environment variable overrides, Docker settings. These override the pre-execution defaults.

This mirrors what ZenML calls "configuration and settings": a base layer of static config, plus a runtime layer of overrides that get merged at execution time.

### The `@flow` decorator as the main config surface

The `@kitaru.flow` decorator is the primary place where configuration flows into an execution. It accepts:

- stack selection
- image / Docker settings
- execution behavior overrides (cache, retries)
- environment variables

Connection credentials (server URL, auth token) are part of the unified config object but are **not** passed through the `@flow` decorator — they are resolved from the environment, user config, or explicit `connect()` calls before any flow runs.

### Config object structure

The unified config object contains these sub-objects:

```python
# Conceptual structure — not necessarily the literal API
KitaruConfig(
    # Connection
    server_url="https://kitaru.mycompany.com",
    auth_token="...",

    # Stack selection
    stack="prod",

    # Image / environment
    image=ImageSettings(
        base_image="python:3.12-slim",
        requirements=["pydantic", "httpx"],
        environment={"API_TIMEOUT": "30"},
    ),

    # Execution behavior
    cache=True,
    retries=0,

    # Project
    local_dir=".kitaru",
    project="my-project",
)
```

## Config sources and precedence

Configuration can come from multiple sources. More specific sources override less specific ones.

### Sources (most specific wins)

1. **Invocation-time overrides** — `my_flow.deploy(..., stack="prod")` or `my_flow.start(..., stack="prod")`
2. **Decorator defaults** — `@kitaru.flow(stack="prod")`
3. **`kitaru.configure()` calls** — explicit runtime configuration
4. **Environment variables** — `KITARU_STACK`, `KITARU_SERVER_URL`, etc.
5. **Project config file** — checked into the repo
6. **User config** — stored on the machine (active stack, auth token)
7. **Built-in defaults** — implicit `local` stack, default settings

### Project config files

Project-level config can live in any of:

- `pyproject.toml` under `[tool.kitaru]`
- `kitaru.toml`
- `.kitaru/` folder (for active project/stack state)

Project-local config shadows global or user-level defaults for settings relevant to that project. This gives a clean experience when switching between projects that target different servers or stacks.

### User config

Stored on the machine (not in the repo):

- server URL
- active stack
- auth token or token path

### Shadowing

If a setting is defined in project config, it takes precedence over the globally active user config for that setting.

## Connection

Connection state is about talking to a Kitaru server.

Under the hood, the Kitaru server **is** the ZenML server. The user does not need to know this — from their perspective, they connect to a Kitaru URL and use Kitaru concepts. The SDK maps Kitaru operations to ZenML API calls internally.

Connection owns:

- server URL
- auth token or API key
- workspace or project (later)

Connection is part of the unified config object but resolves separately from execution-time settings — it must be established before any flow can target a remote stack.

### Python

```python
import kitaru

kitaru.connect("https://kitaru.mycompany.com")
```

### CLI

```bash
kitaru login https://kitaru.mycompany.com
kitaru status
kitaru logout
```

### Connection precedence

1. explicit `connect()` or client args
2. environment variables
3. user config
4. none (local-only mode)

## Stacks

A **stack** is a named execution target — a bundle of infrastructure components that defines where and how an execution runs.

### Stack components

Kitaru exposes four to five core components:

| Component | What it covers | ZenML mapping |
| --- | --- | --- |
| **Runner** | Where and how code executes — combines orchestration, step execution, and optionally sandboxed execution | Orchestrator + Step Operator + Sandbox |
| **Artifact store** | Where artifacts, checkpoint outputs, and execution journal data are persisted | Artifact Store |
| **Container registry** | Where built images are pushed and pulled from | Container Registry |
| **LLM model** | Model provider configuration used by `kitaru.llm()` | New ZenML component |

A possible fifth component (e.g. sandbox as a standalone component separate from runner) may be added as the architecture solidifies.

### Stack-first approach

The path from local to remote should be **stack-first**, not bottom-up component assembly.

Instead of asking users to:

1. create a service connector
2. create individual components
3. assemble them into a stack

Kitaru should focus on **creating and selecting stacks** as the primary operation:

```bash
# The user thinks in terms of stacks, not components
kitaru stack create prod \
    --runner kubernetes \
    --artifact-store s3://my-bucket \
    --container-registry ghcr.io/myorg

kitaru stack use prod
```

Component and service connector configuration happens **as part of stack creation**, not as separate prerequisite steps. Advanced users and platform admins can still work with individual components, but the default workflow is stack-centric.

Service connectors (for cloud credential management) should eventually be exposed, but the priority is making stack creation simple and self-contained.

### Built-in local stack

Kitaru always has an implicit built-in `local` stack.

This gives the zero-config local development story:

```python
@kitaru.flow
def my_flow(...):
    ...
```

This works because Kitaru can resolve a built-in local execution target that:

- runs inline
- stores artifacts locally
- supports local replay

Conceptually, `local` is a real stack, not a special-case hack.

### Remote stacks

In connected mode, remote stacks are typically **server-managed**.

For MVP, the intended developer workflow is:

```bash
kitaru login https://kitaru.mycompany.com
kitaru stack list
kitaru stack use prod
```

Ordinary developers should mostly **select stacks**, not assemble them from infra components.

More advanced stack authoring can remain a platform or admin concern.

### Stack selection precedence

1. `my_flow.deploy(..., stack="prod")` or `my_flow.start(..., stack="prod")` — invocation-time override
2. `@kitaru.flow(stack="prod")` — decorator default
3. environment variable override
4. active user-selected stack
5. implicit `local`

## Image and environment settings

When running remotely, Kitaru needs to know what Docker image to use and what environment to set up. This is configured through **image** settings (called "Docker settings" in ZenML).

```python
@kitaru.flow(
    image=ImageSettings(
        base_image="python:3.12-slim",
        requirements=["pydantic", "httpx"],
        dockerfile="Dockerfile.agent",
        environment={"API_KEY": "{{secrets.api_key}}"},
    ),
)
def my_agent(prompt: str) -> str:
    ...
```

Image settings include:

- base image
- additional Python requirements
- custom Dockerfile
- environment variables injected into the runtime
- build-time vs runtime environment separation

These are part of the unified config object and can be set at the project level, the `@flow` decorator level, or overridden at invocation time.

## Execution behavior settings

These control how an execution runs, separate from where it runs:

- **cache** — whether checkpoint outputs should be reused from previous executions. **On by default** — most agent workflows benefit from not re-executing expensive checkpoints. (Note: this is distinct from replay, which reuses outputs within the same execution lineage.)
- **retries** — automatic retry count on failure
- **timeout** — execution time limits (future)

These can be set at the project level or overridden per-flow or per-checkpoint.

### Notes

- secrets are referenced by environment variable name or stored in stack/secret config, not resolved directly in app config
- durability is a core runtime behavior, so it should not be described as optional "cache" behavior

## Frozen resolved execution spec

At flow start, Kitaru computes a **fully resolved execution spec** and persists it with the execution.

This spec is the merged result of all config sources and includes:

- selected stack
- resolved image settings
- app config used by the execution
- flow-level defaults
- connection context if relevant
- source or code version info if available

This matters because otherwise configuration can drift while an execution is waiting.

### Rule of thumb

- **Resume** of an existing execution should use the original frozen execution spec
- **Retry** of a failed execution should use the original frozen execution spec
- **Replay** creates a new execution and therefore a new execution spec, unless explicitly told to inherit from the old one

## Capability checks

Stacks should expose runtime capabilities.

For example, a stack may or may not support:

- durable external resume after `wait()`
- webhook-based resume
- background remote start

If a feature is unavailable on the current stack, Kitaru should fail clearly rather than pretending it works the same way everywhere.

## Local vs connected support matrix

| Feature | Local only | Connected / server-backed |
| --- | --- | --- |
| `@flow`, `@checkpoint`, `kitaru.llm()` | Yes | Yes |
| Local replay | Yes | Yes |
| Artifact inspection | Yes | Yes |
| Durable external resume after `wait()` | Limited or no | Yes |
| Webhook resume | No or limited | Yes |
| Dashboard-backed input | No or limited | Yes |
| Background execution across process exit | No | Yes |
| Remote stack selection | No | Yes |

### OSS vs Pro considerations

The polished demo experience — where a user answers a wait question in the dashboard and the execution resumes automatically — may depend on Pro-backed server/workspace plumbing.

Manual and client-driven resume exists in the MVP direction for OSS workflows. But dashboard-triggered connected resume for released compute requires server capabilities that may only be fully available in the Pro-backed deployment path.

The spec should not overpromise a fully independent OSS dashboard experience for all resume flows.
