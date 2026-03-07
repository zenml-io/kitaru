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
    # Connection (server URL is a ZenML server URL)
    server_url="https://my-zenml-server.mycompany.com",
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
5. **Project config** — `pyproject.toml` under `[tool.kitaru]` (checked into the repo)
6. **Global user config** — stored on the machine (active stack, auth token, default server/stack selection)
7. **Built-in defaults** — implicit `local` stack, default settings

### Project config

Project-level config lives in `pyproject.toml` under `[tool.kitaru]`. There is no separate `kitaru.toml` file.

```toml
[tool.kitaru]
stack = "prod"
project = "my-agent"
```

Project-local config shadows global or user-level defaults for settings relevant to that project. This gives a clean experience when switching between projects that target different servers or stacks.

Note: rich project-level configuration is likely not in the MVP scope. The primary config surface for the MVP is global user config + decorator/invocation overrides.

### Global user config

Stored on the machine (not in the repo):

- server URL (this is a ZenML server URL under the hood)
- active stack
- auth token or token path
- default server and stack selection

The global config can influence default server/stack selection. If a setting is defined in project config, it takes precedence over the globally active user config for that setting.

## Connection

Connection state is about talking to a server.

Under the hood, the Kitaru server **is** the ZenML server. All server URLs are ZenML server URLs. The user does not need to know this — from their perspective, they connect to a server URL and use Kitaru concepts. The SDK maps Kitaru operations to ZenML API calls internally.

**Important:** Kitaru does not have its own HTTP API endpoints separate from ZenML. There is no distinct Kitaru API surface — all server communication goes through ZenML's API.

Connection owns:

- server URL (ZenML server)
- auth token or API key
- workspace or project (later)

Connection is part of the unified config object but resolves separately from execution-time settings — it must be established before any flow can target a remote stack.

### Python

```python
import kitaru

kitaru.connect("https://my-zenml-server.mycompany.com")
```

### CLI

```bash
kitaru login https://my-zenml-server.mycompany.com
kitaru status
kitaru logout
```

### Connection precedence

1. explicit `connect()` or client args
2. environment variables
3. global user config
4. none (local-only mode)

## Stacks

A **stack** is a named execution target — a bundle of infrastructure components that defines where and how an execution runs.

### Stack components

Kitaru stacks focus on execution infrastructure:

| Component | What it covers | ZenML mapping |
| --- | --- | --- |
| **Runner** | Where and how code executes — combines orchestration, step execution, and optionally sandboxed execution | Orchestrator + Step Operator + Sandbox |
| **Artifact store** | Where artifacts, checkpoint outputs, execution journal data, and logs are persisted | Artifact Store |
| **Container registry** | Where built images are pushed and pulled from | Container Registry |

A possible fourth component (e.g. sandbox as a standalone component separate from runner) may be added as the architecture solidifies.

**Note:** LLM model configuration is **not** part of a stack. Model aliases and credentials are managed through the local model registry (`kitaru model register`) and are independent of stack selection. See [Chapter 8](08-kitaru-llm.md) for details.

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
```

**Note on stack creation:** The simple `--runner kubernetes` form is a starting point, but stack creation must also expose deeper infrastructure details and credentials that map to ZenML service connectors and components underneath. For example, a Kubernetes runner needs cluster credentials, namespace configuration, and resource limits. The CLI should expose these as part of stack creation rather than requiring users to separately configure service connectors.

```bash
# More complete stack creation (illustrative)
kitaru stack create prod \
    --runner kubernetes \
    --runner-namespace ml-agents \
    --runner-service-account kitaru-sa \
    --artifact-store s3://my-bucket \
    --artifact-store-role arn:aws:iam::123:role/kitaru \
    --container-registry ghcr.io/myorg
```

The exact flags and UX for credential/detail configuration are not frozen — the principle is that the CLI must make this possible as part of stack creation, not force it into separate prerequisite steps.

```bash
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

`pip install kitaru[local]` is effectively equivalent to `pip install zenml[local]` — it gives you the same local development capabilities.

### Deploy-time stack defaults

When Kitaru is deployed remotely (via Helm chart, Docker, or similar), the deployment should configure a **default remote stack** as part of the installation. This means:

- The Helm chart / deployment config includes variables for the initial artifact store, runner, container registry, and optionally log store
- On first deploy, a default remote stack is created and ready to use
- Users do not need to manually create a stack before running their first remote flow

This is especially important for the **artifact store** — a remote bucket must be part of the deployment so that Kitaru can write logs, artifacts, and visualizations to it by default. The artifact store is a key part of the deployment and should simply be a Helm chart variable.

```yaml
# Illustrative Helm values
kitaru:
  defaultStack:
    artifactStore: s3://my-kitaru-bucket
    runner: kubernetes
    containerRegistry: ghcr.io/myorg
```

This pattern can extend to other stack components (runner, log store, container registry) as the deployment story matures.

### Remote stacks

In connected mode, remote stacks are typically **server-managed**.

For MVP, the intended developer workflow is:

```bash
kitaru login https://my-zenml-server.mycompany.com
kitaru stack list
kitaru stack use prod
```

Ordinary developers should mostly **select stacks**, not assemble them from infra components.

More advanced stack authoring can remain a platform or admin concern.

### Stack selection precedence

1. `my_flow.deploy(..., stack="prod")` or `my_flow.start(..., stack="prod")` — invocation-time override
2. `@kitaru.flow(stack="prod")` — decorator default
3. environment variable override
4. active user-selected stack (from global config)
5. implicit `local`

## Image and environment settings

When running remotely, Kitaru needs to know what Docker image to use and what environment to set up. This is configured through **image** settings (called "Docker settings" in ZenML).

```python
@kitaru.flow(
    image=ImageSettings(
        base_image="python:3.12-slim",
        requirements=["pydantic", "httpx"],
        dockerfile="Dockerfile.agent",
        environment={"API_KEY": "{{api_secret.api_key}}"},
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

## Secrets

Kitaru uses ZenML's **centralized secret store** for managing sensitive credentials. The Kitaru server is the ZenML server, so the secret store infrastructure is already available — no additional server setup is needed.

### Secret store architecture

ZenML splits secrets into two layers:

- **Secret metadata** (name, owner, visibility) lives in the server database
- **Secret values** live in a configurable backend (SQL, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault, HashiCorp Vault, or custom)

Clients always access secrets through the server API. Remote step containers authenticate using a workload API token and can fetch secrets at runtime.

### Two secret mechanisms

ZenML provides two distinct ways to use secrets:

1. **Secret references** — for config/settings values, using the syntax `{{secret_name.secret_key}}` (no spaces, dot-separated). These resolve lazily through ZenML's `SecretReferenceMixin`.

2. **Runtime env injection** — pipelines, steps, stacks, and components can declare `secrets=["my_secret"]`, and ZenML injects all key-value pairs from that secret into the runtime process environment.

### Kitaru secrets surface

Kitaru wraps ZenML's secret store with a simpler, more opinionated interface:

```bash
kitaru secrets set openai-creds --OPENAI_API_KEY=sk-...
kitaru secrets show openai-creds
kitaru secrets list
kitaru secrets delete openai-creds
```

Secrets created through Kitaru are **private by default** (only the creating user can access them). Secret keys should use the actual environment variable names that downstream tools expect (e.g. `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`) — this ensures compatibility with both ZenML's env injection and LiteLLM's native env var reading.

### Secrets in image environment

Secret references can be used in image environment settings:

```python
@kitaru.flow(
    image=ImageSettings(
        environment={
            "DATABASE_URL": "{{db_secret.connection_string}}",
        },
    ),
)
def my_agent(prompt: str) -> str:
    ...
```

### LLM credentials in remote runs

Model aliases remain separate from stacks, but aliases may reference ZenML secrets for remote execution. When `kitaru.llm()` runs remotely, it fetches the referenced secret via the ZenML client (authenticated by the workload API token) and makes the credentials available to LiteLLM. See [Chapter 8](08-kitaru-llm.md) for the full credential resolution model.

## Execution behavior settings

These control how an execution runs, separate from where it runs:

- **cache** — whether checkpoint outputs should be reused from previous executions. **On by default** — most agent workflows benefit from not re-executing expensive checkpoints. (Note: this is distinct from replay, which reuses outputs within the same execution lineage.)
- **retries** — automatic retry count on failure (retry behavior is backed by ZenML)

These can be set at the project level or overridden per-flow or per-checkpoint.

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
| Dashboard-backed input | No or limited | Yes (Pro) |
| Background execution across process exit | No | Yes |
| Remote stack selection | No | Yes |
| Dashboard-triggered replay/resume | No | Yes (Pro) |

### OSS vs Pro considerations

The polished demo experience — where a user answers a wait question in the dashboard and the execution resumes automatically — depends on Pro-backed server/workspace plumbing.

Manual and client-driven resume exists in the MVP direction for OSS workflows. But dashboard-triggered connected resume for released compute requires server capabilities that may only be fully available in the Pro-backed deployment path.

Features that rely on snapshots triggering from the dashboard (replays, resume, etc.) are Pro-only in their full form, though local-first OSS versions exist. The spec should not overpromise a fully independent OSS dashboard experience for all resume flows.
