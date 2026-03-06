# 4. Connection, Stacks, and Configuration

One of the most important clarifications in Kitaru is that these are different concerns:

- **connection** — how the SDK talks to a Kitaru or ZenML server
- **stack** — where an execution runs and persists data
- **app config** — project-level runtime defaults
- **execution overrides** — per-flow, per-checkpoint, or per-call choices

These should not be collapsed into one giant config hierarchy.

For the MVP, configuration should stay close to ZenML's existing primitives: stored stack/component config on one side, runtime-provided settings on the other. The only innovation needed is mapping Kitaru's compute and artifact primitives to the underlying ZenML stack components.

## Connection

Connection state is about talking to a Kitaru server.

Under the hood, the Kitaru server **is** the ZenML server. The user does not need to know this — from their perspective, they connect to a Kitaru URL and use Kitaru concepts. The SDK maps Kitaru operations to ZenML API calls internally.

It owns:

- server URL
- auth token or API key
- possibly workspace or project later

It does **not** own:

- model aliases
- default model
- flow retry policy
- business logic defaults

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

## Stacks

A **stack** is a named execution target or infrastructure profile.

A stack answers questions like:

- does this run locally or remotely?
- what runner is used?
- where do artifacts and execution journal data go?
- what runtime capabilities are available?

A stack should **not** define:

- model aliases
- default retry policy
- business logic defaults

## Built-in local stack

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

## Remote stacks

In connected mode, remote stacks are typically **server-managed**.

For MVP, the intended developer workflow is:

```bash
kitaru login https://kitaru.mycompany.com
kitaru stack list
kitaru stack use prod
```

Ordinary developers should mostly **select stacks**, not assemble them from infra components.

More advanced stack authoring can remain a platform or admin concern.

## App config

App config is **project-level runtime configuration**, not connection state and not infrastructure selection.

For MVP, it should stay narrow.

### Good uses

- local runtime directory
- project-level defaults that influence execution behavior

### Example

```python
kitaru.configure(
    local_dir=".kitaru",
)
```

### LLM provider configuration

Kitaru needs an abstraction layer over model providers to support `kitaru.llm()`.

The MVP direction is likely a wrapper over an existing multi-provider SDK (e.g. a LiteLLM-like approach), but the exact provider/backend shape is **not yet finalized**.

What is clear:

- `kitaru.llm()` remains the thin user-facing call surface
- model/provider credentials likely belong closer to stack/secret configuration than to `kitaru.configure()` alone
- call-time `model=` overrides are a valid surface

What is still being decided:

- whether the provider abstraction is a stack component flavor or a lightweight wrapper
- whether `kitaru.configure(models={...})` is the right place for model aliases, or whether that belongs in stack/secret config
- the exact mapping between provider config and ZenML secret/component primitives

For MVP, avoid over-specifying the LLM config architecture. The user-facing `kitaru.llm()` contract is stable; the infrastructure/config plumbing behind it is intentionally flexible.

### Notes

- secrets are referenced by environment variable name or stored in stack/secret config, not resolved directly in app config
- durability is a core runtime behavior, so it should not be described as optional "cache" behavior

## Config files

Kitaru should conceptually split config into two levels.

### Project config

Checked into the repo:

- local data dir
- project-level defaults

This can live in:

- `pyproject.toml`
- or `kitaru.toml`

Project-local config can shadow global or user-level defaults for settings that are relevant to that project.

### User config

Stored on the machine:

- server URL
- active stack
- auth token or token path

This should not normally live in project config.

### Shadowing

If a setting is defined in project config, it should take precedence over the globally active user config for that setting. This gives a cleaner experience when switching between projects that target different servers or stacks.

## Execution overrides

These are per-run or per-call choices.

Examples:

- `@flow(stack="prod")`
- `@flow(retries=2)`
- `@checkpoint(retries=3)`
- `kitaru.llm(model="fast")`
- `flow.start(..., stack="local")`

These are runtime choices, not global project settings.

## Precedence rules by namespace

Kitaru should use **separate precedence rules** for different kinds of settings.

### Connection precedence

1. explicit `connect()` or client args
2. environment variables
3. user config
4. none

### Stack selection precedence

1. `my_flow.start(..., stack="prod")` — invocation-time override
2. `@kitaru.flow(stack="prod")` — decorator default (simple to enable; 1-liner in the decorator implementation)
3. environment variable override
4. active user-selected stack
5. implicit `local`

### App config precedence

1. `kitaru.configure(...)`
2. project config file
3. built-in defaults

### Execution behavior precedence

For things like retries:

1. explicit decorator or call-time values
2. framework defaults

Stacks should not secretly change logical behavior like retry policy in the MVP.

## Frozen resolved execution spec

At flow start, Kitaru should compute a **fully resolved execution spec** and persist it with the execution.

This spec should include the resolved snapshot of:

- selected stack
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
