---
description: Register, inspect, select, and delete Kitaru Agents through the CLI, SDK, and MCP server
icon: robot
---

# Agents

A **Kitaru Agent** is a registered AI application on a Kitaru server. Registration gives the application a stable identity, so Kitaru can associate later runs, versions, deployments, and traces with the same Agent.

For example, a PydanticAI application named `support-agent` can be registered once and then changed over time. Each registered implementation becomes an Agent version, while the Agent name remains the stable resource users select and inspect.

{% hint style="info" %}
Kitaru still uses **Project** terminology for connection configuration. Each initialized backing Project represents exactly one Agent. `kitaru login --project`, `KITARU_PROJECT`, `.kitaru/`, and `kitaru.configure(project=...)` select that Agent and its storage context.
{% endhint %}

## Register before running

A `KitaruAgent` must be registered before you run it directly:

```python
from pydantic_ai import Agent

from kitaru.adapters.pydantic_ai import KitaruAgent

agent = KitaruAgent(
    Agent(
        "openai:gpt-4o-mini",
        name="support-agent",
        system_prompt="Answer customer questions clearly.",
    )
)

version = agent.register(label="stable")
result = agent.run_sync("How do I reset my password?")
```

Registration creates or updates the Agent resource and records the current implementation as a version. It does not call the model, create a run or execution, or deploy anything.

The first local registration initializes the active default Project as the backing Project for that Agent. Creating, selecting, or deleting additional Agents through the management APIs requires ZenML Pro/Cloud.

### Registration identity

Registration stores the Agent identity on that Python object. If you change an identity-defining field such as its name after registration, Kitaru rejects the next direct run rather than recording data under the wrong Agent:

```python
agent.register()
agent.name = "different-agent"
agent.run_sync("Hello")  # Raises an identity-drift error.
```

Create a new `KitaruAgent` instance and register it when you intend to create a different Agent identity.

The underlying Pipeline UUID is the immutable Agent-version identifier. A registration label such as `stable` is also immutable once assigned. Registering a different version with the same label fails instead of moving the label.

## Inspect Agents from the CLI

The canonical commands are:

```bash
kitaru agents list
kitaru agents current
kitaru agents show support-agent
```

Use JSON output when another program consumes the result:

```bash
kitaru agents list --output json
kitaru agents current --output json
```

CLI, SDK, and MCP responses serialize an Agent consistently:

```json
{
  "agent_id": "agent-id",
  "name": "support-agent",
  "display_name": "Support Agent",
  "description": "Answers customer questions",
  "is_active": true,
  "default_agent_version_id": "pipeline-id",
  "default_executable": {
    "kind": "entrypoint",
    "entrypoint": "support_agent:agent",
    "repo_root_marker": ".kitaru"
  },
  "agent_version_aliases": {
    "stable": "pipeline-id"
  },
  "agent_versions": [
    {
      "schema_version": 1,
      "agent_version_id": "pipeline-id",
      "pipeline_id": "pipeline-id",
      "pipeline_name": "support_agent__av_12345678_abcdef123456",
      "fingerprint": "sha256:abcdef",
      "git_sha": "1234567890abcdef",
      "git_dirty": false,
      "working_tree_hash": null,
      "configuration_hash": "sha256:configuration",
      "worldview_hash": "sha256:worldview",
      "entrypoint": "support_agent:agent",
      "registered_at": "2026-07-17T10:00:00Z",
      "source": "registration",
      "aliases": ["stable"]
    }
  ],
  "version_count": 1
}
```

`display_name` and `description` are `null` when they are not set.

## Create and select Agents

Agent creation and selection through the management APIs require ZenML Pro/Cloud:

```bash
kitaru agents create evaluation-agent
kitaru agents use support-agent
```

By default, `agents create` activates the new Agent. Use `--no-activate` to create it without changing the active Agent:

```bash
kitaru agents create evaluation-agent --no-activate
```

The JSON create response includes the serialized `agent`, the `previous_active_agent` or `null`, and an `activated` boolean.

## Delete an Agent

Deletion is deliberately explicit:

```bash
kitaru agents delete evaluation-agent --yes
```

Without `--yes`, the CLI refuses before it calls the backend. Deletion requires ZenML Pro/Cloud and removes durable server state.

## List an Agent's experiments

Durable replay attempts are catalogued per Agent:

```bash
kitaru agents experiments
```

The SDK equivalent is `client.agents.experiments.list()` and
`client.agents.experiments.get(name_or_id)`. See
[Debug and test on real runs](replay-and-overrides.md#durable-experiments-with-a-registered-agent)
for how experiments are created.

## Python SDK

Use `client.agents` for the canonical lifecycle API:

```python
from kitaru import KitaruClient

client = KitaruClient()
current = client.agents.current()
agents = client.agents.list()
support = client.agents.get("support-agent")
```

Mutations require ZenML Pro/Cloud:

```python
created = client.agents.create("evaluation-agent", activate=False)
active = client.agents.use("support-agent")
deleted = client.agents.delete("evaluation-agent")
```

Use `KitaruClient.for_agent_management()` when the process must inspect or select an Agent before a normal Agent-scoped client can be created:

```python
client = KitaruClient.for_agent_management()

for agent in client.agents.list():
    print(agent.name)

client.agents.use("support-agent")
```

The top-level lifecycle functions and result models are also exported from `kitaru`:

```python
from kitaru import create_agent, current_agent, delete_agent, get_agent, list_agents, use_agent
```

## MCP tools

The MCP server exposes the canonical Agent tools:

- `kitaru_agents_list`
- `kitaru_agents_current`
- `kitaru_agents_show`
- `kitaru_agents_create`
- `kitaru_agents_use`
- `kitaru_agents_delete`

Deletion requires `confirm=true`. With the default `confirm=false`, the tool returns an error before calling the backend.

A useful MCP instruction is:

```text
Check the current Kitaru Agent. If it is not support-agent, select support-agent before listing its deployments.
```

## Deprecated Project-named compatibility

For one compatibility release, the hidden `kitaru project ...` command group and `client.projects` continue to delegate to the same lifecycle backend. Legacy text-mode CLI commands print a deprecation warning to stderr. JSON mode suppresses that warning and keeps the legacy `project.*` envelopes so stdout on success or stderr on failure remains one parseable JSON document.

New integrations should use `kitaru agents ...`, `client.agents`, and the `kitaru_agents_*` MCP tools. The MCP server does not expose deprecated Project-named aliases.

## Project connection selection

Kitaru maps one initialized backing Project to one Agent. Selecting a Project therefore selects that Agent and the storage context for its runs, versions, deployments, and traces. Multiple Agents are represented by multiple backing Projects, not by several independently selectable Agents inside one Project.

These existing connection interfaces intentionally retain Project terminology:

```bash
kitaru login https://kitaru.example.com --project production
export KITARU_PROJECT=production
```

Connection Project selection resolves from lower to higher priority:

1. The persisted Project selected by `kitaru login --project` or `kitaru agents use`
2. The compatibility `ZENML_ACTIVE_PROJECT_ID` environment variable
3. The public `KITARU_PROJECT` environment variable
4. Process-local `kitaru.configure(project=...)`

For headless execution, set `KITARU_SERVER_URL`, `KITARU_AUTH_TOKEN`, and `KITARU_PROJECT` explicitly.

## Related pages

- [Configuration](configuration.md)
- [Authentication](authentication.md)
- [Deployments](deployments.md)
- [MCP Server](../agent-native/mcp-server.md)
- [Python and CLI reference](https://sdkdocs.kitaru.ai)
