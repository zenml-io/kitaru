---
description: The agent is the identity, the session is the recording of every model call, tool call, and decision a run left behind.
icon: film
---

# Agents & Sessions

Most traces are transcripts: you read them. A Kitaru **session** is a recording you can run. It holds everything one agent run did, including every model call, tool call, and decision, in order, with inputs and outputs. That is what [replay](replay.md) needs to re-execute the run against your real code.

Two nouns carry the whole data model:

- An **agent** is the stable identity your runs attach to. You register it once, and every session, cohort, and experiment references it.
- A **session** is one recorded run of that agent. Sessions arrive three ways: recorded live by an adapter, [imported](../getting-started/import-your-traces.md) from your existing traces, or produced by a replay. All three are the same object with a different `origin`: `recorded`, `imported`, or `replay`.

## Agents and agent versions

Register an agent with the CLI:

```bash
kitaru agent register support-agent \
  --command "python support.py" \
  --description "Resolves support tickets"
```

This creates the agent and its first **agent version** in one step. A version pins what "the agent" meant at a point in time: a run spec (the command that starts your agent, its working directory, environment, secrets, and timeout) plus optional capability metadata (tools, MCP servers, skills). The run spec is what a [worker](workers.md) executes when a replay or experiment re-runs the agent in your environment.

Versions are server-numbered (1, 2, 3, …); `--display-version` attaches your own label, such as a semver, a git SHA, or a branch name:

```bash
kitaru agent version register support-agent \
  --command "python support.py" \
  --display-version "pr-1284"
```

Register a new version when the code changes. An [experiment](experiments.md) is precisely "replay this cohort on that agent version and see what moved."

## Runtime capabilities

The run spec also declares what the runtime can do during a re-run. `runtime_capabilities` holds two booleans, both `true` by default: `overrides`, whether the runtime can apply [replay](replay.md) overrides (model, prompts, model params), and `tool_policies`, whether it can apply non-passthrough [tool policies](../guides/tool-policies.md). Both work by intercepting model and tool calls inside the agent process. Some runtimes execute the agent for real and cannot intercept anything, and the server cannot tell that from the run command, so the version declares it.

Recording adapters intercept calls and keep the defaults. Declare both `false` when the version runs an importer-backed adapter, which records by importing the provider trace after the run and never intercepts a call. Set the declaration in the run spec at registration, via a spec document:

```yaml
run_spec:
  command: python agent.py
  env:
    KITARU_AGENT_ID: <agent-id>
  runtime_capabilities:
    overrides: false
    tool_policies: false
```

```bash
kitaru agent version register support-agent --spec spec.yaml
```

Creating a replay or starting an experiment run is rejected with 422 when its config carries an override or a non-passthrough tool policy the version's declared capabilities cannot apply. Runtimes that cannot apply them also fail the run when such a config reaches them anyway.

## What a session records

A session carries its top-level `inputs`, `outputs`, `status` (`in_progress` / `completed` / `failed`), timing, and rolled-up totals: `cost`, `tokens` (input / output / cached / reasoning), `llm_call_count`, and `tool_call_count`.

The step-by-step recording lives in the session's **nodes**: an ordered tree with one node per event.

| Node type | What it records |
| --- | --- |
| `llm_call` | Requested and resolved model, inputs and outputs, token usage, cost, model params |
| `tool_call` | Tool name, arguments, result, plus the cache key replay uses to answer the same call from the recording |
| `subagent_call` | A delegated run by a sub-agent |
| `span` | Any other grouping the adapter or importer wants to preserve |

Adapters record nodes automatically. The [PydanticAI adapter](../adapters/pydantic-ai.md) batches them to the server as the run progresses; importers write the same structure from your existing traces. There is one shape, so replay and evaluators never care where a session came from.

## One session is one end-to-end run

This is the most important thing to get right when you bring your own traces, and the easiest to get wrong.

A session is **the whole run, from the request that started it to the answer that ended it**, including every model call, tool call and sub-agent hop in between. It is not one model call, and it is not one span. Replay re-executes a session from the top, so a session that holds half a run can only ever reproduce half a run, and a cohort of them measures nothing you care about.

Adapters get this for free: the wrapper opens the session when your agent is invoked and closes it when the call returns. Importing needs a decision from you, because observability tools do not agree on what a trace is:

- Some emit **one trace per run**, which maps to one session directly. Nothing to do.
- Many emit **one trace per conversation turn**, so a five-turn support conversation arrives as five traces. If that is one run in your product, those five traces are one session.
- Some emit **one trace per model call**, which almost never matches a session on its own.

You do not have to reshape the export yourself. Importers group related traces into one session using the provider's own conversation or session identifier, and `--join-on` names the field to group on when the identity lives somewhere else. See [Join provider traces into sessions](../guides/importing-sessions.md). When no identifier is present, each trace becomes its own session, which is the safe default but rarely the one you want for multi-turn agents.

So the question before importing is not "what does my tool call a trace" but **"what does my product call one run"**. Then make the import produce that. If the answer is "it depends on how we configured tracing", resolve that upstream if you can: consistent session identity in your traces is what makes cohorts, experiments, and regression suites mean the same thing every time.

If you are joining a format no importer understands, do the joining in your [custom importer](../guides/importing-sessions.md) rather than after the fact. Sessions are not merged once they land.

## Reading sessions back

The Python client is async; every resource follows the same `list` / `iter` / `get` pattern:

```python
import asyncio
from kitaru.client import KitaruAPIClient
from kitaru.api_models.v1.session import SessionListParams
from kitaru.api_models.v1.session_node import SessionNodeListParams


async def main() -> None:
    client = KitaruAPIClient()  # KITARU_API_URL, KITARU_API_KEY
    page = await client.sessions.list(SessionListParams())
    for session in page.items:
        print(session.id, session.origin, session.status, session.cost)

    nodes = await client.sessions.list_nodes(
        page.items[0].id, SessionNodeListParams(include_payloads=True)
    )
    for node in nodes.items:
        print(node.index, node.node_type, node.name)


asyncio.run(main())
```

Node payloads (inputs, outputs) are returned only when you ask (`include_payloads=True`); listings stay cheap by default. The CLI mirrors both reads:

```bash
kitaru session list --agent support-agent --origin recorded
kitaru session nodes <session-id> --include-payloads
```

Sessions attach to the rest of the system by reference: a [cohort version](cohorts.md) pins a set of session ids, an [evaluation](evaluators.md) row evaluates one session, and a [replay](replay.md) points at its baseline session and produces a result session. **Tags** group resources ad hoc before they graduate into a cohort or another durable structure. A tag can link to a session, cohort, cohort version, agent version, experiment, or experiment run. Apply one to a whole import with `kitaru session import --tag ...`, then select on it anywhere that resource supports a `tag` filter, such as `kitaru session evaluate --tag ...`.

The native MCP server can list and filter tags, use existing filtered reads to rediscover tagged resources, and create, rename, link, unlink, or delete tags according to its capability mode. It cannot enumerate every link belonging to a tag. Deleting a tag removes all of its resource links; it does not delete the linked resources.

## Where sessions come from

- **Recorded:** wrap your agent with an adapter and run it as usual. See the [adapter overview](../adapters/README.md).
- **Imported:** bring the traces you already collect. Langfuse stays your system of record; Kitaru gets a runnable copy. See [Import your traces](../getting-started/import-your-traces.md).
- **Replay:** every replay produces a new session with `origin: replay`, evaluated by the same evaluators as any other session. See [Replay](replay.md).
