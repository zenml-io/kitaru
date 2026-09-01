# Kitaru Claude Agent SDK adapter

Record one-shot Claude Agent SDK queries as Kitaru sessions, then rerun recorded inputs with bounded prompt, model, and in-process SDK MCP tool overrides.

## Install

```bash
uv add kitaru-claude-agent-sdk
```

This package currently supports `claude-agent-sdk>=0.2.149,<0.3` and Python 3.11 or newer.

## Record a query

`KitaruClaudeRunner.query()` delegates to the Claude Agent SDK's public `query()` function and yields the exact native messages it produces:

```python
import contextlib
import os
import uuid

from claude_agent_sdk import ClaudeAgentOptions
from kitaru_claude_agent_sdk import KitaruClaudeRunner


async def run() -> None:
    runner = KitaruClaudeRunner(agent_id=uuid.UUID(os.environ["KITARU_AGENT_ID"]))
    stream = runner.query(
        prompt="Investigate ticket 4821.",
        options=ClaudeAgentOptions(model="claude-sonnet-4-5"),
    )
    async with contextlib.aclosing(stream) as messages:
        async for message in messages:
            print(message)
```

Use `contextlib.aclosing()` whenever the consumer may stop before the terminal message. It closes the Claude iterator and finalizes the partial Kitaru recording promptly.

Standalone queries require either `agent_id` or `agent_version_id`. A Kitaru worker supplies the task-bound identity automatically.

The public facade is `KitaruClaudeRunner(agent_id=None, agent_version_id=None, session_name=None)`. Its `query(*, prompt, options=None, replayable_servers=(), transport=None)` method accepts the same optional transport injection as the underlying SDK, but deliberately restricts `prompt` to a string.

## Declare replayable SDK MCP tools

Only in-process SDK MCP tools declared with `replayable_sdk_mcp_server()` are eligible for static or history substitution. Declaring a server is provider-free and does not execute the handler:

```python
from claude_agent_sdk import SdkMcpTool
from kitaru_claude_agent_sdk import replayable_sdk_mcp_server


async def lookup(arguments: dict[str, object]) -> dict[str, object]:
    return {
        "content": [
            {"type": "text", "text": f"Result for {arguments['query']}"}
        ]
    }


support_server = replayable_sdk_mcp_server(
    name="support",
    tools=[
        SdkMcpTool(
            name="lookup",
            description="Look up a support record.",
            input_schema={
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
            },
            handler=lookup,
        )
    ],
)
```

Pass the definition to every query that may use the tool:

```python
messages = runner.query(
    prompt="Look up ticket 4821.",
    options=ClaudeAgentOptions(
        tools=[],
        mcp_servers={},
        allowed_tools=["mcp__support__lookup"],
    ),
    replayable_servers=[support_server],
)
```

The adapter creates a fresh SDK MCP server for each query, so handler and history state are not shared across concurrent runs.

`replayable_sdk_mcp_server(name=..., tools=..., version="1.0.0")` returns the frozen `ReplayableSdkMcpServer` definition accepted by `query()`. The helper is the preferred construction API.

## Replay boundary

A replay starts a fresh Claude query from the recorded root input. It is a rerun, not playback of the original message trajectory.

Supported replay changes are:

- root prompt replacement;
- system-prompt replacement;
- direct or current-model-keyed model replacement; and
- `static`, `history`, `error_result`, and `passthrough` behavior for exact adapter-wrapped SDK MCP tool identities such as `mcp__support__lookup`.

Static and history replay results use a versioned, bounded envelope containing text MCP content blocks and an optional boolean `is_error`. Other result shapes are recorded as non-replayable.

Substituting tools requires an isolated `ClaudeAgentOptions(tools=[])` configuration. Pre-existing MCP servers, unwrapped allowed tools, inline or filesystem settings, plugins, skills, agents, and extra CLI arguments are rejected because they can introduce tools that the public SDK boundary cannot safely deny. An all-passthrough replay does not require that isolation.

`passthrough`, including a static or history `on_miss="passthrough"`, calls the original handler. Treat it as a real side effect.

Baseline history consumes matching results by occurrence. Concurrent calls with the same tool identity and canonical arguments are ambiguous and fail; use static replay or distinct identities or arguments instead.

## Not supported in v1

- async prompt iterables;
- `ClaudeSDKClient`, resume, continue, or fork workflows;
- LLM-based tool substitution;
- substitution of Claude built-in tools or tools from external MCP servers;
- `model_params` replay overrides;
- arbitrary mid-run state restoration or broad trajectory playback.

## Data and quality

Prompts, tool arguments, tool results, model output, reasoning text, and failure summaries are ordinary Kitaru trace data. The adapter bounds recorded values and excludes some provider-only fields, but it does not provide an adapter-specific redaction policy. Redact or filter data in your application before it reaches the model or tool when needed, and apply suitable access and retention rules to the Kitaru server.

Replay proves that the new run completed under the selected inputs and policies. It does not prove that the answer is better. Add an evaluator for the behavior you need to measure.

## Optional live smoke test

After configuring your normal Anthropic credentials and a Kitaru agent identity, run the recording example above with a harmless prompt and no side-effecting tools. This is deliberately opt-in because it sends a real provider request and may incur cost. The package test suite itself uses public SDK types and local fakes, so it requires no provider credential.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru/adapters/claude-agent-sdk)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
