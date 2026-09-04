# Kitaru Claude Agent SDK adapter

Record one-shot Claude Agent SDK queries as Kitaru sessions, then rerun the same inputs with prompt, model, and SDK MCP tool overrides.

## Install

```bash
uv add kitaru-claude-agent-sdk
```

This package currently supports `claude-agent-sdk>=0.2.149,<0.3` and Python 3.11 or newer.

## Record a query

`KitaruClaudeRunner.query()` calls the Claude Agent SDK's public `query()` function and yields its message objects unchanged:

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

Create the runner with `KitaruClaudeRunner(agent_id=None, agent_version_id=None, session_name=None)`. Its `query(*, prompt, options=None, replayable_servers=(), transport=None)` method accepts the same optional transport injection as the underlying SDK. This adapter only accepts string prompts.

## Declare replayable SDK MCP tools

Only in-process SDK MCP tools declared with `replayable_sdk_mcp_server()` can use static or history substitution. Creating the definition does not call Claude or run the handler:

```python
from claude_agent_sdk import SdkMcpTool
from kitaru_claude_agent_sdk import replayable_sdk_mcp_server


async def lookup(arguments: dict[str, object]) -> dict[str, object]:
    return {"content": [{"type": "text", "text": f"Result for {arguments['query']}"}]}


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

The adapter creates a new SDK MCP server for each query, so concurrent runs do not share handler or history state.

`replayable_sdk_mcp_server(name=..., tools=..., version="1.0.0")` returns the frozen `ReplayableSdkMcpServer` definition accepted by `query()`. Most code should use the helper rather than construct the dataclass directly.

## Replay boundary

A replay starts a fresh Claude query from the recorded root input. It is a rerun, not playback of the original message trajectory.

Supported replay changes are:

- root prompt replacement;
- system-prompt replacement;
- direct or current-model-keyed model replacement; and
- `static`, `history`, and `passthrough` tool policies for exact adapter-wrapped SDK MCP tool identities such as `mcp__support__lookup`.

An agent version running this adapter keeps the default Kitaru runtime capabilities, `overrides` and `tool_policies` both `true`, because the adapter intercepts model and tool calls inside the agent process. A `model_params` override is therefore accepted when the replay is created and rejected by this adapter's preflight when the run starts, before Claude is called.

## Tool policies

The three policy types this adapter supports, and the `llm` type it does not:

| Policy | Behavior |
| --- | --- |
| `passthrough` | Calls the original handler. Any network request, database write, message send, or other side effect happens for real. |
| `static` | Returns the first exact or shallow-subset argument match without calling the handler. On a miss, `fail`, `error_result`, and `passthrough` behavior is supported. |
| `history` | Looks up a recorded result by the exact tool identity and canonical JSON arguments. Baseline scope consumes repeated matches by occurrence; broader scopes use the server-selected match. On a miss, `fail`, `error_result`, and `passthrough` behavior is supported. |
| `llm` | Rejected before the adapter creates a session or calls Claude; this policy is not supported by this adapter. |

`error_result` is a value of `on_miss`, not a policy `type`. A config sent as `{"type": "error_result"}` is rejected by the server with HTTP 422. On a static or history miss, `on_miss: error_result` returns a valid Claude MCP result with text content and `is_error: true`, so Claude reads the failure and can continue the run.

A replay that substitutes any tool must set `tool_policy.default` to an empty `static` policy with `on_miss: fail`. Every other default, `passthrough` included, is rejected with `Claude SDK MCP replay requires an empty static fail default policy` before the adapter creates a Kitaru session or calls Claude. A `passthrough` default is valid only for an all-passthrough replay, where no tool carries a substituting policy.

```json
{
  "default": {"type": "static", "cases": [], "on_miss": "fail"},
  "tools": {
    "mcp__support__lookup": {"type": "static", "cases": [], "on_miss": "error_result"}
  }
}
```

The Kitaru [tool policies guide](https://docs.zenml.io/kitaru/guides/tool-policies) covers the shared behavior of these types and their scopes.

Static and history results use a versioned format with text MCP content blocks and an optional boolean `is_error`. Kitaru records other result shapes but cannot substitute them during replay.

Tool substitution requires `ClaudeAgentOptions(tools=[])`. The adapter rejects existing MCP servers, unwrapped allowed tools, explicit settings, plugins, skills, agents, and extra CLI arguments because they can add tools outside Kitaru's wrappers. It also sets `setting_sources=[]` and `strict_mcp_config=True` on a copy of the options, which stops user settings, project settings, and `.mcp.json` files from adding an unwrapped server. Your original options stay unchanged. An all-passthrough replay does not need this isolation.

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

Kitaru stores prompts, tool arguments and results, model output, reasoning text, and failure summaries as trace data. The adapter limits the size of recorded values and excludes some provider-only fields. It does not add its own redaction policy. Filter data in your application when needed, and set suitable access and retention rules on the Kitaru server.

The session's root node records the prompt string actually sent to Claude as the `effective_prompt` attribute. A replay that overrides the prompt keeps the baseline prompt in the session inputs, the shared Kitaru convention that lets a cohort compare arms on one task input, so the root attribute is where you read the text the model received. A prompt longer than the adapter's recorded-value limit is stored as `{"value": "...", "truncated": true}` rather than as a silently shortened string.

Claude reports per-call token usage as a snapshot taken before generation finishes, so the count on a single `llm_call` node is a partial. The authoritative totals arrive on the terminal `ResultMessage`, and Kitaru records the difference between those totals and the per-call counts as usage on the root node, the same place it records the run's total cost. Session totals therefore add up to Claude's own numbers, and Claude's thinking tokens appear in Kitaru's reasoning-token field. The difference is never recorded as a negative number, so a field whose per-call counts already exceed Claude's total keeps a session total above that number with the root node carrying nothing. A run that never reaches a terminal message records no cost and keeps only the partial per-call counts.

When Claude's terminal message reports a failure, the recorded reason is the most readable cause it carries: the errors Claude reported, then the result text, then the terminal reason, then the subtype. A terminal reason or subtype that reads as a success, `success` and `completed`, is skipped, and a message carrying no cause at all records `Claude reported a failed result`. A provider API error such as a 529 overload arrives with the subtype `success` and the cause in the result text, and its HTTP status is kept in the session's terminal metadata as `api_error_status`.

If the consumer stops the stream early and Kitaru then fails to close the session or to close the Claude iterator, the adapter logs a warning naming the session ID on the `kitaru_claude_agent_sdk.runner` logger. Closing an asynchronous generator discards the exception raised inside it, notes included, so the log is the only report that survives.

Replay proves that the new run completed under the selected inputs and policies. It does not prove that the answer is better. Add an evaluator for the behavior you need to measure.

## Optional live smoke test

After configuring your normal Anthropic credentials and a Kitaru agent identity, run the recording example above with a harmless prompt and no side-effecting tools. The live test is opt-in because it sends a real provider request and may incur cost. The package test suite uses public SDK types and local fakes, so it requires no provider credential.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru/adapters/claude-agent-sdk)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
