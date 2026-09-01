---
description: Record one-shot Claude Agent SDK queries and rerun them with prompt, model, and SDK MCP tool overrides
icon: robot
---

# Claude Agent SDK

The Kitaru Claude Agent SDK adapter records a one-shot `query()` call as a Kitaru session. Your code still receives the original Claude message objects. In parallel, Kitaru records the input and the model, tool, subagent, usage, cost, and failure data exposed by the SDK message stream.

{% hint style="warning" %}
The adapter ships as the separately versioned `kitaru-claude-agent-sdk` distribution. It is not exported by the `kitaru` package and is not installed in the Kitaru server's default plugin catalog.
{% endhint %}

## Install

```bash
uv add kitaru-claude-agent-sdk
```

The first release supports `claude-agent-sdk>=0.2.149,<0.3` and Python 3.11 or newer.

## Record a query

Construct `KitaruClaudeRunner` with a Kitaru agent or agent-version ID, then consume its asynchronous message stream:

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

The adapter calls the Claude Agent SDK's public `query()` function and yields each message unchanged. It copies `ClaudeAgentOptions` before adding recording hooks, so it does not modify the options or hook collections you passed in.

Use `contextlib.aclosing()` if the consumer may stop before the terminal `ResultMessage`. Otherwise, cleanup has to wait for Python to close the asynchronous generator. `aclosing()` closes the Claude iterator and finalizes the partial Kitaru session when the consumer exits.

A standalone query must set either `agent_id` or `agent_version_id`. You may also set `session_name`; otherwise it falls back to `KITARU_SESSION_NAME`. Under a Kitaru worker, `KITARU_TASK_ID` links the result session to the task and supplies the agent identity.

Create the runner with `KitaruClaudeRunner(agent_id=None, agent_version_id=None, session_name=None)`. Its `query(*, prompt, options=None, replayable_servers=(), transport=None)` method accepts the same optional transport injection as the underlying SDK. This adapter only accepts string prompts.

## What replay means here

Replay starts a new Claude query with the recorded root input. It does not send the old assistant messages back to Claude or resume the provider session. Claude can take a different path through the new run.

When a worker runs the same program with a selected replay, the adapter can apply these changes before Claude starts:

- replace the root prompt;
- replace the system prompt;
- replace the model directly or through a mapping keyed by the current model; and
- apply supported tool policies to adapter-wrapped, in-process SDK MCP tools.

The adapter rejects `model_params`, `resume`, `continue_conversation`, and `fork_session` during replay because the public one-shot boundary cannot enforce those changes without combining the new run with hidden provider state.

## Make an SDK MCP server replayable

Claude's public `SdkMcpTool` handler is the tool boundary the adapter can replace. Declare those tools with `replayable_sdk_mcp_server()` instead of constructing their server yourself:

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

Creating this definition does not call Claude or run the handler. Pass it to each query that can use the tool:

```python
stream = runner.query(
    prompt="Look up ticket 4821.",
    options=ClaudeAgentOptions(
        tools=[],
        mcp_servers={},
        allowed_tools=["mcp__support__lookup"],
    ),
    replayable_servers=[support_server],
)
```

The policy identity is `mcp__<server name>__<tool name>`, or `mcp__support__lookup` in this example. The adapter creates a new SDK MCP server for each query. Wrapped handlers and replay state are therefore not shared between runs.

`replayable_sdk_mcp_server(name=..., tools=..., version="1.0.0")` returns the frozen `ReplayableSdkMcpServer` definition accepted by `query()`. You can construct the dataclass directly, though most code should use the helper.

## Tool replay policies

The adapter supports the shared Kitaru tool-policy behavior only for tools declared through `replayable_sdk_mcp_server()`:

| Policy | Behavior |
| --- | --- |
| `static` | Return the first exact or shallow-subset argument match without calling the handler. |
| `history` | Look up a recorded result by the exact tool identity and canonical JSON arguments. Baseline scope consumes repeated matches by occurrence; broader scopes use the server-selected match. |
| `error_result` | On a static or history miss, return a valid Claude MCP result with text content and `is_error: true`. |
| `passthrough` | Call the original handler. This includes `on_miss: passthrough` and can perform real, irreversible side effects. |

Static and history results must fit the adapter's versioned result format: a mapping with text MCP `content` blocks and an optional boolean `is_error`. A result may contain up to 100 text blocks and 64 KiB. This release cannot substitute images, embedded resources, audio, extra fields, or larger results.

A failed history match raises `ToolPolicyError` rather than trying to recreate the original exception class. A missing static or history result with `on_miss: fail` raises `ToolPolicyMissError`. The SDK MCP server normally converts handler exceptions into tool error results for Claude. Kitaru remembers these policy failures and raises them again from the outer query, then marks the session as failed.

Baseline history cannot assign a deterministic occurrence when two identical calls are in flight at once. The adapter rejects that case instead of guessing which recorded result belongs to which call. Use static replay or give the calls distinct tool identities or arguments.

## Isolation required for tool substitution

Tool substitution is fail-closed only when the adapter can prove that every enabled tool is one of the wrapped SDK MCP tools. Use `ClaudeAgentOptions(tools=[])` for a replay that contains static or history substitution. The adapter injects the wrapped SDK MCP tools separately.

The adapter rejects these configurations before it creates a Kitaru session or calls Claude:

- pre-existing `mcp_servers` entries;
- `allowed_tools` entries that are not exact wrapped identities;
- inline settings or filesystem setting sources;
- plugins, skills, or agent definitions; and
- extra CLI arguments.

Each of those options can add a tool outside Kitaru's wrappers. The public SDK has no single per-run switch that lets Kitaru inspect and deny every such tool. Remove the options for substituting replay, or use an all-passthrough policy. Recording-only runs and all-passthrough replays keep your tool configuration unchanged.

Before a substituting replay calls Claude, the adapter sets `setting_sources=[]` and `strict_mcp_config=True` on its private copy of the options. User settings, project settings, and `.mcp.json` files cannot add an unwrapped MCP server. Your original `ClaudeAgentOptions` object is unchanged.

{% hint style="danger" %}
Passthrough is a live call, not a simulation or transaction. A database write, message send, filesystem change, or external API call can happen again during replay. Put side-effecting tools in a disposable sandbox or choose a substituting policy when rerunning production failures.
{% endhint %}

## Capability matrix

| Capability | Recording | Replay |
| --- | --- | --- |
| One-shot string `query()` | Yes | Fresh rerun from the recorded root input |
| Native asynchronous message stream | Yielded unchanged and recorded | Yielded unchanged from the new run |
| Prompt, system prompt, and model | Recorded when exposed by the public SDK | Replacement supported |
| `model_params` | Not a replay boundary | Not supported |
| Adapter-wrapped in-process SDK MCP tools | Recorded | Static, history, error-result, and passthrough policies |
| Claude built-in tools | Recorded when exposed by public messages and hooks | Passthrough only; substitution is not supported |
| External MCP servers | Recorded when exposed by public messages and hooks | Passthrough only; substitution is not supported |
| LLM tool policy | Not applicable | Not supported |
| Async prompt iterable | Not supported | Not supported |
| `ClaudeSDKClient`, resume, continue, or fork | Not supported | Not supported |
| Original message trajectory or arbitrary mid-run state | Observed where the public stream exposes it | Not restored or played back |

## Recording failures and retries

Kitaru creates the session before it calls Claude, then writes the public messages as they arrive. If initial session creation fails, Claude does not run. If Kitaru fails after Claude or a tool has started, the adapter raises `KitaruRecordingError` and marks `retry_safe=False` and `side_effects_possible=True`. Automatic retry could call the model again, charge twice, or repeat a tool side effect.

The exception carries the Kitaru `session_id` when available, the failing `phase`, and the terminal Claude message when recording failed after one was produced. Preserve that evidence instead of blindly rerunning the query.

## Data and evaluation

Kitaru stores prompts, tool arguments and results, model output, reasoning text, and failure summaries as trace data. The adapter limits the size of recorded values and excludes provider-only fields such as thinking signatures. It does not add its own redaction policy. Decide what your application may send to Claude and its tools, and give the resulting Kitaru data the same access and retention controls as the source data.

Replay tells you what the changed program did on the same recorded input under the selected tool policy. It does not tell you whether the new answer is correct or better. Add an [evaluator](../guides/write-an-evaluator.md) for the behavior that matters, freeze the relevant sessions into a cohort, and compare the resulting evaluations.

## Fit for production-failure replay

You can use this adapter to select a failed production run, change the code, prompt, or model, and run the case again in a sandbox. Whether tool replay works depends on how the application is built:

1. The production entrypoint must use the one-shot string `query()` API rather than `ClaudeSDKClient`, an async prompt, resume, continue, or fork.
2. Each tool that must be substituted must originate as an in-process `SdkMcpTool` that can be declared through `replayable_sdk_mcp_server()`. Claude built-ins and external MCP tools can be observed, but not safely substituted.
3. Required state must be reconstructible from the recorded root input and tool results. Provider-session state, arbitrary filesystem state, and hidden process state are not restored.
4. Credentials and the worker command must be available in the rerun environment. Kitaru does not move provider or application secrets into the sandbox for you.
5. Any passthrough tool must be safe to execute again in that sandbox.

Check these points against the application's code and deployment before promising replay coverage. If they hold, Kitaru can rerun the same root case with changed code, prompt, or model and compare the runs with an evaluator. If they do not, the recording is still useful for diagnosis, but Kitaru cannot safely substitute every dependency in the run.

## Optional live smoke

The repository tests exercise the public Claude Agent SDK types and local fakes without a provider credential. For an opt-in live check, configure your normal Anthropic credentials plus `KITARU_API_URL`, `KITARU_API_KEY`, and `KITARU_AGENT_ID`, then run the recording example with a harmless prompt and no side-effecting tools. This sends a real provider request and may incur cost; it is not part of the default test suite.
