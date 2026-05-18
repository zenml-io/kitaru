# Claude Agent SDK adapter example

This example shows the public `kitaru.adapters.claude_agent_sdk` API on a small
real Claude Agent SDK invocation.

Story in one line: a Kitaru flow asks Claude for a short summary, and
`KitaruClaudeRunner` records that whole Claude SDK invocation as one checkpoint.

The mental model is deliberately simple:

```text
flow body calls KitaruClaudeRunner
  -> Kitaru opens one adapter-created checkpoint
  -> Claude SDK runs once
  -> Kitaru stores ClaudeRunResult
flow continues
```

Keep this pattern in your own flows: call the runner from the flow body, not
from inside another `@kitaru.checkpoint`, so the adapter can create the Claude
invocation checkpoint itself.

If a later part of the flow fails, Kitaru can replay from the saved
`ClaudeRunResult` instead of calling Claude again for that completed invocation.

## Setup

```bash
cd examples/integrations/claude_agent_sdk_agent
uv sync --extra local --extra claude-agent-sdk
uv run kitaru init
export ANTHROPIC_API_KEY='<your-anthropic-api-key>'
```

The Claude SDK also supports Bedrock and Vertex modes if you configure those
provider-specific credentials instead. The example exits early with a friendly
message when no Claude/Anthropic credential path is visible.

## Run

```bash
uv run python claude_agent_sdk_adapter.py
```

To check the script without making an API call:

```bash
uv run python claude_agent_sdk_adapter.py --help
```

Optional prompt override:

```bash
uv run python claude_agent_sdk_adapter.py \
  --prompt "Explain Kitaru checkpoints using a simple cooking metaphor."
```

## What to look for in Kitaru UI

The flow contains one adapter-created checkpoint named like
`claude_sdk_summary_claude_invocation`. That checkpoint is the replay boundary
for the Claude SDK call.

A good way to read the run is:

1. The flow body creates a `ClaudeRunRequest`.
2. `KitaruClaudeRunner.run_sync(...)` turns that request into one SDK call.
3. The checkpoint output is a `ClaudeRunResult`.
4. The printed artifact names point to the captured messages, final output,
   usage, event log, and run summary for that one boundary.

The script prints:

- Claude's final text
- the Claude session ID
- the local transcript path when the SDK reports one
- cost and usage details when the SDK reports them
- Kitaru artifact names for messages, output, usage, event log, and run summary
- warnings when best-effort capture or event/log persistence has a problem

## Important durability boundary

This adapter checkpoints the **outer invocation result**. It does not checkpoint
Claude's internal model calls, Bash commands, built-in tool calls, MCP calls,
custom tool side effects, hooks, file edits, or workspace snapshots one by one.

In this example, we keep the run non-destructive by setting `allowed_tools=[]`
and using a prompt that asks Claude not to use tools. In your own agent, if
Claude uses tools that mutate the world, such as writing files or calling an MCP
server that updates a ticket, Kitaru stores the final invocation output and
captured audit envelope. It does not automatically replay or restore every
internal side effect.

For the concept walkthrough, see
[Claude Agent SDK Adapter](https://kitaru.ai/docs/guides/claude-agent-sdk-adapter).

For the full catalog, see [../../README.md](../../README.md).
