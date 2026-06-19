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

## Live streaming example

Use `claude_agent_sdk_streaming.py` when you want to watch live Claude stream
updates while the Kitaru checkpoint is still running.

From the repository root:

```bash
uv sync --extra local --extra claude-agent-sdk
uv run kitaru init
export ANTHROPIC_API_KEY='<your-anthropic-api-key>'
uv run python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py
```

The script submits a Kitaru flow, starts a watcher for
`claude_agent_sdk.stream.*` events, and then waits for the final durable
`ClaudeRunResult`.

A few details are worth knowing before you run it:

- Live watching requires a REST-backed Kitaru backend with stream-event support.
  If that support is unavailable, the script still reads the saved result with
  `.wait()` and prints a friendly explanation.
- The example uses `allowed_tools=[]` and `max_turns=1`, so it is safe and
  predictable.
- The example disables checkpoint caching for the demo. In normal code, a
  repeated stream call can hit the stream cache; if that happens, Kitaru reuses
  the saved `ClaudeRunResult` and there may be no fresh live events.
- Text deltas are hidden from live event payloads by default. If you deliberately
  want clipped live text deltas, configure
  `ClaudeCapturePolicy(include_stream_text_deltas=True)`. Prompts, full tool
  input JSON, full options, raw SDK events, final result text, and structured
  output are not sent as live event payloads by default.
- The final `ClaudeRunResult` is the durable record. Treat live events as
  progress updates, like a radio feed while the checkpoint is running.

## Kitaru sandbox command tool example

Use `claude_agent_sdk_sandbox_tool.py` when you want Claude to run a command
through the sandbox attached to the active Kitaru stack.

From the repository root:

```bash
uv sync --extra local --extra claude-agent-sdk
uv run kitaru init
uv run kitaru stack create claude-sandbox --sandbox local
export ANTHROPIC_API_KEY='<your-anthropic-api-key>'
uv run python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_sandbox_tool.py
```

The script gives Claude a small temporary working directory, uses Claude Code's
`--bare` mode, selects the tool-capable `sonnet` model alias, and sets a `$0.10`
Claude SDK budget cap by default. That keeps a tiny sandbox demo from
accidentally loading your whole repository context into Claude Code. If you want
Claude to see a specific project directory, pass `--claude-cwd /path/to/project`
deliberately. Pass `--model <model>` to use another Claude model, and pass
`--max-budget-usd 0` only when you want to disable the demo budget cap.

If you already have a stack with exactly one sandbox component, use
`uv run kitaru stack use <stack-name>` instead of creating `claude-sandbox`.

This example gives Claude one MCP tool named `mcp__kitaru__run_command` and
denies Claude's built-in `Bash`. When Claude calls that tool, Kitaru runs the
command with `kitaru.run_sandbox_command(...)`, which uses the active stack's
sandbox component.

The default command is safe and read-only:

```text
python --version
```

The active stack must have exactly one sandbox component. If the active stack has
no sandbox, or has more than one, the tool returns a structured failure to
Claude instead of guessing where to run the command.

Optional overrides:

```bash
uv run python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_sandbox_tool.py \
  --command "pwd" \
  --sandbox-cwd /workspace \
  --max-turns 3 \
  --model sonnet \
  --max-budget-usd 0.10
```

This is different from Claude built-in `Bash`: Kitaru does not secretly reroute
`Bash`. The example removes that tool and gives Claude the explicit Kitaru MCP
command tool instead.

## What to look for in Kitaru UI

The flow contains one adapter-created checkpoint. In the sandbox command tool
example, it is named like
`claude_sdk_kitaru_sandbox_tool_claude_invocation`. That checkpoint is the
replay boundary for the Claude SDK call.

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

In the baseline adapter example, we keep the run non-destructive by setting
`allowed_tools=[]` and using a prompt that asks Claude not to use tools. In the
sandbox command tool example, Claude gets `tools=[]`, `permission_mode="dontAsk"`,
and only one pre-approved MCP tool: `mcp__kitaru__run_command`. That command goes
through the active Kitaru stack sandbox. In your own agent, if Claude uses tools
that mutate the world, such as
writing files or calling an MCP server that updates a ticket, Kitaru stores the
final invocation output and captured audit envelope. It does not automatically
replay or restore every internal side effect.

For the concept walkthrough, see
[Claude Agent SDK Adapter](https://docs.zenml.io/kitaru/adapters/claude-agent-sdk/).

For the full catalog, see [../../README.md](../../README.md).
