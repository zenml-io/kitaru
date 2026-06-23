---
description: Wrap an OpenAI Agents SDK Agent with KitaruRunner so calls are durable and replayable inside Kitaru flows
icon: robot
---

# OpenAI Agents Adapter

`KitaruRunner` wraps an existing OpenAI Agents SDK `Agent` so every model and
tool call runs as a durable checkpoint inside a Kitaru flow. You keep your agent
logic unchanged; you gain durable execution, and with it the ability to
[replay a real run with one input changed](../guides/replay-and-overrides.md)
and diff the result.

```python
from agents import Agent
from kitaru.adapters.openai_agents import KitaruRunner

agent = Agent(name="researcher", model=your_model)
runner = KitaruRunner(agent, checkpoint_strategy="runner_call")
```

The runtime default is `checkpoint_strategy="calls"` (per-call checkpoints —
see below); pass `"runner_call"` whenever you want a single terminal
checkpoint so `flow.run(...).wait()` returns the run result directly.

You run the agent through `runner.run(...)` or `runner.run_sync(...)` with an
`OpenAIRunRequest`.

## Install

```bash
uv add "kitaru[openai-agents,local]"
```

Then initialize/login as usual:

```bash
kitaru init
kitaru login
kitaru status
```

{% hint style="info" %}
Migrating an existing OpenAI Agents SDK project? The
[`zenml-io/kitaru-skills`](https://github.com/zenml-io/kitaru-skills) package
includes `/kitaru:kitaru-openai-agents-migration` for moving to `KitaruRunner`,
choosing `calls` vs `runner_call`, and checking approval/resume state. See
[Agent Skills](../agent-native/claude-code-skill.md).
{% endhint %}

## Minimal flow

```python
from kitaru import flow
from kitaru.adapters.openai_agents import KitaruRunner, OpenAIRunRequest

runner = KitaruRunner(agent, checkpoint_strategy="runner_call")

@flow
def research(prompt: str) -> str:
    result = runner.run_sync(OpenAIRunRequest.start(prompt))
    return str(result.final_output)
```

## Fresh-run context

OpenAI Agents SDK tools and guardrails often use a local application context:
for example, "which team is this user in?", "which thread is this request part
of?", or "which plugin settings are active?" Pass that object to Kitaru the same
way you pass it to the OpenAI SDK: as a runner-call argument, not as part of the
serializable `OpenAIRunRequest`.

```python
from dataclasses import dataclass
from typing import Any

from agents import RunContextWrapper, function_tool
from kitaru.adapters.openai_agents import KitaruRunner, OpenAIRunRequest

@dataclass(frozen=True)
class WorkerContext:
    team_id: str
    user_id: str
    thread_id: str
    message_id: str
    tool_settings: dict[str, Any]

@function_tool
def lookup_customer(ctx: RunContextWrapper[WorkerContext], customer_id: str) -> str:
    # The context stays local to your Python process. The model only sees what
    # your tool chooses to return.
    return f"team={ctx.context.team_id}, customer={customer_id}"

runner = KitaruRunner(
    agent,
    context_cache_identity=lambda ctx: {
        "team_id": ctx.team_id,
        "user_id": ctx.user_id,
        "thread_id": ctx.thread_id,
        "tool_settings": ctx.tool_settings,
    },
)

result = runner.run_sync(
    OpenAIRunRequest.start("Look up customer 123"),
    context=WorkerContext(
        team_id="team_abc",
        user_id="user_123",
        thread_id="thread_456",
        message_id="msg_this_run_only",
        tool_settings={"include_private_notes": False},
    ),
)
```

A concrete way to think about this: the `OpenAIRunRequest` is the written travel
plan Kitaru can save and replay. `context=` is the live badge the worker carries
while doing the trip. Tools and guardrails can inspect the badge through
`RunContextWrapper.context`, but Kitaru does not save that badge as a visible
artifact or send it to the model automatically. Kitaru still uses the context
identity internally for safe replay, without adding your raw context or projection
to visible tool input artifacts.

Context does matter for safe replay. Imagine two teams both call
`lookup_customer(customer_id="123")`. The visible tool arguments are identical,
but team A and team B may be allowed to see different customer records. Kitaru
therefore includes a context identity in adapter cache keys. If your context is
plain data, Kitaru can derive a structural identity. For production contexts,
prefer `context_cache_identity=` so you can include stable fields such as team,
user, thread, project, plugin, and JSON-primitive `tool_settings`, while
excluding per-run fields such as `message_id`, `trace_id`, or a changing
document cursor. That keeps replay safe without making every new message miss
the cache unnecessarily.

`context=` is different from `metadata=` on `OpenAIRunRequest.start(...)`:
metadata is Kitaru run/checkpoint metadata; context is local OpenAI Agents SDK
runtime state for your tools, guardrails, handoffs, and hooks.

Fresh context is only for new `kind="start"` requests. Interrupted/resumed runs
use the saved OpenAI `RunState`; `context_serializer=` and
`context_deserializer=` on `KitaruRunner` remain the way to serialize and rebuild
context that is already inside an interrupted SDK state. With
`checkpoint_strategy="calls"`, tool checkpoint cache keys use that restored SDK
context identity when it is available, so an approved resumed tool call for team
A does not accidentally reuse a cached tool result from team B. Kitaru uses only
the derived cache key for that separation; it does not save the raw context or
your `context_cache_identity=` projection in visible tool input artifacts.

One more boundary to remember: if your guardrail manually calls raw
`agents.Runner.run(...)` for a nested evaluator, that nested call is not managed
by Kitaru automatically. Wrap the nested evaluator with its own `KitaruRunner` if
you need Kitaru checkpoints there too.

## Checkpoint strategy choices

You choose how Kitaru places checkpoints with `checkpoint_strategy=`.

### `checkpoint_strategy="runner_call"` (recommended for `.wait()`)

Kitaru places one checkpoint around the outer OpenAI `Runner.run(...)` call. That
single checkpoint becomes the flow's terminal artifact, so
`flow.run(...).wait()` returns the run result cleanly. The name
`"runner_call"` is deliberately specific: it means Kitaru is wrapping the outer
OpenAI runner call, not claiming to own every SDK-internal step.

Use this when you want one coarse replay boundary for the whole agent run, or
whenever you want a clean Python value back from `.wait()`.

### `checkpoint_strategy="calls"` (default)

Kitaru catches supported model/tool calls individually as separate peer
checkpoints under the flow.

Use this when you want finer replay units: if call 6 fails, calls 1–5 can come
from cache, and you can replay from any individual call's checkpoint.

Because the per-call checkpoints are siblings under the flow with no single
sink, `flow.run(...).wait()` cannot pick one as "the" return value and raises
`KitaruAmbiguousFlowResultError`. The per-checkpoint artifacts are still
fully visible in the Kitaru UI and retrievable via `KitaruClient` — the
error message points at them. If you need a clean `.wait()` return value,
switch to `checkpoint_strategy="runner_call"`. Wrapping the `runner.run_sync()`
call in your own `@checkpoint` is **not** a workaround here — the adapter
guards against it and will raise, because per-call checkpoints cannot be
nested inside another Kitaru checkpoint.

## Sandbox command tool

Use `sandbox_command_tool(...)` when you want an OpenAI agent to run a command
through the sandbox on your current Kitaru stack:

```python
from agents import Agent

from kitaru.adapters.openai_agents import KitaruRunner, sandbox_command_tool

agent = Agent(
    name="diagnostic_agent",
    instructions=(
        "Use kitaru_sandbox_command for shell inspection tasks. The tool returns "
        "JSON. Check exit_code before trusting stdout."
    ),
    model="gpt-5-nano",
    tools=[
        sandbox_command_tool(
            max_chars=20_000,
            timeout_seconds=30,
            cleanup="destroy",
        )
    ],
)

runner = KitaruRunner(agent, checkpoint_strategy="runner_call")
```

Here is what happens, step by step:

```text
OpenAI model emits a local function-tool call
→ OpenAI Agents SDK invokes the Kitaru tool callback
→ the callback parses {"command": "...", "cwd": "..."}
→ Kitaru calls run_sandbox_command(...)
→ your current stack's sandbox runs the command
→ the callback returns compact JSON to the model
```

The model can provide only:

- `command` — required, non-empty string
- `cwd` — optional working directory inside the sandbox

The application keeps control of `max_chars`, `timeout_seconds`, and `cleanup`
when it creates the tool. The model cannot set `env`, `max_chars`, the runtime
limit, or cleanup behavior. `sandbox_command_tool(...)` uses a finite default
`timeout_seconds=30.0`; pass a different finite value when your app needs a
shorter or longer command budget.

{% hint style="warning" %}
This is not a secret-protection boundary. The model cannot pass a custom `env`
object to the tool, but it still chooses the command. If the sandbox can
read `/workspace/.env`, cloud credentials, SSH keys, internal network endpoints,
or inherited environment variables, then a model-chosen command can try to read
or exfiltrate them. In concrete terms: `sandbox_command_tool(...)` blocks "run
this command with these extra environment variables", but it does not block
"print the environment that is already there" or "read this file that the
sandbox user can already read".

Use the least-privileged sandbox you can: no unnecessary secrets, no broad cloud
credentials, and only the network access the task truly needs. For prompts or
users you do not fully trust, wrap the tool in your own allowlist or validator so
only approved commands and working directories reach `run_sandbox_command(...)`.
{% endhint %}

The compact JSON returned to the model contains:

```json
{
  "stdout": "...",
  "stderr": "...",
  "exit_code": 0,
  "stdout_truncated": false,
  "stderr_truncated": false,
  "timed_out": false,
  "cleanup_succeeded": true,
  "cleanup_error": null
}
```

The tool deliberately omits stack, sandbox, and session IDs from the model-visible
result. Those are operational details for Kitaru, not facts the model needs in
order to answer the user. Tell the model to inspect `exit_code` first: a command
can write to `stdout` and still fail. If `timed_out` is `true`, Kitaru stopped
waiting for the command after the app-owned timeout and returns `exit_code: -1`.

Your current Kitaru stack must have exactly one sandbox component. If there is no sandbox,
Kitaru raises an error and does not run the command. If there is more than one
sandbox, Kitaru raises instead of guessing. The bad version would be: you
expected a cheap local sandbox, Kitaru silently picked a different one, and the
model ran a command somewhere you did not intend.

Strategy behavior is the same as other local `FunctionTool` work:

- With `checkpoint_strategy="runner_call"`, Kitaru saves one outer checkpoint for
  the whole OpenAI runner call. The sandbox command runs during that SDK run.
- With `checkpoint_strategy="calls"`, Kitaru also saves the sandbox command as a
  separate `tool_call` checkpoint. Replaying the same command with the same tool
  settings can reuse that checkpoint instead of calling the sandbox again.

This helper does **not** redirect OpenAI-hosted tools into Kitaru's sandbox.
`CodeInterpreterTool`, hosted shell containers, hosted MCP, and other
provider-hosted tools run on OpenAI's side. Kitaru can still wrap the outer
runner call around them, but their execution environment is not the sandbox on
your current Kitaru stack.

## Streaming with Kitaru durability

Use `run_stream(...)` / `run_stream_sync(...)` when you want OpenAI Agents SDK
stream updates while the Kitaru checkpoint is still running. This is supported
for `checkpoint_strategy="runner_call"` only.

The concrete story is: OpenAI sends radio chatter while the agent runs. Kitaru
forwards useful pieces of that chatter as live events named
`openai_agents.stream.*`. When OpenAI's stream iterator finishes, Kitaru saves
the same durable `OpenAIRunResult` shape that `run(...)` / `run_sync(...)` save.
The live events help a UI or terminal feel alive; the final `OpenAIRunResult` is
the saved record.

Async flow:

```python
from kitaru import flow
from kitaru.adapters.openai_agents import KitaruRunner, OpenAIRunRequest

runner = KitaruRunner(agent, checkpoint_strategy="runner_call")

@flow
async def research(prompt: str) -> str:
    result = await runner.run_stream(OpenAIRunRequest.start(prompt))
    return str(result.final_output)
```

Sync flow:

```python
from kitaru import flow
from kitaru.adapters.openai_agents import KitaruRunner, OpenAIRunRequest

runner = KitaruRunner(agent, checkpoint_strategy="runner_call")

@flow
def research(prompt: str) -> str:
    result = runner.run_stream_sync(OpenAIRunRequest.start(prompt))
    return str(result.final_output)
```

To watch the live events, submit the flow and read execution events from the
client:

```python
import threading

from kitaru.adapters.openai_agents import (
    OPENAI_STREAM_EVENT_KINDS,
    OPENAI_STREAM_TERMINAL_EVENT_KINDS,
)
from kitaru.client import KitaruClient

handle = research.run("summarize the incident", cache=False)
stop_watching = threading.Event()


def watch_openai_events() -> None:
    try:
        for event in KitaruClient().executions.events(
            handle.exec_id,
            kinds=list(OPENAI_STREAM_EVENT_KINDS),
        ):
            if stop_watching.is_set():
                return
            data = event.payload.get("data", {})
            print(data.get("display", event.kind))
            if event.kind in OPENAI_STREAM_TERMINAL_EVENT_KINDS:
                return
    except Exception as exc:
        print(f"Live event watching unavailable; reading saved result instead: {exc}")


watcher = threading.Thread(target=watch_openai_events, daemon=True)
watcher.start()

result = handle.wait()
stop_watching.set()
watcher.join(timeout=1.0)
```

The watcher is optional and runs in the background so it cannot block the durable
result path. If event streaming is unavailable on the active backend, or if the
watcher sees no live events before it is stopped, `handle.wait()` still reads the
saved result after the flow finishes.

The runnable version lives at
[`examples/integrations/openai_agents_agent/openai_agents_streaming.py`](https://github.com/zenml-io/kitaru/blob/develop/examples/integrations/openai_agents_agent/openai_agents_streaming.py).
It uses a real customer-support agent, submits the flow, prints
`openai_agents.stream.*` events from `KitaruClient().executions.events(...)`, and
then prints the final durable `OpenAIRunResult`.

A few gotchas matter:

- Live events are best effort. Missing stream events do **not** mean the agent
  failed; check the final `OpenAIRunResult`.
- Replay may emit stream events again. If the process crashes after chunk six,
  replay may run the OpenAI stream again and you may see chunks one through six a
  second time.
- Cache hits may emit no OpenAI stream events. Kitaru found the saved final
  runner-call result, so there is no fresh OpenAI stream to forward.
- Kitaru does not save a durable token-by-token transcript in this slice. The
  durable value is the final `OpenAIRunResult`.
- `checkpoint_strategy="calls"` streaming is intentionally unsupported for now.
  Per-call streaming needs a buffering design so replay knows which chunks were
  already emitted.

Streaming also works with approval interruptions. The first streamed run may
finish with `status="interrupted"`; after the approval bridge returns a resume
request, stream the resumed request without passing fresh `context=`:

```python
from kitaru import flow
from kitaru.adapters.openai_agents import (
    KitaruRunner,
    OpenAIRunRequest,
    wait_for_approval,
)

runner = KitaruRunner(agent, checkpoint_strategy="runner_call")

@flow
async def publish_with_gate(prompt: str) -> str:
    result = await runner.run_stream(OpenAIRunRequest.start(prompt))

    if result.status == "interrupted":
        resume_request = wait_for_approval(
            result,
            name="approve_openai_tool",
            timeout=600,
        )
        result = await runner.run_stream(resume_request)

    return str(result.final_output)
```

## Structured outputs, guardrails, and nested agents

OpenAI Agents SDK structured outputs work through the adapter. If your agent is
created with `Agent(output_type=...)`, Kitaru preserves the SDK result object and
its typed `final_output` in both supported strategies:

- `checkpoint_strategy="runner_call"` records the outer runner call and returns
  the structured result from `.wait()` cleanly.
- `checkpoint_strategy="calls"` records supported model and tool calls
  individually, while the SDK still produces the typed final output for your
  Python code.

For tool-input guardrails, use `checkpoint_strategy="calls"` when you need to
see blocked tool attempts. In that strategy, Kitaru records a rejected tool
attempt as an existing `tool_call` event with guardrail metadata before the tool
function runs. It does not create a new event type, and it does not save a tool
checkpoint for arguments that the guardrail rejected.

Privacy follows the capture policy here too. If `save_input=False`, Kitaru omits
raw tool input artifacts and also redacts guardrail rejection messages and
unexpected guardrail exception details from persisted event metadata, because
those strings may repeat the user/tool input the guardrail just inspected. The
event still shows that a guardrail blocked the call, which guardrail did it, and
whether the behavior was `reject_content`, `raise_exception`, or an exception.

`checkpoint_strategy="runner_call"` still only sees the outer `Runner.run(...)`
boundary. That is useful for a single durable result, but it cannot show each
individual tool guardrail decision. Choose `"calls"` when per-tool guardrail
observability matters.

One more boundary to remember: raw nested `agents.Runner.run(...)` calls remain
outside Kitaru unless you wrap that evaluator agent with its own `KitaruRunner`.
Raw nested agents are fine for quick ephemeral checks. If their inputs, outputs,
or guardrail decisions need Kitaru observability, run them through
`KitaruRunner` too.

## Important guardrail

`checkpoint_strategy="calls"` must run from flow scope (not from inside another
`@checkpoint`), because the adapter needs room to open inner checkpoints for
model/tool calls.

## Approval interruptions

The adapter preserves OpenAI Agents SDK runs that stop for human approval. In story form: the agent reaches a tool approval, the SDK returns an interrupted run, Kitaru stores the serialized run state, and your flow can turn that interruption into a normal durable `kitaru.wait()`.

```python
from kitaru import flow
from kitaru.adapters.openai_agents import (
    KitaruRunner,
    OpenAIRunRequest,
    wait_for_approval,
)

runner = KitaruRunner(agent, checkpoint_strategy="runner_call")

@flow
def publish_with_gate(prompt: str) -> str:
    result = runner.run_sync(OpenAIRunRequest.start(prompt))

    if result.status == "interrupted":
        resume_request = wait_for_approval(
            result,
            name="approve_openai_tool",
            timeout=600,
        )
        result = runner.run_sync(resume_request)

    return str(result.final_output)
```

`wait_for_approval(...)` asks Kitaru to wait for a boolean approval and then returns an `OpenAIRunRequest.resume(...)` object. Approving resumes the saved OpenAI run state; rejecting sends the SDK a rejection message. If you already collected the decision somewhere else, use `build_resume_request(result, approve=True)` or pass an explicit `OpenAIApprovalDecision` to `OpenAIRunRequest.resume(...)`.

Keep this bridge at flow scope. If you put it inside a Kitaru checkpoint, the flow would be trying to pause from inside a step that is meant to finish or fail as one unit.

## Capture and checkpoint configuration

`KitaruRunner` exposes the same two kinds of knobs most teams need in production:

- **Capture policy:** what gets saved for observability.
- **Checkpoint policy:** how retries and dashboard grouping apply to adapter-created checkpoints.

```python
from kitaru.adapters.openai_agents import KitaruRunner, OpenAICapturePolicy

runner = KitaruRunner(
    agent,
    checkpoint_strategy="calls",
    capture=OpenAICapturePolicy(
        save_input=False,              # privacy: do not persist full user input
        save_final_output=True,
        save_run_state=True,           # needed for approval resume
        save_interruption_payloads=True,
        save_response_items=False,      # opt in only when you need raw SDK items
        save_usage=True,
    ),
    model_checkpoint_config={"retries": 2},
    tool_checkpoint_config={"retries": 1},
    tool_checkpoint_config_by_name={
        "charge_card": False,          # do not checkpoint side-effectful tool
        "search_docs": {"retries": 3},
    },
)
```

`OpenAICapturePolicy` defaults are designed for useful traces: child events, input, final output, run state, interruption payloads, usage, and OTel correlation are on; raw response items are off by default because they can be noisy.

## Usage and cost statistics

When `save_usage=True` (the default), each completed or interrupted runner call logs one canonical `llm_usage_v1` record. That record uses the OpenAI Agents result usage payload when the SDK exposes one, and it uses the runner name plus the adapter run label as the stable record identity.

If you pass a `cost_calculator=` to `KitaruRunner`, Kitaru stores the returned value as `estimated_cost_usd`. Calculator failures are non-fatal: the runner still returns the OpenAI result, adds a warning, and leaves the estimated cost empty. OpenAI Agents records do not store provider-reported actual cost in this adapter path, so `actual_cost_usd` is normally empty.

These records roll up into the execution-level LLM usage summary after your code observes the terminal execution with `FlowHandle.wait()` or `FlowHandle.get()`. Set `save_usage=False` when you do not want the adapter to persist canonical usage metadata for that runner call.

Two privacy switches are worth calling out:

- `save_input=False` keeps raw model/tool inputs out of artifacts and redacts
  tool-input guardrail messages or exception text that may contain those inputs.
- `save_interruption_payloads=False` keeps approval interruption summaries
  usable for resume decisions — index, kind, tool name, call ID, and message when
  the SDK exposes them — but omits raw `arguments` and `arguments_preview`.

Checkpoint config accepts `retries`, `type`, and `runtime`. `runtime="isolated"` is rejected for adapter-managed checkpoints today because those synthetic checkpoint closures capture live OpenAI SDK objects; use inline runtime or omit `runtime`.

For interrupted OpenAI runs, the SDK stores its own `RunState` so the run can resume later. If that saved `RunState` contains context objects that are not JSON-serializable, pass `context_serializer=` and `context_deserializer=` to `KitaruRunner`. These hooks are for serializing resume state after an interruption; they do not control the fresh-run `context=` object you pass when starting a new run. By default `strict_context=True`, so Kitaru fails loudly instead of saving a resume state that cannot be reconstructed later.

## Runnable example

This example uses the real OpenAI API (not a stub model), so set your key:

```bash
uv sync --extra local --extra openai-agents
uv run kitaru init
uv run kitaru stack create dev
export OPENAI_API_KEY='OPENAI_API_KEY_VALUE'
# default model in the example is gpt-5-nano
# optional override: any OpenAI model you have access to
# export OPENAI_AGENTS_MODEL='<another-openai-model>'
uv run python examples/integrations/openai_agents_agent/openai_agents_adapter.py

# sandbox command tool example
uv run python examples/integrations/openai_agents_agent/openai_agents_sandbox_tool.py

# streaming runner-call example
uv run python examples/integrations/openai_agents_agent/openai_agents_streaming.py
```

`uv run kitaru stack create dev` creates and activates the local stack whose
sandbox is used by the sandbox command tool example. If you switch stacks,
make sure your current stack has exactly one sandbox component before running
that example.

## End-to-end research bot example

For a larger example, run the OpenAI research bot:

```bash
cd examples/end_to_end/openai_research_bot
uv sync --extra local --extra openai-agents
uv run kitaru init
export OPENAI_API_KEY='OPENAI_API_KEY_VALUE'
uv run python research_bot.py "AI agent durability" --max-searches 2
```

The workflow keeps the original research-bot shape:

```text
planner → submitted search fan-out → writer report
```

The planner and writer run at flow scope through `KitaruRunner` with
`checkpoint_strategy="runner_call"`. The planned searches fan out with
`run_search_item.submit(...)`, so each search is its own durable checkpoint.
Ordinary Kitaru checkpoints publish stable dashboard artifacts such as the
normalized research plan, search summaries, and final report.

The example also uses a local OpenAI Agents SDK `@function_tool` named
`search_web` instead of the hosted `WebSearchTool`. The local tool calls the
OpenAI Responses API with `web_search`, which makes the checkpoint trace clearer
with the adapter's current public behavior.

Look for these artifacts in the Kitaru UI:

- `research_plan`
- `search_summaries`
- `durability_drill`
- `final_report`
- `research_report_metadata`

To test the durable-retry story directly, set
`KITARU_RESEARCH_BOT_FAIL_AFTER_SEARCHES=1` before running the example. It will
fail after the submitted searches complete. Unset the flag and run
`kitaru executions replay <EXECUTION_ID> --from durability_drill_gate`; the
replay should reuse the completed planner/search checkpoints and continue into
the writer. `retry` tries to restart the same failed execution and may be
unavailable on server-backed stacks after a run has concluded.

See also: [Replay and overrides](../guides/replay-and-overrides.md).
