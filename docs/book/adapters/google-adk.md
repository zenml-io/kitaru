---
description: Experimental Google ADK adapter support with runner-level and explicit model/tool checkpointing
icon: flask
---

# Google ADK Adapter

The Google ADK adapter is experimental. Use it when you already have a Google ADK agent and want Kitaru to record the parts of the run that ADK exposes to your Python process.

There are two useful paths today. Both persist checkpoints when the wrapped ADK call runs inside a Kitaru flow:

1. **Whole-runner checkpointing** — `KitaruADKRunner(..., checkpoint_strategy="runner_call")` wraps one ADK runner turn. ADK runs the agent, and Kitaru stores the resulting `ADKRunResult`.
2. **Explicit model/tool checkpointing** — `KitaruADKModel(...)` and `KitaruADKTool(...)` wrap ADK model and tool objects you pass into the agent yourself. This is the path for checkpointing the actual model call or tool call.

Runner-level arbitrary `calls` mode is intentionally blocked for now. The public ADK runner API verified by Kitaru does not expose a safe hook where Kitaru can put itself around every internal model/tool call in an arbitrary runner. If Kitaru only sees “before the step” and “after the step,” it cannot honestly replay the step body. So `checkpoint_strategy="calls"` raises with instructions to use `runner_call` or explicit wrappers.

## Install in an isolated ADK environment

`google-adk` currently resolves a newer FastAPI/Starlette stack than Kitaru's local server extra. Until that dependency conflict is resolved, run ADK checks in a no-dev environment and do not combine `--extra google-adk` with `--extra local`.

```bash
UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_adapter.py --help
```

For provider-backed runs, set one Google credential variable:

```bash
export GEMINI_API_KEY='<your-gemini-api-key>'
# or
export GOOGLE_API_KEY='<your-google-api-key>'
```

## Whole-runner checkpointing

Use `runner_call` inside a Kitaru flow when you want one durable result for a whole ADK turn:

```python
from kitaru.adapters.google_adk import ADKRunRequest, KitaruADKRunner

kitaru_runner = KitaruADKRunner(
    adk_runner,
    name="support_agent",
    checkpoint_strategy="runner_call",
)

result = await kitaru_runner.run(
    ADKRunRequest(
        user_id="user-123",
        session_id="session-456",
        message=user_content,
    )
)
```

A concrete failure story:

```text
1. Your Kitaru flow calls the ADK runner.
2. ADK calls Gemini, runs any ADK-owned tools, and emits events.
3. Kitaru stores the final ADK result as one checkpoint.
4. A later Kitaru checkpoint fails while writing your report.
5. You replay the flow.
```

On replay, Kitaru can return the saved `ADKRunResult` for that ADK turn instead of asking ADK to run the same turn again. It cannot replay only ADK's second internal tool call, because Kitaru did not control that call body.

`ADKRunResult.status` is one of:

- `completed` — ADK produced a final result.
- `requires_action` — ADK emitted a pending human-action request.
- `failed` — reserved in the serializable result model for captured failure paths.

Today, runner exceptions from `KitaruADKRunner.run(...)` and `run_sync(...)` normally raise to the caller instead of returning a `failed` result. The `failed` status exists so older and future serialized results can round-trip through `ADKRunResult`.

Use `final_output_preview(result.final_output)` for display text. ADK often returns a structured Google GenAI `Content` object, not a string.

```python
from kitaru.adapters.google_adk import final_output_preview

print(final_output_preview(result.final_output))
```

## Model/tool checkpointing

Use `KitaruADKModel` and `KitaruADKTool` when you control the ADK objects passed into the agent:

```python
from kitaru.adapters.google_adk import KitaruADKModel, KitaruADKTool

agent = LlmAgent(
    name="local_agent",
    model=KitaruADKModel(my_adk_model),
    tools=[KitaruADKTool(my_adk_tool)],
)
```

Mechanically, this means:

- ADK still decides when to call the model or tool.
- When ADK calls `generate_content_async(...)` on the wrapped model, Kitaru can put that provider call inside a checkpoint if the code is running inside a Kitaru flow.
- When ADK calls `run_async(...)` on the wrapped tool, Kitaru can put that tool call inside a checkpoint if the code is running inside a Kitaru flow.

That matters when the expensive or risky work is the model/tool call itself. If the model call completed and a later local step failed, replay can reuse the model checkpoint. If the tool call completed and changed replay-safe ADK tool state, replay can restore the recorded state mutation instead of running the tool again.

## Tool state replay rules

`KitaruADKTool` snapshots `tool_context.state` before and after the tool runs.

For replay to be safe:

- `tool_context.state` must behave like a mutable mapping.
- keys must be strings.
- values must be JSON-like values: `None`, strings, numbers, booleans, lists, and dictionaries.
- the starting state is part of the checkpoint identity.

If a cached tool checkpoint says “starting state was `{seed: same}` and ending state was `{seed: same, answer: cats}`,” Kitaru will only apply that mutation to the same starting state. If the current state is different, Kitaru refuses to apply the cached mutation and tells you why. That prevents a stale cached tool result from being pasted onto the wrong ADK session state.

## Human-action reporting

Kitaru reports ADK-emitted pending actions in `ADKRunResult.handoffs`. This is observational support: Kitaru tells you what ADK asked for, but it does not yet provide automatic ADK resume helpers.

The current support level is:

| ADK path | Kitaru v1 support |
|---|---|
| Tool confirmation | Proven through installed ADK runner flows. Kitaru reports a `tool_confirmation` handoff with tool name, tool args, function-call ids, invocation id, and message when ADK emits `adk_request_confirmation`. |
| Credential request | Proven through installed ADK runner flows. Kitaru reports a `credential_request` handoff with tool name, function-call ids, invocation id, and auth config when ADK emits `adk_request_credential`. |
| Graph human input | Proven at event-shape level through ADK's `adk_request_input` event. Kitaru reports a `human_input` handoff with message, payload, and response schema when that event appears. Full graph runner resume support is not claimed yet. |

`ADKHandoffRequest.kind` can be `tool_confirmation`, `credential_request`, or `human_input`.

## MCP tools inside ADK

ADK has its own MCP tool support. Kitaru's ADK adapter does not restore ADK-hosted MCP sessions.

The supported claim is narrower:

```text
ADK exposes a tool call/result
  -> you wrap that ADK tool with KitaruADKTool
  -> Kitaru can checkpoint that exposed ADK tool call/result
```

The unsupported claim would be:

```text
ADK starts a stateful MCP process
  -> the process disappears
  -> Kitaru restarts that process and restores the live ADK MCP session
```

Kitaru does not do that today. If the MCP session itself owns important state, make that state explicit in your workflow or let ADK recreate the session in its own documented way.

## Runnable example

The runnable example lives at:

```text
examples/integrations/google_adk_agent/google_adk_adapter.py
```

Run local no-provider mode:

```bash
UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_adapter.py
```

Run live Gemini mode:

```bash
export GEMINI_API_KEY='<your-gemini-api-key>'

UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_adapter.py --mode live
```

The example prints status, final output preview, event count, handoff count, and checkpoint strategy. It runs directly so it does not submit a Kitaru flow or persist a checkpoint by itself; it proves the ADK/Kitaru wrapper wiring you would place inside a flow for replay.

## Verification commands

Provider-free installed ADK contract checks:

```bash
UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  KITARU_REQUIRE_GOOGLE_ADK_CONTRACT=1 \
  uv run --python 3.12 --no-dev --extra google-adk --with pytest \
  pytest -o addopts='-vv' \
    tests/test_google_adk_installed_contract.py tests/test_google_adk_example.py
```

Optional live Gemini check:

```bash
export GEMINI_API_KEY='<your-gemini-api-key>'

UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk --with pytest \
  pytest -o addopts='-vv' tests/live/test_google_adk_provider_core.py -m "live_gemini"
```

## References

- [Google ADK Python quickstart](https://adk.dev/get-started/python/)
- [ADK tool confirmation](https://adk.dev/tools-custom/confirmation/)
- [ADK authentication and credential requests](https://adk.dev/tools-custom/authentication/)
- [ADK graph human input](https://adk.dev/graphs/human-input/)
- [ADK MCP tools](https://adk.dev/tools-custom/mcp-tools/)
