---
description: Experimental Google ADK adapter support with runner-level and explicit model/tool checkpointing
icon: flask
---

# Google ADK Adapter

The Google ADK adapter is experimental. Use it when you already have a Google ADK agent and want Kitaru to record the parts of the run that ADK exposes to your Python process.

{% hint style="info" %}
**Looking for Gemini Interactions or Antigravity?** Use the [Gemini Interactions adapter](gemini-interactions.md) if your code calls `client.interactions.create(...)`, polls `client.interactions.get(...)`, or uses the Antigravity managed agent. This page is for code built with `google.adk` agents, runners, models, and tools.
{% endhint %}

There are two useful paths today. Both persist checkpoints when the wrapped ADK call runs inside a Kitaru flow:

1. **Whole-runner checkpointing** — `KitaruADKRunner(..., checkpoint_strategy="runner_call")` wraps one ADK runner turn. ADK runs the agent, and Kitaru stores the resulting `ADKRunResult`.
2. **Explicit model/tool checkpointing** — `KitaruADKRunner(..., checkpoint_strategy="calls")` runs ADK directly while `KitaruADKModel(...)` and `KitaruADKTool(...)` checkpoint the model and tool objects you passed into the agent yourself.

`calls` mode is deliberately narrow. Kitaru does not inject plugins into the ADK runner, does not modify `ADKRunRequest.run_kwargs`, and does not checkpoint arbitrary unmodified ADK internals. The public ADK runner API verified by Kitaru does not expose a safe hook where Kitaru can put itself around every internal model/tool call in an arbitrary runner. If Kitaru only sees “before the step” and “after the step,” it cannot honestly replay the step body.

## Temporarily unavailable on this customer-demo branch

{% hint style="warning" %}
The `google-adk` extra is intentionally excluded on this branch. ZenML 0.96.2 requires OpenTelemetry 1.43.0, while currently released Google ADK versions require OpenTelemetry 1.42.1 or earlier. The adapter source remains for import-safety and source-level tests, but Google ADK installation and live checks are not supported on this branch. Commands below that use `--extra google-adk` are retained as reference and will not resolve here.
{% endhint %}

For provider-backed live runs, choose one Google auth path.

**Gemini Developer API key:**

```bash
export GEMINI_API_KEY='<your-gemini-api-key>'
# or
export GOOGLE_API_KEY='<your-google-api-key>'
```

**Local/manual Vertex AI with Application Default Credentials (ADC):**

```bash
gcloud auth application-default login
export GOOGLE_GENAI_USE_VERTEXAI=true
export GOOGLE_CLOUD_PROJECT='<your-google-cloud-project>'
export GOOGLE_CLOUD_LOCATION='<your-google-cloud-region>'
```

Kitaru only checks that the environment has one of those shapes. It does not call `gcloud`, read ADC files, create a Google credential object, or prove that your Google account can use Vertex AI. The concrete story is:

```text
1. You set GOOGLE_GENAI_USE_VERTEXAI=true, project, and location.
2. Kitaru says: "This looks like a valid Vertex setup."
3. ADK starts the model call.
4. Google GenAI looks for ADC.
5. If ADC is missing or expired, Google raises the auth error.
```

That is intentional: ADK / Google GenAI owns the real authentication step.

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

Today, runner exceptions from `KitaruADKRunner.run(...)` and `run_sync(...)` raise to the caller instead of returning a `failed` result. The `failed` status exists so older and future serialized results can round-trip through `ADKRunResult`. If you want local fallback behavior, catch exceptions around `runner.run(...)` or `runner.run_sync(...)`.

Use `final_output_preview(result.final_output)` for display text. ADK often returns a structured Google GenAI `Content` object, not a string.

```python
from kitaru.adapters.google_adk import final_output_preview

print(final_output_preview(result.final_output))
```

## Model/tool checkpointing

Use `KitaruADKRunner(..., checkpoint_strategy="calls")` together with `KitaruADKModel` and `KitaruADKTool` when you control the ADK objects passed into the agent:

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
- The Kitaru runner does not open a checkpoint around the whole ADK turn.
- While ADK runs, Kitaru installs a tracker context so explicit wrappers can report their events.
- When ADK calls `generate_content_async(...)` on the wrapped model, Kitaru can put that provider call inside a checkpoint if the code is running inside a Kitaru flow.
- When ADK calls `run_async(...)` on the wrapped tool, Kitaru can put that tool call inside a checkpoint if the code is running inside a Kitaru flow.
- If ADK uses an unwrapped model or tool internally, Kitaru reports a warning and does not invent a checkpoint for that hidden call.

That matters when the expensive or risky work is the model/tool call itself. If the model call completed and a later local step failed, replay can reuse the model checkpoint. If the tool call completed and changed replay-safe ADK tool state, replay can restore the recorded state mutation instead of running the tool again.

## Tool state replay rules

`KitaruADKTool` can snapshot `tool_context.state` before and after the tool runs when ADK gives it replayable state.

For state replay to be safe:

- `tool_context.state` must behave like a mutable mapping.
- keys must be strings.
- values must be JSON-like values: `None`, strings, numbers, booleans, lists, and dictionaries.
- the starting state is part of the checkpoint identity.

If a cached tool checkpoint says “starting state was `{seed: same}` and ending state was `{seed: same, answer: cats}`,” Kitaru will only apply that mutation to the same starting state. If the current state is different, Kitaru refuses to apply the cached mutation and tells you why. That prevents a stale cached tool result from being pasted onto the wrong ADK session state.

Some installed ADK paths pass a context whose `.state` exists but is not a mutable mapping. In that case, Kitaru cannot safely include the hidden state in the checkpoint identity or replay mutations onto it, so the tool runs directly and Kitaru records metadata only. If the tool is truly stateless and replay-safe, make that explicit through normal replayable inputs or mutable ADK state before relying on cached tool results.

## Human-action reporting and tool-confirmation resume

Kitaru reports ADK-emitted pending actions in `ADKRunResult.handoffs`. `ADKHandoffRequest.kind` can be `tool_confirmation`, `credential_request`, or `human_input`.

Tool confirmation has the strongest support today. Kitaru has a provider-free installed ADK contract test for the full loop:

```text
1. ADK asks for confirmation before running a tool.
2. Kitaru returns `ADKRunResult(status="requires_action")` with one `tool_confirmation` handoff.
3. You approve or deny that handoff.
4. Kitaru builds ADK's follow-up `Content` message.
5. You run the ADK runner again with that message.
6. ADK matches the response id to its pending confirmation request and continues.
```

Use the lower-level request helper when your application already has the human decision:

```python
from kitaru.adapters.google_adk import ADKRunRequest, build_tool_confirmation_request

first = await kitaru_runner.run(
    ADKRunRequest(user_id="user-123", session_id="session-456", message=user_content)
)

if first.status == "requires_action":
    handoff = first.handoffs[0]
    followup = build_tool_confirmation_request(
        handoff,
        confirmed=True,
        user_id="user-123",
        session_id="session-456",
    )
    second = await kitaru_runner.run(followup)
```

`build_tool_confirmation_request(...)` wraps `build_tool_confirmation_message(...)`. The message is a Google GenAI user-role `Content` containing one `function_response` named `adk_request_confirmation`. Its `id` is copied from `handoff.request_function_call_id`, because ADK uses that id to match the answer to the pending synthetic confirmation call. If that id is missing, Kitaru refuses to build the message.

Inside a Kitaru flow body, you can pause for a boolean approval decision and get the ADK follow-up request back:

```python
from kitaru.adapters.google_adk import wait_for_tool_confirmation

first = await kitaru_runner.run(request)
if first.status == "requires_action":
    followup = wait_for_tool_confirmation(
        first,
        user_id=request.user_id,
        session_id=request.session_id,
    )
    second = await kitaru_runner.run(followup)
```

Call `wait_for_tool_confirmation(...)` from the flow body, not inside `@checkpoint`. It calls `kitaru.wait(schema=bool, ...)`. Approval sends `{"confirmed": True}` to ADK. Denial sends `{"confirmed": False}`. If you pass `payload=...`, Kitaru includes that payload in the ADK confirmation response.

The current support level is:

| ADK path | Kitaru v1 support |
|---|---|
| Tool confirmation | Proven through installed ADK runner flows. Kitaru reports a `tool_confirmation` handoff and provides `build_tool_confirmation_message(...)`, `build_tool_confirmation_request(...)`, and `wait_for_tool_confirmation(...)`. |
| Credential request | Observed through installed ADK runner flows. Kitaru reports a `credential_request` handoff with tool name, function-call ids, invocation id, and auth config when ADK emits `adk_request_credential`. Kitaru does not export public credential resume helpers yet. |
| Graph human input | Proven at event-shape level through ADK's `adk_request_input` event. Kitaru reports a `human_input` handoff with message, payload, and response schema when that event appears. Kitaru does not export public graph human-input resume helpers yet. |

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

Kitaru does not do that today. The safe pattern is concrete: ADK hands Python a normal tool object; you pass that object to `KitaruADKTool`; ADK later calls `run_async(...)` on the wrapper; Kitaru checkpoints that Python call and its JSON-like result/state mutation. Kitaru does not restart an ADK-hosted MCP process, restore an MCP session, replay hidden MCP server state, or manage ADK's MCP connection lifecycle. If the MCP session itself owns important state, make that state explicit in your workflow or let ADK recreate the session in its own documented way.

## Streaming status

ADK streaming is intentionally deferred. The durable record today is the final `ADKRunResult` returned by `run(...)` / `run_sync(...)`; no ADK `run_stream(...)` API ships yet.

## Runnable examples

The direct adapter wiring example lives at:

```text
examples/integrations/google_adk_agent/google_adk_adapter.py
```

Run local no-provider mode:

```bash
UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_adapter.py
```

Run live Gemini mode with an API key:

```bash
export GEMINI_API_KEY='<your-gemini-api-key>'
# or
export GOOGLE_API_KEY='<your-google-api-key>'

UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_adapter.py --mode live
```

Or run live Gemini mode locally/manually with Vertex ADC:

```bash
gcloud auth application-default login
export GOOGLE_GENAI_USE_VERTEXAI=true
export GOOGLE_CLOUD_PROJECT='<your-google-cloud-project>'
export GOOGLE_CLOUD_LOCATION='<your-google-cloud-region>'

UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_adapter.py --mode live
```

The direct example prints status, final output preview, event count, handoff count, and checkpoint strategy. It runs directly so it does not submit a Kitaru flow or persist a checkpoint by itself; it proves the ADK/Kitaru wrapper wiring.

The persisted workflow example lives at:

```text
examples/integrations/google_adk_agent/google_adk_workflow.py
```

Run local no-provider workflow mode:

```bash
UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_workflow.py
```

That script submits a Kitaru flow. It passes explicit `KitaruADKModel` and `KitaruADKTool` objects into ADK, uses `KitaruADKRunner(checkpoint_strategy="calls")`, gets a tool-confirmation handoff on the first ADK turn, builds the ADK follow-up request, and returns structured output with the final answer, approval source, status history, event counts, and tracked model/tool event kinds. By default it injects a deterministic approval for tests and smoke checks. Pass `--interactive-wait` to pause the flow with `wait_for_tool_confirmation(...)` instead.

## Verification commands

Provider-free installed ADK contract checks:

```bash
UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  KITARU_REQUIRE_GOOGLE_ADK_CONTRACT=1 \
  uv run --python 3.12 --no-dev --extra google-adk --with pytest \
  pytest -o addopts='-vv' \
    tests/test_google_adk_installed_contract.py tests/test_google_adk_example.py
```

Optional live Gemini check with an API key:

```bash
export GEMINI_API_KEY='<your-gemini-api-key>'
# or
export GOOGLE_API_KEY='<your-google-api-key>'

UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk --with pytest \
  pytest -o addopts='-vv' tests/live/test_google_adk_provider_core.py -m "live_gemini"
```

Optional live Gemini check with local/manual Vertex ADC:

```bash
gcloud auth application-default login
export GOOGLE_GENAI_USE_VERTEXAI=true
export GOOGLE_CLOUD_PROJECT='<your-google-cloud-project>'
export GOOGLE_CLOUD_LOCATION='<your-google-cloud-region>'

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
