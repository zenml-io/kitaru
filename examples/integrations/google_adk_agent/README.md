# Google ADK adapter example

This example shows the experimental Kitaru adapter for [Google ADK](https://adk.dev/).

The default direct run is deterministic and local: it uses real installed ADK classes, an ADK in-memory runner, a local dummy ADK model, and a local dummy ADK tool. No Gemini credentials and no hosted provider call are needed.

This directory now has two scripts:

- `google_adk_adapter.py` — a direct ADK wiring smoke test. It proves the adapter can wrap real installed ADK runner/model/tool objects.
- `google_adk_workflow.py` — a persisted Kitaru flow. It uses `checkpoint_strategy="calls"`, explicit `KitaruADKModel` / `KitaruADKTool` wrappers, tool-confirmation resume, and structured workflow output.

## What it demonstrates

There are two useful ADK paths today:

1. **Whole-runner checkpointing** with `KitaruADKRunner(checkpoint_strategy="runner_call")`.
   Inside a Kitaru flow, Kitaru wraps one ADK runner turn and returns an `ADKRunResult` with serialized events, status, handoffs, usage when ADK reports it, and the final output.
2. **Model/tool checkpointing** with explicit wrappers and `KitaruADKRunner(checkpoint_strategy="calls")`.
   ADK still decides when to call the model and tool. Kitaru only checkpoints calls that go through the `KitaruADKModel(...)` and `KitaruADKTool(...)` objects you passed into ADK yourself. It does not checkpoint arbitrary unwrapped ADK internals.

## Install

Use an isolated no-dev environment for ADK:

```bash
UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_adapter.py --help
```

> **Important:** Keep using the isolated no-dev ADK environment for now. As of 2026-06-29, `google-adk` resolves FastAPI `0.138.0` / Starlette `1.3.1`, and this project still intentionally blocks combining `google-adk` with the local/dev extras until the full local server path is certified with that newer stack.

## Run the direct local mode

From the repository root:

```bash
UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_adapter.py
```

Expected shape:

```text
=== ADK result ===
Checkpoint strategy: runner_call
Status: completed
Final output preview: final local answer: local-cat-fact for cats
Event count: ...
Handoff count: 0
```

The story is: ADK asks the local model what to do, the local model asks for the `local_lookup` tool, ADK runs that wrapped local tool, and the model returns a final answer containing `local-cat-fact`. In this direct script, no Kitaru flow is submitted; the output proves the ADK/Kitaru wrapper wiring.

## Run the persisted workflow mode

From the repository root:

```bash
UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_workflow.py
```

Expected shape:

```text
=== Workflow result ===
{
  "checkpoint_strategy": "calls",
  "final_answer": "final workflow answer: workflow-local-cat-fact for cats",
  "human_decision_happened": true,
  "approval_source": "injected_decision",
  ...
}
```

This script submits a real Kitaru flow. The first ADK turn asks for tool confirmation. The default path injects a deterministic approval so the example can run in tests and smoke checks without pausing. Pass `--interactive-wait` to use `wait_for_tool_confirmation(...)`, which pauses at flow scope and returns the ADK follow-up request after the human decision.

## Run optional live mode

Live mode uses ADK's Gemini model path and keeps Kitaru at the whole-runner level.

Choose one Google auth path.

**Gemini Developer API key:**

```bash
export GEMINI_API_KEY='<your-gemini-api-key>'
# or
export GOOGLE_API_KEY='<your-google-api-key>'

UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_adapter.py --mode live
```

**Local/manual Vertex AI with Application Default Credentials (ADC):**

```bash
gcloud auth application-default login
export GOOGLE_GENAI_USE_VERTEXAI=true
export GOOGLE_CLOUD_PROJECT='<your-google-cloud-project>'
export GOOGLE_CLOUD_LOCATION='<your-google-cloud-region>'

UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_adapter.py --mode live
```

Kitaru only checks that the environment has one of those shapes. It does not call `gcloud`, read ADC files, create a Google credential object, or prove that your Google account can use Vertex AI. ADK starts the model call, then Google GenAI looks for credentials and raises the auth error if ADC is missing or expired.

## Output fields

The direct script prints:

- `Status` — `completed`, `requires_action`, or `failed`.
- `Final output preview` — produced with `final_output_preview(...)`, because ADK often returns a structured Google GenAI `Content` object rather than a plain string.
- `Event count` — how many serialized ADK events Kitaru captured.
- `Handoff count` — how many ADK human-action requests Kitaru observed.
- `Checkpoint strategy` — `runner_call` for `google_adk_adapter.py`.

The workflow script returns a structured dictionary. Its important fields are:

- `checkpoint_strategy` — `calls`.
- `final_answer` — display text from the final ADK result.
- `human_decision_happened` and `approval_source` — whether ADK asked for confirmation and whether approval came from deterministic injection or `kitaru.wait()`.
- `first_turn` and `final_turn` — event counts, handoff counts, tracked model/tool event kinds, and checkpoint names.

## Current limitations

This adapter is experimental/beta.

- `runner_call` is broad but coarse: one checkpoint around the whole ADK runner turn.
- `calls` is explicit-wrapper-only: `KitaruADKModel` and `KitaruADKTool` are the model/tool checkpoint path for ADK objects you pass into the agent yourself.
- Tool-confirmation resume is supported with `build_tool_confirmation_request(...)` and `wait_for_tool_confirmation(...)`; credential requests and graph human input are reported but do not yet have public resume helpers.
- ADK streaming is deferred. The durable record is the final `ADKRunResult`; no `run_stream(...)` API ships for ADK yet.
- ADK-hosted MCP sessions are not restored by Kitaru. If ADK exposes a replay-safe MCP-backed tool as an ADK `BaseTool`-like object and you wrap that tool with `KitaruADKTool`, Kitaru can checkpoint the exposed ADK tool call/result. It does not restart a process, restore a session, replay hidden MCP server state, or manage ADK's MCP connection lifecycle.

