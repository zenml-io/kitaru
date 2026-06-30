# Google ADK adapter example

This example shows the experimental Kitaru adapter for [Google ADK](https://adk.dev/).

Start with `google_adk_workflow.py`: it runs a real Kitaru flow around a real installed ADK in-memory runner. The default path is deterministic and local, so it needs no Gemini credentials and makes no hosted provider call. Inside the flow, a local ADK model chooses two tools to calculate `(97 * 31) + 42`; the first tool goes through ADK tool confirmation, then the agent continues and calls the second tool.

This directory has two scripts:

- `google_adk_workflow.py` — the main example. It submits a persisted Kitaru flow, uses `checkpoint_strategy="calls"`, explicit `KitaruADKModel` / `KitaruADKTool` wrappers, tool-confirmation resume, and structured workflow output.
- `google_adk_adapter.py` — a lower-level direct ADK wiring smoke test. It proves the adapter can wrap installed ADK runner/model/tool objects outside a Kitaru flow.

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

## Run the persisted workflow mode

From the repository root, initialize the Kitaru project marker once and then run the workflow:

```bash
uv run kitaru init
UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_workflow.py
```

Expected shape:

```text
=== Workflow result ===
{
  "checkpoint_strategy": "calls",
  "final_answer": "final workflow answer: workflow-tool-calculation=3049 for cats",
  "human_decision_happened": true,
  "approval_source": "injected_decision",
  ...
}
```

This script submits a real Kitaru flow. ADK first asks to call `multiply_numbers(97, 31)`, which requires tool confirmation. The default path injects a deterministic approval so the example can run in tests and smoke checks without pausing. ADK then runs the multiplication tool, calls the `add_offset(3007, 42)` tool, and returns the final answer. Pass `--interactive-wait` to use `wait_for_tool_confirmation(...)`, which pauses at flow scope and returns the ADK follow-up request after the human decision.

## Run the direct local smoke test

From the repository root:

```bash
UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_adapter.py
```

This direct script does not submit a Kitaru flow. It is useful when you want to check the lower-level ADK runner/model/tool wrapper wiring.

## Run optional live workflow mode

Live mode submits the same Kitaru workflow, but the ADK agent uses Gemini or Vertex AI instead of the deterministic local model. The live agent gets two calculation tools and is prompted to call them, so the demo still proves that the ADK turn ran from inside the flow.

Choose one Google auth path.

**Gemini Developer API key:**

```bash
export GEMINI_API_KEY='<your-gemini-api-key>'
# or
export GOOGLE_API_KEY='<your-google-api-key>'

uv run kitaru init
UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_workflow.py --mode live
```

**Local/manual Vertex AI with Application Default Credentials (ADC):**

```bash
gcloud auth application-default login
export GOOGLE_GENAI_USE_VERTEXAI=true
export GOOGLE_CLOUD_PROJECT='<your-google-cloud-project>'
export GOOGLE_CLOUD_LOCATION='<your-google-cloud-region>'

uv run kitaru init
UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_workflow.py --mode live
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

- `mode` — `local` for the deterministic flow path, or `live` for Gemini/Vertex through ADK.
- `checkpoint_strategy` — `calls` for local model/tool checkpointing, or `runner_call` for live whole-turn checkpointing.
- `final_answer` — display text from the final ADK result.
- `human_decision_happened` and `approval_source` — local-mode fields that show whether ADK asked for confirmation and whether approval came from deterministic injection or `kitaru.wait()`.
- `first_turn` and `final_turn` — local-mode event counts, handoff counts, tracked model/tool event kinds, and checkpoint names.
- `turn`, `usage`, and `model` — live-mode event summary, token usage when ADK reports it, and the selected Gemini/Vertex model.

## Current limitations

This adapter is experimental/beta.

- `runner_call` is broad but coarse: one checkpoint around the whole ADK runner turn.
- `calls` is explicit-wrapper-only: `KitaruADKModel` and `KitaruADKTool` are the model/tool checkpoint path for ADK objects you pass into the agent yourself.
- Tool-confirmation resume is supported with `build_tool_confirmation_request(...)` and `wait_for_tool_confirmation(...)`; credential requests and graph human input are reported but do not yet have public resume helpers.
- ADK streaming is deferred. The durable record is the final `ADKRunResult`; no `run_stream(...)` API ships for ADK yet.
- ADK-hosted MCP sessions are not restored by Kitaru. If ADK exposes a replay-safe MCP-backed tool as an ADK `BaseTool`-like object and you wrap that tool with `KitaruADKTool`, Kitaru can checkpoint the exposed ADK tool call/result. It does not restart a process, restore a session, replay hidden MCP server state, or manage ADK's MCP connection lifecycle.

