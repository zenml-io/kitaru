# Google ADK adapter example

This example shows the experimental Kitaru adapter for [Google ADK](https://adk.dev/).

The default run is deterministic and local: it uses real installed ADK classes, an ADK in-memory runner, a local dummy ADK model, and a local dummy ADK tool. No Gemini credentials and no hosted provider call are needed. The script is a direct adapter wiring demo; put the same runner/model/tool calls inside a Kitaru flow when you want persisted checkpoints and replay.

## What it demonstrates

There are two useful ADK paths today:

1. **Whole-runner checkpointing** with `KitaruADKRunner(checkpoint_strategy="runner_call")`.
   Inside a Kitaru flow, Kitaru wraps one ADK runner turn and returns an `ADKRunResult` with serialized events, status, handoffs, usage when ADK reports it, and the final output.
2. **Model/tool checkpointing** with explicit wrappers: `KitaruADKModel(...)` and `KitaruADKTool(...)`.
   In the local example, the ADK agent receives a wrapped local model and a wrapped local tool. When this pattern runs from inside a Kitaru flow, those wrapped calls are the point where Kitaru can checkpoint the actual model/tool work.

Runner-level arbitrary `calls` mode is intentionally unsupported for ADK right now. ADK's public runner API does not currently expose a safe hook where Kitaru can inject itself around every internal model/tool call in an arbitrary runner. Use `runner_call` for a broad whole-turn checkpoint, or wrap the concrete ADK model and tool objects you control.

## Install

Use an isolated no-dev environment for ADK:

```bash
UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_adapter.py --help
```

> **Important:** Do **not** combine `--extra google-adk` with `--extra local` yet. `google-adk` currently resolves a newer FastAPI/Starlette stack than Kitaru's local server path. The adapter is tested in a no-dev ADK environment until that dependency conflict is resolved.

## Run local mode

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

The story is: ADK asks the local model what to do, the local model asks for the `local_lookup` tool, ADK runs that wrapped local tool, and the model returns a final answer containing `local-cat-fact`. In this direct script, no Kitaru flow is submitted; the output proves the ADK/Kitaru wrapper wiring that you would place inside a flow for persisted checkpoints.

## Run optional live mode

Live mode uses ADK's Gemini model path and keeps Kitaru at the whole-runner level.

```bash
export GEMINI_API_KEY='<your-gemini-api-key>'

UV_PROJECT_ENVIRONMENT=.venv-google-adk \
  uv run --python 3.12 --no-dev --extra google-adk \
  python examples/integrations/google_adk_agent/google_adk_adapter.py --mode live
```

You can also set `GOOGLE_API_KEY`; the script accepts either variable.

## Output fields

The example prints:

- `Status` — `completed`, `requires_action`, or `failed`.
- `Final output preview` — produced with `final_output_preview(...)`, because ADK often returns a structured Google GenAI `Content` object rather than a plain string.
- `Event count` — how many serialized ADK events Kitaru captured.
- `Handoff count` — how many ADK human-action requests Kitaru observed.
- `Checkpoint strategy` — always `runner_call` in this example.

## Current limitations

This adapter is experimental/beta.

- `runner_call` is broad but coarse: one checkpoint around the whole ADK runner turn.
- `KitaruADKModel` and `KitaruADKTool` are the model/tool checkpoint path for ADK objects you pass into the agent yourself.
- Runner-level `calls` remains intentionally unsupported.
- HITL reporting is observational. Kitaru reports ADK-emitted pending actions in `ADKRunResult.handoffs`; it does not yet provide automatic ADK resume helpers.
- ADK-hosted MCP sessions are not restored by Kitaru. If ADK exposes an MCP-backed tool as an ADK `BaseTool` and you wrap that tool with `KitaruADKTool`, Kitaru can checkpoint the exposed ADK tool call/result. It does not restart or restore ADK's live MCP process/session.

