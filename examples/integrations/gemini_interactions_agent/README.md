# Gemini Interactions adapter example

This example shows the public `kitaru.adapters.gemini` API on a Gemini
Interactions API call.

Story in one line: a Kitaru flow sends one Gemini interaction request, and
`KitaruGeminiInteractionsRunner` records the stable response as one checkpoint.

The mental model is deliberately simple:

```text
flow body calls KitaruGeminiInteractionsRunner
  -> Kitaru opens one adapter-created checkpoint
  -> Gemini Interactions API runs once
  -> Kitaru stores GeminiInteractionResult
flow continues

If you pass `--stream`, the middle of that story gets a window: Kitaru publishes
best-effort live stream events while Gemini works, then still stores the same
final stable `GeminiInteractionResult`.
```

For Antigravity, Google still owns the managed-agent sandbox and internal tool
loop. Kitaru stores the outer stable interaction result; it does not snapshot the
Google-hosted sandbox filesystem.

## Setup

```bash
cd examples/integrations/gemini_interactions_agent
uv sync --extra local --extra gemini
uv run kitaru init
```

Then pick **one** of the two ways to authenticate with Google.

### Option A: API key (Gemini Developer API)

The simplest path for individual use:

```bash
export GEMINI_API_KEY='<your-gemini-api-key>'
```

`GOOGLE_API_KEY` is also accepted. If you set only `GOOGLE_API_KEY`, the script
copies it into `GEMINI_API_KEY` for the Google SDK process before making the
call.

### Option B: Application Default Credentials (Vertex AI)

If your organization blocks raw API keys (common in enterprise Google Cloud),
use Application Default Credentials (ADC) through Vertex AI instead. ADC is a
credential the Google libraries discover automatically after you log in with
`gcloud`; you never pass a key by hand. Switch the SDK to the Vertex AI backend
and tell it which project and region to use:

```bash
gcloud auth application-default login        # creates your ADC credentials once
export GOOGLE_GENAI_USE_VERTEXAI=true         # use Vertex AI instead of an API key
export GOOGLE_CLOUD_PROJECT='<your-gcp-project-id>'
export GOOGLE_CLOUD_LOCATION=global           # the agent backend lives in 'global'
```

With `GOOGLE_GENAI_USE_VERTEXAI=true` set, the SDK ignores API keys entirely and
authenticates with your ADC login. If you have a leftover `GEMINI_API_KEY` in
your shell, `unset GEMINI_API_KEY` so it cannot quietly send the example down the
API-key path instead.

**What Vertex AI supports today:** the Interactions API on Vertex currently serves
**agent** interactions (the `--mode antigravity` path), not raw **model**
interactions: every model returns `Unsupported model interaction`. So on ADC/Vertex,
use `--mode antigravity`. If you specifically want `--mode model`, you need an API
key (Option A); raw model interactions are an AI Studio (Developer API) feature.
The agent backend is only deployed to the `global` location, so set
`GOOGLE_CLOUD_LOCATION=global`. The first agent call is slow while Google provisions
a remote sandbox, so use `--timeout` to bound how long the background job and
same-id observation/polling path may run.

## Check without credentials

The help path is safe for smoke tests:

```bash
uv run python gemini_interactions_adapter.py --help
```

The dry-run path also avoids credentials, network calls, sandbox commands, and
Kitaru flow execution:

```bash
uv run python gemini_interactions_adapter.py --dry-run --mode antigravity
uv run python gemini_interactions_adapter.py --dry-run --stream
uv run python gemini_interactions_adapter.py --dry-run --mode sandbox-function
```

The `sandbox-function` dry run prints three concrete objects: a fake Gemini
`requires_action` result, a fake Kitaru sandbox command result, and the matching
fake `function_result` request that would be sent back to Gemini.

## Run a cheap model interaction

Model interactions require an **API key** (Option A); they run on the AI Studio
(Developer API) backend. On ADC/Vertex this mode is rejected, so use
`--mode antigravity` there instead.

The default real path uses `gemini-3.5-flash`:

```bash
uv run python gemini_interactions_adapter.py --mode model
uv run python gemini_interactions_adapter.py --mode model --stream
```

`--mode model --stream` is the direct Gemini Developer API streaming route: the
adapter calls `interactions.create(..., stream=True)` and reconstructs one stable
result from the stream.

Optional prompt override:

```bash
uv run python gemini_interactions_adapter.py \
  --mode model \
  --prompt "Explain Kitaru checkpoints using a simple train-station metaphor."
```

## Run the caller-owned sandbox function showcase

This mode demonstrates the supported custom-function path. Gemini asks for a
function named `sandbox_python_version`. Your application has explicitly
registered that name as a Kitaru sandbox command. Kitaru then runs
`python --version` in the active stack's sandbox and sends the command output
back to Gemini as a `function_result`.

```text
Gemini model interaction
  -> returns requires_action for sandbox_python_version
Kitaru checkpoint
  -> runs python --version in the active Kitaru sandbox
Gemini model interaction
  -> receives the function_result and writes the final answer
```

Real runs need both pieces:

1. Gemini Developer API credentials (`GEMINI_API_KEY` or `GOOGLE_API_KEY`).
2. An active Kitaru stack with exactly one sandbox component. For a local stack:

```bash
uv run kitaru stack create sandbox-demo
uv run kitaru stack current
```

Then run:

```bash
uv run python gemini_interactions_adapter.py --mode sandbox-function
```

V1 is deliberately static-command based. It does **not** parse model-supplied
function arguments. If Gemini asks for the registered function, the example runs
the command your code registered for that function name.

This does not redirect Antigravity internals, built-in Gemini code execution,
hosted MCP, web execution, or any Google-owned tool body into Kitaru. It only
covers the custom function body your Python application owns and explicitly
registered.

## Run the Antigravity managed-agent demo

Antigravity is a Google managed-agent preview and may be slower or costlier than
the cheap model path. It is the mode that works on ADC/Vertex. Run it explicitly:

```bash
uv run python gemini_interactions_adapter.py --mode antigravity
```

On ADC/Vertex, set `GOOGLE_CLOUD_LOCATION=global` first (the agent backend only
runs there). Antigravity defaults to `background=True`: the adapter creates one
background interaction, observes that same interaction id with streaming when the
backend supports it, and falls back to polling that same id if the stream drops.
Within that same live invocation, it does **not** start a duplicate provider job
after the interaction id is known.

```bash
uv run python gemini_interactions_adapter.py --mode antigravity --timeout 300
uv run python gemini_interactions_adapter.py --mode antigravity --stream --timeout 300
```

For this example, `--stream` shows clipped Gemini text chunks by default so you
can confirm streaming from the terminal. `[text_delta]` lines include an indented
`text_delta: ...` line with the actual chunk content.

If you want event labels only, hide the chunks explicitly:

```bash
uv run python gemini_interactions_adapter.py \
  --mode antigravity \
  --stream \
  --hide-text-deltas \
  --timeout 300
```

This affects the example display only. In SDK code, live stream text deltas are
still hidden by default unless you opt into
`GeminiInteractionCapturePolicy(include_stream_text_deltas=True)`.

If a preview endpoint explicitly rejects background mode, force foreground mode:

```bash
uv run python gemini_interactions_adapter.py \
  --mode antigravity \
  --foreground-antigravity \
  --timeout 300
```

The default Antigravity prompt asks for a high-level, non-destructive inspection
plan. Change the prompt if you want a different managed-agent task.

## What to look for in Kitaru UI

The flow contains one adapter-created checkpoint named like
`gemini_interactions_example_gemini_interaction`. That checkpoint is the replay
boundary for the Gemini interaction.

A good way to read the run is:

1. The flow body creates a `GeminiInteractionRequest`.
2. `KitaruGeminiInteractionsRunner.run_sync(...)` turns that request into one
   Interactions API call.
3. The checkpoint output is a `GeminiInteractionResult`.
4. The printed artifact names point to the captured redacted request manifest,
   output, usage, event log, and run summary. Raw input, raw interaction, and
   raw step artifacts are disabled by default unless you opt in.

One local-orchestrator wrinkle: the script submits the flow first, then starts a
watcher thread. On a very fast local run, some stream events may print only after
the flow submission returns, or the final result may appear before you see many
live lines. That is a display timing issue, not a second Gemini call. The durable
adapter assertion is still: one interaction id is created, observed or polled,
and saved as one stable `GeminiInteractionResult`.

The script prints:

- whether streaming was enabled
- interaction status
- interaction ID and previous interaction ID when reported
- environment ID when Google reports one
- output preview
- step summaries from `interaction.steps`
- usage when reported
- stream metadata when `--stream` was used
- Kitaru artifact names
- warnings from best-effort capture or compatibility handling

## Important durability boundary

This adapter checkpoints the **outer interaction response**. It does not
checkpoint Gemini's internal reasoning, Google-owned tools, web/code execution,
hosted MCP calls, Antigravity sandbox mutations, or managed-agent environment
state one by one.

If a later part of your Kitaru flow fails, Kitaru can replay from the saved
`GeminiInteractionResult` instead of calling Google again for that completed or
`requires_action` interaction. On a checkpoint cache hit, Kitaru returns the
saved final result; it should not be expected to replay fresh live stream events.
If Gemini reports another status, Kitaru raises instead of saving that unfinished
remote job as a successful checkpoint; poll the same interaction ID rather than
starting a duplicate job. If a file or other output must be durable in your
workflow, explicitly return it from Gemini or write it in a later Kitaru-owned
checkpoint.

For the concept walkthrough, see
[Gemini Interactions Adapter](https://docs.zenml.io/kitaru/adapters/gemini-interactions/).

For the full catalog, see [../../README.md](../../README.md).
