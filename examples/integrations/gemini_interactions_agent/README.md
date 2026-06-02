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
`gcloud` — you never pass a key by hand. Switch the SDK to the Vertex AI backend
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
interactions — every model returns `Unsupported model interaction`. So on ADC/Vertex,
use `--mode antigravity`. If you specifically want `--mode model`, you need an API
key (Option A); raw model interactions are an AI Studio (Developer API) feature.
The agent backend is only deployed to the `global` location, so set
`GOOGLE_CLOUD_LOCATION=global`. The first agent call is slow while Google provisions
a remote sandbox, which is why `--mode antigravity` submits a background job and
polls (see `--timeout`).

## Check without credentials

The help path is safe for smoke tests:

```bash
uv run python gemini_interactions_adapter.py --help
```

The dry-run path also avoids credentials, network calls, and Kitaru flow
execution:

```bash
uv run python gemini_interactions_adapter.py --dry-run --mode antigravity
```

## Run a cheap model interaction

Model interactions require an **API key** (Option A) — they run on the AI Studio
(Developer API) backend. On ADC/Vertex this mode is rejected, so use
`--mode antigravity` there instead.

The default real path uses `gemini-3.5-flash`:

```bash
uv run python gemini_interactions_adapter.py --mode model
```

Optional prompt override:

```bash
uv run python gemini_interactions_adapter.py \
  --mode model \
  --prompt "Explain Kitaru checkpoints using a simple train-station metaphor."
```

## Run the Antigravity managed-agent demo

Antigravity is a Google managed-agent preview and may be slower or costlier than
the cheap model path. It is the mode that works on ADC/Vertex. Run it explicitly:

```bash
uv run python gemini_interactions_adapter.py --mode antigravity
```

On ADC/Vertex, set `GOOGLE_CLOUD_LOCATION=global` first (the agent backend only
runs there). The example submits the agent as a background job and polls until it
finishes; the first call is slow while Google provisions a remote sandbox, so raise
`--timeout` if you hit a timeout:

```bash
uv run python gemini_interactions_adapter.py --mode antigravity --timeout 300
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

The script prints:

- interaction status
- interaction ID and previous interaction ID when reported
- environment ID when Google reports one
- output preview
- step summaries from `interaction.steps`
- usage when reported
- Kitaru artifact names
- warnings from best-effort capture or compatibility handling

## Important durability boundary

This adapter checkpoints the **outer interaction response**. It does not
checkpoint Gemini's internal reasoning, Google-owned tools, web/code execution,
hosted MCP calls, Antigravity sandbox mutations, or managed-agent environment
state one by one.

If a later part of your Kitaru flow fails, Kitaru can replay from the saved
`GeminiInteractionResult` instead of calling Google again for that completed or
`requires_action` interaction. If Gemini reports another status, Kitaru raises
instead of saving that unfinished remote job as a successful checkpoint; poll the
same interaction ID rather than starting a duplicate job. If a file or other
output must be durable in your workflow, explicitly return it from Gemini or
write it in a later Kitaru-owned checkpoint.

For the concept walkthrough, see
[Gemini Interactions Adapter](https://kitaru.ai/docs/guides/gemini-interactions-adapter).

For the full catalog, see [../../README.md](../../README.md).
