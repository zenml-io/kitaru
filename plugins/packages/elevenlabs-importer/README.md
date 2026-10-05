# Kitaru ElevenLabs importer

Import finished ElevenLabs Agents conversations as Kitaru sessions, including transcript events, tool calls, source analysis, and recording references. This is an unreleased development package. It has not been published to PyPI or included in the server-default importer catalog.

Use the file parser when you already retain ElevenLabs conversation responses in your own database. The optional API fetcher reads selected conversations from ElevenLabs. Neither path starts a voice conversation or runs the source agent.

## Accepted payloads

The parser accepts a full conversation detail response from `GET /v1/convai/conversations/{conversation_id}`, a JSON array of full detail responses, an object containing `{"conversations": [details]}`, or a `post_call_transcription` webhook envelope with the detail record in `data`. Conversation-list summaries are insufficient because they do not contain the complete transcript. The upload must be one JSON document, not JSONL.

Only terminal `done` and `failed` conversations are accepted. Import finished records because a later import with the same conversation ID is skipped, not applied as an update. Parser parameters must be empty: `{}`. Conversation grouping and `--join-on` are unsupported.

A malformed conversation yields an isolated `ImportFailure`, allowing valid neighboring conversations to import. An unreadable JSON document or unsupported parser parameters fail the upload.

## Preserved data

Each conversation becomes one session identified by its ElevenLabs conversation ID. Kitaru deduplicates that ID together with the importer provider. Native IDs, agent identity, status, timestamps, selected call metadata, analysis, and raw billing data remain available in the imported payloads and metadata. `metadata.elevenlabs.provenance` records the upload format and the webhook timestamp when supplied.

Each transcript event becomes a generic span that preserves its role, message, time within the call, interruption information, and available usage. These spans describe recorded conversation events. They do not claim that an utterance is a complete underlying model request or response. Text selectors show each event's message in the Kitaru node thread. Session selectors identify the first user message and the last agent message when those messages exist.

Tool calls become tool nodes linked to their transcript event. Requests and results are matched by `request_id`. JSON-encoded arguments and results are decoded for inspection, with the originals preserved in metadata. Failed results remain failures. A request without a result remains an in-progress node, even in a finished conversation, and a result without a request records its missing request in metadata.

Single-model usage can populate model, token, and reported model-cost fields. Usage involving several models stays in metadata rather than being assigned to a fabricated model call. Conversation charges and ElevenLabs credits remain separate billing metadata. Missing model inputs, system prompts, timing, usage, and costs stay missing.

When the source reports audio availability, `metadata.elevenlabs.recording_url` references `GET /v1/convai/conversations/{conversation_id}/audio`, and `recording_requires_authentication` is `true`. That endpoint requires ElevenLabs authorization. The importer does not download or host audio, produce a public playback URL, or add an audio player to Kitaru.

## Use the development package

Run these commands from a Kitaru checkout. The parser entrypoint is `kitaru_elevenlabs_importer:parse`. The `kitaru_elevenlabs_importer:importer` entrypoint supports both file parsing and API fetching.

```bash
uv sync --frozen --extra cli --extra worker
uv sync --project plugins --frozen --all-packages
uv run --project plugins python - <<'PY'
from pathlib import Path
from kitaru_elevenlabs_importer import parse

for item in parse(Path("elevenlabs-conversations.json").read_bytes(), {}):
    print(item.model_dump_json())
PY
```

For a local server and worker, build and validate a candidate wheel before registration:

```bash
uv run --no-sync python scripts/smoke_plugin_artifacts.py \
  --package plugins/packages/elevenlabs-importer \
  --candidate-dir /tmp/kitaru-elevenlabs-wheels
```

Start a worker that can resolve the local wheel. Configure `ELEVENLABS_API_KEY` in that worker's environment only if you want API imports. File imports need no ElevenLabs credentials.

```bash
UV_FIND_LINKS=/tmp/kitaru-elevenlabs-wheels \
  uv run --no-sync kitaru worker start \
  --server http://localhost:8000 \
  --name elevenlabs-import-worker \
  --claim importer
```

In another terminal, register the exact candidate version and import a file:

```bash
uv run --no-sync kitaru importer register elevenlabs-local \
  --server http://localhost:8000 \
  --provider elevenlabs \
  --package kitaru-elevenlabs-importer==0.1.0 \
  --entrypoint kitaru_elevenlabs_importer:importer \
  --display-version 0.1.0

uv run --no-sync kitaru session import elevenlabs-conversations.json \
  --server http://localhost:8000 \
  --importer elevenlabs-local@latest \
  --agent your-agent@latest \
  --wait
```

These commands create state on the selected development server. Registration stores the package requirement and entrypoint, not wheel bytes. The worker resolves the candidate wheel through `UV_FIND_LINKS`. Register a new immutable importer version when the implementation changes.

## Fetch from the ElevenLabs API

The API fetcher requires `ELEVENLABS_API_KEY` in the worker environment with **ElevenAgents Read** permission. Keep the key out of uploaded records, parser parameters, query parameters, and registration metadata. The fetcher uses the official ElevenLabs API endpoint and does not accept a custom host.

Select individual conversations using the standard Kitaru `--trace-id` flag:

```bash
uv run --no-sync kitaru session import \
  --server http://localhost:8000 \
  --importer elevenlabs-local@latest \
  --agent your-agent@latest \
  --trace-id your-conversation-id \
  --wait
```

Alternatively, select a time window and optionally an ElevenLabs agent:

```bash
uv run --no-sync kitaru session import \
  --server http://localhost:8000 \
  --importer elevenlabs-local@latest \
  --agent your-agent@latest \
  --since 7d \
  --query '{"agent_id":"your-elevenlabs-agent-id","limit":100}' \
  --wait
```

An explicit conversation selection cannot be combined with a time window or `agent_id`. For a time window, `since` is required and `until` defaults to the current time. `--until` selects an explicit upper bound. API query settings are:

| Field | Accepted value |
|---|---|
| `trace_ids` | Conversation IDs, supplied through repeatable `--trace-id` flags or `--query`. |
| `since`, `until` | Timestamps supplied through `--since`, `--until`, or `--query`. |
| `agent_id` | Optional ElevenLabs agent filter for a time window. |
| `concurrency` | Concurrent detail requests, from 1 through 16. Default: 4. |
| `page_size` | Listing page size, from 1 through 100. Default: 100. |
| `limit` | Maximum scanned conversation-list rows or explicit IDs, from 1 through 10,000. Default: 1,000. |

API IDs contain only ASCII letters, digits, underscores, or hyphens and are at most 256 characters long. Explicit selections reject duplicate IDs. Unknown query fields are rejected.

The fetcher paginates conversation summaries and retrieves full details before parsing. The same `limit` also bounds listing requests, so an endless sequence of empty pages fails instead of running indefinitely. A time-window import skips active conversations, which still count toward the scan limit. An explicit ID for an active conversation produces an `ImportFailure`. Reimporting a conversation already stored in Kitaru does not refresh its transcript, analysis, or recording reference.

## Evaluation and replay limits

Imported sessions can be inspected, annotated, and evaluated in Kitaru. Source analysis remains ElevenLabs metadata. It does not automatically become a Kitaru evaluator result.

This package supplies no runnable ElevenLabs agent adapter. Importing a transcript does not provide model or prompt overrides, controlled tool substitution, voice replay, or audio-quality evaluation. Those operations require a separate runtime integration and an appropriate evaluator.

Licensed under Apache-2.0.
