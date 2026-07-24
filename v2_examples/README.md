# PydanticAI and Langfuse import example

This example runs one unwrapped PydanticAI agent in a production-like path and
emits its trace only to Langfuse. It then imports a downloaded Langfuse JSONL
export into Kitaru. The registered agent version uses `KitaruAgent` later when
Kitaru launches a replay or experiment.

The complete example is one Python file:
[`pydantic_langfuse_import.py`](pydantic_langfuse_import.py).

## 1. Start Kitaru

From the repository root:

```bash
docker compose up -d server
curl --fail http://localhost:8000/health/live
cd v2_examples
```

The remaining commands assume the current directory is `v2_examples`.

The development Compose configuration uses:

- API URL: `http://localhost:8000`
- Account: `default`
- Password: `password`

## 2. Create an API key

Log in with the development account:

```bash
TOKEN=$(
  curl -s -X POST http://localhost:8000/v1/login \
    -d "username=default" \
    -d "password=password" |
  jq -r '.access_token'
)

curl -s -X POST http://localhost:8000/v1/api-keys \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"pydantic-langfuse-demo"}'
```

Copy the `key` value from the response. Kitaru returns its plaintext only when
the key is created.

If the server runs with `KITARU_SERVER_AUTH_SCHEME=none`, omit
`KITARU_API_KEY`. The development Compose server uses local authentication, so
it requires the key.

## 3. Configure the example

Create `.env`:

```dotenv
OPENAI_API_KEY=...

LANGFUSE_PUBLIC_KEY=...
LANGFUSE_SECRET_KEY=...
LANGFUSE_BASE_URL=https://cloud.langfuse.com

KITARU_API_URL=http://localhost:8000
KITARU_API_KEY=kat_...
```

The file is ignored by Git. Load it into the shell:

```bash
set -a
source .env
set +a
```

## 4. Generate the production trace

```bash
uv run --extra pydantic-ai --with langfuse \
  python pydantic_langfuse_import.py run
```

On its first run, the script:

1. Creates or reuses the `pydantic-langfuse-demo` Kitaru agent.
2. Creates or reuses its `langfuse-import-demo` runnable version.
3. Builds a regular, unwrapped PydanticAI agent.
4. Runs a weather-tool request.
5. Sends the model and tool activity to Langfuse.

This production execution does not use `KitaruAgent` and does not create a
Kitaru session. The registered version's runner command wraps the same agent
with `KitaruAgent` only when Kitaru launches it for a replay or experiment.

The output includes:

```text
kitaru_agent_id=...
kitaru_agent_version_id=...
langfuse_trace_id=...
```

Use the printed Langfuse trace ID to find the run in Langfuse.

## 5. Download and import the trace

Find the trace in Langfuse, download its trace or observations as JSONL, and
save the file locally. The importer accepts
trace-per-line exports, observation-per-line exports, and legacy ingestion
events.

Upload the exported file:

```bash
uv run --extra pydantic-ai --with langfuse \
  python pydantic_langfuse_import.py \
  import path/to/traces.jsonl \
  --source-instance prod
```

`--source-instance` identifies the Langfuse project. It can be omitted when
the export contains a project ID.

The command creates a background import job, waits for it, and prints:

```text
import_job_id=...
import_status=completed
created=1
deduplicated=0
kitaru_session_id=...
```

This import is when the production execution first becomes a Kitaru session.
Each Langfuse session becomes one Kitaru session. Multiple traces with the same
Langfuse session ID become turns in that session. A trace without a session ID
becomes a single-turn session.

Importing the same normalized evidence again returns the existing session and
increments `deduplicated`. Importing changed evidence creates a new immutable
revision.

## 6. Inspect the imported execution

List every imported Langfuse session for this agent version, including its
inputs, outputs, source metadata, replay-readiness report, and complete node
payloads:

```bash
uv run --extra pydantic-ai \
  python pydantic_langfuse_import.py list
```

The command prints JSON with one object per imported session:

```json
[
  {
    "session": {
      "id": "...",
      "origin": "imported",
      "provider": "langfuse",
      "inputs": {},
      "outputs": {}
    },
    "nodes": [
      {
        "node_type": "llm_call",
        "name": "...",
        "inputs": {},
        "outputs": {}
      }
    ]
  }
]
```

## Scope

The example uploads an exported file. It does not call the Langfuse API to
download traces. The Kitaru importer performs no network calls, and the
temporary uploaded bytes are discarded when the import job finishes.
