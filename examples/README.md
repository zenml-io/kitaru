# Improve an agent from imported traces

The canonical example starts with a prepared Langfuse JSONL export from a
PydanticAI document extractor. It contains 13 trace rows grouped into 12
sessions across three public NIST PDFs:

- 3 correct controls
- 7 extraction edge cases, including metadata, date, taxonomy, ordering, and
  multi-turn retry failures
- 2 telemetry edge cases, including a failed provider call and a missing parent

The example drives Kitaru through its CLI. It tests and registers the importer,
registers the candidate agent, starts a scoped worker, imports the traces, and
builds cohorts from Langfuse tags. You can inspect the resulting sessions
before spending model tokens on a replay.

## Run it

Install the server and worker dependencies:

```bash
uv sync --extra cli --extra server --extra worker
cp .env.example .env
```

Edit `.env` with the credentials required for the steps you plan to run. The
file is ignored by Git.

Start Kitaru and PostgreSQL:

```bash
docker compose up -d db server
```

Import the prepared traces and create the cohorts:

```bash
./examples/document_processing/run.sh --import-only
```

This path does not call OpenAI or Langfuse. The local Docker server accepts
requests without authentication. For another server, configure its URL and API
key:

```bash
# Set KITARU_API_URL and KITARU_API_KEY in .env first.
./examples/document_processing/run.sh --import-only
```

To run the full improvement loop, install PydanticAI, configure OpenAI, and omit
`--import-only`:

```bash
uv sync --extra cli --extra server --extra worker --extra pydantic-ai
# Set OPENAI_API_KEY in .env first.
./examples/document_processing/run.sh
```

The full run replays the extraction edge-case cohort through the candidate
extractor, evaluates both sides, and prints the completed experiment-run ID.

## Optional step -1: generate fresh traces

The checked-in export makes trace import the starting point. To regenerate the
baseline evidence through PydanticAI and Langfuse, install the example
dependencies and provide both services' credentials:

```bash
uv sync --extra cli --extra server --extra worker --extra pydantic-ai --extra examples
# Set the OpenAI and Langfuse credentials in .env first.
./examples/document_processing/run.sh --bootstrap-traces
```

You can also import another Langfuse JSON or JSONL export:

```bash
./examples/document_processing/run.sh \
  --trace-export path/to/langfuse-traces.jsonl \
  --import-only
```

## Read the example

Start with [`document_processing/traces/langfuse-traces.jsonl`](document_processing/traces/langfuse-traces.jsonl)
and [`document_processing/run.sh`](document_processing/run.sh). The runner keeps
the CLI commands visible and uses JSON receipts to pass exact resource IDs
between steps. The other files separate the moving parts:

- [`corpus.py`](document_processing/corpus.py) pins the PDF URLs, checksums, and
  reviewed labels.
- [`langfuse_capture.py`](document_processing/langfuse_capture.py) implements
  optional step -1 by instrumenting PydanticAI and exporting Langfuse traces.
- [`agent.py`](document_processing/agent.py) runs the candidate extractor during
  Kitaru replay.
- [`evaluator.py`](document_processing/evaluator.py) scores each structured
  field against the reviewed record.
