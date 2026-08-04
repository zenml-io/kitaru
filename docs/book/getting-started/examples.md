---
description: Import document traces, form cohorts, and improve an extractor.
icon: flask
---

# Example

Kitaru has one canonical example. It starts with a prepared Langfuse export from
a PydanticAI agent that extracts structured records from three public NIST PDFs.
The export contains 12 sessions:

- 3 correct controls
- 7 extraction edge cases
- 2 telemetry edge cases, including a failed call and an incomplete span graph

The example uses Kitaru CLI commands to register the agent and Langfuse
importer, run a worker, import the export, and create cohorts from trace tags.
Trace import is the first required step.

## Run the example

From the Kitaru repository:

```bash
uv sync --extra cli --extra server --extra worker
cp .env.example .env
docker compose up -d db server
./examples/document_processing/run.sh --import-only
```

The `.env` file is ignored by Git. This command does not call OpenAI or
Langfuse. For another Kitaru server, set its URL and API key in `.env`:

```bash
KITARU_API_URL=https://kitaru.example.com
KITARU_API_KEY=...
```

To replay the extraction failures through a candidate agent, install PydanticAI,
configure OpenAI, and run without `--import-only`:

```bash
uv sync --extra cli --extra server --extra worker --extra pydantic-ai
# Set OPENAI_API_KEY in .env first.
./examples/document_processing/run.sh
```

## Generate fresh traces

Trace generation is optional step -1. It requires OpenAI and Langfuse
credentials:

```bash
uv sync --extra cli --extra server --extra worker --extra pydantic-ai --extra examples
# Set the OpenAI and Langfuse credentials in .env first.
./examples/document_processing/run.sh --bootstrap-traces
```

Pass `--trace-export path/to/export.jsonl` to use another Langfuse JSON or JSONL
export.

## Follow the code

[`examples/document_processing/run.sh`](https://github.com/zenml-io/kitaru/blob/develop/examples/document_processing/run.sh)
orchestrates the example through CLI commands and JSON receipts. `corpus.py`
pins the source documents and labels, `traces/langfuse-traces.jsonl` holds the
prepared evidence, `langfuse_capture.py` implements optional trace generation,
`agent.py` defines the candidate, and `evaluator.py` defines field accuracy.
