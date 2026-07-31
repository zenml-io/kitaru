---
description: Process real PDFs, import Langfuse traces, and compare two extractors.
icon: flask
---

# Example

Kitaru has one canonical example. It processes three public NIST standards PDFs
through a complete improvement loop:

1. Run a typed PydanticAI extractor over the PDFs.
2. Capture and export the baseline traces with Langfuse.
3. Register Kitaru's Langfuse importer service and import the traces.
4. Freeze the sessions in a cohort and register reviewed field labels.
5. Replay the PDFs through a revised extractor and compare field accuracy.

## Run the example

From the Kitaru repository:

```bash
uv sync --extra server --extra worker --extra pydantic-ai --extra examples
docker compose up -d db server
export OPENAI_API_KEY=...
export LANGFUSE_PUBLIC_KEY=...
export LANGFUSE_SECRET_KEY=...
export LANGFUSE_BASE_URL=https://cloud.langfuse.com
uv run python -m examples.document_processing
```

The local server authenticates with the defaults from `docker-compose.yml`.
For another server, provide credentials before running the example:

```bash
export KITARU_API_URL=https://kitaru.example.com
export KITARU_API_KEY=...
uv run python -m examples.document_processing
```

The command prints one row per PDF with baseline and candidate field accuracy.

## Follow the code

[`examples/document_processing/__main__.py`](https://github.com/zenml-io/kitaru/blob/develop/examples/document_processing/__main__.py)
orchestrates the example through the Python SDK. `corpus.py` pins the source
documents and labels, `langfuse_capture.py` creates the baseline evidence,
`agent.py` defines the candidate, and `evaluator.py` defines field accuracy.
