# Document processing improvement loop

The canonical example extracts structured records from three public NIST PDFs.
It connects the tools used across the lifecycle:

1. Download the AI RMF 1.0, Generative AI Profile, and Cybersecurity Framework
   2.0 PDFs from NIST.
2. Run a typed PydanticAI extractor and capture its baseline traces in Langfuse.
3. Export those traces and import them with Kitaru's Langfuse importer service.
4. Freeze the imported sessions as a cohort and register field-level labels.
5. Replay the PDFs through a revised extractor and compare field accuracy.

## Run it

Install the server, worker, PydanticAI, and example dependencies:

```bash
uv sync --extra server --extra worker --extra pydantic-ai --extra examples
```

Start Kitaru and PostgreSQL:

```bash
docker compose up -d db server
```

Configure OpenAI and Langfuse, then run the example from the repository root:

```bash
export OPENAI_API_KEY=...
export LANGFUSE_PUBLIC_KEY=...
export LANGFUSE_SECRET_KEY=...
export LANGFUSE_BASE_URL=https://cloud.langfuse.com
uv run python -m examples.document_processing
```

The local Docker setup uses `default` / `password`. Override these values when
pointing the example at another server:

```bash
export KITARU_API_URL=https://kitaru.example.com
export KITARU_API_KEY=...
uv run python -m examples.document_processing
```

The command prints baseline and candidate field accuracy for every PDF. Set
`BASELINE_MODEL` or `CANDIDATE_MODEL` to compare another model pair.

## Read the example

Start with
[`document_processing/__main__.py`](document_processing/__main__.py). The other
files separate the moving parts:

- [`corpus.py`](document_processing/corpus.py) pins the PDF URLs, checksums, and
  reviewed labels.
- [`langfuse_capture.py`](document_processing/langfuse_capture.py) instruments
  PydanticAI and exports the resulting Langfuse traces.
- [`agent.py`](document_processing/agent.py) runs the candidate extractor during
  Kitaru replay.
- [`evaluator.py`](document_processing/evaluator.py) scores each structured
  field against the reviewed record.
