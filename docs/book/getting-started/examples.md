---
description: Import document traces, score baselines, and run an experiment.
icon: flask
---

# Example

Kitaru has one canonical example. A PydanticAI agent extracts structured fields
from three public NIST PDFs. Langfuse records the real model calls. Kitaru then
imports, scores, and replays the traces.

The example teaches these CLI operations:

- Use the default Langfuse importer.
- Test and register the example agent and evaluator.
- Start a local worker for a remote Kitaru server.
- Import a Langfuse JSONL export as Kitaru sessions.
- Score the imported baseline sessions.
- Create an immutable cohort version.
- Create and run an experiment against an improved agent version.

## Start the example

```bash
cd examples/document_processing
cp .env.example .env
uv sync --extra cli --extra worker --extra pydantic-ai --extra examples
docker compose -f ../../docker-compose.yml up -d --build db server
./generate.sh
```

Add the OpenAI and Langfuse credentials to `.env` before you generate the
traces. The local Kitaru server runs at `http://localhost:8000` without API-key
authentication.

Continue with `examples/document_processing/README.md`. The README shows each
Kitaru CLI command directly.

## Follow the code

- `generate.sh` creates the real Langfuse export.
- `traces/langfuse-traces.jsonl` contains the exported traces.
- `agent.py` defines the improved document agent.
- `evaluator.py` defines exact field-accuracy scoring.
- `README.md` contains the complete CLI journey.
