---
description: Import production traces, replay them with a candidate agent, and compare results.
icon: flask
---

# Example

Kitaru has one canonical example. It demonstrates the improvement loop as one
runnable path:

1. Register a PEP 723 importer script.
2. Import production support traces as sessions with nested nodes and metadata.
3. Freeze those sessions in a cohort version.
4. Register an evaluator and a new agent version.
5. Replay the cohort and compare baseline and candidate evaluations.

The candidate uses a deterministic PydanticAI model and a local order lookup
tool. You do not need a model provider key.

## Run the example

From the Kitaru repository:

```bash
uv sync --extra server --extra worker --extra pydantic-ai
docker compose up -d db server
uv run python -m examples.support_agent
```

The local server authenticates with the defaults from `docker-compose.yml`.
For another server, provide credentials before running the example:

```bash
export KITARU_API_URL=https://kitaru.example.com
export KITARU_API_KEY=...
uv run python -m examples.support_agent
```

The command prints one row per imported trace. Each row contains the baseline
evaluation and the evaluation for the replayed candidate.

## Follow the code

[`examples/support_agent/__main__.py`](https://github.com/zenml-io/kitaru/blob/develop/examples/support_agent/__main__.py)
orchestrates the example through the Python SDK. Its registered components are:

- `trace_importer.py`: single-file PEP 723 JSONL importer
- `evaluator.py`: exact expected-outcome evaluator
- `agent.py`: PydanticAI candidate wrapped by `KitaruAgent`
- `production_traces.jsonl`: sample production export

The importer is a useful starting point for an internal tracing system. Replace
the JSONL field mapping while preserving the `ParsedSession` and `ParsedNode`
output contract.
