# Support agent improvement loop

This repository has one canonical example. It follows a change from production
evidence to an evaluated agent version:

1. Register a single-file importer.
2. Import support traces from another observability system.
3. Turn the imported sessions into a versioned cohort.
4. Register an evaluator and a revised agent.
5. Replay the cohort and compare the baseline and candidate scores.

The agent uses PydanticAI's `FunctionModel`, so the example needs no model API
key and produces the same result on every run.

## Run it

Install the server, worker, and PydanticAI dependencies:

```bash
uv sync --extra server --extra worker --extra pydantic-ai
```

Start Kitaru and PostgreSQL:

```bash
docker compose up -d db server
```

Run the example from the repository root:

```bash
uv run python -m examples.support_agent
```

The local Docker setup uses `default` / `password`. Override these values when
pointing the example at another server:

```bash
export KITARU_API_URL=https://kitaru.example.com
export KITARU_API_KEY=...
uv run python -m examples.support_agent
```

The final table shows whether each imported production response met its
expected outcome and whether the replayed candidate response did.

## Read the example

Start with [`support_agent/__main__.py`](support_agent/__main__.py). It is the
control plane for the walkthrough. The other files are artifacts it registers:

- [`trace_importer.py`](support_agent/trace_importer.py) is a PEP 723 script
  plugin that converts JSONL records into Kitaru sessions and nodes.
- [`evaluator.py`](support_agent/evaluator.py) compares a session output with
  its expected output.
- [`agent.py`](support_agent/agent.py) is the candidate agent executed by a
  Kitaru worker during replay.
- [`production_traces.jsonl`](support_agent/production_traces.jsonl) represents
  an export from a production tracing system.
