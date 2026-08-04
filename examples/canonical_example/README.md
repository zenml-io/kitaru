## Optional Step 0: Generate real traces

The example can generate its own Langfuse export. Copy the environment template
and add your OpenAI and Langfuse credentials:

```bash
cp .env.example .env
```

Generate 12 traces:

```bash
./generate.sh
```

The script downloads three public NIST PDFs and runs the document agent with
four prompts per PDF. It writes the exported traces to
`traces/langfuse-traces.jsonl`.

## Step 1: Start Kitaru locally

Start PostgreSQL, the Kitaru API, and the dashboard with Docker Compose:

```bash
docker compose -f ../../docker-compose.yml up -d --build
```

Install the Kitaru CLI from the repository and connect it to the local server:

```bash
uv sync --extra cli
uv run kitaru login --local
uv run kitaru status
```

Open [http://localhost:8000](http://localhost:8000) to use the dashboard. The
PostgreSQL volume keeps local data when the containers stop.

> **WIP:** The intended command is `kitaru login --local`, which will start the
> local deployment when necessary. The current CLI only connects to a server
> that is already running, so this example starts Docker Compose first.

To stop the deployment without deleting its data:

```bash
docker compose -f ../../docker-compose.yml down
```

## Step 2: Find, prepare, and register the agent

This repository contains a PydanticAI document extraction agent in `agent.py`.
Its relevant facts are:

- **Purpose:** extract catalog fields from standards PDFs.
- **Inputs:** a document ID and a repository-relative PDF path.
- **Outputs:** title, publication identifier, publication month, and framework
  functions.
- **State:** stateless; each run reads one local PDF.
- **Tools:** none.
- **MCP servers:** none.
- **Skills:** none.

Kitaru supports PydanticAI, so the example provides a separate Kitaru entrypoint
without changing the original agent behavior. Install the worker and PydanticAI
dependencies:

```bash
uv sync --extra cli --extra worker --extra pydantic-ai
```

Register the agent and its first version:

```bash
uv run kitaru agent register \
  document-agent \
  --entrypoint examples.canonical_example.agent:main \
  --description "Purpose: extract catalog fields from standards PDFs. Inputs: document ID and repository-relative PDF path. Outputs: title, publication identifier, publication month, and framework functions. State: stateless; each run reads one local PDF." \
  --display-version candidate-v1 \
  --working-dir ../.. \
  --timeout-seconds 180
```

Confirm that Kitaru stored the agent:

```bash
uv run kitaru agent get document-agent
```

The registration creates the `document-agent` parent and version `1`. The
version records the entrypoint and execution configuration used for later
replays. Kitaru stores the registration on the server and does not add a YAML
file or `.kitaru` directory to the repository. No worker or model call is
required during registration.

## Step 3: Start a worker

Open a second terminal in `examples/canonical_example` and start a worker in
the current Python environment:

```bash
uv run --env-file .env kitaru worker start \
  --name canonical-example-worker
```

Keep this process running. The worker polls the local Kitaru server and runs
jobs using the agent code and dependencies from this repository.

In the first terminal, confirm that the worker is active:

```bash
uv run kitaru worker list
```

A job remains pending until a compatible worker claims it. Press `Ctrl+C` in
the worker terminal when the example is complete.

## Step 4: Import sessions

Import the Langfuse traces into the registered agent:

```bash
uv run kitaru session import \
  traces/langfuse-traces.jsonl \
  --importer langfuse@latest \
  --agent document-agent \
  --tag document-baseline \
  --params '{"source_instance":"canonical-example"}' \
  --media-type application/x-ndjson \
  --wait
```

The worker converts the 12 source traces into Kitaru sessions and nodes. Each
trace becomes one session because the generator assigns it a distinct Langfuse
session ID.

List the imported sessions:

```bash
uv run kitaru session list \
  --agent document-agent \
  --origin imported \
  --size 20
```

Import does not run evaluators. This keeps one upload from creating an
unbounded evaluation queue.

## Step 5: Find useful starting points

Run the built-in evaluators against every session from this import:

```bash
uv run kitaru session evaluate \
  --tag document-baseline \
  --evaluator cost@latest \
  --evaluator latency@latest \
  --evaluator tool-call-patterns@latest \
  --wait
```

The tag is the selection boundary. You do not need to list sessions or copy
their IDs. To evaluate every session in Kitaru instead, replace
`--tag document-baseline` with `--all`.

These evaluators make no model calls. They report recorded cost, elapsed time,
and repeated tool usage. This document agent has no tools, so
`tool-call-patterns` should report `no-tool-calls`. Cost and latency can still
surface sessions worth reviewing first.

List the stored results:

```bash
uv run kitaru evaluation list --size 100
```

Each evaluator remains a separate column and signal. Kitaru does not combine
them into one score or decide whether a session is good.
