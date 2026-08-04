# Resolve returns tickets with Kitaru

This example runs an autonomous returns agent against ten synthetic customer emails. The agent investigates each order, checks the relevant policy or shipment, records a mock action, and drafts the reply. Langfuse captures the real PydanticAI executions, and Kitaru imports them as replayable sessions.

All customers, orders, shipments, and actions are synthetic. Refund and replacement tools only modify an in-memory store.

Run every command from `examples/canonical_example`.

## Optional Step 0: Generate real traces

Copy the environment template and add your OpenAI and Langfuse credentials:

```bash
cp .env.example .env
```

Generate ten baseline traces:

```bash
./generate.sh
```

The script makes real model calls and writes the Langfuse export to `traces/langfuse-traces.jsonl`. It does not connect to Kitaru or create Kitaru resources.

## Step 1: Start Kitaru locally

Start PostgreSQL, the Kitaru API, and the dashboard:

```bash
docker compose -f ../../docker-compose.yml up -d --build
```

Install the dependencies and connect the CLI:

```bash
uv sync --extra cli --extra worker --extra pydantic-ai --extra examples
uv run kitaru login --local
uv run kitaru status
```

Open [http://localhost:8000](http://localhost:8000) to use the dashboard.

## Step 2: Register the baseline agent

The PydanticAI entrypoint is `agent.py`. Each invocation resolves one incoming email without a human turn.

- **Purpose:** investigate and resolve returns, refunds, and missing shipments.
- **Input:** one synthetic support email with a ticket ID, customer identity, subject, and body.
- **Output:** action, amount, reason, and customer reply.
- **State:** one isolated in-memory commerce store per invocation.
- **Tools:** `lookup_order`, `get_return_policy`, `check_shipping`, `issue_refund`, `create_replacement`, and `escalate_to_human`.
- **MCP servers:** none.
- **Skills:** none.

Register the baseline:

```bash
uv run kitaru agent register \
  returns-resolver \
  --entrypoint examples.canonical_example.agent:main \
  --description "Resolve one synthetic returns or delivery ticket, execute one mock action, and draft the customer reply." \
  --display-version baseline-v1 \
  --working-dir ../.. \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
```

Registration creates the `returns-resolver` agent and version `1`.

## Step 3: Start a worker

Open a second terminal in this directory and keep the worker active:

```bash
uv run --env-file .env kitaru worker start \
  --name returns-example-worker
```

Confirm that Kitaru can see it:

```bash
uv run kitaru worker list
```

## Step 4: Import the baseline sessions

Import the Langfuse traces under the exact baseline agent version:

```bash
uv run kitaru session import \
  traces/langfuse-traces.jsonl \
  --importer langfuse@latest \
  --agent returns-resolver@1 \
  --tag returns-baseline \
  --params '{"source_instance":"canonical-returns-example"}' \
  --media-type application/x-ndjson \
  --wait
```

The importer preserves the LLM calls, tool calls, tool results, final resolution, source trace IDs, and baseline agent version.

Check what the import produced:

```bash
uv run kitaru session list \
  --tag returns-baseline \
  --origin imported \
  --size 20
```

## Step 5: Find useful starting points

Run Kitaru's deterministic evaluators across the imported baseline:

```bash
uv run kitaru session evaluate \
  --tag returns-baseline \
  --evaluator cost@latest \
  --evaluator latency@latest \
  --evaluator tool-call-patterns@latest \
  --wait
```

These evaluators make no model calls. Cost and latency surface expensive tickets, while tool-call patterns expose repeated lookups and different investigation paths.

List the stored results:

```bash
uv run kitaru evaluation list --size 100
```
