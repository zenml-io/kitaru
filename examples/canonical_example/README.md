# Resolve returns tickets with Kitaru

This example runs an autonomous returns agent against ten synthetic customer emails. The agent investigates each order, checks the relevant policy or shipment, records a mock action, and drafts the reply. Langfuse captures the real PydanticAI executions, and Kitaru imports them as replayable sessions.

All customers, orders, shipments, and actions are synthetic. Refund and replacement tools only modify an in-memory store.

Run every command from `examples/canonical_example`.

Copy the local environment template before choosing either path:

```bash
cp .env.example .env
```

The template already points the CLI at `http://localhost:8000`. Model and Langfuse credentials are only required when regenerating the traces.

## Optional Step 0: Generate real traces

Add your OpenAI and Langfuse credentials to `.env`.

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
uv run --env-file .env kitaru login --local
uv run --env-file .env kitaru status
```

Seed the development importers and evaluators into the fresh local database:

```bash
uv run --env-file .env python ../../scripts/seed_default_plugins.py
```

Confirm that the `langfuse` importer and the `cost`, `latency`, and `tool-call-patterns` evaluators are available:

```bash
uv run --env-file .env kitaru importer list
uv run --env-file .env kitaru evaluator list
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
uv run --env-file .env kitaru agent register \
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
uv run --env-file .env kitaru worker list
```

## Step 4: Import the baseline sessions

Import the Langfuse traces under the exact baseline agent version:

```bash
uv run --env-file .env kitaru session import \
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
uv run --env-file .env kitaru session list \
  --tag returns-baseline \
  --origin imported \
  --size 20
```

## Step 5: Find useful starting points

Run Kitaru's deterministic evaluators across the imported baseline:

```bash
uv run --env-file .env kitaru session evaluate \
  --tag returns-baseline \
  --evaluator cost@latest \
  --evaluator latency@latest \
  --evaluator tool-call-patterns@latest \
  --wait
```

These evaluators make no model calls. Cost and latency surface expensive tickets, while tool-call patterns expose repeated lookups and different investigation paths.

List the stored results:

```bash
uv run --env-file .env kitaru evaluation list --size 100
```

## Step 6: Create cost cohorts

Use cost as the first signal for dividing the baseline into two small cohorts. List only the cost results:

```bash
uv run --env-file .env kitaru evaluation list \
  --filter '{"field":"name","op":"eq","value":"cost"}' \
  --size 20
```

From the table, choose the session IDs for the three lowest costs and the three highest costs. The sessions between those groups do not need to belong to either cohort.

Create an immutable snapshot of the three cheapest sessions. Replace the placeholders with the session IDs from the cost results:

```bash
uv run --env-file .env kitaru cohort create cheap-baseline \
  --agent returns-resolver \
  --description "The three baseline sessions with the lowest recorded cost." \
  --session CHEAP_SESSION_ID_1 \
  --session CHEAP_SESSION_ID_2 \
  --session CHEAP_SESSION_ID_3
```

Create another snapshot for the three most expensive sessions:

```bash
uv run --env-file .env kitaru cohort create expensive-baseline \
  --agent returns-resolver \
  --description "The three baseline sessions with the highest recorded cost." \
  --session EXPENSIVE_SESSION_ID_1 \
  --session EXPENSIVE_SESSION_ID_2 \
  --session EXPENSIVE_SESSION_ID_3
```

Confirm the membership of both cohort versions:

```bash
uv run --env-file .env kitaru session list \
  --cohort cheap-baseline@1 \
  --size 20

uv run --env-file .env kitaru session list \
  --cohort expensive-baseline@1 \
  --size 20
```

The cohort versions are fixed snapshots. The next part of the example will use `expensive-baseline@1` as an optimization target and `cheap-baseline@1` as a comparison group.

## Step 7: Define policy-correct behavior

The support policy gives each synthetic ticket one reviewed terminal action. The evaluator in `evaluator.py` applies one binary criterion:

- **Pass:** the final action is the reviewed refund, replacement, or escalation outcome for that ticket.
- **Fail:** the final action differs from the reviewed outcome.

For example, ticket 001 passes when the agent refunds the defective shoes. Tickets 004 and 007 fail when the agent refunds before the required approval or risk review.

Test the evaluator contract locally:

```bash
uv run --env-file .env kitaru evaluator test \
  evaluator.py \
  --entrypoint evaluate
```

Register its first immutable version:

```bash
uv run --env-file .env kitaru evaluator register \
  returns-policy \
  --script evaluator.py \
  --entrypoint evaluate \
  --description "Check whether a returns ticket ends with its reviewed policy action." \
  --display-version 1.0
```

## Step 8: Score the baseline

Apply the policy evaluator to every imported baseline session:

```bash
uv run --env-file .env kitaru session evaluate \
  --tag returns-baseline \
  --evaluator returns-policy@1 \
  --wait
```

List the policy results:

```bash
uv run --env-file .env kitaru evaluation list \
  --filter '{"field":"name","op":"eq","value":"policy_correct"}' \
  --size 20
```

The checked-in baseline contains eight passes and two failures. Tickets 004 and 007 issue refunds where the reviewed policy requires escalation.

## Step 9: Register an improved agent version

The baseline instructions tell the agent to assume that action tools enforce approval limits. The improved version removes that assumption and requires the agent to inspect risk flags, approval thresholds, final-sale rules, and return windows before calling `issue_refund`.

Register the same entrypoint with strict policy instructions enabled:

```bash
uv run --env-file .env kitaru agent version register \
  returns-resolver \
  --entrypoint examples.canonical_example.agent:main \
  --description "Check approval and risk rules before issuing a refund." \
  --display-version strict-policy-v2 \
  --working-dir ../.. \
  --env RETURNS_POLICY_MODE=strict \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
```

This creates `returns-resolver@2`. The imported sessions remain attached to version 1.

## Step 10: Create the experiment

Create one reusable experiment that records policy correctness and cost. Kitaru resolves each evaluator reference to an immutable version when it creates the experiment:

```bash
uv run --env-file .env kitaru experiment create \
  improve-returns-policy \
  --description "Replay cost cohorts with strict refund approval rules." \
  --evaluator returns-policy@1 \
  --evaluator cost@latest
```

The mock commerce tools are safe to call again, so the experiment uses Kitaru's default passthrough tool policy. Every replay receives a new isolated in-memory store.

## Step 11: Replay both cohorts

Resolve the two cohort references to the UUIDs required by `experiment run start`:

```bash
CHEAP_COHORT_VERSION_ID="$(
  uv run --env-file .env kitaru --output json \
    cohort version get cheap-baseline@1 \
  | jq -r '.item.id'
)"

EXPENSIVE_COHORT_VERSION_ID="$(
  uv run --env-file .env kitaru --output json \
    cohort version get expensive-baseline@1 \
  | jq -r '.item.id'
)"
```

Replay the cheap cohort through agent version 2 and score both the imported baselines and replayed sessions:

```bash
uv run --env-file .env kitaru experiment run start \
  improve-returns-policy \
  --cohort-version "$CHEAP_COHORT_VERSION_ID" \
  --agent returns-resolver@2 \
  --evaluate-baselines \
  --wait \
  --timeout 1800
```

Run the same experiment against the expensive cohort:

```bash
uv run --env-file .env kitaru experiment run start \
  improve-returns-policy \
  --cohort-version "$EXPENSIVE_COHORT_VERSION_ID" \
  --agent returns-resolver@2 \
  --evaluate-baselines \
  --wait \
  --timeout 1800
```

Each baseline input now has a separate replayed session. Kitaru applies the same `returns-policy@1` and resolved cost-evaluator version to both sides.

## Step 12: Compare the evidence

List the completed experiment runs:

```bash
uv run --env-file .env kitaru experiment run list --size 20
```

Each `experiment run start` receipt prints exact `get` and `jobs` commands in `next_actions`. Run those commands to inspect the replay and evaluator jobs. They have this form:

```bash
uv run --env-file .env kitaru experiment run get YOUR_RUN_UUID
uv run --env-file .env kitaru experiment run jobs YOUR_RUN_UUID --size 20
```

List the replayed sessions and policy evaluations:

```bash
uv run --env-file .env kitaru session list \
  --agent returns-resolver \
  --origin replay \
  --size 20

uv run --env-file .env kitaru evaluation list \
  --filter '{"field":"name","op":"eq","value":"policy_correct"}' \
  --size 100
```

Open [http://localhost:8000](http://localhost:8000) to compare each imported session with its replay, inspect the changed tool path, and review policy correctness and cost together. A failed replay remains useful evidence: inspect it, change the agent again, register version 3, and rerun the same immutable experiment and cohort versions.

## Step 13: Stop the local server

After the walkthrough, stop the containers:

```bash
docker compose -f ../../docker-compose.yml down
```

The PostgreSQL volume retains the agents, sessions, evaluations, cohorts, and experiment runs.
