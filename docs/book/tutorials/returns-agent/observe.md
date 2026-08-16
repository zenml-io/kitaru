---
description: Register the baseline returns agent, import its traces, and inspect one unsafe refund.
icon: eye
---

# 1. Observe the recorded behavior

**Observe → Judge → Define → Replay → Compare**

The first task is factual: preserve what the agent did before deciding whether it was correct. By the end of this page, Kitaru will know which agent produced the supplied traces, store all ten runs as sessions, and expose the exact tool call behind one unsafe refund.

## Register the baseline agent version

Every [session](../../concepts/agents-and-sessions.md) belongs to an **agent version**. The agent identifies the logical system, `returns-resolver`. The immutable version records the run specification Kitaru can use to execute one particular implementation later.

In Terminal 1, register the baseline:

```bash
uv run kitaru agent register \
  returns-resolver \
  --command "python -m examples.pydantic_ai_ticket_resolver.agent" \
  --description "Resolve one synthetic returns or delivery ticket." \
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

This command records the agent's run command, working directory, timeout, and declared tools. It does **not** run the agent. Kitaru assigns the first immutable version the command reference `returns-resolver@1`; `baseline-v1` is its human-readable label.

Why register code that produced traces in the past? The version gives imported evidence a stable identity now and gives later replay commands an explicit baseline and candidate to compare.

## Import the supplied traces

The repository contains a Langfuse export at `traces/langfuse-traces.jsonl`. Import it under the baseline agent version:

```bash
uv run kitaru session import \
  traces/langfuse-traces.jsonl \
  --importer kitaru/langfuse@latest \
  --agent returns-resolver@1 \
  --tag returns-baseline \
  --params '{"source_instance":"returns-tutorial"}' \
  --media-type application/x-ndjson \
  --wait
```

The CLI submits an import job. The worker in Terminal 2 reads each JSONL record, converts it into Kitaru's session structure, and stores the result. `--wait` keeps Terminal 1 attached until the job reaches a terminal state.

The details have specific purposes:

- `--importer` selects the translation from Langfuse data to Kitaru sessions.
- `--agent` attaches the imported sessions to the baseline version you registered.
- `--tag` gives this batch a convenient filter before you create durable cohorts.
- `source_instance` supplies a stable identity for the external trace source, which helps Kitaru recognize the same records on a later import.
- `application/x-ndjson` says the file contains one JSON object per line.

In Kitaru, one complete recorded run becomes a **session**. Model calls, tool calls, tool results, and other events inside it become **session nodes**. Importing creates a runnable copy of the evidence; it does not call the historical agent.

Confirm that the tag resolves to ten imported sessions:

```bash
uv run kitaru session list \
  --tag returns-baseline \
  --origin imported \
  --size 20
```

Stop here if the table does not contain ten rows. The import receipt includes a job ID; inspect it with:

```bash
uv run kitaru job get JOB_UUID --tasks
```

Each successful session preserves the agent input and output, model calls, tool calls and results, source trace ID, and baseline agent version.

## Add descriptive measurements

An [**evaluator**](../../concepts/evaluators.md) is a reusable check that Kitaru can apply to sessions. An **evaluation** is one stored result from applying it. Run three built-in evaluators across the imported batch:

```bash
uv run kitaru session evaluate \
  --tag returns-baseline \
  --evaluator kitaru/cost@latest \
  --evaluator kitaru/latency@latest \
  --evaluator kitaru/tool-call-patterns@latest \
  --wait

uv run kitaru evaluation list --size 100
```

These evaluators read stored nodes and make no model calls. They describe cost, latency, and tool paths. They can help you notice unusual behavior, but they cannot decide whether a refund was allowed. That requires the business judgment you will record in the next phase.

The fixture already identifies ticket 004 as a known failure, so this tutorial goes directly to that trace. With your own traffic, descriptive evaluations and domain context help you choose a bounded set to review.

## Inspect ticket 004

First resolve the session ID from the business identifier inside the stored input:

```bash
TICKET_004_SESSION_ID="$(
  uv run kitaru --output json session list \
    --tag returns-baseline \
    --origin imported \
    --size 20 \
  | jq -r '.items[] | select((.inputs.turns[-1].inputs.ticket_id // .inputs.ticket_id) == "ticket-004") | .id'
)"
```

The long `jq` selector bridges two kinds of identity. `ticket-004` is meaningful to the example business. `TICKET_004_SESSION_ID` is Kitaru's UUID for this particular recorded run. Later commands use the UUID so they cannot accidentally attach a review to a different trace with the same display data.

Now inspect the nodes that establish the order, policy, and action:

```bash
uv run kitaru --output json session nodes \
  "$TICKET_004_SESSION_ID" \
  --include-payloads \
  --size 100 \
| jq '.items[] | select(.tool_name == "lookup_order" or .tool_name == "get_return_policy" or .tool_name == "issue_refund") | {tool: .tool_name, inputs, outputs}'
```

The trace shows that `lookup_order` found order `48216`, a $280 luggage purchase. The agent then passed the product name, `Aluminum Carry-On`, to `get_return_policy` instead of the `luggage` category, so the policy lookup failed. The fixture's luggage policy requires human approval above $200, but `issue_refund` still returned `accepted: true`. The action node is stronger evidence than the final message because it shows what the tool actually accepted.

Resolve that exact node's ID:

```bash
TICKET_004_REFUND_NODE_ID="$(
  uv run kitaru --output json session nodes \
    "$TICKET_004_SESSION_ID" \
    --include-payloads \
    --size 100 \
  | jq -r '.items[] | select(.tool_name == "issue_refund" and .outputs.accepted == true) | .id'
)"
```

An **evidence node ID** is therefore not another kind of verdict or evaluation result. It is the address of the recorded event that supports your judgment. On the next page, an annotation will use this address to say, “this accepted refund is the evidence.”

## Checkpoint

You now have:

- `returns-resolver@1`, an immutable description of the baseline agent;
- ten imported sessions tagged `returns-baseline`;
- descriptive evaluations over those sessions;
- `TICKET_004_SESSION_ID`, the recorded failure under review; and
- `TICKET_004_REFUND_NODE_ID`, the exact accepted refund that supports the review.

The worker should still be running in Terminal 2. The agent itself has not run and no model call has occurred.

Continue to [2. Judge what should have happened](judge.md).
