---
description: Register the returns agent, import its traces, and select evidence for human review.
icon: eye
---

# 1. Observe the recorded behavior

**Observe → Judge → Define → Replay → Compare**

The first task is factual: preserve what the agent did and inspect the population before deciding what was right or wrong. By the end of this page, Kitaru will store all ten supplied runs as sessions and you will have a bounded, varied worklist for human review.

## Register the recorded agent

Every [session](../../concepts/agents-and-sessions.md) belongs to an **agent version**. The agent identifies the logical system, `returns-resolver`. Its immutable version stores the command, working directory, timeout, and declared tools that Kitaru can use for later replay.

In Terminal 1, register the baseline:

```bash
uv run kitaru agent register \
  returns-resolver \
  --command "python -m returns_agent.agent" \
  --description "Resolve one synthetic returns or delivery request." \
  --display-version baseline-v1 \
  --working-dir . \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
```

Registration does not run the agent. Kitaru assigns the first version the reference `returns-resolver@1`. The receipt's `Parent ID` identifies the agent across versions; its `Version ID` identifies this exact run specification.

## Import the supplied traces

The repository contains a Langfuse export at `traces/langfuse-traces.jsonl`. Import it under the baseline version:

```bash
uv run kitaru session import \
  traces/langfuse-traces.jsonl \
  --importer kitaru/langfuse@latest \
  --agent returns-resolver@1 \
  --tag returns-baseline \
  --params '{"source_instance":"canonical-returns-example"}' \
  --media-type application/x-ndjson \
  --wait
```

The worker translates each JSONL record into Kitaru's session structure. One complete recorded run becomes a **session**; model calls, tool calls, tool results, and other events inside it become **session nodes**. Importing preserves the evidence and its source identity. It does not call the historical agent.

Confirm that the tag resolves to ten imported sessions:

```bash
uv run kitaru session list \
  --tag returns-baseline \
  --origin imported \
  --size 20
```

Stop if the table does not contain ten rows. Use the job ID from the import receipt to inspect failed or skipped tasks:

```bash
uv run kitaru job get JOB_UUID --tasks
```

## Survey before judging

An [**evaluator**](../../concepts/evaluators.md) is a reusable measurement. An **evaluation** is one stored result from applying a particular evaluator version to one session.

Run low-cost deterministic evaluators across the population:

```bash
uv run kitaru session evaluate \
  --tag returns-baseline \
  --evaluator kitaru/session-diagnostics@latest \
  --evaluator kitaru/tool-health@latest \
  --evaluator kitaru/trajectory-signals@latest \
  --evaluator kitaru/llm-call-signals@latest \
  --evaluator kitaru/cost@latest \
  --evaluator kitaru/timing-profile@latest \
  --wait

uv run kitaru evaluation list --size 100
```

These evaluators read stored nodes and make no model calls. They can reveal missing data, failed tools, unusual trajectories, model-call patterns, cost, and timing. They cannot decide whether a refund, replacement, or escalation was correct.

Print a compact inventory:

```bash
uv run kitaru --output json session list \
  --tag returns-baseline \
  --origin imported \
  --size 20 \
| jq -r '.items[] | [.id, .name, .status, .outputs.action, .cost, .llm_call_count, .tool_call_count] | @tsv'
```

Select a bounded worklist that you can review carefully. Include different final actions and tool paths, at least one operational outlier, and at least one random session. Summary fields help choose where to look; they are not verdicts.

## Inspect complete traces

Set the UUID of one selected session and inspect every node with its payload:

```bash
SESSION_ID="YOUR_SESSION_UUID"

uv run kitaru session nodes \
  "$SESSION_ID" \
  --include-payloads \
  --size 100
```

Repeat this command for each selected session. Read the input, model decisions, tool inputs, tool results, and final output together. A final response may claim that an action happened while the tool result proves otherwise; a tool failure may explain behavior that looks irrational in the summary.

Record the session UUIDs and any node UUIDs that contain useful evidence. A node ID is an address for a recorded event, not a judgment about that event.

For each session, write down:

| Field | What to capture |
| --- | --- |
| Selection reason | Why this trace belongs in a varied review worklist. |
| Open question | One concrete point that requires human judgment. |
| Evidence | Exact nodes or fields that help answer the question without stating the answer. |

Keep each question neutral and specific to its trace. “Was this handled correctly?” is too generic. “Given the policy result and accepted action shown here, was escalation required?” identifies the decision without supplying its verdict.

## Checkpoint

You now have:

- `returns-resolver@1`, the registered baseline agent version;
- ten imported sessions tagged `returns-baseline`;
- deterministic survey evaluations;
- a bounded, varied worklist chosen from observed evidence; and
- complete trace notes with exact session and node UUIDs.

The agent itself has not run and no model call has occurred. Continue to [2. Judge the selected behavior](judge.md).
