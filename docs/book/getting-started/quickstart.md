---
description: Follow a returns-agent investigation from imported traces to a replayed policy fix.
icon: rocket
---

# Quickstart

A returns agent has handled ten synthetic customer emails. Eight outcomes are correct. In two, it issued a refund when company policy required human approval. The example repository contains all ten recorded runs as a Langfuse export, giving you a small, known trace set to investigate.

The goal is to understand those failures and test a fix. You will import the traces, open one unsafe refund, record what the agent should have done, and turn that judgment into an evaluator. Then you will replay a stricter version of the agent against the two failed tickets and three correct refunds. Those control cases matter: a change that prevents unsafe refunds by stopping every refund is not an improvement.

The investigation phase reads stored traces and makes no model calls. A worker processes the import and evaluation jobs, but the agent itself runs only when you reach [Replay the change](#8-replay-the-change). That is also when you need model credentials. If you want to begin with fresh traces instead, the [optional trace-generation step](#no-traces-yet) explains how.

## Before you start

Install Git, Docker, [`uv`](https://docs.astral.sh/uv/), and `jq`, then check out the example. The commands on this page use Bash or Zsh on macOS or Linux.

```bash
git clone --branch develop https://github.com/zenml-io/kitaru.git
cd kitaru/examples/pydantic_ai_ticket_resolver
uv sync
```

The example's lockfile installs the published Kitaru packages used to test this walkthrough; it does not install the cloned repository source. The example uses synthetic customers, orders, and actions. Its refund and escalation tools only change an in-memory store.

Start a local Kitaru workspace and confirm the connection:

```bash
uv run kitaru login --local
uv run kitaru status
```

The local workspace opens at [http://localhost:8000](http://localhost:8000). To use an existing deployment instead, run `uv run kitaru login https://your-kitaru-workspace.example.com`.

## 1. Register the agent that produced the traces

Imported sessions belong to an agent version. Register the example's baseline version before importing anything:

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

This records how Kitaru can run the agent later, including the tools it may call. It does not start the agent now. Kitaru assigns this first immutable version the command reference `returns-resolver@1`; `baseline-v1` is its human-readable label.

Open a second terminal in the same example directory and start a [worker](../concepts/workers.md):

```bash
uv run kitaru worker start --name returns-quickstart-worker
```

Leave it running. The worker processes the import and evaluator tasks in the next steps. Those tasks read the recorded data and do not call the agent or a model.

## 2. Import the traces

The repository contains a Langfuse export at `traces/langfuse-traces.jsonl`. Import it under the baseline agent version and give the batch a tag:

```bash
uv run kitaru session import \
  traces/langfuse-traces.jsonl \
  --importer kitaru/langfuse@latest \
  --agent returns-resolver@1 \
  --tag returns-baseline \
  --params '{"source_instance":"quickstart"}' \
  --media-type application/x-ndjson \
  --wait
```

The media type tells the importer that the file contains one JSON object per line. `source_instance` gives those external traces a stable source identity, so Kitaru can recognize the same records if you import the file again.

In Kitaru, each recorded agent run becomes a [session](../concepts/agents-and-sessions.md). Its model calls, tool calls, and results are stored as session nodes. Check that the import created ten sessions:

```bash
uv run kitaru session list \
  --tag returns-baseline \
  --origin imported \
  --size 20
```

The table should contain ten rows. If it does not, stop before continuing. The import receipt includes a job ID; run `uv run kitaru job get JOB_UUID --tasks` to inspect any failed or skipped items. Each session preserves the agent input and output, model calls, tool calls and results, source trace ID, and the baseline agent version.

## 3. Find a trace worth reviewing

An evaluator is a reusable check that Kitaru can apply to many sessions. Each stored result is an evaluation. Start with Kitaru's descriptive evaluators:

```bash
uv run kitaru session evaluate \
  --tag returns-baseline \
  --evaluator kitaru/cost@latest \
  --evaluator kitaru/latency@latest \
  --evaluator kitaru/tool-call-patterns@latest \
  --wait

uv run kitaru evaluation list --size 100
```

These evaluators make no model calls. They orient you to differences in cost, latency, and tool paths, but they cannot decide whether a refund was safe. This fixture already identifies ticket 004 as a known failure, so you will review that trace directly. With your own traces, the descriptive results can help you choose where to start.

Select ticket 004 and inspect its trace:

```bash
TICKET_004_SESSION_ID="$(
  uv run kitaru --output json session list \
    --tag returns-baseline \
    --origin imported \
    --size 20 \
  | jq -r '.items[] | select((.inputs.turns[-1].inputs.ticket_id // .inputs.ticket_id) == "ticket-004") | .id'
)"

uv run kitaru --output json session nodes \
  "$TICKET_004_SESSION_ID" \
  --include-payloads \
  --size 100 \
| jq '.items[] | select(.tool_name == "lookup_order" or .tool_name == "get_return_policy" or .tool_name == "issue_refund") | {tool: .tool_name, inputs, outputs}'
```

The trace shows that `issue_refund` accepted a $280 refund. The applicable business rule for this example requires human approval above $150, so the correct action was to escalate. Resolve the node ID for that accepted refund:

```bash
TICKET_004_REFUND_NODE_ID="$(
  uv run kitaru --output json session nodes \
    "$TICKET_004_SESSION_ID" \
    --include-payloads \
    --size 100 \
  | jq -r '.items[] | select(.tool_name == "issue_refund" and .outputs.accepted == true) | .id'
)"
```

## 4. Record the review

An [investigation](../concepts/investigations.md) is a focused review of a chosen set of sessions. It keeps the question, evidence-backed answers, and final verdicts together, so you can see the human reasoning behind an evaluator. This investigation contains one session and asks whether its outcome was acceptable and what should have happened instead:

```bash
INVESTIGATION_ID="$(
  uv run kitaru --output json investigation create refund-policy-review \
    --agent returns-resolver \
    --description "Review whether risky refunds require human approval." \
    --session "$TICKET_004_SESSION_ID" \
    --session-question "$TICKET_004_SESSION_ID:outcome=Is this outcome acceptable, problematic, or uncertain, and what should the agent have done instead?" \
  | jq -r '.item.id'
)"

INVESTIGATION_SESSION_ID="$(
  uv run kitaru --output json investigation session list \
    "$INVESTIGATION_ID" \
    --size 20 \
  | jq -r '.items[0].id'
)"
```

An annotation records an answer. You can attach it to a specific trace node as evidence or to the session as a whole. The verdict is your overall classification of the session. Store all three parts of the review:

```bash
uv run kitaru annotation create \
  --investigation-session "$INVESTIGATION_SESSION_ID" \
  --question-key outcome \
  --selector "{\"node_id\":\"$TICKET_004_REFUND_NODE_ID\"}" \
  --value '"The amount exceeds the automatic approval threshold."'

uv run kitaru annotation create \
  --investigation-session "$INVESTIGATION_SESSION_ID" \
  --question-key outcome \
  --value '{"action":"escalate","reason":"Human approval is required before a refund."}'

uv run kitaru investigation session verdict \
  "$INVESTIGATION_ID" \
  "$TICKET_004_SESSION_ID" \
  problematic

uv run kitaru investigation update \
  "$INVESTIGATION_ID" \
  --status completed
```

The two annotations do not overwrite each other because one belongs to the refund node and the other to the whole session. Kitaru now contains the imported evidence, a completed review tied to the exact trace node, and a settled verdict. The agent has not run. If you only need to investigate existing traffic, you can stop here.

## 5. Turn the judgment into an evaluator

To test a change, turn the rule into an [evaluator](../concepts/evaluators.md). The full example uses `policy_correct`, a deterministic check that compares the reported outcome and accepted terminal tool call with the expected result for each synthetic ticket.

This short path records one review so you can see how investigations work, then uses the expected outcomes supplied with the fixture for the other cases. Do not take that shortcut with your own traces. Review every session before you encode its expected outcome or add it to a target or control cohort. The example's [coding-agent walkthrough](https://github.com/zenml-io/kitaru/blob/develop/examples/pydantic_ai_ticket_resolver/README_AGENT_GUIDED.md) shows the full five-session review.

From the example directory, scaffold the file:

```bash
uv run kitaru evaluator scaffold \
  returns-policy \
  --path evaluator.py
```

The scaffold only supplies the evaluator's shape. Open `README.md` in the example directory and replace `evaluator.py` with the implementation under **Step 8: Create a policy evaluator**. You can also read that section [on GitHub](https://github.com/zenml-io/kitaru/tree/develop/examples/pydantic_ai_ticket_resolver#step-8-create-a-policy-evaluator). Then return here and run:

```bash
uv run kitaru evaluator test \
  evaluator.py \
  --entrypoint evaluate

uv run kitaru evaluator register \
  returns-policy \
  --script evaluator.py \
  --entrypoint evaluate \
  --description "Check whether returns actions match the reviewed policy outcome." \
  --display-version 1.0
```

`evaluator test` checks that the file loads and exposes the requested entry point. The experiment later checks its behavior against the imported baselines and new replays. Kitaru assigns the registered evaluator the reference `returns-policy@1`; `1.0` is its display label.

Do not reduce the evaluator to "did the agent call `issue_refund`?" That would pass a duplicate refund, a refund for the wrong amount, or a refund followed by an escalation. The evaluator must match the decision you plan to make from its result.

## 6. Freeze target and control cohorts

A cohort groups sessions that you want to test together. Each cohort version freezes an exact membership list, so later runs use the same evidence. Resolve the five session IDs used in this quickstart:

```bash
SESSIONS_JSON="$(
  uv run kitaru --output json session list \
    --tag returns-baseline \
    --origin imported \
    --size 20
)"

session_id() {
  jq -r --arg ticket "$1" \
    '.items[] | select((.inputs.turns[-1].inputs.ticket_id // .inputs.ticket_id) == $ticket) | .id' \
    <<<"$SESSIONS_JSON"
}

TICKET_001_SESSION_ID="$(session_id ticket-001)"
TICKET_004_SESSION_ID="$(session_id ticket-004)"
TICKET_007_SESSION_ID="$(session_id ticket-007)"
TICKET_009_SESSION_ID="$(session_id ticket-009)"
TICKET_010_SESSION_ID="$(session_id ticket-010)"
```

Create one cohort for the known failures and one for nearby behavior that must stay correct:

```bash
uv run kitaru cohort create unsafe-refund-baseline \
  --agent returns-resolver \
  --description "Refunds that should have required human approval." \
  --session "$TICKET_004_SESSION_ID" \
  --session "$TICKET_007_SESSION_ID"

uv run kitaru cohort create safe-refund-control \
  --agent returns-resolver \
  --description "Valid refunds that must remain correct." \
  --session "$TICKET_001_SESSION_ID" \
  --session "$TICKET_009_SESSION_ID" \
  --session "$TICKET_010_SESSION_ID"
```

The target cohort asks whether the change fixes the known failure. The control cohort asks whether it breaks similar cases that were already correct. Both cohort versions are immutable, so reruns keep using the same evidence.

These five synthetic cases form a regression check, not a representative sample of production traffic. Passing them supports a claim about the fixture cases. It does not, by itself, prove that the change is safe for every request your agent receives.

## 7. Register the change

The baseline agent assumes its action tools enforce refund limits. The candidate checks the policy before it acts. Register that candidate as agent version 2:

```bash
uv run kitaru agent version register \
  returns-resolver \
  --command "python -m examples.pydantic_ai_ticket_resolver.agent" \
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

Registration still does not run the agent. It gives Kitaru an immutable description of the candidate you are about to test.

## 8. Replay the change

Replay is the point where Kitaru runs your agent. Create `.env`, add a valid `OPENAI_API_KEY`, then export the file in the first terminal:

```bash
cp -n .env.example .env
# Edit .env before continuing.
set -a; source .env; set +a
```

In the second terminal, stop the worker with `Ctrl-C`. Export `.env`, then restart it so the agent subprocess inherits the model credentials:

```bash
set -a; source .env; set +a
uv run kitaru worker start --name returns-quickstart-worker
```

Leave the worker running. It claims each replay and starts the registered agent command in your environment. The five replayed agent runs use `BASELINE_MODEL` from `.env`, which defaults to `openai:gpt-5-mini`; each run may make more than one paid model request.

Back in the first terminal, create the experiment. The experiment fixes the evaluator set and replay tool policy. Each experiment run then combines that definition with one cohort version and one candidate agent version.

```bash
uv run kitaru experiment create \
  improve-returns-policy \
  --agent returns-resolver \
  --description "Replay risky and valid refunds with strict approval rules." \
  --tool-policy '{"default":{"type":"passthrough"},"tools":{}}' \
  --evaluator returns-policy@1 \
  --evaluator kitaru/latency@latest \
  --evaluator kitaru/tool-call-patterns@latest
```

This example uses passthrough because every action tool writes to a fresh in-memory store. Do not copy that policy for tools that charge a card, send a message, or change production data. Use recorded results or explicit mocks instead. See [Replay and overrides](../guides/replay-and-overrides.md) for the available tool policies.

Resolve both cohort versions:

```bash
TARGET_COHORT_VERSION_ID="$(
  uv run kitaru --output json cohort version get unsafe-refund-baseline@1 \
  | jq -r '.item.id'
)"

CONTROL_COHORT_VERSION_ID="$(
  uv run kitaru --output json cohort version get safe-refund-control@1 \
  | jq -r '.item.id'
)"
```

Replay both cohorts through the candidate:

```bash
uv run kitaru experiment run start \
  improve-returns-policy \
  --cohort-version "$TARGET_COHORT_VERSION_ID" \
  --agent returns-resolver@2 \
  --evaluate-baselines \
  --wait \
  --timeout 1800

uv run kitaru experiment run start \
  improve-returns-policy \
  --cohort-version "$CONTROL_COHORT_VERSION_ID" \
  --agent returns-resolver@2 \
  --evaluate-baselines \
  --wait \
  --timeout 1800
```

`--evaluate-baselines` applies the same evaluator versions to the imported sessions and the new replays. Without it, you would have candidate results but no like-for-like baseline. The earlier `--timeout-seconds 180` limits each agent process; `--timeout 1800` limits how long this command waits for the whole cohort run.

## 9. Interpret the result

List the two completed runs:

```bash
uv run kitaru experiment run list --size 20
```

Each run receipt prints the exact commands for its result and child jobs. They have this form:

```bash
uv run kitaru experiment run get YOUR_RUN_UUID
uv run kitaru experiment run jobs YOUR_RUN_UUID --size 20
```

Join the session and evaluation output so each ticket's baseline and replay appear together:

```bash
COMPARISON_SESSIONS="$(
  uv run kitaru --output json session list \
    --agent returns-resolver \
    --size 100
)"

COMPARISON_EVALUATIONS="$(
  uv run kitaru --output json evaluation list \
    --filter '{"field":"name","op":"eq","value":"policy_correct"}' \
    --size 100
)"

jq -n \
  --argjson sessions "$COMPARISON_SESSIONS" \
  --argjson evaluations "$COMPARISON_EVALUATIONS" '
  [$sessions.items[] as $session
    | ($session.inputs.turns[-1].inputs.ticket_id // $session.inputs.ticket_id) as $ticket
    | select((["ticket-001", "ticket-004", "ticket-007", "ticket-009", "ticket-010"] | index($ticket)) != null)
    | $evaluations.items[]
    | select(.session_id == $session.id)
    | {ticket: $ticket, origin: $session.origin, status: $session.status, policy_correct: .passed}]
  | sort_by(.ticket, .origin)'
```

You should see two rows per ticket: its imported baseline and its replay. The candidate succeeds on this regression check when tickets 004 and 007 change from fail to pass, tickets 001, 009, and 010 remain passes, and every replay completes. Open the dashboard at [http://localhost:8000](http://localhost:8000) to inspect the paired traces and see how each tool path changed. The evaluator tells you whether the reviewed rule passed; the trace shows how the agent got there.

| Conclusion | What the evidence says |
| --- | --- |
| Improved | The target cases pass and the controls stay correct. |
| Regressed | The change breaks a target or control case. |
| Trade-off | The policy result improves while a guardrail gets worse. |
| Inconclusive | A replay failed, evidence is missing, or the sample is too small for the claim you need to make. |

Inconclusive is not a near-pass. Inspect the failed replay or add the missing evidence, then rerun the same cohort versions against a new agent version.

## No traces yet?

Generating traces is an optional entry step, not a prerequisite for learning Kitaru. Add valid OpenAI and Langfuse credentials to `.env`, export them, and run:

```bash
cp -n .env.example .env
# Edit .env before continuing.
set -a; source .env; set +a
./generate.sh
```

The script makes ten paid agent runs, each of which may make several model requests, and replaces the tracked `traces/langfuse-traces.jsonl` fixture with a new Langfuse export. Model runs vary, so your new traces may not contain the ticket 004 and 007 failures used by this walkthrough. Import the new file, inspect what happened, and choose target and control cases from your own evidence instead of assuming the later ticket IDs and expected result still apply.

For your own agent, keep collecting traces where you already collect them and use [Import your traces](import-your-traces.md) to choose the matching importer. You can evaluate and investigate imported sessions even when the historical agent code is no longer runnable. Replay requires a compatible registered agent version and a worker that can execute it.

When you are finished, stop the worker with `Ctrl-C`, then run `uv run kitaru logout`. For a CLI-managed local workspace, logout stops its containers but keeps the PostgreSQL data volume.

## Where to go next

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Import your traces</strong></td><td>Use Langfuse, LangSmith, Braintrust, or Kitaru JSONL data.</td><td><a href="import-your-traces.md">import-your-traces.md</a></td></tr><tr><td><strong>Replay and overrides</strong></td><td>Control models, tools, history, and replay safety.</td><td><a href="../guides/replay-and-overrides.md">../guides/replay-and-overrides.md</a></td></tr><tr><td><strong>Build a regression suite</strong></td><td>Grow reviewed failures into a reusable CI gate.</td><td><a href="../guides/regression-suite.md">../guides/regression-suite.md</a></td></tr><tr><td><strong>Write an evaluator</strong></td><td>Turn a domain rule into a versioned measurement.</td><td><a href="../guides/write-an-evaluator.md">../guides/write-an-evaluator.md</a></td></tr><tr><td><strong>Mastra example</strong></td><td>Try the same workflow with a TypeScript support agent.</td><td><a href="https://github.com/zenml-io/kitaru/tree/develop/v2_examples/mastra_support_triage">https://github.com/zenml-io/kitaru/tree/develop/v2_examples/mastra_support_triage</a></td></tr><tr><td><strong>Vercel AI SDK example</strong></td><td>Start with support triage or follow the complete ticket-resolver walkthrough.</td><td><a href="https://github.com/zenml-io/kitaru/tree/develop/v2_examples/vercel_ai_support_triage">https://github.com/zenml-io/kitaru/tree/develop/v2_examples/vercel_ai_support_triage</a></td></tr><tr><td><strong>Vercel ticket resolver</strong></td><td>Run the full TypeScript import, review, cohort, and replay path.</td><td><a href="https://github.com/zenml-io/kitaru/tree/develop/v2_examples/vercel_ai_ticket_resolver">https://github.com/zenml-io/kitaru/tree/develop/v2_examples/vercel_ai_ticket_resolver</a></td></tr></tbody></table>
