# Improve a returns agent with Kitaru and your coding agent

This walkthrough starts with ten recorded executions from an autonomous returns agent. Your coding agent uses Kitaru to help you understand those executions, asks what behavior matters to you, turns your answers into review cohorts and an evaluator, replays the same cases through a candidate version, and helps you decide whether the change worked.

You do not need to know Kitaru terminology, choose session IDs, or write an evaluator. The `kitaru-session-improvement` skill explains each concept when it becomes useful, operates the CLI and MCP server, and asks you only for domain judgment, cohort approval, and approval before paid model calls.

All customers, orders, shipments, and actions are synthetic. Refund and replacement tools only modify an in-memory store.

## What the coding agent will do

The workflow follows the evidence from real use:

1. Import the baseline sessions.
2. Measure broad signals.
3. Interview the user about desired behavior.
4. Create a target cohort and a control cohort from that interview.
5. Turn the approved behavior into a tested evaluator.
6. Register the candidate agent version.
7. Replay both cohorts.
8. Compare the evidence and make a decision.

The skill introduces each Kitaru concept immediately before it uses it. Cost, latency, and tool patterns help select sessions to inspect. Your judgment decides which behavior matters.

## Step 1: Prepare Kitaru

Run every command from `examples/canonical_example`.

Create the local environment file and export it once in each terminal:

```bash
cp .env.example .env
set -a; source .env; set +a
```

Start PostgreSQL, the Kitaru API, and the dashboard:

```bash
docker compose -f ../../docker-compose.yml up -d --build
```

Install the example, worker, CLI, and MCP dependencies:

```bash
uv sync \
  --extra cli \
  --extra worker \
  --extra pydantic-ai \
  --extra examples \
  --extra mcp
```

Connect to the local server and seed its bundled development plugins:

```bash
uv run kitaru login --local
uv run kitaru status
uv run python ../../scripts/seed_default_plugins.py
```

Start a worker in a second terminal and leave it running:

```bash
set -a; source .env; set +a
uv run kitaru worker start --name returns-example-worker
```

The checked-in Langfuse export is enough for the walkthrough. To generate a fresh export with paid OpenAI calls, add the OpenAI and Langfuse credentials to `.env` and run `./generate.sh`.

## Step 2: Connect your coding agent

Find the MCP executable:

```bash
uv run which kitaru-mcp
```

Configure your coding agent to start it in standard mode against the local server:

```json
{
  "mcpServers": {
    "kitaru": {
      "command": "/absolute/path/to/kitaru/.venv/bin/kitaru-mcp",
      "args": ["--mode", "standard", "--server", "http://localhost:8000"]
    }
  }
}
```

Standard mode lets the coding agent read Kitaru state and create cohorts and experiments. Registration, local trace upload, session evaluation, and experiment-run starts remain visible CLI operations.

The skill is stored at `.agents/skills/kitaru-session-improvement/SKILL.md`. Restart the coding-agent session after pulling or editing repository skills so its catalog is refreshed.

## Step 3: Give the coding agent the repository and traces

Start with one prompt:

```text
Use $kitaru-session-improvement to help me improve the returns agent in this
repository. Assume I do not know Kitaru yet and explain each concept when we
need it.

The agent is examples/canonical_example/agent.py. The trace export is
examples/canonical_example/traces/langfuse-traces.jsonl. Register the baseline
as returns-resolver and import the traces with the tag returns-baseline.

Use the CLI for registration, local file upload, evaluation starts, and replay
starts. Use Kitaru MCP for inspection, cohort management, and experiment
management. Ask before paid model calls or cohort writes.
```

The skill inspects the repository and explains the agent before acting:

- It resolves one incoming returns or delivery email without a human turn.
- It looks up orders, reads return policies, checks shipments, records one mock terminal action, and drafts a reply.
- Each invocation uses a new in-memory commerce store.
- The imported sessions preserve inputs, outputs, LLM calls, tool calls, tool results, cost, timing, and source trace IDs.

The skill then registers or reuses an exact baseline version, imports the ten sessions, and verifies their tag and agent-version identity. It shows the selected server, agent, and dashboard link before creating resources so duplicate records are visible. If source trace IDs already exist under another agent, it explains the collision and offers a non-destructive copy with new source IDs before discussing deletion.

## Step 4: Measure the baseline without deciding the goal

The skill explains that an evaluator applies the same check to every recorded run, then runs the bundled cost, latency, and tool-call-pattern evaluators across the exact ten baseline sessions. These evaluators make no model calls.

It summarizes what is present in the trace set using ticket labels rather than UUIDs:

- refund, replacement, and escalation outcomes;
- routine and unusual policy cases;
- repeated lookups or tool calls;
- cost and latency ranges;
- failed or incomplete sessions.

This gives the interview concrete evidence. A high-cost session may be worth inspecting, but cost alone does not make it an improvement target. The skill asks the first business question after this summary instead of teaching the remaining Kitaru object model upfront.

## Step 5: Let the skill interview you

The skill now asks one question at a time. You supply the business judgment that traces and generic evaluators cannot provide. For each likely problem, the skill inspects the final output, checks the supporting evidence, opens the relevant tool calls, and identifies the first earlier decision that made the result risky or wrong.

A canonical conversation looks like this:

> **Coding agent:** What is the most important outcome for this agent to get right?
>
> **You:** It must not issue refunds when policy requires human review.

> **Coding agent:** I found a $280 refund above the $200 automatic approval threshold and a refund on an account with a takeover-risk flag. What should the agent do in those cases?
>
> **You:** Escalate both.

> **Coding agent:** Which successful behavior must the change preserve? I found three valid refunds: an ordinary in-window defect, a request that must be capped at the amount paid, and a defect found after retrying the order lookup by email.
>
> **You:** Keep all three as refunds and never refund more than the amount paid.

> **Coding agent:** What evidence would make you comfortable using the candidate version?
>
> **You:** Both risky cases must escalate, all valid-refund controls must remain correct, no replay may fail, and I want to see any cost or latency increase.

The skill retains a plain-language observation for every reviewed case. An observation records what happened, the relevant trace evidence, the earlier decision or tool action that matters, and why you consider it acceptable, problematic, or uncertain. Persistent observations are planned but unavailable, so the current workflow keeps them in the investigation brief.

Before proposing cohorts, the skill tests the emerging idea against both supporting sessions and counterexamples. The canonical hypothesis is:

> Refunds above the automatic threshold or on risk-flagged orders must escalate without an accepted refund. Ordinary, correctly capped, and lookup-recovered refunds must remain valid. The unresolved risk is whether stricter instructions over-escalate those valid cases.

The skill summarizes these answers before creating anything:

| Field | Agreed meaning |
|---|---|
| Goal | Prevent automatic refunds when approval or risk rules require review. |
| Failure rule | Refunding above the automatic threshold or with a risk flag. |
| Expected behavior | Escalate without issuing the refund. |
| Observations | Tickets 004 and 007 contain accepted refunds after evidence requiring review. Tickets 001, 009, and 010 show valid refund paths. |
| Hypothesis | Strict policy checks remove unsafe refunds without breaking valid refunds. |
| Target group | Baseline sessions that issued one of those unsafe refunds. |
| Control group | Valid refund sessions that should remain refunds. |
| Primary measure | Policy correctness. |
| Guardrails | Correct refund amount, replay completion, cost, latency, and tool-path changes. |

The skill asks you to correct or approve this brief. This is the point where domain knowledge becomes an explicit experiment design.

## Step 6: Create meaningful cohorts

After approval, the skill maps the brief to exact trace evidence and proposes two cohorts:

| Cohort | Sessions | Why they belong together |
|---|---|---|
| `unsafe-refund-baseline` | tickets 004 and 007 | The baseline refunded despite an approval threshold or risk flag that requires escalation. |
| `safe-refund-control` | tickets 001, 009, and 010 | These are valid refunds using ordinary, capped-amount, and lookup-retry paths that must remain correct. |

The proposal includes each ticket's baseline action, relevant policy or order evidence, and broad evaluator results. You approve the meaning and membership, while the skill carries the exact session IDs into `kitaru_cohorts_manage` and verifies the resulting immutable versions.

These cohorts express a hypothesis:

- the target cohort should change from refund to escalation;
- the control cohort should preserve correct refund behavior.

## Step 7: Let the coding agent create the evaluator

The skill explains the distinction:

- cohorts identify the cases that matter;
- an evaluator decides whether each case is correct.

The session-improvement skill turns the approved brief into one binary rubric with a pass definition, a fail definition, reviewed examples, and reasons. Your approval of the investigation brief confirms the behavior. You do not need to approve Python details separately.

The skill then hands the rubric and representative sessions to `$kitaru-evaluator-authoring`. No completed `evaluator.py` exists in the starter example. The evaluator-authoring skill creates it, checks that its evidence exists in imported and replayed sessions, validates the script, tests representative pass, fail, conflicting-action, incorrect-amount, and missing-evidence cases, and registers an immutable `returns-policy` version.

The generated evaluator checks the reviewed terminal action, accepted terminal tool calls, and refund amount. This matters for ticket 007: a final response that says `escalate` must still fail if the agent already issued an accepted refund.

The coding agent reports the result in plain language:

> I created a policy check that verifies the final action, accepted action tools, and refund amount. It passes eight baseline tickets and identifies tickets 004 and 007 as failures.

It evaluates all ten baseline sessions and verifies the expected starting point:

- eight sessions pass;
- ticket 004 fails because the $280 refund requires approval;
- ticket 007 fails because the account risk flag requires review.

If the measured result disagrees with the interview, the skill stops and resolves the disagreement instead of continuing with a misleading experiment.

## Step 8: Select the candidate version

The baseline prompt assumes action tools enforce approval policy. The candidate mode in `agent.py` requires the agent to inspect approval thresholds, risk flags, final-sale rules, and return windows before calling `issue_refund`.

The skill explains why this change targets the reviewed failures and why the control cohort matters. It then registers or reuses the entrypoint with `RETURNS_POLICY_MODE=strict` as a new immutable agent version.

Before replaying, the skill asks for approval because the next step makes paid model calls.

## Step 9: Replay the target and control cohorts

The skill creates an `improve-returns-policy` experiment using exact versions of:

- `returns-policy` as the primary measure;
- cost and latency as efficiency guardrails;
- tool-call patterns as an investigation-path guardrail.

It runs the strict agent version against both cohort versions with baseline evaluation enabled. Kitaru evaluates the imported baseline and candidate replay with the same evaluator versions.

The skill polls each experiment run, replay, worker job, and evaluator task until terminal. Replay completion is the current proof that a session can run through the candidate version.

## Step 10: Decide from the evidence

The skill produces one row for every target and control replay:

| Ticket | Cohort | Baseline action | Candidate action | Policy | Cost and latency | Tool-path change |
|---|---|---|---|---|---|---|
| 004 | target | refund | escalate | fail → pass | measured delta | approval rule applied before action |
| 007 | target | refund | escalate | fail → pass | measured delta | risk rule applied before action |
| 001 | control | refund | refund | pass → pass | measured delta | reviewed for regression |
| 009 | control | refund | refund | pass → pass | measured delta | refund amount remains capped |
| 010 | control | refund | refund | pass → pass | measured delta | lookup retry still succeeds |

Every conclusion includes the exact session, evaluation, replay, cohort-version, and experiment-run IDs. Failed replays remain in the table with their error.

The skill reports a metric as unavailable when baseline and replay evidence are not comparable. Native replay sessions may contain token counts without model-cost data, so a recorded replay cost of zero is not presented as a cost improvement.

The final recommendation answers the interview's questions:

- Did both unsafe refunds become escalations?
- Did every valid refund remain correct?
- Did cost, latency, tool behavior, or replay reliability regress?
- Is the evidence sufficient under the agreed shipping criterion?

## Current persistence boundary

The conversation holds the observations, hypothesis, interview answers, and accepted rubric. Kitaru persists sessions, evaluations, cohort membership, evaluator versions, agent versions, experiments, and replays. The generated `evaluator.py` remains in the local example directory and is ignored by Git.

The planned investigation model can later store each answer as an annotation tied to a session or exact session node. The current skill retains those identifiers in its evidence report so the same interview can become durable without changing the improvement loop. See the [refined investigation and annotation proposal](https://app.notion.com/p/3b3f8dff253881ada592e8a293337e70).
