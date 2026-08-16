---
description: Import PydanticAI traces, review them with human evidence, and test one change through replay.
icon: rocket
---

# Quickstart

This quickstart uses ten synthetic returns-agent sessions recorded by PydanticAI and exported from Langfuse. You will import the sessions, investigate observed behavior, preserve human judgments as evidence, and prepare a replay experiment.

The example does not tell you which sessions are good or bad. Review the traces before you define a behavior, assign a verdict, create a cohort, or write an evaluator.

The investigation phase reads stored traces and makes no model calls. A worker processes import and evaluation tasks. Replays run the agent and can make paid model calls.

## Before you start

Install Git, Docker, [`uv`](https://docs.astral.sh/uv/), Node.js, and `jq`. Then check out the example:

```bash
git clone --branch develop https://github.com/zenml-io/kitaru.git
cd kitaru/examples/pydantic_ai_ticket_resolver
uv sync
```

The example uses synthetic customers, orders, shipments, and actions. Its action tools modify one isolated in-memory store.

Start a local Kitaru workspace and confirm the connection:

```bash
uv run kitaru login --local
uv run kitaru status
```

The local workspace opens at [http://localhost:8000](http://localhost:8000). To use an existing deployment, run `uv run kitaru login https://your-kitaru-workspace.example.com`.

## 1. Register the recorded agent

Imported sessions belong to an agent version. Register the baseline version:

```bash
uv run kitaru agent register \
  returns-resolver \
  --command "python -m examples.pydantic_ai_ticket_resolver.agent" \
  --description "Resolve one synthetic returns or delivery request." \
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

Registration stores the command and declared capabilities. It does not run the agent.

The receipt's `Parent ID` identifies the agent across versions. Its `Version ID` identifies the exact registered version. This quickstart uses `returns-resolver@1`, so you do not need to copy either UUID.

Imports and deterministic evaluations do not need an OpenAI key. Replays use `openai:gpt-5-nano`, make paid OpenAI API calls, and require `OPENAI_API_KEY`.

For this local walkthrough, open a second terminal in the same directory. Export the key in that shell, then start a [worker](../concepts/workers.md):

```bash
export OPENAI_API_KEY="your-openai-key"
uv run kitaru worker start --name returns-quickstart-worker
```

You can also use a secret manager that injects `OPENAI_API_KEY` into the worker process. For a deployed worker, configure the environment in your deployment system or attach a [Kitaru secret](../deploy/secrets.md) to the agent version.

The worker runs in the foreground. The `starting: {...}` message means that it is ready and waiting for tasks. Leave this terminal open and run the remaining commands in your first terminal. Press Ctrl-C to stop the worker.

## 2. Import the Langfuse sessions

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

The import preserves session inputs and outputs, model calls, tool calls and results, source trace identity, cost, tokens, and the baseline agent version.

Verify the imported population:

```bash
uv run kitaru session list \
  --tag returns-baseline \
  --origin imported \
  --size 20
```

The table should contain ten sessions. If it does not, inspect the import job from the receipt before you continue.

## 3. Install the Kitaru skills

Install the agent skills with the cross-host installer:

```bash
npx skills add zenml-io/kitaru-skills
```

The skills guide a coding agent through evidence selection, human review, cohort confirmation, evaluator selection, and safe replay. They use Kitaru MCP for typed operations and the structured CLI when a local upload or wait operation is required.

Find the MCP executable:

```bash
uv run which kitaru-mcp
```

Configure your coding agent to start it in `standard` mode. Replace the command and server values:

```json
{
  "mcpServers": {
    "kitaru": {
      "command": "/absolute/path/to/.venv/bin/kitaru-mcp",
      "args": [
        "--mode",
        "standard",
        "--server",
        "http://localhost:8000"
      ]
    }
  }
}
```

Restart the coding-agent session after you add the MCP server.

## 4. Start an evidence-led investigation

Give your coding agent this prompt:

```text
Use the kitaru-investigation skill to investigate the returns-resolver agent.
The imported population has the tag returns-baseline.

Begin with read-only inspection. Map the public agent entrypoint and registered
version. Run relevant built-in deterministic evaluators, then select a bounded,
diverse review worklist from observed evidence and a random component. Base
every judgment and cohort proposal on recorded evidence and my decisions. Do
not use fixture implementation details or a prewritten candidate as an answer
key.

Inspect the complete trace for each selected session before writing its
question. Create one distinct, neutral question per session about a concrete
decision, tool interaction, inconsistency, operational signal, or missing piece
of evidence visible in that trace. Make each question concise and
self-contained for a reviewer using the Kitaru frontend without this chat. Do
not assume an expected outcome, reveal a verdict, repeat generic wording, or use
fixture knowledge.

Attach neutral highlights to the exact nodes, JSON fields, or character spans
that help answer each question. Give every highlight a specific description
that explains why the evidence is relevant without stating a conclusion. Before
creation, show me the ordered sessions, selection reasons, questions, and
highlights. Ask me to confirm the complete review plan.

Create the durable investigation from the confirmed plan. Give me its frontend
review link and ask me to complete the questions and verdicts there. After I
return, read the persisted annotations and verdicts before continuing. Do not
ask the same questions again in chat. If no review link is available, review one
session at a time in chat. Record a whole-session verdict only after I confirm
it.

After enough review, synthesize up to three observable behavior candidates from
persisted human evidence. Show supporting sessions, counterexamples, ambiguity,
and missing external evidence. Ask me to accept one exact behavior and confirm
exact cohort membership before any cohort write. Check the installed evaluator
catalog before creating one narrow custom evaluator.

If I approve a candidate change, continue with the kitaru-replay-experiment
skill. Show the complete run card and ask before writes, code changes, live tool
effects, or paid replay. Supervise the run and report exact paired evidence as
improved, regressed, trade-off, or inconclusive. Leave deployment to me.
```

`kitaru-investigation` treats you as the judge. The coding agent can select, summarize, and organize evidence, but it cannot turn its own suggestion into your annotation or verdict.

The skill creates a fixed review worklist with a distinct question for each session. The question and highlights use the recorded trace evidence and stand alone in the frontend review. Each answer becomes an [annotation](../concepts/investigations.md) and can point to an exact node, JSON field, or character range. The complete-session verdict remains separate from question answers and investigation status.

When the evidence supports one behavior, the skill asks you to confirm an exact [cohort version](../concepts/cohorts.md). It checks the installed evaluator catalog before it proposes custom code. A custom evaluator must use observable trace evidence and must not map session identifiers to expected answers.

If you approve a change, `kitaru-replay-experiment` requires an exact candidate agent version, cohort version, evaluator versions and parameters, adapter support, and explicit tool policy. It keeps failed, canceled, and missing cases visible and returns one evidence conclusion: `improved`, `regressed`, `trade-off`, or `inconclusive`.

## 5. Follow the manual route

The example [README on GitHub](https://github.com/zenml-io/kitaru/tree/develop/examples/pydantic_ai_ticket_resolver) shows the same workflow with CLI commands.

The manual route covers:

1. Run six built-in deterministic evaluators to survey session completeness, tool health, trajectory, model-call signals, cost, and timing.
2. Select a diverse worklist without assigning labels from summary fields.
3. Create an investigation with fixed neutral questions and optional highlights.
4. Store human annotations with node, JSON-pointer, and character-span selectors.
5. Record `acceptable`, `problematic`, or `uncertain` verdicts separately.
6. Accept one observable behavior and freeze reviewed evidence into an immutable cohort version.
7. Select an installed evaluator or create and calibrate one narrow evaluator.
8. Register one candidate and run one bounded experiment with an explicit tool policy.
9. Compare paired baseline and replay evidence without dropping failed or missing cases.

## Use your own traces

The checked-in Langfuse export is the input for this quickstart. To investigate your own agent, use [Import your traces](import-your-traces.md) to choose the correct importer. You can evaluate and investigate imported sessions even when the historical agent code is unavailable. Replay requires a compatible registered agent version, its runtime credentials, and an active worker.

When you finish, stop the worker with `Ctrl-C`, then run `uv run kitaru logout`. Local logout stops the containers and retains the PostgreSQL volume.

## Where to go next

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Investigations and annotations</strong></td><td>Review sessions and attach human evidence to exact trace locations.</td><td><a href="../concepts/investigations.md">../concepts/investigations.md</a></td></tr><tr><td><strong>Agent skills</strong></td><td>Use the investigation and replay procedures from a coding agent.</td><td><a href="../agent-native/skills.md">../agent-native/skills.md</a></td></tr><tr><td><strong>Replay and overrides</strong></td><td>Control models, prompts, tools, history, and replay safety.</td><td><a href="../guides/replay-and-overrides.md">../guides/replay-and-overrides.md</a></td></tr><tr><td><strong>Build a regression suite</strong></td><td>Grow reviewed evidence into a reusable comparison.</td><td><a href="../guides/regression-suite.md">../guides/regression-suite.md</a></td></tr><tr><td><strong>Write an evaluator</strong></td><td>Turn an accepted behavior into a versioned measurement.</td><td><a href="../guides/write-an-evaluator.md">../guides/write-an-evaluator.md</a></td></tr><tr><td><strong>Mastra example</strong></td><td>Try the workflow with a TypeScript support agent.</td><td><a href="https://github.com/zenml-io/kitaru/tree/develop/v2_examples/mastra_support_triage">https://github.com/zenml-io/kitaru/tree/develop/v2_examples/mastra_support_triage</a></td></tr><tr><td><strong>Vercel AI SDK example</strong></td><td>Try the workflow with a TypeScript triage agent.</td><td><a href="https://github.com/zenml-io/kitaru/tree/develop/v2_examples/vercel_ai_support_triage">https://github.com/zenml-io/kitaru/tree/develop/v2_examples/vercel_ai_support_triage</a></td></tr><tr><td><strong>Vercel ticket resolver</strong></td><td>Run the complete TypeScript import, review, cohort, and replay path.</td><td><a href="https://github.com/zenml-io/kitaru/tree/develop/v2_examples/vercel_ai_ticket_resolver">https://github.com/zenml-io/kitaru/tree/develop/v2_examples/vercel_ai_ticket_resolver</a></td></tr></tbody></table>
