# Improve a returns agent with Kitaru and your coding agent

This walkthrough starts with ten recorded executions from an autonomous returns agent. Your coding agent uses Kitaru to find useful sessions, asks for your business judgment, stores the review, creates an evaluator, replays a candidate, and helps you decide whether the change worked.

You do not need to know Kitaru terminology, select session IDs, or write evaluator code. Your coding agent uses the connected Kitaru MCP server and CLI, introduces each concept when it becomes useful, and operates Kitaru for you.

All customers, orders, shipments, and actions are synthetic. Refund and replacement tools modify an in-memory store.

## What happens

1. Import baseline sessions.
2. Measure broad signals.
3. Investigate a diverse sample selected from the evidence.
4. Store your answers as annotations tied to exact trace evidence.
5. Create target and control cohorts from the review.
6. Generate and register an evaluator from the approved behavior.
7. Replay both cohorts through a candidate version.
8. Compare baseline and candidate evidence.

## Step 1: Prepare Kitaru

Run every command from `examples/pydantic_ai_ticket_resolver`.

Create and load the environment file in each terminal:

```bash
cp .env.example .env
set -a; source .env; set +a
```

Start PostgreSQL, the API, and the dashboard:

```bash
docker compose -f ../../docker-compose.yml up -d --build
```

Install the example, worker, CLI, and MCP dependencies:

```bash
uv sync \
  --extra cli \
  --extra worker \
  --extra examples \
  --extra mcp
uv pip install --editable '../../plugins/packages/pydantic-ai[openai]'
uv run --no-sync python ../../scripts/smoke_plugin_artifacts.py \
  --candidate-dir ../../plugins/candidate-wheels
export UV_FIND_LINKS="$(cd ../../plugins/candidate-wheels && pwd)"
```

Connect and confirm the server-provided plugins:

```bash
uv run kitaru login --local
uv run kitaru status
uv run kitaru importer get kitaru/langfuse
uv run kitaru evaluator get kitaru/cost
```

Start a worker in a second terminal and leave it running:

```bash
set -a; source .env; set +a
export UV_FIND_LINKS="$(cd ../../plugins/candidate-wheels && pwd)"
uv run kitaru worker start --name returns-example-worker
```

Keep `UV_FIND_LINKS` set while running a worker from a source checkout whose plugin versions have not been published yet.

The checked-in Langfuse export is enough for the walkthrough. To generate a fresh export with paid OpenAI calls, add the OpenAI and Langfuse credentials to `.env` and run `./generate.sh`.

## Step 2: Connect your coding agent

Find the MCP executable:

```bash
uv run which kitaru-mcp
```

Configure your coding agent to start it in standard mode:

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

Standard mode lets the coding agent inspect sessions, run evaluations and replays, conduct investigations, store annotations, and manage cohorts and experiments. Local trace upload plus agent and evaluator registration remain CLI operations because they use files from your repository.

Restart the coding-agent session after adding the MCP configuration so it can discover Kitaru's tools.

## Step 3: Start the guided improvement

Use this prompt:

```text
Help me improve the returns agent in this repository using the connected Kitaru
MCP server and the Kitaru CLI. Assume I do not know Kitaru and explain each
concept when it first becomes useful.

The agent is examples/pydantic_ai_ticket_resolver/agent.py. The trace export is
examples/pydantic_ai_ticket_resolver/traces/langfuse-traces.jsonl. Register the baseline
as returns-resolver and import the traces with the tag returns-baseline.

Use the bundled cost, latency, and tool-call-pattern evaluators as broad signals.
Then create an investigation and select a small diverse review set yourself. Ask
me one plain-language question at a time, persist every answer as an annotation,
and anchor evidence to exact trace nodes when possible. Do not ask me to choose
session IDs or write evaluator code.

Refund-policy safety is a useful starting hypothesis for this demo. Tickets 004
and 007 may be target cases, while tickets 001, 009, and 010 may be controls,
but verify this from the sessions and my answers instead of assuming membership.
Use separate target and control cohorts. After I approve the behavior brief,
create, test, and register the evaluator from the approved behavior brief.
Ask before cohort writes, agent changes, or paid replay calls.
```

The returns-specific hints make the demo repeatable. The coding agent must still verify behavior from the connected agent and its sessions.

## Step 4: Inspect and measure the baseline

The skill explains the agent before acting. One invocation handles one support email, looks up commerce data, reads policy, performs one mocked terminal action, and drafts a reply.

It registers or resolves the baseline version, imports the ten sessions, and runs the bundled cost, latency, and tool-call-pattern evaluators over that exact set. These checks make no model calls. They expose resource variation, repeated paths, outcomes, and unusual cases without deciding what good behavior means.

## Step 5: Investigate the behavior

The skill asks what outcome matters most. For this path, answer:

> The agent must not issue refunds when policy requires human review.

It uses that goal and the broad signals to select a bounded mix of likely problems, nearby successes, and counterexamples. You do not select IDs. Before review begins, it creates one investigation and asks about each selected session:

- Is this outcome acceptable, problematic, or uncertain, and why?
- What should the agent have done?
- Which policy condition or trace evidence determines the judgment?

For each case, the coding agent shows a curated view of the relevant output and tool evidence. It asks one question at a time, stores the answer as an annotation, attaches the exact node selector when relevant, and marks the case complete after the review is done. The investigation can resume later without repeating completed work.

The expected domain answers for the canonical path are:

- a refund above the $200 automatic threshold must escalate;
- an order with an account-takeover risk flag must escalate;
- an ordinary valid refund must remain a refund;
- a requested refund must stay capped at the amount paid;
- a valid defect found after lookup retry must remain a refund.

The skill tests the emerging rule against both problem and control cases, then presents an evidence-backed behavior brief. Approve it only when the failure rule, expected action, controls, missing-evidence behavior, success measure, and guardrails match your intent.

Kitaru now persists the session questions, selected sessions, progress, answers, and evidence selectors. The investigation-wide shipping criterion remains in the approved brief because current answers are stored per session.

## Step 6: Create behavioral cohorts

After approval, the skill derives separate immutable cohorts from the annotations and verifies the membership:

| Cohort | Expected sessions | Purpose |
|---|---|---|
| `unsafe-refund-baseline` | tickets 004 and 007 | Behavior that must change from refund to escalation. |
| `safe-refund-control` | tickets 001, 009, and 010 | Nearby refund behavior that must remain correct. |

These are expected results for this trace set. The coding agent must establish them from the review instead of hardcoding them.

## Step 7: Generate the evaluator

The approved brief becomes one observable binary rubric. The coding agent creates `examples/pydantic_ai_ticket_resolver/evaluator.py`, tests it against imported and replay-shaped evidence, and registers an immutable `returns-policy` version.

The generated evaluator checks the final action, accepted terminal tool calls, and refund amount. It rejects a reported escalation if a refund was already accepted earlier. The baseline should produce eight passes and failures on tickets 004 and 007. If it does not, the coding agent revisits the brief or implementation before continuing.

The starter repository does not contain `evaluator.py`. In the guided path, evaluator code is a product of the review.

## Step 8: Select the candidate

The candidate mode in `agent.py` checks approval thresholds, risk flags, final-sale rules, and return windows before calling `issue_refund`. The skill explains the proposed change and its control risk, then registers an exact version with `RETURNS_POLICY_MODE=strict`.

It asks for approval before replay because replay can make paid model calls.

## Step 9: Replay target and control

The skill creates an experiment with the policy evaluator as the primary measure and cost, latency, and tool-call patterns as guardrails. It sets passthrough tool policy explicitly because these tools are safe mocked actions.

It starts one run for the target cohort and one for the control cohort through MCP, evaluates baseline sessions with the same evaluator versions, and polls every run and child job until terminal. A completed replay is the current compatibility proof for a session.

## Step 10: Decide from evidence

The final comparison contains one row per reviewed replay with its cohort, baseline and candidate action, policy transition, comparable guardrail evidence, tool-path change, replay status, and exact Kitaru identities. The default replay uses the same model as the checked-in baseline, so latency is comparable unless `BASELINE_MODEL` is overridden. Replay cost remains unavailable until the adapter records provider cost; do not interpret that missing value as zero.

The intended result is:

| Ticket | Cohort | Baseline | Candidate | Policy |
|---|---|---|---|---|
| 004 | target | refund | escalate | fail to pass |
| 007 | target | refund | escalate | fail to pass |
| 001 | control | refund | refund | pass to pass |
| 009 | control | refund | refund | pass to pass |
| 010 | control | refund | refund | pass to pass |

Failed replays remain visible. A metric is marked unavailable when baseline and replay evidence are not comparable. The recommendation answers the approved decision rule: did every target improve, did every control remain correct, did a guardrail regress, and is the evidence sufficient?
