# Improve the TypeScript returns agent with a coding agent

This path asks a coding agent to conduct the same evidence-first walkthrough through Kitaru's standard-mode MCP server and the CLI. You do not choose session IDs or write evaluator code. The coding agent explains concepts as they become useful, asks one question at a time, persists your judgment, and reports exact resource identities.

All commerce data and actions are synthetic and each invocation has a fresh in-memory store. The default model is scripted integration proof, not evidence that prompting caused a real model to improve. Its recordings use requested model ID `openai/gpt-5-nano` and fixed synthetic token counts, so token and cost figures are scripted rather than measured. Optional OpenAI calls are paid and require separate approval.

## Prepare the checkout and baseline

Run the setup, registration, worker, and direct-recording steps in [README.md](README.md) through the creation of `.state/baseline-sessions.json`. Direct Vercel-adapter recording replaces the Python example's Langfuse import. Python and uv remain required because the Kitaru evaluator and worker ABI is Python.

The coding agent must select only session IDs from `.state/baseline-sessions.json`. A normal restart resumes missing tickets. `--fresh` archives the old manifest and starts a new evidence set, so it requires approval. If `.state/attempts/` contains an uncommitted session file, the agent must inspect the exact remote session and ask before using `--adopt ticket-id=session-id` when it completed or `--retry ticket-id=session-id` when it failed. It must never guess or silently discard the orphan. Retry archives the local orphan marker, records a new remote session, and never deletes remote state.

Recorded prompts contain ticket ID, sender, subject, and body. They do not contain `scenario` or `expected_action`. During discovery, the coding agent must not read those fixture oracle fields or use the expected target/control list as proof.

## Connect standard-mode MCP

Find the executable:

```bash
uv run which kitaru-mcp
```

Configure your coding agent to start that absolute executable with `--mode standard --server http://localhost:8000`, then restart the coding-agent session. For example:

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

Standard mode can read sessions and nodes, run bounded evaluations and experiment workflows, conduct investigations, store annotations, and manage cohorts and experiments. It cannot read a repository file or upload local evaluator source. Local builds, `evaluator.py` creation and testing, agent registration, and file-backed evaluator registration therefore stay explicit CLI operations.

## Give the coding agent this prompt

```text
Help me improve the TypeScript returns agent in
v2_examples/vercel_ai_ticket_resolver using the connected Kitaru MCP server
in standard mode and the Kitaru CLI. Explain each Kitaru concept when it first
becomes useful.

Use only the exact recorded session IDs in
v2_examples/vercel_ai_ticket_resolver/.state/baseline-sessions.json. Direct
Vercel-adapter recording has already replaced Langfuse import. Do not inspect
fixture scenario or expected_action oracle fields during evidence discovery.

Run the bundled cost, latency, and tool-call-pattern evaluators as broad
signals. Select a small diverse review set from actual session and node
evidence. Ask me one plain-language question at a time. Persist every answer as
an annotation, anchor judgments to exact completed tool-call nodes where
possible, and finish each investigation session only after its questions are
answered. Do not ask me to choose session IDs or write evaluator code.

Refund-policy safety is a hypothesis, not an answer key. Evidence may support
ticket-004 and ticket-007 as targets and ticket-001, ticket-009, and ticket-010
as controls, but derive membership from the reviewed traces and my answers.
Present a behavior brief containing the failure rule, expected action, missing
evidence behavior, controls, success threshold, and guardrails.

After I approve the brief, draft evaluator.py from the stable evaluator source
in README.md, test it against Vercel outputs.text and completed accepted
terminal tool nodes, and use the CLI to register it. The starter does not ship
evaluator.py. Use standard-mode MCP for supported remote investigation,
annotation, cohort, experiment, evaluation, replay, and polling operations.
Use the CLI only where local source or registration requires it.

Ask for approval before every cohort or experiment write, evaluator or agent
registration, source change, --fresh, --adopt, or --retry recovery action, replay start,
or optional paid model call. Never enable OpenAI unless I explicitly approve,
RETURNS_ALLOW_PAID_MODEL=1 is set, and OPENAI_API_KEY exists.

Poll each asynchronous run and every child job to completed, failed, or
canceled. Do not report a submitted job as success. Preserve failures and
report the exact investigation, annotation, evaluator-version, cohort-version,
agent-version, experiment-run, job, baseline-session, and replay-session IDs.
```

## The guided review

The agent first describes one invocation: a TypeScript AI SDK agent receives one rendered email string, investigates synthetic commerce data with six snake_case tools, records one terminal mock action, and returns structured JSON under `session.outputs.text`.

It runs broad evaluators over only the manifest sessions, then selects likely problems, nearby successes, and counterexamples from session and node evidence. It asks these fixed questions one at a time:

1. Is this outcome acceptable, problematic, or uncertain, and why?
2. What should the agent have done instead?
3. Which policy condition and exact trace node support that judgment?

Each answer becomes an annotation. Outcome evidence should point at an exact completed terminal tool-call node whose parsed output says `accepted: true`. The investigation remains resumable until every selected case is completed or explicitly skipped.

The expected reviewed rule is that above-threshold refunds and risk-flagged orders require escalation, while ordinary valid refunds remain correctly capped. The canonical evidence should lead to target `ticket-004` and `ticket-007`, with controls `ticket-001`, `ticket-009`, and `ticket-010`, but the agent must show the evidence and obtain your approval before writing either cohort.

## Evaluator and replay gates

After the behavior brief is approved, the coding agent creates `evaluator.py` locally from README.md. The evaluator parses Vercel `session.outputs.text`, supports the older imported-turn shape for comparison, and accepts only one matching completed `NodeType.TOOL_CALL` terminal node with `accepted: true`. Missing text, malformed JSON, missing or conflicting accepted actions, and mismatched refund amounts are errors rather than guesses.

The agent uses CLI commands for the local evaluator test and file-backed registration. The deterministic baseline must produce eight passes and failures only on `ticket-004` and `ticket-007`. If it does not, the agent stops and investigates. If an approved paid run uses `--fresh`, it regenerates `.state/baseline-session-ids.txt`, `.state/target-session-ids.txt`, and `.state/control-session-ids.txt` from the new manifest before scoring or creating cohorts; otherwise those files point to archived evidence.

After separate approval, it registers the strict TypeScript version, creates the exact target and control cohort versions and experiment through standard-mode MCP, and starts both runs. Passthrough is safe only because every tool call uses a new isolated in-memory store.

The agent polls the experiment run and every replay and evaluation job to a terminal state. A failed or canceled item remains evidence and blocks a success claim.

## Decision report

The final report includes one row per reviewed session with exact identities, cohort, baseline action, candidate action, policy transition, accepted terminal tool evidence, replay status, latency and tool-path guardrails, and any unavailable metric.

For the scripted path, success means `ticket-004` and `ticket-007` change from refund/fail to escalate/pass; `ticket-001`, `ticket-009`, and `ticket-010` stay refund/pass; and every replay settles successfully. This proves the deterministic workflow contract, not causal prompt improvement. Real OpenAI observations must be labeled paid and non-deterministic and must not promise this table.

To finish, the coding agent gives you the shutdown commands, reminds you that `.state/baseline-sessions.json` identifies the exact evidence set, and leaves the worker, containers, or files running only with your knowledge.
