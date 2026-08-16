---
description: Compare baseline and replay evaluations and decide what the returns-agent experiment supports.
icon: code-compare
---

# 5. Compare the evidence

**Observe → Judge → Define → Replay → Compare**

The experiment is useful only if you compare the original and changed behavior on the same cases with the same evaluator versions. In this final phase, you will inspect run health, join each ticket's baseline and replay evaluations, and state the narrow conclusion supported by the fixture.

## Confirm both runs completed

List the experiment runs:

```bash
uv run kitaru experiment run list --size 20
```

Each run receipt prints exact commands for the run and its child jobs. They have this form:

```bash
uv run kitaru experiment run get YOUR_RUN_UUID
uv run kitaru experiment run jobs YOUR_RUN_UUID --size 20
```

Confirm that both runs completed and every expected replay produced a result session. A missing or failed replay makes the comparison inconclusive; it must not silently reduce the denominator.

## Join the policy results by ticket

Fetch the relevant sessions and `policy_correct` evaluations:

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
```

Join each evaluation to its session, then sort the imported baseline and replay rows beside one another:

```bash
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

You should see two rows per ticket: one with `origin: imported` and one with `origin: replay`. The expected policy result is:

| Tickets | Imported baseline | Strict-policy replay | Meaning |
| --- | --- | --- | --- |
| 004 and 007 | Fail | Pass | The candidate fixes both fixture-defined unsafe-refund cases. |
| 001, 009, and 010 | Pass | Pass | The candidate preserves the three valid-refund controls. |

Open [http://localhost:8000](http://localhost:8000) to inspect the paired traces and see how each tool path changed. The evaluator tells you whether the reviewed policy passed. The trace tells you how the agent arrived at the outcome. Read both when the result is surprising.

## State the conclusion at the right size

If all five replays completed with the expected results, the experiment supports this conclusion:

> On the human-reviewed ticket 004 and fixture-defined ticket 007, the strict-policy agent escalated instead of refunding. It preserved the fixture-defined correct refund behavior on the three selected controls.

It does **not** support “the returns agent is now safe.” Five synthetic sessions do not represent every amount, policy, tool failure, or conversation the agent may encounter.

Use these four outcomes when reading any experiment:

| Conclusion | What the evidence says | What to do next |
| --- | --- | --- |
| **Improved** | Target cases pass and controls stay correct. | Expand the reviewed population or use the frozen cohorts as a regression gate. |
| **Regressed** | The change breaks a target or control case. | Inspect the paired traces, revise the change, and register a new agent version. |
| **Trade-off** | The policy result improves while an important guardrail gets worse. | Decide whether the trade-off is acceptable or change the candidate. |
| **Inconclusive** | A replay failed, evidence is missing, or the population cannot support the needed claim. | Repair the replay or add the missing evidence before deciding. |

An inconclusive result is not a near-pass. It identifies a gap in execution or evidence. Keep the same cohort versions when the population should remain fixed, and use a new immutable agent or evaluator version when their implementations change.

## Optional: start from fresh traces

The supplied export makes the tutorial repeatable. If you want to generate new traces, add valid OpenAI and Langfuse credentials to `.env`, export them, and run:

```bash
cp -n .env.example .env
# Edit .env before continuing.
set -a; source .env; set +a
./generate.sh
```

The script makes ten paid agent runs, each of which may make several model requests, and replaces the tracked `traces/langfuse-traces.jsonl` fixture with a new Langfuse export. Model runs vary, so the new traces may not contain the ticket 004 and 007 failures used by this tutorial.

Import the new file, inspect what actually happened, and choose target and control cases from that evidence. Do not assume the fixture ticket IDs or expected results still apply.

For your own agent, keep collecting traces where you already collect them and use [Import your traces](../../getting-started/import-your-traces.md) to choose the matching importer. You can evaluate and investigate imported sessions even when the historical agent code is no longer runnable. Replay requires a compatible registered agent version and a worker that can execute it.

## Clean up

Stop the worker in Terminal 2 with `Ctrl-C`, then disconnect the CLI:

```bash
uv run kitaru logout
```

For a CLI-managed local workspace, logout stops its containers but keeps the PostgreSQL data volume.

## What you completed

You followed the full evidence chain:

1. **Observed** a recorded failure as a session and exact evidence node.
2. **Judged** the behavior and stored the human reasoning.
3. **Defined** a versioned evaluator with frozen target and control cohorts.
4. **Replayed** a registered candidate under an explicit tool policy.
5. **Compared** original and changed behavior with the same evaluator.

The durable result is not just a passing demo. It is a reviewed behavior, a repeatable population, and an experiment you can rerun against the next candidate.

## Where to go next

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Use kitaru-investigation</strong></td><td>Apply this method to the agent in your own repository.</td><td><a href="../../agent-native/skills.md#the-investigation-skill">../../agent-native/skills.md#the-investigation-skill</a></td></tr><tr><td><strong>Build a regression suite</strong></td><td>Grow reviewed failures into a reusable CI gate.</td><td><a href="../../guides/regression-suite.md">../../guides/regression-suite.md</a></td></tr><tr><td><strong>Replay and overrides</strong></td><td>Control models, tools, history, and replay safety.</td><td><a href="../../guides/replay-and-overrides.md">../../guides/replay-and-overrides.md</a></td></tr><tr><td><strong>Write an evaluator</strong></td><td>Design and calibrate a domain-specific evaluator.</td><td><a href="../../guides/write-an-evaluator.md">../../guides/write-an-evaluator.md</a></td></tr></tbody></table>
