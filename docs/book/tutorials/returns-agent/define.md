---
description: Convert the reviewed refund policy into an evaluator with target and control cohorts.
icon: list-check
---

# 3. Define the regression test

**Observe → Judge → Define → Replay → Compare**

A human judgment applies to the reviewed session. A regression test needs two additional pieces: a reusable check that recognizes the behavior and a frozen population that includes both failures and nearby successes.

## Turn the rule into an evaluator

The reviewed rule is:

> When policy requires human approval, the agent must escalate before any refund is accepted.

An [**evaluator**](../../concepts/evaluators.md) is the reusable implementation of that rule. Each time it runs on a session, Kitaru stores an **evaluation** containing the result.

This tutorial uses a deterministic evaluator named `returns-policy`. It compares the ticket's expected outcome with the agent's reported outcome and accepted terminal tool calls. It also verifies the refund amount where a refund is expected.

From the example directory, scaffold the file:

```bash
uv run kitaru evaluator scaffold \
  returns-policy \
  --path evaluator.py
```

The scaffold supplies the evaluator's shape, not the policy logic. Open the example's `README.md` and replace `evaluator.py` with the implementation under **Step 8: Create a policy evaluator**. You can also read that section [on GitHub](https://github.com/zenml-io/kitaru/tree/develop/examples/pydantic_ai_ticket_resolver#step-8-create-a-policy-evaluator).

Test that the file loads and exposes the requested entry point:

```bash
uv run kitaru evaluator test \
  evaluator.py \
  --entrypoint evaluate
```

Then register an immutable evaluator version:

```bash
uv run kitaru evaluator register \
  returns-policy \
  --script evaluator.py \
  --entrypoint evaluate \
  --description "Check whether returns actions match the reviewed policy outcome." \
  --display-version 1.0
```

Kitaru assigns the registered version the command reference `returns-policy@1`; `1.0` is its display label. `evaluator test` checks that the script can execute. The experiment will check its behavior against both the imported sessions and the new replays.

Do not reduce this rule to “did the agent call `issue_refund`?” That check would accept a refund for the wrong amount, a duplicate refund, or a refund followed by an escalation. The evaluator must distinguish the outcomes that matter to the decision you plan to make.

{% hint style="info" %} This short tutorial records one investigation so you can see the human-review mechanism, then uses expected outcomes supplied with the synthetic fixture for the remaining cases. With your own traces, review every session before encoding its expected result or placing it in a target or control cohort. The example's [coding-agent walkthrough](https://github.com/zenml-io/kitaru/blob/develop/examples/pydantic_ai_ticket_resolver/README_AGENT_GUIDED.md) performs the complete five-session review. {% endhint %}

## Resolve the test population

The fixture contains two failures and three nearby successes that exercise the same refund behavior. Resolve their Kitaru session IDs:

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

The helper converts each business ticket ID into the UUID of the imported session. Keeping the selection explicit makes it possible to inspect the exact population before freezing it.

## Freeze target and control cohorts

A [**cohort**](../../concepts/cohorts.md) is a named population of sessions. Each **cohort version** freezes one exact membership list, so later experiment runs use the same evidence even if the named cohort evolves.

Create a target cohort for behavior that must change:

```bash
uv run kitaru cohort create unsafe-refund-baseline \
  --agent returns-resolver \
  --description "Refunds that should have required human approval." \
  --session "$TICKET_004_SESSION_ID" \
  --session "$TICKET_007_SESSION_ID"
```

Create a control cohort for behavior that must remain correct:

```bash
uv run kitaru cohort create safe-refund-control \
  --agent returns-resolver \
  --description "Valid refunds that must remain correct." \
  --session "$TICKET_001_SESSION_ID" \
  --session "$TICKET_009_SESSION_ID" \
  --session "$TICKET_010_SESSION_ID"
```

The target detects a candidate that fails to fix the unsafe refunds. The control detects an overcorrection that escalates valid refunds. Testing only the target would let “never refund anyone” look successful.

These five synthetic cases form a regression check, not a representative sample of production traffic. Passing them supports a claim about these reviewed fixture cases. It does not prove that every request your agent may receive is safe.

## Checkpoint

You now have:

- `returns-policy@1`, an immutable evaluator version;
- `unsafe-refund-baseline@1`, containing tickets 004 and 007; and
- `safe-refund-control@1`, containing tickets 001, 009, and 010.

The test now states both what must improve and what must not regress. No agent or model has run yet.

Continue to [4. Replay the changed agent](replay.md).
