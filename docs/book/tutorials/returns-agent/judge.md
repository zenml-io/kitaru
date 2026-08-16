---
description: Record a human judgment about the unsafe refund and attach it to the supporting trace evidence.
icon: scale-balanced
---

# 2. Judge what should have happened

**Observe → Judge → Define → Replay → Compare**

The trace proves what happened, but it does not contain the conclusion that the outcome was wrong. In this phase, you will store that domain judgment separately from the raw evidence.

## State the review question

The question is:

> Is this outcome acceptable, problematic, or uncertain, and what should the agent have done instead?

For ticket 004, the answer is:

> The outcome is problematic. The luggage policy requires human approval above $200, so the agent should have recovered from the failed policy lookup and escalated before issuing any refund.

This distinction matters. Kitaru can compute facts such as cost, latency, and called tools. A person who understands the policy must decide whether those facts amount to acceptable behavior.

## Create an investigation

An [**investigation**](../../concepts/investigations.md) organizes a focused review. It keeps the selected sessions, the questions asked about each one, the answers, and the final verdicts together.

Create an investigation containing ticket 004:

```bash
INVESTIGATION_ID="$(
  uv run kitaru --output json investigation create refund-policy-review \
    --agent returns-resolver \
    --description "Review whether risky refunds require human approval." \
    --session "$TICKET_004_SESSION_ID" \
    --session-question "$TICKET_004_SESSION_ID:outcome=Is this outcome acceptable, problematic, or uncertain, and what should the agent have done instead?" \
  | jq -r '.item.id'
)"
```

The investigation links to the existing session; it does not copy or modify the trace. The question key is `outcome`. Kitaru uses that stable key to connect later annotations to the question text.

Resolve the ID of the session's membership inside this investigation:

```bash
INVESTIGATION_SESSION_ID="$(
  uv run kitaru --output json investigation session list \
    "$INVESTIGATION_ID" \
    --size 20 \
  | jq -r '.items[0].id'
)"
```

Three related IDs now have different jobs:

| Variable | What it identifies |
| --- | --- |
| `TICKET_004_SESSION_ID` | The recorded agent run. |
| `TICKET_004_REFUND_NODE_ID` | The accepted refund event inside that run. |
| `INVESTIGATION_SESSION_ID` | Ticket 004's place in this review, including its question and completion state. |

Keeping these addresses separate lets the same session participate in more than one investigation without mixing their questions or answers.

## Attach the judgment to its evidence

An **annotation** is a stored answer. It can apply to the session as a whole or use a selector to point to a specific node, JSON path, or text span.

First record why the accepted refund node is problematic:

```bash
uv run kitaru annotation create \
  --investigation-session "$INVESTIGATION_SESSION_ID" \
  --question-key outcome \
  --selector "{\"node_id\":\"$TICKET_004_REFUND_NODE_ID\"}" \
  --value '"The amount exceeds the automatic approval threshold."'
```

Then record the expected action for the session as a whole:

```bash
uv run kitaru annotation create \
  --investigation-session "$INVESTIGATION_SESSION_ID" \
  --question-key outcome \
  --value '{"action":"escalate","reason":"Human approval is required before a refund."}'
```

These annotations do not overwrite each other. The first is attached to the refund node; the second applies to the whole session. Together they preserve both the evidence and the expected behavior.

## Set the verdict

Answers and verdicts have different purposes. An annotation records the substance of an answer. A verdict classifies the overall session as `acceptable`, `problematic`, or `uncertain` and marks that session's review as complete.

Set the verdict, then complete the investigation:

```bash
uv run kitaru investigation session verdict \
  "$INVESTIGATION_ID" \
  "$TICKET_004_SESSION_ID" \
  problematic

uv run kitaru investigation update \
  "$INVESTIGATION_ID" \
  --status completed
```

The investigation's status describes the review process. It does not claim that the agent has been fixed. At this point, you have only established a reviewed failure and the expected action.

## Why keep the human review?

You could jump directly from a suspicious tool call to evaluator code, but then the reason for the check would live only in the developer's memory. The investigation makes three things inspectable later:

1. Which session was reviewed.
2. Which exact evidence supported the decision.
3. What a human said the agent should have done.

That record is especially useful when an evaluator disagrees with reviewers, a policy changes, or someone asks why a case belongs in the regression suite.

## Checkpoint

You now have a completed `refund-policy-review` investigation containing:

- ticket 004 and its review question;
- one annotation tied to the accepted refund node;
- one session-level annotation describing the expected escalation; and
- a `problematic` verdict.

No agent or model has run. If your goal were only to review historical traffic, you could stop here. To test a code change, continue to [3. Define the regression test](define.md).
