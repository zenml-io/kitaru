---
description: "Where your judgment enters the system: your coding assistant maps the evidence, interviews you in context, and pins your answers to exact trace evidence as annotations."
icon: magnifying-glass-chart
---

# Investigations and annotations

Every eval system runs into the same wall: where do the criteria come from? You never wrote them down. The people who actually judge the agent, your support leads and domain experts, do it every day, in Slack threads and ticket comments, and every one of those corrections is a label nobody keeps.

Investigations are how Kitaru captures them. An **investigation** organizes a review of recorded sessions: which sessions, in what order, what question each one raises, and what the reviewer concluded. An **annotation** is one answer, stored as JSON and pinned to the exact evidence that supports it: a session, a node inside it, a path inside a payload, even a character range. Together they are the ground truth everything downstream is calibrated against. Replay can tell you what a change did; only your recorded judgment can say whether it got better.

## The interview

You rarely build an investigation by hand. [Set up your coding agent](../agent-native/setup.md) and the `kitaru-investigation` skill runs the review as an interview:

1. **It maps the world first.** From one surprising failure, the assistant reads the session fully and builds a small worklist of related sessions plus at least one counterexample. From a vague "something is off," it samples a diverse population, normally 15 to 30 sessions, random picks alongside coverage-based ones.
2. **It creates the investigation**, with a question for each session and highlights that point you at the evidence: the policy lookup that returned nothing, the refund that was accepted anyway.
3. **It asks you, in context.** Not "write down your eval criteria" in the abstract, but "given this recorded policy result and this accepted refund, was escalation required?" Questions are asked against the trace, where you can actually answer them. This is the information Kitaru wants clarity on, extracted one concrete case at a time.
4. **Your answers become annotations; your conclusions become verdicts.** Each reviewed session ends `acceptable`, `problematic`, or `uncertain`. The assistant selects, summarizes, and organizes the evidence; the judgment it records is yours, never its own suggestion.

Two design choices keep the interview honest. Open observations come before proposed failure categories, so an early taxonomy doesn't bias what you look at. And observed behavior stays separate from expected behavior: the procedure distinguishes the agent's actions, dependency behavior, and product requirements instead of labeling every surprise an agent failure.

## What the answers are for

Annotations are labels with addresses, and everything that gates a change is calibrated against them:

- **Evaluators** are checked against them: run the evaluator over the reviewed sessions and [compare its evaluations with the human answers](../guides/write-an-evaluator.md#calibrate-against-human-judgment) before the evaluator judges anything on its own.
- **Cohorts** are justified by them: the sessions confirmed `problematic` become the [cohort](cohorts.md) a regression experiment replays, and the annotation trail is the auditable reason that cohort exists.
- **The next review** builds on them: verdicts and answers stay queryable, so a later investigation starts from what is already known instead of re-litigating it.

An evaluator that gates a deploy should be able to show the human judgments it was calibrated against. Annotations are those judgments.

## What an investigation contains

An investigation belongs to one agent. It contains linked sessions, each with a `position` that determines the review order.

Questions belong to individual linked sessions rather than to the investigation as a whole, so the review can ask different questions about different runs. Each question has a `key`, unique within its session, and display text such as `refund_justified="Was the refund justified?"`. A question can include highlights: each has a selector and a description that point the reviewer at relevant evidence.

The reviewer gives each linked session a verdict of `acceptable`, `problematic`, or `uncertain`. A session remains incomplete until it has a verdict; the investigation reports progress through `completed_sessions` and `total_sessions`, and tracks its own `status` as `pending`, `in_progress`, or `completed`.

```bash
kitaru investigation create refund-complaints --agent support-agent \
  --description "Week-32 refund complaints from the support queue" \
  --session <session-id> \
  --session-question <session-id>:refund_justified="Was the refund justified?"

kitaru investigation session list <investigation-id>
kitaru investigation session verdict <investigation-id> <session-id> problematic
```

Questions and highlights use the form `SESSION:KEY`, and the session must also appear in a `--session` argument. Highlights accept a JSON array with the selector inline:

```bash
kitaru investigation create refund-complaints --agent support-agent \
  --session <session-id> \
  --session-question <session-id>:tone="Did the tone stay professional?" \
  --session-highlights <session-id>:tone='[{"selector": {"node_id": "<node-id>"}, "description": "Reply after the refund was refused"}]'
```

## Annotations: answers with an address

Every answer is an **annotation**, which stores a JSON value against a session. A **selector** attaches it to more specific evidence: a node (`node_id`), an RFC 6901 JSON pointer into the node or session response (`path`), or a character range within the resolved string (`span`, which requires a `path`). Investigation highlights use the same selector format.

An answer to an investigation question uses `investigation_session_id` and `question_key`, and Kitaru stores both on the resulting annotation. A manual annotation uses only `session_id` and can be added to any session, inside an investigation or not. Queries can tell the two apart because only question answers populate `investigation_session_id` and `question_key`.

```bash
# an answer to a question
kitaru annotation create --investigation-session <id> \
  --question-key refund_justified --value 'false'

# a standalone label, pinned to where it happened
kitaru annotation create --session <id> \
  --selector '{"node_id": "<node-id>", "path": "/output/text"}' \
  --value '{"issue": "tone", "severity": "high"}'
```

`value` can contain any JSON: a boolean answer, a rating, a rubric object. Kitaru does not impose a schema; use a consistent shape if you plan to compare annotations or calibrate an evaluator against them. Annotations can be listed, fetched, updated (`--value` only), and deleted.

## Working through a review

A review normally uses three operations:

```bash
kitaru investigation session list <investigation-id>  # what's queued, in position order
kitaru annotation create --investigation-session <id> \
  --question-key refund_justified --value 'false'     # answer, with evidence
kitaru investigation session verdict <investigation-id> <session-id> problematic
```

Answers and verdicts are separate: answers record a value per question, the verdict records the conclusion about the session as a whole, and `completed_sessions` counts only sessions with a verdict. A session can have answers and still be incomplete.

Over [MCP](../agent-native/setup.md), `kitaru_review_read` and `kitaru_review_manage` let a coding assistant read the review queue, answer questions, and create annotations. A human still decides which sessions to review and what verdict to assign. Before creating remote state or using worker or model compute, the skill explains the operation and asks for confirmation; if a required payload, permission, or worker is missing, it records a checkpoint so the interview can resume later. The client mirrors the surface: `client.investigations.*` and `client.annotations.*`.
