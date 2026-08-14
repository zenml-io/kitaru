---
description: Organize a session review, record answers as annotations, and use those labels to calibrate evaluators.
icon: magnifying-glass-chart
---

# Investigations and annotations

An **investigation** organizes a review of recorded sessions. It keeps the sessions in review order, the questions asked about each session, and the reviewer's answers and verdicts. Reviewers can attach their answers to the exact part of a session that supports them.

## What an investigation contains

An investigation belongs to one agent. It contains linked sessions, each with a `position` that determines the review order.

Questions belong to individual linked sessions rather than to the investigation as a whole. This allows the review to ask different questions about different runs. Each question has a `key`, unique within its session, and display text such as `refund_justified="Was the refund justified?"`.

A question can also include highlights. Each highlight has a selector and a description that point the reviewer to relevant evidence in the session.

The reviewer can give each linked session a verdict of `acceptable`, `problematic`, or `uncertain`. A session remains incomplete until it has a verdict. The investigation reports this through `completed_sessions` and `total_sessions`. Its overall `status` is tracked separately as `pending`, `in_progress`, or `completed`.

```bash
kitaru investigation create refund-complaints --agent support-agent \
  --description "Week-32 refund complaints from the support queue" \
  --session <session-id> \
  --session-question <session-id>:refund_justified="Was the refund justified?"

kitaru investigation session list <investigation-id>
kitaru investigation session verdict <investigation-id> <session-id> problematic
```

Questions and highlights use the form `SESSION:KEY`. The session must also be present in a `--session` argument. Highlights accept a JSON array so you can provide the selector directly:

```bash
kitaru investigation create refund-complaints --agent support-agent \
  --session <session-id> \
  --session-question <session-id>:tone="Did the tone stay professional?" \
  --session-highlights <session-id>:tone='[{"selector": {"node_id": "<node-id>"}, "description": "Reply after the refund was refused"}]'
```

## Annotations: answers with an address

Every answer is an **annotation**, which stores a JSON value against a session. A **selector** can attach the annotation to more specific evidence: a node (`node_id`), an RFC 6901 JSON pointer into the node or session response (`path`), or a character range within the resolved string (`span`, which requires a `path`). Investigation highlights use the same selector format.

An answer to an investigation question uses `investigation_session_id` and `question_key`. Kitaru stores both fields on the resulting annotation. A manual annotation uses only `session_id` and can be added to any session, whether or not it belongs to an investigation.

Both operations create an `AnnotationResponse` with `session_id`, `selector`, and `value`. Only answers to investigation questions populate `investigation_session_id` and `question_key`, so queries can distinguish them from manual annotations.

```bash
# an answer to a question
kitaru annotation create --investigation-session <id> \
  --question-key refund_justified --value 'false'

# a standalone label, pinned to where it happened
kitaru annotation create --session <id> \
  --selector '{"node_id": "<node-id>", "path": "/output/text"}' \
  --value '{"issue": "tone", "severity": "high"}'
```

`value` can contain any JSON, such as a boolean answer, a rating, or a rubric object. Kitaru does not impose a schema. Use a consistent shape if you plan to compare annotations or use them to calibrate an evaluator. Annotations can be listed, fetched, updated (`--value` only), and deleted.

## Working through a review

A review normally uses three operations:

```bash
kitaru investigation session list <investigation-id>  # what's queued, in position order
kitaru annotation create --investigation-session <id> \
  --question-key refund_justified --value 'false'     # answer, with evidence
kitaru investigation session verdict <investigation-id> <session-id> problematic
```

Answers and verdicts are separate. Answers record a value for each question. The verdict records the reviewer's conclusion about the session as a whole, and `completed_sessions` counts sessions with a verdict. A session can therefore have answers but remain incomplete until the reviewer sets its verdict.

## Using investigation results

Investigation answers can serve as human labels for [evaluator calibration](../guides/write-an-evaluator.md#calibrate-against-human-judgment). Run the evaluator over the same sessions, then compare its evaluations with the annotations for each question.

Sessions confirmed as failures can also be collected in a [cohort](cohorts.md) and included in later regression experiments.

Over [MCP](../agent-native/mcp-server.md), `kitaru_review_read` and `kitaru_review_manage` let a coding assistant read the review queue, answer questions, and create annotations. A human still decides which sessions to review and what verdict to assign.

The client mirrors the surface: `client.investigations.*` and `client.annotations.*`.
