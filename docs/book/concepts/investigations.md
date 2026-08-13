---
description: Investigations turn "someone should look at these" into a structured review — questions asked, sessions worked through, answers stored as annotations you can calibrate against.
icon: magnifying-glass-chart
---

# Investigations & Annotations

Somewhere between "a complaint came in" and "we fixed it" sits the least
structured part of the loop: a human staring at traces. An
**investigation** gives that work a shape the rest of Kitaru can use —
which sessions were reviewed, what questions were asked, what the
reviewer concluded, and where in the trace they saw it.

## The shape of an investigation

An investigation belongs to one agent and carries:

* **Linked sessions** — the runs under review, each with a `position`
  that fixes the order the reviewer works through them in.
* **Questions, per session** — questions hang off each linked session,
  not off the investigation, so one review can ask different things of
  different runs. Each is a `key` (unique within that session) plus the
  display text: `refund_justified="Was the refund justified?"`.
* **Highlights, per question** — a question can carry highlights that
  point the reviewer at the exact place in the trace it is about. Each
  highlight is a **selector** plus a prose `description`, so the reviewer
  starts where the curator left off instead of re-reading the run.
* **A verdict, per session** — the reviewer settles each linked session
  as `acceptable`, `problematic`, or `uncertain`. The verdict is
  optional until then, and the investigation tracks `completed_sessions`
  (linked sessions that have one) against `total_sessions`. The
  investigation's own `status` is separate: `pending`, `in_progress`, or
  `completed`.

```bash
kitaru investigation create refund-complaints --agent support-agent \
  --description "Week-32 refund complaints from the support queue" \
  --session <session-id> \
  --session-question <session-id>:refund_justified="Was the refund justified?"

kitaru investigation session list refund-complaints
kitaru investigation session verdict <investigation-id> <session-id> problematic
```

Questions and highlights are addressed by `SESSION:KEY`, and every key
must name a session you also passed with `--session`. Highlights take a
JSON array so a selector can be given exactly:

```bash
kitaru investigation create refund-complaints --agent support-agent \
  --session <session-id> \
  --session-question <session-id>:tone="Did the tone stay professional?" \
  --session-highlights <session-id>:tone='[{"selector": {"node_id": "<node-id>"}, "description": "Reply after the refund was refused"}]'
```

## Annotations: answers with an address

Every answer is an **annotation**: a JSON value attached to a session —
optionally pinned, through a **selector**, to a specific node
(`node_id`), an RFC 6901 JSON pointer into it or the session response
(`path`), and a character range within the resolved string (`span`,
which requires a `path`). "The tone turned hostile *here*" is
recordable, not a vibe in a meeting. Highlights on an investigation
question use the same selector, so a curator points at evidence exactly
the way a reviewer annotates it.

Annotations come from two places:

* **Answering an investigation question** — the annotation records which
  question it answers, so an investigation's output is a labeled dataset:
  every session, every question, one value each.
* **Manual annotation** — `kitaru annotation create` on any session,
  investigation or not, for ad-hoc labels.

## Why this feeds everything else

Investigations are how human judgment enters the system in a form the
rest of the loop can consume:

* **Calibration.** An investigation's answers are exactly the human
  labels that [evaluator calibration](../guides/write-an-evaluator.md#calibrate-against-human-judgment)
  needs: run the evaluator over the same sessions and compare its
  evaluations to the annotations, question by question.
* **Cohorts.** The sessions an investigation confirmed as failures are
  the natural seed for a [cohort](cohorts.md) — the complaint that
  triggered the review becomes a regression suite that outlives it.
* **Assistant-driven review.** Over [MCP](../agent-native/mcp-server.md),
  `kitaru_review_read` and `kitaru_review_manage` let a coding assistant
  work through an investigation — read the curated views, answer the
  questions, annotate what it finds — with a human curating the queue.

The client mirrors the surface: `client.investigations.*` and
`client.annotations.*`.
