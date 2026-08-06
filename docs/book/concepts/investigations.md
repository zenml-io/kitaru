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

* **Questions** — what the reviewer should answer for each session, as
  `key` + display text ("Was the refund justified?",
  "Did the agent verify the order first?").
* **Linked sessions** — the runs under review. Each link can carry a
  curated **view**: a summary plus labeled items whose selectors point
  at the exact places in the trace worth looking at, so the reviewer
  starts where the curator left off.
* **Progress** — each linked session is worked through and marked
  **complete** or **skipped**; the investigation tracks
  `completed_sessions` against `total_sessions`.

```bash
kitaru investigation create refund-complaints --agent support-agent \
  --description "Week-32 refund complaints from the support queue" \
  --question refund_justified="Was the refund justified?" \
  --session <id> --session <id>

kitaru investigation session list refund-complaints
kitaru investigation session complete <investigation-session-id>
```

## Annotations: answers with an address

Every answer is an **annotation**: a JSON value attached to a session —
optionally pinned, through a **selector**, to a specific node, a payload
part, a JSON pointer within it, even a character range. "The tone turned
hostile *here*" is recordable, not a vibe in a meeting.

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
