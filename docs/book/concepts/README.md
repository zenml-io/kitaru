---
description: A handful of nouns and one verb — how sessions, replays, evaluators, cohorts, experiments, investigations, and workers fit together.
icon: lightbulb
---

# Overview

Kitaru's object model is small, and every piece exists to serve one loop:
**record → replay → improve**.

* Your production agent leaves **[sessions](agents-and-sessions.md)** —
  recordings of every model call, tool call, and decision — either recorded
  live by an [adapter](../adapters/README.md) or
  [imported](../getting-started/import-your-traces.md) from the traces you
  already collect.
* **[Replay](replay.md)** re-executes a session against your real code.
  Unchanged, it reproduces the original — the faithful baseline. Forked with
  one thing different (a model, a prompt, a code change), it answers a
  counterfactual you can trust.
* **[Evaluators](evaluators.md)** score sessions and write evaluations —
  typed, versioned verdicts. Human labels land in the same table.
* **[Cohorts](cohorts.md)** freeze a population of sessions into immutable
  versions, so results stay comparable.
* **[Experiments](experiments.md)** replay a cohort against a change and
  score both sides — what improved, what regressed, before you ship.
* **[Investigations](investigations.md)** structure human review:
  questions asked over a set of sessions, answers stored as
  **annotations** pinned to exact trace locations — distinct from
  evaluations, and the raw material for calibrating them.
* **[Workers](workers.md)** execute all of it in your environment. The
  server coordinates; your infrastructure runs the code and holds the data.

The mental model in one sentence: traces tell you what happened; Kitaru
re-runs it. A trace you can only read is a transcript. A session is a
recording your test bench can execute — which is what turns production's
past into your test suite.

## How the pieces reference each other

An **agent** is the identity everything attaches to; an **agent version**
pins the code (a run spec a worker can execute). A **session** belongs to an
agent and optionally a version. A **cohort version** pins session ids. An
**experiment** pins the change (override + tool policy + evaluators); an
**experiment run** pins a cohort version and an agent version and fans out
one **replay** per session. Every replay produces a new session, and
**evaluations** land on sessions from either side — which is why comparing a
baseline to a fork is just reading two sets of rows.

Nothing is recomputed behind your back and nothing is mutable where it
matters: cohort versions, agent versions, and evaluator versions are frozen
at creation, so any number you read can be traced to exactly the code,
population, and criteria that produced it.

## Where to start

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Agents &#x26; Sessions</strong></td><td>The identity and the recording.</td><td><a href="agents-and-sessions.md">agents-and-sessions.md</a></td></tr><tr><td><strong>Replay</strong></td><td>Baselines, forks, overrides, and tool policies.</td><td><a href="replay.md">replay.md</a></td></tr><tr><td><strong>Evaluators &#x26; Evaluations</strong></td><td>Evaluating sessions, human labels, calibration.</td><td><a href="evaluators.md">evaluators.md</a></td></tr><tr><td><strong>Cohorts</strong></td><td>Immutable populations for comparable results.</td><td><a href="cohorts.md">cohorts.md</a></td></tr><tr><td><strong>Experiments</strong></td><td>A change, replayed and evaluated at population scale.</td><td><a href="experiments.md">experiments.md</a></td></tr><tr><td><strong>Investigations &#x26; Annotations</strong></td><td>Structured human review; labels with an address.</td><td><a href="investigations.md">investigations.md</a></td></tr><tr><td><strong>Workers</strong></td><td>Execution in your environment.</td><td><a href="workers.md">workers.md</a></td></tr><tr><td><strong>Under the Hood</strong></td><td>Server, workers, tasks, and blobs — the machinery.</td><td><a href="under-the-hood.md">under-the-hood.md</a></td></tr></tbody></table>
