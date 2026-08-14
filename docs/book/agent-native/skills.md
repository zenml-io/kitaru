---
description: Agent skills teach your coding assistant how to run an investigation, build an adapter or importer, and interpret a replay — judgment to go with the MCP server's interface.
icon: wand-magic-sparkles
---

# Agent Skills

The [MCP server](mcp-server.md) gives a coding assistant a bounded
*interface* to Kitaru. Skills give it *judgment*: which sessions are
worth reviewing, when a behavior is real enough to freeze into a cohort,
what a replay result does and does not prove.

They ship separately from Kitaru, as Markdown procedures in
[`zenml-io/kitaru-skills`](https://github.com/zenml-io/kitaru-skills).
Nothing new runs on your machine — a skill is a document your assistant
reads so it follows the current Kitaru patterns instead of improvising.

## Install

{% tabs %}
{% tab title="Any skill-aware host" %}
```bash
npx skills add zenml-io/kitaru-skills
```
{% endtab %}

{% tab title="Claude Code plugin" %}
```
/plugin marketplace add zenml-io/kitaru-skills
/plugin install kitaru@kitaru
```
{% endtab %}
{% endtabs %}

If your host supports neither, copy the skill directory you want into
wherever it reads skills from.

The CLI knows whether they are present: `kitaru` with no arguments
discovers installed Kitaru skills — project and user locations, and the
Claude Code marketplace — and offers the install command when it finds
none. Machine-readable output reports the same under a `skills` key, so
an assistant can check its own footing before it starts.

## The investigation skill

`kitaru-investigation` is the front door. It runs one continuous journey
from the sessions you have to evidence you can act on, and it is the
skill to reach for when you are new to Kitaru, when a session surprised
you, or when you suspect a recurring problem you can't yet name.

It picks one of two entry paths from what you already have:

| You have | The skill does |
|---|---|
| A specific session that went wrong | Reads it fully, then builds a small worklist around it — related sessions plus at least one deliberate counterexample |
| A population but no clear failure | Builds a diverse sample, normally 15–30 sessions, weighted toward coverage with a random remainder |

From there it alternates breadth and depth, and ends at a
[cohort](../concepts/cohorts.md) version: the reviewed sessions that
demonstrate an accepted behavior, frozen so they can be replayed later.
If you want repeatable measurement it continues into evaluators,
preferring an installed one that already expresses your criterion over
authoring a new one.

Three things about how it works are worth knowing before you start,
because they are deliberate:

* **You are the judge.** The skill selects, summarizes and organizes
  evidence. It never converts its own suggestion into a human label — an
  [annotation](../concepts/investigations.md) records your judgment, not
  the assistant's guess.
* **It separates what happened from what should have happened.** A
  session records observed behavior. Whether that behavior was *correct*
  is a separate claim, and the skill keeps agent behavior, dependency
  behavior, and product intent apart rather than collapsing them into
  "the agent failed".
* **It explains writes before making them.** Anything that creates
  remote state or spends worker or model compute is described first, and
  cohort membership needs explicit confirmation. When something is
  missing — a payload, a permission, a worker — it stops at a durable
  checkpoint you can resume from instead of guessing.

It also resists a specific failure mode: it takes open observations
before proposing a taxonomy, so your first review batch isn't primed by
categories the assistant invented.

## The other skills

| Skill | Use it when |
|---|---|
| `kitaru-investigation` | Getting started, or turning sessions into reviewed evidence and a cohort. The front door. |
| [`kitaru-replay-experiment`](../guides/replay-and-overrides.md) | Testing one candidate change against an accepted cohort with pinned evaluators, and reading whether the evidence improved, regressed, traded off, or stayed inconclusive |
| [`kitaru-adapter-builder`](../adapters/README.md) | Your framework has no [adapter](../adapters/README.md) yet — builds the smallest honest one inside your project, in Python or TypeScript, and reports exactly what it can and cannot observe |
| [`kitaru-importer-builder`](../guides/importing-sessions.md) | A provider export has no importer — turns a representative export into a locally validated one, finishing locally unless you approve registration |

The replay skill deliberately stops short of the deployment decision: it
reports what the evidence supports and leaves the call to you. The two
builder skills default to finishing on your machine, and register or
upload only when you ask for each step.

## Skills, MCP and the CLI

The three surfaces do different jobs, and they compose:

* **Skills** supply the method — the order to do things in, and which
  judgments are yours.
* **The [MCP server](mcp-server.md)** supplies bounded operations, with
  destructive ones gated. Skills prefer it, and drop to the structured
  CLI only where it is genuinely needed, such as a local file upload or
  a built-in wait.
* **The CLI** is the same loop for you, by hand.

None of them execute your agent. A replay a skill sets up still runs on
a [worker](../concepts/workers.md) you control, in the environment you
configured — that separation is the safety property worth preserving.
