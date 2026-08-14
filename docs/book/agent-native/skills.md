---
description: Install procedures that help coding assistants investigate sessions, test changes, and build Kitaru integrations.
icon: wand-magic-sparkles
---

# Agent skills

The [MCP server](mcp-server.md) gives coding assistants access to Kitaru
operations. Agent skills are Markdown procedures that tell an assistant
how to use those operations for a specific task, such as reviewing
sessions or testing a change against a cohort.

They ship separately from Kitaru, as Markdown procedures in
[`zenml-io/kitaru-skills`](https://github.com/zenml-io/kitaru-skills).
A skill does not start another service or process. Your assistant reads
the document and follows its procedure using the tools already available
in the host.

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

Running `kitaru` with no arguments searches for installed Kitaru skills
in project and user locations and in the Claude Code marketplace. If it
finds none, it prints the installation command. Machine-readable output
reports the result under the `skills` key, which lets an assistant check
whether the procedures are installed.

## The investigation skill

`kitaru-investigation` helps you review sessions and collect evidence
about a suspected behavior. Use it when you have one surprising session
or a larger population that you want to sample before defining a failure
category.

It picks one of two entry paths from what you already have:

| You have | The skill does |
|---|---|
| A specific session that went wrong | Reads it fully, then builds a small worklist of related sessions and at least one counterexample |
| A population but no clear failure | Builds a diverse sample, normally 15–30 sessions, with a random subset alongside coverage-based selections |

It begins by surveying the selected sessions, then examines relevant ones
in detail. If the review identifies a useful set of cases, it can help
you create a [cohort](../concepts/cohorts.md) version for later replays.
It can also select an installed evaluator that matches your criterion.
It writes a new evaluator only if none of the installed ones fit.

You assign the human labels. The assistant selects, summarizes, and
organizes evidence, but an [annotation](../concepts/investigations.md)
should record your judgment rather than the assistant's suggestion.

Observed behavior stays separate from expected behavior. The procedure
distinguishes the agent's actions, dependency behavior, and product
requirements instead of labeling all unexpected outcomes as agent
failures.

Before creating remote state or using worker or model compute, the skill
explains the operation and asks for confirmation where required. You must
confirm cohort membership explicitly. If a required payload, permission,
or worker is missing, the skill records a checkpoint so the investigation
can resume later.

Open observations come before proposed failure categories. This reduces
the risk that an early taxonomy biases the first review batch.

## The other skills

| Skill | Use it when |
|---|---|
| `kitaru-investigation` | Reviewing sessions, recording evidence, and creating a cohort from confirmed cases |
| [`kitaru-replay-experiment`](../guides/replay-and-overrides.md) | Testing one candidate change against an accepted cohort with pinned evaluators, and reading whether the evidence improved, regressed, traded off, or stayed inconclusive |
| [`kitaru-adapter-builder`](../adapters/README.md) | Building a Python or TypeScript [adapter](../adapters/README.md) for a framework that Kitaru does not support yet, with explicit recording and replay capabilities |
| [`kitaru-importer-builder`](../guides/importing-sessions.md) | Building and locally validating an importer for an unsupported provider export; registration requires separate approval |

The replay skill deliberately stops short of the deployment decision: it
reports what the evidence supports and leaves the call to you. The two
builder skills default to finishing on your machine, and register or
upload only when you ask for each step.

## Skills, MCP and the CLI

Skills define the procedure and identify decisions that require human
judgment. The [MCP server](mcp-server.md) provides bounded Kitaru
operations and gates destructive ones. Skills use the structured CLI for
operations that MCP does not cover, such as uploading a local file or
waiting for a job. You can also follow the same procedures manually with
the CLI.

Skills, MCP, and the CLI do not execute your agent on the server. Replays
run on a [worker](../concepts/workers.md) that you control, using the
environment you configured for that worker.
