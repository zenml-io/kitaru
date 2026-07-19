---
description: What a finished run leaves behind — the execution, Kitaru's one durable artifact.
icon: film
---

# Executions — the recording

An **execution** is the one artifact Kitaru produces: the full recording of a
single run. A recorded or imported trace lands as an execution, and from there it
is the thing you inspect, replay, and diff. Below the fold there is really only
this one noun.

Most traces are transcripts you read. An execution re-executes. It carries
everything the original run saw — every model call, tool call, and checkpoint
output — so your real code can run again against it. This page is what a finished
run leaves behind, and why some executions can run again and some cannot.

## What a finished run leaves behind

Every execution has a stable identity and a recorded body:

* **An execution ID (`exec_id`).** The durable handle to this run. Save it, log
  it, pass it to `replay`. Everything else hangs off it.
* **Checkpoints.** Each recorded boundary the run crossed, with its inputs and
  output stored durably. On replay, checkpoints before your cut point return
  these recorded outputs instead of running again — that is what keeps a
  reproduced run faithful.
* **Artifacts.** Every checkpoint output is written to your object store as a
  typed, versioned artifact. Step through them, diff them across runs, and trace a
  bad final output back to the exact checkpoint that produced it.
* **Status.** `running`, `waiting`, `completed`, `failed`, or `cancelled` — plus
  failure details (`execution.failure`) and pending-wait details
  (`execution.pending_wait`) when they apply.
* **Lineage.** A replay is itself a new execution whose `original_exec_id` points
  back at the run it came from. The source is never mutated; it stays as evidence
  while replays branch off it.

You read all of this from `KitaruClient`, the CLI, or the dashboard:

```python
import kitaru

client = kitaru.KitaruClient()

execution = client.executions.get(exec_id)
print(execution.exec_id, execution.flow_name, execution.status)

for checkpoint in execution.checkpoints:
    print(checkpoint.name, checkpoint.status)
```

For the full inspection surface — listing, statistics, logs, and lifecycle
actions — see [Inspect & Manage Executions](../guides/execution-management.md).

## An execution is immutable evidence

A recorded run does not change. Retry resumes the *same* execution in place;
replay produces a *new* execution that reuses the recording as its world. Neither
edits the original. That immutability is the whole reason a diff is trustworthy:
the baseline you compare against is a fixed record, not something the last run
overwrote.

Because the record is fixed, an execution is also a reproducible test case. Every
incident is a run you can replay; "would the cheaper model have held?" is an
experiment over a real recording instead of a guess. Widen that from one
execution to a list of them and the cohort is a regression test — see
[Build a regression suite from production](../guides/regression-suite.md).

## Imported traces are data, not code

Executions arrive two ways, and the difference decides what you can do with them.

A **natively recorded** execution is born bound to the code that produced it.
When the flow ran, Kitaru captured a snapshot of it, so the recording and the
exact functions that made it travel together. Replay can re-enter that code with
confidence because the code is part of the record.

An **imported** trace is not. Kitaru can land a trace recorded elsewhere — a
Langfuse observations JSONL export, or a single trace fetched straight from
Langfuse — as an execution, through `kitaru import langfuse` on the CLI or
`client.imports.langfuse(...)` in the SDK. It shows up in your dashboard and
lineage like any other execution. But the import carries the *data* of what
happened, not the *code* that did it. There is no captured flow snapshot behind
it. The full walkthrough, including source attribution and storage consent, is
[Import Langfuse Traces](../guides/import-langfuse-traces.md).

That distinction is a hard line, not a rough edge:

* An imported execution is an **immutable, read-only record**. You can read its
  checkpoints, artifacts, and model traffic. Its steps never ran real Kitaru flow
  code, so there is nothing for native checkpoint replay to re-enter — Kitaru
  permanently refuses native checkpoint replay, resume, retry, and cancel on
  it.
* Running code against an imported trace means **you supply the code** — by
  [registering the Agent](../guides/agents.md) whose version served the trace.
  An import declares which registered AgentVersion it belongs to, and provider
  version stamps in the trace mark that attribution as source-verified or
  caller-supplied. From there, a registered adapter agent can run a **candidate
  experiment** from the imported evidence: a new execution, linked to the import
  by lineage, that re-runs the agent from the recorded root input or from a
  recorded message boundary while recorded tool responses answer matching tool
  calls — and every miss, including any write-capable call, is blocked rather
  than sent to the live world. The candidate run never changes the imported
  record. (In the SDK the candidate call is spelled `agent.replay(...)` — but it
  is this candidate mechanism, not the refused native checkpoint replay.) See
  [Scoring Executions](../guides/scoring.md) for how candidates are scored, and
  [verdicts and protections](../guides/replay-and-overrides.md#verdicts-and-protections)
  for how experiments are gated.

The discipline that follows is worth building early: treat the recorded code
version as part of a trace's identity. A trace is only evidence against the
code that produced it, whether that code was captured natively or declared at
import through a registered AgentVersion. If the code has moved on, replay of a
natively recorded run can raise a
[divergence error](../guides/replay-and-overrides.md#divergence) rather than
quietly reproduce the wrong thing — and a candidate run from an imported trace
reports how comparable it stayed instead of pretending the paths match.

### Run a native replay from the same project root

That code-identity rule has a concrete counterpart for the executions you replay
today. Replay re-imports your flow's source **by the module path recorded on the
run**, so the replayed code has to be importable under that same path. In
practice:

* Run replay from the **same project root** as the original run. `kitaru init`
  pins that root, and the recorded module path is relative to it — start the
  replay from somewhere else and the import can miss.
* Kitaru does try to recover: after a direct import it falls back to an
  already-loaded module, to a matching `__main__` when you launched the source as
  a script or with `python -m`, and to a temporary import from the current working
  directory. If all of those miss, replay fails with guidance to run from the
  project directory or set `PYTHONPATH`.
* The replay environment also needs the flow's dependencies installed, since
  importing the module runs its imports.

{% hint style="info" %}
Import lands a trace as a read-only execution. It is described here in prose
because there is no runnable code sample for it yet: an imported execution's
steps cannot be executed until the matching code version is present to replay
against. Natively recorded executions are runnable today — start there.
{% endhint %}

## Where to go next

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Flows</strong></td><td>The boundary that produces an execution.</td><td><a href="flows.md">flows.md</a></td></tr><tr><td><strong>Checkpoints</strong></td><td>The recorded boundaries replay reads back.</td><td><a href="checkpoints.md">checkpoints.md</a></td></tr><tr><td><strong>Debug and test on real runs</strong></td><td>Reproduce, fork, and diff a recorded execution.</td><td><a href="../guides/replay-and-overrides.md">../guides/replay-and-overrides.md</a></td></tr></tbody></table>
