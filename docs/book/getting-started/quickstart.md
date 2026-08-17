---
description: "From an agent in production to your first replay-backed decision, driven by your coding assistant: every step is a prompt first, a command second."
icon: rocket
---

# Quickstart

**You probably already have an agent in production.** It serves real users, it occasionally does something wrong, and when it does, you read the trace, tweak a prompt, and hope. This page is the way out of that loop: get the agent's runs into Kitaru, judge one bad behavior, and test a fix against recorded evidence instead of a fresh demo prompt.

And you don't work Kitaru by memorizing commands. The product is built to be driven by your coding assistant: you ask, it operates Kitaru through the MCP server and the agent skills, and you keep the judgment calls. Every step below is a prompt first; the equivalent command exists when you want your hands on it.

{% hint style="info" %}
**No agent in production yet?** Prepare the public [`kitaru-template`](https://github.com/zenml-io/kitaru-template), a ready PydanticAI agent with checked-in Langfuse traces, and follow the [returns-agent tutorial](../tutorials/returns-agent/README.md). Everything on this page applies to it unchanged.
{% endhint %}

Before starting, [install Kitaru and log in](installation.md), then [set up your coding agent](../agent-native/setup.md): the MCP server gives it bounded Kitaru operations, and the skills teach it the procedures.

## First: get your runs into Kitaru

Nothing else works until your agent's runs land in Kitaru as [sessions](../concepts/agents-and-sessions.md). Both ways in are one ask away:

{% tabs %}
{% tab title="Import the traces you already have" %}
```
Here is an export of our agent's traces from Langfuse: langfuse-export.jsonl. Register the agent in Kitaru as support-agent, import the export, tag the sessions imported-baseline, and tell me what landed and what was skipped.
```

Prefer to do it by hand? It is two commands:

```bash
kitaru agent register support-agent --command "python support.py"
kitaru session import langfuse-export.jsonl \
  --importer kitaru/langfuse@latest \
  --agent support-agent@latest --tag imported-baseline --wait
```

See [Import your traces](import-your-traces.md) for the full walkthrough, and the [Langfuse](../guides/import-langfuse-traces.md), [LangSmith](../guides/import-langsmith-traces.md), and [Braintrust](../guides/import-braintrust-traces.md) guides for each provider's contract.
{% endtab %}

{% tab title="Record with an adapter" %}
```
Add the Kitaru adapter to our PydanticAI agent so every run is recorded as a session. Register the agent as support-agent first and wire its agent id into the wrapper. Don't change any agent behavior.
```

The wrapper it adds is one line around the agent you already have:

```python
from pydantic_ai import Agent
from kitaru_pydantic_ai import KitaruAgent

agent = Agent("openai:gpt-5.4", name="support-agent")
support = KitaruAgent(agent, agent_id=AGENT_ID)
support.run_sync("Refund order #4821, the card reader double-charged me.")
```

See the [adapter overview](../adapters/README.md) for your framework.
{% endtab %}
{% endtabs %}

Which one? Both, eventually:

- **Import is the fastest start.** Your history becomes reviewable today, with no code change and nothing new in production.
- **You will want the adapter anyway.** Replays and experiments re-run *your agent's code*; the adapter is what answers its tool calls from the recording. Import your backlog now, add the adapter with your next deploy.

## Then: let your assistant drive the loop

The whole method fits in one ask. `kitaru-investigation` is the skill that runs it with you:

```
Use kitaru-investigation to investigate this agent and help me test one meaningful improvement. Assume I am new to Kitaru. Show me the recorded evidence before asking for a judgment, and ask before creating resources, changing code, or starting paid replay.
```

The assistant selects sessions, walks the review, drafts the evaluator, and runs the experiment. You supply the domain judgments and approve consequential actions. These five steps are the record → replay → improve loop in working form: recording already got you the sessions above, observing, judging, and defining are where improvement gets its criteria, and replaying and comparing close the loop. What follows is what the assistant is doing at each step, with a worked example from a support agent that refunds, replaces, or escalates return requests, and the prompt you would use to drive that step alone.

{% stepper %}
{% step %}
### Observe a recorded behavior

```
Run the deterministic evaluators over support-agent's recent sessions, show me which ones look worst and why, and walk me through the worst one node by node.
```

Observation starts wide: sweep the whole history before you stare at any single trace. Kitaru ships ten [deterministic evaluators](../guides/deterministic-evaluations.md), covering session diagnostics, tool health, trajectory signals, timing, and LLM-call signals, that read stored sessions without running the agent or calling a model. The sweep is cheap and repeatable, and the failures, retries, and tool errors it surfaces tell you which sessions deserve a human look. One of the surfaced sessions contains this path:

| Session node | Result |
| --- | --- |
| Customer request | The customer asks for a high-value refund. |
| `lookup_order` | The order exists; amount and category returned. |
| `get_return_policy` | No usable approval rule comes back. |
| `issue_refund` | The tool accepts the refund. |
| Agent response | The agent says the refund was issued. |

Each model call, tool call, and result is a **session node**. The `issue_refund` node matters because it proves the action occurred; the final message alone only tells you what the agent claimed. At this point Kitaru has preserved the behavior, not judged it.
{% endstep %}

{% step %}
### Judge what should have happened

```
Open an investigation on this session. I will give the verdicts; record each one as an annotation pinned to the exact nodes that support it.
```

This is the interview, and it is the crucial step of the whole game. Your assistant has already mapped your sessions and built a worklist (related failures plus at least one counterexample); now it creates an [**investigation**](../concepts/investigations.md) and asks you, against the evidence on screen, the questions Kitaru needs clarity on. Not "write down your eval criteria," but "given this policy lookup that returned nothing and this refund that was accepted anyway, was escalation required?"

The expert answers:

> When the agent cannot establish whether approval is required, it should escalate instead of issuing the refund.

Each answer is stored as an **annotation** pinned to the exact nodes that support it, and the conclusion becomes the session's verdict. Statistics can surface an unusual trace, but they cannot infer your business policy; every judgment recorded here is the ground truth the next three steps are calibrated against.
{% endstep %}

{% step %}
### Define the behavior to test

```
Turn my accepted judgment into a deterministic evaluator, and freeze the reviewed cases, including at least one counterexample, into a cohort.
```

The accepted judgment becomes a reusable [**evaluator**](../concepts/evaluators.md). One bad case is not enough, so the review also keeps a counterexample:

| Reviewed case | Expected behavior | Role |
| --- | --- | --- |
| Approval cannot be established | Escalate without a refund | **Target:** what should change. |
| Valid low-risk refund | Issue the refund | **Counterexample:** what must not break. |

Both are frozen into a [**cohort**](../concepts/cohorts.md) version. The target catches a change that does nothing; the counterexample catches a blunt one such as "never issue refunds."
{% endstep %}

{% step %}
### Replay the changed agent

```
Register my working tree as a new version of support-agent and replay the cohort against it. Answer every tool call from the recorded history and fail on any missing result.
```

Kitaru [**replays**](../concepts/replay.md) the frozen cohort against the candidate inside an [**experiment**](../concepts/experiments.md): each replay starts from the recorded input and produces a new session.

{% hint style="warning" %}
Re-running an agent can re-run its tools, so every tool call needs a policy: **recorded history** (answer from the recording; the default for side effects), **static results**, **passthrough** (live call, only for intentionally safe tools), or **fail on a missing result**. Replay never means repeating production side effects. Insist on the recorded-history default in assistant-written replays.
{% endhint %}
{% endstep %}

{% step %}
### Compare the evidence

```
Compare evaluations between the baseline and the candidate across the cohort, and tell me what improved, what regressed, and what is inconclusive.
```

The same evaluator version checks the original and replayed sessions:

| Reviewed case | Original | Candidate | Conclusion |
| --- | --- | --- | --- |
| Approval cannot be established | Refund accepted, fail | Escalation, pass | The reviewed failure improved. |
| Valid low-risk refund | Refund accepted, pass | Refund accepted, pass | The counterexample held. |

Four honest outcomes stay available: **improved**, **regressed**, **trade-off**, and **inconclusive**. Inconclusive is information too; it names the missing evidence or execution control before you trust the change. The deployment decision stays with you.
{% endstep %}
{% endstepper %}

The five steps form a loop, not a one-time pipeline: a replay can expose a new failure, which becomes the next observation to review.

{% hint style="info" %}
**Every step also has a manual form.** The CLI covers the whole loop with `--output json`, and the [Python and TypeScript SDKs](../deploy/sdks.md) reach everything. The [guides](../guides/replay-and-overrides.md) and the [returns-agent tutorial](../tutorials/returns-agent/README.md) teach the manual path so you can see each object and boundary for yourself.
{% endhint %}

## Glossary

| Term | Plain meaning in this example |
| --- | --- |
| **Agent / agent version** | The support agent, and one immutable run specification for it. |
| **Session / session node** | One complete run, and one event inside it such as `issue_refund`. |
| **Investigation / annotation** | The organized human review, and a verdict pinned to exact evidence. |
| **Evaluator / evaluation** | The reusable behavior check, and its result on one session. |
| **Cohort / cohort version** | A named test population, and one frozen membership list. |
| **Replay** | A new run of candidate code from a recorded input under an explicit tool policy. |
| **Experiment / experiment run** | The reusable replay-and-measurement definition, and one execution of it. |

You do not need to memorize these before starting; each one preserves a step of the reasoning, and your assistant knows them already.

## Where to go next

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Set up your coding agent</strong></td><td>The MCP server and skills that make all of this one ask away.</td><td><a href="../agent-native/setup.md">../agent-native/setup.md</a></td></tr><tr><td><strong>Import your traces</strong></td><td>Bring in Langfuse, LangSmith, Braintrust, or Kitaru JSONL data.</td><td><a href="import-your-traces.md">import-your-traces.md</a></td></tr><tr><td><strong>Kitaru template</strong></td><td>Prepare the synthetic PydanticAI agent and checked-in Langfuse traces.</td><td><a href="https://github.com/zenml-io/kitaru-template">https://github.com/zenml-io/kitaru-template</a></td></tr><tr><td><strong>Complete tutorial</strong></td><td>Run the five-step method manually from the prepared template.</td><td><a href="../tutorials/returns-agent/README.md">../tutorials/returns-agent/README.md</a></td></tr><tr><td><strong>Core concepts</strong></td><td>Read precise references for each Kitaru resource.</td><td><a href="../concepts/README.md">../concepts/README.md</a></td></tr></tbody></table>
