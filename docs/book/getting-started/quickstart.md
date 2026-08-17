---
description: From an agent in production to your first replay-backed decision, with the five-step method that gets you there.
icon: rocket
---

# Quickstart

**You probably already have an agent in production.** It serves real users, it occasionally does something wrong, and when it does, you read the trace, tweak a prompt, and hope. This page is the way out of that loop: get the agent's runs into Kitaru, judge one bad behavior, and test a fix against recorded evidence instead of a fresh demo prompt.

{% hint style="info" %}
**No agent in production yet?** Prepare the public [`kitaru-template`](https://github.com/zenml-io/kitaru-template), a ready PydanticAI agent with checked-in Langfuse traces, and follow the [returns-agent tutorial](../tutorials/returns-agent/README.md). Everything on this page applies to it unchanged.
{% endhint %}

This page assumes you have [installed Kitaru and logged in](installation.md).

## First: get your runs into Kitaru

Nothing else works until your agent's runs land in Kitaru as [sessions](../concepts/agents-and-sessions.md). There are two ways in:

{% tabs %}
{% tab title="Import the traces you already have" %}
If your agent logs to Langfuse, LangSmith, or Braintrust, export and import; anything else converts to Kitaru JSONL. Your production path does not change.

```bash
kitaru agent register support-agent --command "python support.py"
kitaru session import langfuse-export.jsonl \
  --importer kitaru/langfuse@latest \
  --agent support-agent@latest --wait
```

See [Import your traces](import-your-traces.md) for the full walkthrough.
{% endtab %}

{% tab title="Record with an adapter" %}
Wrap the agent once and every run is recorded, wherever it executes:

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

## Then: work the five-step method

Kitaru's loop moves you from **"something went wrong in this trace"** to **"I tested a fix against the recorded evidence, and here is what it supports."** The example below follows a support agent that refunds, replaces, or escalates return requests. You do not need to run anything to follow it.

{% stepper %}
{% step %}
### Observe a recorded behavior

One recorded session contains this path:

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

A domain expert reviews the session and concludes:

> When the agent cannot establish whether approval is required, it should escalate instead of issuing the refund.

Kitaru records the review as an [**investigation**](../concepts/investigations.md) and the verdict as an **annotation**, pinned to the exact nodes that support it. This step is deliberately human: statistics can surface an unusual trace, but they cannot infer your business policy.
{% endstep %}

{% step %}
### Define the behavior to test

The accepted judgment becomes a deterministic [**evaluator**](../concepts/evaluators.md), a reusable check. One bad case is not enough, so the review also keeps a counterexample:

| Reviewed case | Expected behavior | Role |
| --- | --- | --- |
| Approval cannot be established | Escalate without a refund | **Target:** what should change. |
| Valid low-risk refund | Issue the refund | **Counterexample:** what must not break. |

Both are frozen into a [**cohort**](../concepts/cohorts.md) version. The target catches a change that does nothing; the counterexample catches a blunt one such as "never issue refunds."
{% endstep %}

{% step %}
### Replay the changed agent

The developer makes one bounded change and registers a new **agent version**. Kitaru [**replays**](../concepts/replay.md) the frozen cohort against it inside an [**experiment**](../concepts/experiments.md): each replay starts from the recorded input and produces a new session.

{% hint style="warning" %}
Re-running an agent can re-run its tools, so every tool call needs a policy: **recorded history** (answer from the recording; the default for side effects), **static results**, **passthrough** (live call, only for intentionally safe tools), or **fail on a missing result**. Replay never means repeating production side effects.
{% endhint %}
{% endstep %}

{% step %}
### Compare the evidence

The same evaluator version checks the original and replayed sessions:

| Reviewed case | Original | Candidate | Conclusion |
| --- | --- | --- | --- |
| Approval cannot be established | Refund accepted, fail | Escalation, pass | The reviewed failure improved. |
| Valid low-risk refund | Refund accepted, pass | Refund accepted, pass | The counterexample held. |

Four honest outcomes stay available: **improved**, **regressed**, **trade-off**, and **inconclusive**. Inconclusive is information too; it names the missing evidence or execution control before you trust the change.
{% endstep %}
{% endstepper %}

The five steps form a loop, not a one-time pipeline: a replay can expose a new failure, which becomes the next observation to review.

## Do it on your agent, assisted

The fastest way to run this loop for real is to let your coding assistant drive it. Install the [Kitaru agent skills](../agent-native/skills.md), open your agent repository in Claude Code, Codex, or Cursor, and ask:

> Use `kitaru-investigation` to investigate this agent and help me test one meaningful improvement. Assume I am new to Kitaru. Show me the recorded evidence before asking for a judgment, and ask before creating resources, changing code, or starting paid replay.

The assistant connects or imports traces, walks the review with you, and drafts the evaluator. You supply the domain judgments and approve consequential actions.

## The concepts, in one table

| Term | Plain meaning in this example |
| --- | --- |
| **Agent / agent version** | The support agent, and one immutable run specification for it. |
| **Session / session node** | One complete run, and one event inside it such as `issue_refund`. |
| **Investigation / annotation** | The organized human review, and a verdict pinned to exact evidence. |
| **Evaluator / evaluation** | The reusable behavior check, and its result on one session. |
| **Cohort / cohort version** | A named test population, and one frozen membership list. |
| **Replay** | A new run of candidate code from a recorded input under an explicit tool policy. |
| **Experiment / experiment run** | The reusable replay-and-measurement definition, and one execution of it. |

You do not need to memorize these before starting; each one preserves a step of the reasoning.

## Where to go next

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Import your traces</strong></td><td>Bring in Langfuse, LangSmith, Braintrust, or Kitaru JSONL data.</td><td><a href="import-your-traces.md">import-your-traces.md</a></td></tr><tr><td><strong>Kitaru template</strong></td><td>Prepare the synthetic PydanticAI agent and checked-in Langfuse traces.</td><td><a href="https://github.com/zenml-io/kitaru-template">https://github.com/zenml-io/kitaru-template</a></td></tr><tr><td><strong>Complete tutorial</strong></td><td>Run the five-step method from the prepared template.</td><td><a href="../tutorials/returns-agent/README.md">../tutorials/returns-agent/README.md</a></td></tr><tr><td><strong>Use kitaru-investigation</strong></td><td>Apply the method inside your own agent repository.</td><td><a href="../agent-native/skills.md#the-investigation-skill">../agent-native/skills.md#the-investigation-skill</a></td></tr><tr><td><strong>Core concepts</strong></td><td>Read precise references for each Kitaru resource.</td><td><a href="../concepts/README.md">../concepts/README.md</a></td></tr></tbody></table>
