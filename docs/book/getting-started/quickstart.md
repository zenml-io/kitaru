---
description: Understand Kitaru's five-step method for debugging observed agent behavior and testing an improvement.
icon: rocket
---

# Quickstart

Kitaru helps you move from **“something went wrong in this agent trace”** to **“I understand the failure, I can reproduce it, and I know whether my change improved it.”**

An observability tool records what happened. Kitaru starts with that trace, helps you inspect and judge the behavior, turns the judgment into a reusable test, and re-runs changed agent code against the same evidence. You can debug a specific failure and improve the agent without guessing from a new demo prompt or losing sight of behavior that already worked.

You do not need to install or run anything to follow this page. The example uses a customer-support agent that looks up orders and policies, then decides whether to refund, replace, or escalate a return request. We call it the **returns agent**. When the method makes sense, you can [use it on your own agent](#use-kitaru-on-your-agent) or [follow the complete hands-on tutorial](../tutorials/returns-agent/README.md).

## The method in five steps

| Step | Question | What Kitaru adds |
| --- | --- | --- |
| **1. Observe** | What happened, and where did the behavior go wrong? | A trace becomes a session that preserves the agent input, output, model calls, tool calls, and tool results. |
| **2. Judge** | What should have happened? | A human judgment is stored beside the exact evidence that supports it. |
| **3. Define** | How will we recognize the behavior again? | The judgment becomes an evaluator, with failure cases and controls frozen into cohorts. |
| **4. Replay** | What would the changed agent do in the same situation? | Kitaru runs the changed agent while controlling how tool calls interact with the outside world. |
| **5. Compare** | Did the change fix the failure without causing a regression? | The same evaluator checks the original and replayed sessions so improvements, regressions, trade-offs, and missing evidence stay visible. |

The five steps are a loop rather than a one-time pipeline. A replay may expose a new failure, which becomes the next observation to judge and preserve as a regression case.

## 1. Observe one recorded failure

A customer asks a returns agent to refund a $280 order. The stored trace contains this path:

| Trace node | Result |
| --- | --- |
| Customer request | “Please refund order 004.” |
| `lookup_order` | The order exists and the customer paid $280. |
| `get_return_policy` | No policy matched because the agent passed the product name instead of the `luggage` category returned by `lookup_order`. |
| `issue_refund` | The tool accepts a $280 refund. |
| Agent response | “Your refund has been issued.” |

Kitaru stores one complete agent run as a [**session**](../concepts/agents-and-sessions.md). Each model call, tool call, tool result, and other event inside it is a **session node**. The order belongs to the `luggage` category, whose fixture policy requires human approval above $200. The `issue_refund` node matters because it proves that the unsafe action occurred; the final message alone would only tell us what the agent claimed.

At this point, Kitaru has preserved the behavior. It has not decided whether that behavior was good.

## 2. Judge what should have happened

A domain expert reviews the trace and states:

> This outcome is problematic. The amount exceeds the automatic refund threshold, so the agent should have escalated for human approval instead of issuing the refund.

Kitaru records the overall verdict in an [**investigation**](../concepts/investigations.md) and stores the answer as an **annotation**. The annotation can point to the exact `issue_refund` node as evidence.

This human step is deliberate. Cost, latency, and tool-call statistics can help you find an unusual trace, but they cannot infer your refund policy or decide which trade-off your business accepts. Kitaru keeps the human judgment so the later automated check has an auditable reason to exist.

## 3. Define the behavior to test

The judgment becomes a deterministic [**evaluator**](../concepts/evaluators.md) named `returns-policy`. An evaluator is a reusable check. An **evaluation** is the stored result of applying that evaluator to one session.

For this example, the evaluator checks the accepted terminal action and refund amount. It should reject a refund that needed approval, including a misleading sequence where the agent issues a refund and then escalates.

One failure case is not enough. We also preserve a nearby success:

| Case | Expected behavior | Role in the test |
| --- | --- | --- |
| $280 refund request | Escalate for approval | **Target:** behavior that must change. |
| Valid $98 refund request | Issue the refund | **Control:** behavior that must remain correct. |

Kitaru stores each group as a [**cohort**](../concepts/cohorts.md). A cohort version freezes the exact sessions used in the test. The target catches a fix that does nothing. The control catches a blunt fix such as “never issue refunds.”

## 4. Replay the changed agent

The developer changes the agent so it checks the policy before choosing an action, then registers a new [**agent version**](../concepts/agents-and-sessions.md#agents-and-agent-versions). Registration describes how a worker can run the agent; it does not execute the code or snapshot the source tree.

Kitaru then [**replays**](../concepts/replay.md) both recorded situations against the new version. Each replay starts from the original input and produces a new session. An [**experiment**](../concepts/experiments.md) binds together the candidate agent version, frozen cohort version, evaluator versions, and replay policy so the comparison can be repeated.

### Replay safety

Re-running an agent can re-run its tools. That is safe only when the replay policy says what should happen for each tool call.

- **Recorded history** returns a stored result instead of calling the live tool. This is the usual choice for payments, messages, database writes, and other side effects.
- **Static results** return a result you specify for the test.
- **Passthrough** calls the tool for real. Use it only when the tool is intentionally safe, such as this tutorial's isolated in-memory fixture.
- **Fail on a missing result** stops the replay when Kitaru cannot safely answer a tool call.

The right policy depends on the agent. “Replay” does not mean “repeat every production side effect.” It means run the agent code again while making the outside world explicit and controlled.

## 5. Compare the evidence

Kitaru applies the same evaluator version to the original sessions and the new replay sessions:

| Case | Original | Changed agent | Conclusion |
| --- | --- | --- | --- |
| $280 request | Refund, fail | Escalate, pass | The known failure improved. |
| $98 request | Refund, pass | Refund, pass | The control behavior was preserved. |

That supports a narrow claim: the change fixes the reviewed failure without breaking this control. It does not prove that every refund request is safe. More reviewed traces make the claim broader.

Kitaru keeps four honest outcomes available:

| Outcome | Meaning |
| --- | --- |
| **Improved** | Target cases improve and controls remain correct. |
| **Regressed** | A target or control gets worse. |
| **Trade-off** | One measure improves while another important measure gets worse. |
| **Inconclusive** | A replay failed, evidence is missing, or the test population cannot support the claim. |

Inconclusive is useful information. It tells you what evidence or replay control is missing before you trust the change.

## The concepts, in context

| Term | Plain meaning in this example |
| --- | --- |
| **Agent / agent version** | The returns agent, and one immutable description of how to run a particular version of it. |
| **Session / session node** | One complete run, and one event inside that run such as `issue_refund`. |
| **Investigation / annotation** | The organized human review, and an answer attached to the session or exact evidence node. |
| **Evaluator / evaluation** | The reusable policy check, and its result on one session. |
| **Cohort / cohort version** | A named test population, and one frozen membership list. |
| **Replay** | A new run of agent code based on a recorded situation and an explicit tool policy. |
| **Experiment / experiment run** | The reusable change-and-measurement definition, and one execution against a cohort version and agent version. |

You do not need to memorize these nouns before using Kitaru. Each exists to preserve one part of the reasoning: what ran, what evidence you reviewed, what rule you derived, what population you tested, and what changed.

## Use Kitaru on your agent

If you already have an agent, start there. Install the [Kitaru agent skills](../agent-native/skills.md), open your agent repository in Codex, Claude Code, or Cursor, and ask it to use [`kitaru-investigation`](../agent-native/skills.md#the-investigation-skill):

> Use `kitaru-investigation` to investigate this agent and help me test one meaningful improvement. Assume I am new to Kitaru. Explain each concept when it becomes useful, and ask me for one judgment at a time.

The coding agent can inspect your framework, connect or import traces, and guide the review. You still provide the domain judgment about what should have happened, and you approve any cohort writes, agent changes, or paid replay calls.

If you prefer to learn every command in a controlled example first, follow [Improve a returns agent](../tutorials/returns-agent/README.md). It implements all five steps with ten supplied traces, two known failures, three controls, and a real replay.

## Where to go next

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Complete tutorial</strong></td><td>Run the five-step method with a synthetic returns agent.</td><td><a href="../tutorials/returns-agent/README.md">../tutorials/returns-agent/README.md</a></td></tr><tr><td><strong>Use kitaru-investigation</strong></td><td>Apply the method inside your own agent repository.</td><td><a href="../agent-native/skills.md#the-investigation-skill">../agent-native/skills.md#the-investigation-skill</a></td></tr><tr><td><strong>Import your traces</strong></td><td>Bring in Langfuse, LangSmith, Braintrust, or Kitaru JSONL data.</td><td><a href="import-your-traces.md">import-your-traces.md</a></td></tr><tr><td><strong>Core concepts</strong></td><td>Read precise references for each Kitaru resource.</td><td><a href="../concepts/README.md">../concepts/README.md</a></td></tr></tbody></table>
