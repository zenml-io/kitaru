---
description: Understand Kitaru's five-step method for debugging observed agent behavior and testing an improvement.
icon: rocket
---

# Quickstart

Kitaru helps you move from **“something went wrong in this agent trace”** to **“I understand the behavior, I can test a change against it, and I know what the evidence supports.”**

An observability tool records what happened. Kitaru starts with those traces, helps you inspect and judge the behavior, turns accepted judgments into repeatable measurements, and re-runs changed agent code against the same reviewed situations. You can improve an agent without guessing from a new demo prompt or losing sight of behavior that already worked.

You do not need to install or run anything to follow this page. The example below is illustrative rather than an answer key for the [hands-on returns-agent tutorial](../tutorials/returns-agent/README.md). That tutorial asks you to inspect its supplied traces and reach your own judgments.

## The method in five steps

| Step | Question | What Kitaru adds |
| --- | --- | --- |
| **1. Observe** | What happened, and where might the behavior have gone wrong? | A trace becomes a session that preserves the agent input, output, model calls, tool calls, and tool results. |
| **2. Judge** | What should have happened? | A human judgment is stored beside the exact evidence that supports it. |
| **3. Define** | How will we recognize the behavior again? | The accepted judgment becomes an evaluator, with reviewed cases and counterexamples frozen into a cohort. |
| **4. Replay** | What would the changed agent do in the same situations? | Kitaru runs the candidate while controlling how tool calls interact with the outside world. |
| **5. Compare** | Did the change improve the behavior without causing a regression? | The same evaluator checks the original and replayed sessions so improvements, regressions, trade-offs, and missing evidence stay visible. |

The five steps form a loop rather than a one-time pipeline. A replay may expose a new failure, which becomes the next observation to review and preserve.

## 1. Observe a recorded behavior

Imagine a customer-support agent that looks up orders and policies, then decides whether to refund, replace, or escalate a return request. One recorded trace contains this path:

| Trace node | Result |
| --- | --- |
| Customer request | The customer asks for a high-value refund. |
| `lookup_order` | The order exists and its amount and product category are returned. |
| `get_return_policy` | The policy lookup does not return a usable approval rule. |
| `issue_refund` | The tool accepts the refund. |
| Agent response | The agent says that the refund was issued. |

Kitaru stores one complete agent run as a [**session**](../concepts/agents-and-sessions.md). Each model call, tool call, tool result, and other event inside it is a **session node**. The `issue_refund` node matters because it proves that the action occurred; the final message alone only tells you what the agent claimed.

At this point, Kitaru has preserved the behavior. It has not decided whether the behavior was acceptable.

## 2. Judge what should have happened

A domain expert reviews the trace and might conclude:

> This outcome is problematic. When the agent cannot establish whether approval is required, it should escalate instead of issuing the refund.

Kitaru records the verdict in an [**investigation**](../concepts/investigations.md) and stores the answer as an **annotation**. The annotation can point to the exact policy lookup and accepted refund nodes as evidence.

This human step is deliberate. Cost, latency, and tool-call statistics can help you find an unusual trace, but they cannot infer your business policy or decide which trade-off you accept. Kitaru keeps the judgment so the later automated check has an auditable reason to exist.

## 3. Define the behavior to test

After reviewing enough evidence, the team accepts a precise behavior:

> When required approval cannot be established from the recorded evidence, no refund should be accepted before escalation.

That behavior becomes a deterministic [**evaluator**](../concepts/evaluators.md). An evaluator is a reusable check. An **evaluation** is the stored result of applying one evaluator version to one session.

One problematic case is not enough. The review also preserves a nearby counterexample:

| Reviewed case | Expected behavior | Role in the test |
| --- | --- | --- |
| Approval cannot be established | Escalate without an accepted refund | **Target:** behavior that should change. |
| A valid low-risk refund | Issue the refund | **Counterexample:** behavior that should remain correct. |

Kitaru stores the reviewed population as a [**cohort**](../concepts/cohorts.md). A cohort version freezes the exact sessions used in the test. The target catches a change that does nothing. The counterexample catches a blunt change such as “never issue refunds.”

## 4. Replay the changed agent

The developer makes one bounded change, then registers a new [**agent version**](../concepts/agents-and-sessions.md#agents-and-agent-versions). Registration describes how a worker can run the candidate; it does not execute the code or snapshot a mutable source directory.

Kitaru then [**replays**](../concepts/replay.md) the frozen cohort against that candidate. Each replay starts from the recorded session input and produces a new session. An [**experiment**](../concepts/experiments.md) fixes the replay configuration and evaluator versions for the agent. Each experiment run supplies the candidate agent version and frozen cohort version.

### Replay safety

Re-running an agent can re-run its tools. The replay policy must therefore say what happens for each tool call.

- **Recorded history** returns a stored result instead of calling the live tool. This is the usual choice for payments, messages, database writes, and other side effects.
- **Static results** return a result supplied for the test.
- **Passthrough** calls the tool for real. Use it only when the tool is intentionally safe, such as an isolated in-memory example.
- **Fail on a missing result** stops the replay when Kitaru cannot answer a tool call safely.

“Replay” does not mean “repeat every production side effect.” It means run the agent code again while making its interaction with the outside world explicit and controlled.

## 5. Compare the evidence

Kitaru applies the same evaluator version to the original and replayed sessions:

| Reviewed case | Original | Candidate | Possible conclusion |
| --- | --- | --- | --- |
| Approval cannot be established | Refund accepted, fail | Escalation, pass | The reviewed failure improved. |
| Valid low-risk refund | Refund accepted, pass | Refund accepted, pass | The counterexample was preserved. |

That would support a narrow claim: the candidate improved the behavior on this frozen reviewed population without breaking its included counterexample. It would not prove that every refund request is safe. More varied reviewed evidence supports a broader claim.

Kitaru keeps four honest outcomes available:

| Outcome | Meaning |
| --- | --- |
| **Improved** | The target behavior improves and reviewed counterexamples remain correct. |
| **Regressed** | A target or counterexample gets worse. |
| **Trade-off** | One important measure improves while another gets worse. |
| **Inconclusive** | A replay failed, evidence is missing, or the population cannot support the claim. |

Inconclusive is useful information. It identifies what execution control or evidence is missing before you trust the change.

## The concepts, in context

| Term | Plain meaning in this example |
| --- | --- |
| **Agent / agent version** | The support agent, and one immutable run specification for a particular version. |
| **Session / session node** | One complete run, and one event inside it such as `issue_refund`. |
| **Investigation / annotation** | The organized human review, and an answer attached to the session or exact evidence. |
| **Evaluator / evaluation** | The reusable behavior check, and its result on one session. |
| **Cohort / cohort version** | A named test population, and one frozen membership list. |
| **Replay** | A new run of candidate code from a recorded input under an explicit tool policy. |
| **Experiment / experiment run** | The reusable replay-and-measurement definition, and one execution against a cohort version and agent version. |

You do not need to memorize these nouns before using Kitaru. Each preserves one part of the reasoning: what ran, what evidence was reviewed, what behavior was accepted, which population was tested, and what changed.

## Use Kitaru on your agent

If you already have an agent, start there. Install the [Kitaru agent skills](../agent-native/skills.md), open your agent repository in Codex, Claude Code, or Cursor, and ask it to use [`kitaru-investigation`](../agent-native/skills.md#the-investigation-skill):

> Use `kitaru-investigation` to investigate this agent and help me test one meaningful improvement. Assume I am new to Kitaru. Explain each concept when it becomes useful, show me the recorded evidence before asking for a judgment, and ask before creating resources, changing code, or starting paid replay.

The coding agent can inspect your framework, connect or import traces, and guide the review. You still supply the domain judgments and approve consequential actions.

If you prefer to learn each command in a controlled synthetic environment, follow [Investigate and improve a returns agent](../tutorials/returns-agent/README.md). The walkthrough does not reveal or use the example's test-only expected outcomes: it teaches you how to inspect the traces and reach a bounded conclusion.

## Where to go next

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Complete tutorial</strong></td><td>Run the five-step method with a synthetic returns agent.</td><td><a href="../tutorials/returns-agent/README.md">../tutorials/returns-agent/README.md</a></td></tr><tr><td><strong>Use kitaru-investigation</strong></td><td>Apply the method inside your own agent repository.</td><td><a href="../agent-native/skills.md#the-investigation-skill">../agent-native/skills.md#the-investigation-skill</a></td></tr><tr><td><strong>Import your traces</strong></td><td>Bring in Langfuse, LangSmith, Braintrust, or Kitaru JSONL data.</td><td><a href="import-your-traces.md">import-your-traces.md</a></td></tr><tr><td><strong>Core concepts</strong></td><td>Read precise references for each Kitaru resource.</td><td><a href="../concepts/README.md">../concepts/README.md</a></td></tr></tbody></table>
