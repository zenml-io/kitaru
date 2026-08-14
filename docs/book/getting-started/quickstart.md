---
description: Wrap an agent, record a run, replay it unchanged, and compare it with a replay that uses a cheaper model.
icon: rocket
---

# Quickstart

This guide records one support-agent run as a [session](../concepts/agents-and-sessions.md). You will first replay the session unchanged, then replay it with a cheaper model and compare the results. The example uses this ticket:

> Refund order #4821: the card reader was double-charged.

You need a running server and a connection, covered in [Installation](installation.md). If you already collect traces in Langfuse or another tracing system, start with [Import your traces](import-your-traces.md). The rest of this guide applies from [step 4](#4-write-the-evaluator).

## 1. Register the agent

The agent is a support copilot; tell Kitaru it exists and how to start it:

```bash
kitaru agent register support-agent \
  --command "python support.py" \
  --description "Resolves support tickets"
export KITARU_AGENT_ID=<id from the output>
```

The `--command` is the agent's run spec. A [worker](../concepts/workers.md) uses it to start the agent during replay. The agent calls an OpenAI model, so it also needs an API key:

```bash
export OPENAI_API_KEY=sk-...
```

## 2. Wrap the agent and record a run

Wrap the existing agent with `KitaruAgent`, then run it as usual:

```python
# support.py
import os
import uuid

from pydantic_ai import Agent
from kitaru_pydantic_ai import KitaruAgent

agent = Agent(
    "openai:gpt-5.4",
    name="support-agent",
    system_prompt="You resolve support tickets. Issue refunds when the customer was overcharged.",
)

@agent.tool_plain
def refund_payment(order_id: str) -> str:
    # Replace this deterministic stand-in with your real refund API.
    return f"Refunded {order_id}"

support = KitaruAgent(agent, agent_id=uuid.UUID(os.environ["KITARU_AGENT_ID"]))

if __name__ == "__main__":
    result = support.run_sync(
        "Refund order #4821: the card reader was double-charged."
    )
    print(result.output)
```

```bash
python support.py
```

The run created one session containing the model requests, the `refund_payment` call and result, token usage, and cost. The same script is the replay target. When a worker runs it, the adapter supplies the recorded inputs automatically.

## 3. Look at the recording

The Python client is async and reads its connection from the environment:

```python
# show_session.py
import asyncio

from kitaru.client import KitaruAPIClient
from kitaru.api_models.v1.session import SessionListParams
from kitaru.api_models.v1.session_node import SessionNodeListParams

async def main() -> None:
    client = KitaruAPIClient()
    sessions = await client.sessions.list(SessionListParams(size=1))
    session = sessions.items[0]
    print(session.id, session.status, session.cost, session.tokens)

    nodes = await client.sessions.list_nodes(
        session.id, SessionNodeListParams(include_payloads=True)
    )
    for node in nodes.items:
        print(f"{node.index:3} {node.node_type:12} {node.name}")

asyncio.run(main())
```

```text
  0 llm_call     support-agent
  1 tool_call    refund_payment
  2 llm_call     support-agent
```

Keep the session ID this prints and export it for the replay snippets in steps 6 and 7:

```bash
export KITARU_SESSION_ID=<session-id>
```

The recording shows what the model received, what the tool returned, and what the run cost.

<a id="write-the-evaluator"></a>

## 4. Write the evaluator

A replay requires at least one evaluator. Scaffold an [evaluator](../concepts/evaluators.md), which is a Python function that reads the session:

```bash
kitaru evaluator scaffold refund-check
```

```python
# refund_check_evaluator.py
from kitaru.task.evaluator import EvaluationResult, SessionView


def evaluate(session: SessionView, **params) -> EvaluationResult:
    refunded = any(
        node.node_type == "tool_call" and node.tool_name == "refund_payment"
        for node in session.nodes
    )
    return EvaluationResult(name="refund_issued", score=refunded, passed=refunded)
```

Try it offline, then register it:

```bash
kitaru evaluator test refund_check_evaluator.py --entrypoint evaluate
kitaru evaluator register refund-check \
  --script refund_check_evaluator.py --entrypoint evaluate
```

Kitaru also includes built-in evaluators such as `kitaru/cost` and the deterministic `kitaru/output-contract`. They are useful for initial triage. Custom evaluators such as `refund-check` encode criteria specific to your application.

## 5. Start a worker

Replays execute in **your** environment, not on the server. Open a second terminal in the same directory and virtualenv:

```bash
kitaru worker start
```

Leave it running. The worker will claim the replay, run `support.py` as a subprocess, and then run the evaluator.

## 6. Replay it unchanged

First, prove the recording is faithful. Re-run the session with nothing changed and every tool call answered from the recording:

```python
# replay.py
import asyncio
import os
import uuid

from kitaru.client import KitaruAPIClient
from kitaru.api_models.v1.replay import ReplayCreateRequest
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig,
    HistoryConfig,
    ToolPolicy,
)

RECORDED_TOOLS = ToolPolicy(default=HistoryConfig(scope="baseline", on_miss="fail"))
SESSION_ID = uuid.UUID(os.environ["KITARU_SESSION_ID"])

async def main() -> None:
    client = KitaruAPIClient()
    baseline = await client.replays.create(
        ReplayCreateRequest(
            baseline_session_id=SESSION_ID,   # from step 3
            evaluators=[EvaluatorConfig(evaluator="refund-check")],
            tool_policy=RECORDED_TOOLS,
            evaluate_baselines=True,
        )
    )
    print(baseline.id, baseline.job_id)

asyncio.run(main())
```

Watch it finish, then read the result:

```bash
kitaru job watch <job-id>
```

The worker ran your code again, but the recording supplied the result of `refund_payment`, so the replay did not call the live refund service. With `on_miss="fail"`, the replay stops if the agent makes a tool call that the recording cannot answer.

Check that this unchanged replay follows the expected path and passes the same evaluations as the original session. If it does not, investigate the difference before testing a model or code change. Otherwise you cannot tell whether the next result came from your change or from an unreliable replay.

## 7. Fork it with one thing changed

Now ask the question you actually care about: _would the cheaper model have handled this ticket?_ Create `fork.py` with the same recorded-tool policy and one override:

```python
import asyncio
import os
import uuid

from kitaru.client import KitaruAPIClient
from kitaru.api_models.v1.replay import ReplayCreateRequest
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig,
    HistoryConfig,
    ReplayOverride,
    ToolPolicy,
)

SESSION_ID = uuid.UUID(os.environ["KITARU_SESSION_ID"])
RECORDED_TOOLS = ToolPolicy(default=HistoryConfig(scope="baseline", on_miss="fail"))


async def main() -> None:
    client = KitaruAPIClient()
    replay = await client.replays.create(
        ReplayCreateRequest(
            baseline_session_id=SESSION_ID,
            override=ReplayOverride(model={"openai:gpt-5.4": "openai:gpt-5-nano"}),
            evaluators=[EvaluatorConfig(evaluator="refund-check")],
            tool_policy=RECORDED_TOOLS,
        )
    )
    print(replay.id, replay.job_id)


asyncio.run(main())
```

The agent now runs from the beginning with the cheaper model. Recorded tool results remain the same.

## 8. Read the diff

Each replay produced a new session (`origin: replay`), already evaluated. Compare the fork against the baseline:

```python
from kitaru.api_models.v1.evaluation import EvaluationListParams
from kitaru.api_models.v1.filter import FilterCondition, FilterOp

async def show(client: KitaruAPIClient, session_id) -> None:
    session = await client.sessions.get(session_id)
    print(f"cost=${session.cost}  llm_calls={session.llm_call_count}")
    async for e in client.evaluations.iter(EvaluationListParams(
        filter=FilterCondition(field="session_id", op=FilterOp.EQ, value=session_id)
    )):
        print(f"  {e.name}: score={e.score} passed={e.passed}")
```

If `refund_issued` still passes and the recorded cost is lower, this session supports using the cheaper model for this kind of ticket. One session is not enough to establish how the model performs across your traffic, which is what the next step addresses.

## 9. Widen it into a regression suite

One replay tells you about one ticket. To test the change across more cases, freeze a set of recorded sessions into a [cohort](../concepts/cohorts.md), define the model swap as an [experiment](../concepts/experiments.md), and replay every session in the cohort. [Build a regression suite from production](../guides/regression-suite.md) covers that process.

## Scale it to a population

One replay answers a question about one session. The same machinery over a *population* answers the question that decides whether you ship, and the steps are the same ones you just did — only the unit changes.

Start from traffic rather than a single run. Import what you already collect, and tag it while you can:

```bash
kitaru session import traces/langfuse-traces.jsonl \
  --importer kitaru/langfuse@latest \
  --agent support-agent@1 \
  --tag returns-baseline \
  --media-type application/x-ndjson --wait
```

Run the built-in descriptive evaluators over the tag first. They do not know what *good* means for your business, but they show where cost, latency and tool behavior are unusual, which is where to look:

```bash
kitaru session evaluate --tag returns-baseline \
  --evaluator kitaru/cost@latest \
  --evaluator kitaru/latency@latest \
  --evaluator kitaru/tool-call-patterns@latest --wait
```

Then review what actually happened. Open an [investigation](../concepts/investigations.md), answer a question per session with the evidence pinned to the node that shows it, and settle each session with a verdict:

```bash
kitaru annotation create --investigation-session "$INVESTIGATION_SESSION_ID" \
  --question-key outcome --value '{"judgment":"problematic"}'
kitaru investigation session verdict "$INVESTIGATION_ID" "$SESSION_ID" problematic
```

Resist naming the failure category before you have looked — a taxonomy invented up front is the most common way to review fifty sessions and learn nothing.

Freeze the reviewed sessions into two [cohorts](../concepts/cohorts.md), because a change that fixes the broken cases while wrecking the healthy ones is not a fix:

```bash
kitaru cohort create unsafe-refund-baseline --agent support-agent \
  --session "$TICKET_004" --session "$TICKET_007"
kitaru cohort create safe-refund-control --agent support-agent \
  --session "$TICKET_001" --session "$TICKET_002"
```

Cohort versions are immutable: `unsafe-refund-baseline@1` means the same sessions next month as today, which is the point. Register the fix as a new agent version, then make the change an [experiment](../concepts/experiments.md) and replay both cohorts:

```bash
kitaru agent version register support-agent \
  --command "python support.py" --env RETURNS_POLICY_MODE=strict

kitaru experiment create improve-returns-policy --agent support-agent \
  --evaluator refund-check@1 --evaluator kitaru/cost@latest \
  --tool-policy '{"default":{"type":"passthrough"},"tools":{}}'

kitaru experiment run start improve-returns-policy \
  --cohort-version "$TARGET_COHORT_VERSION_ID" \
  --agent support-agent@2 --evaluate-baselines --wait
```

`--evaluate-baselines` scores the original sessions too; without it you have new numbers and nothing to compare them against. Read the result with `kitaru experiment run get "$RUN_ID"`, and read it honestly: improved, regressed, traded off (better on one evaluator, worse on another), or inconclusive. Inconclusive is a real result on a small cohort — the answer is more evidence, not a rounder number.

The cohort that caught this failure is now a regression suite. Replay it against the next change too; that is how the loop compounds.

## Run the whole thing

Everything above is checked into the repository as a working example — a returns agent that sometimes refunds when it should escalate, with traces, an evaluator, and the full journey from import to experiment:

```bash
cd examples/pydantic_ai_ticket_resolver
```

Its [README](https://github.com/zenml-io/kitaru/tree/develop/examples/pydantic_ai_ticket_resolver) walks all fifteen steps, and CI runs it, so the commands there are the ones that actually work. The [MCP example](https://github.com/zenml-io/kitaru/tree/develop/examples/v2/mcp) shows the same loop driven from a coding assistant.

For TypeScript, start with the focused [Mastra support-triage](https://github.com/zenml-io/kitaru/tree/develop/v2_examples/mastra_support_triage) or [Vercel AI SDK support-triage](https://github.com/zenml-io/kitaru/tree/develop/v2_examples/vercel_ai_support_triage) examples. The [Vercel AI SDK ticket resolver](https://github.com/zenml-io/kitaru/tree/develop/v2_examples/vercel_ai_ticket_resolver) is the complete recording, review, cohort, evaluator, worker, and replay walkthrough.

Prefer to be walked through it? `kitaru-investigation` is an [agent skill](../agent-native/skills.md) that runs this journey conversationally — it picks the review batch, keeps the labels yours, and stops at checkpoints you can resume from:

```bash
npx skills add zenml-io/kitaru-skills
```

## Where to go next

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Import your traces</strong></td><td>Your existing history, as replayable sessions.</td><td><a href="import-your-traces.md">import-your-traces.md</a></td></tr><tr><td><strong>Replay a failure and fork it</strong></td><td>Overrides, tool policies, and reading a comparison.</td><td><a href="../guides/replay-and-overrides.md">../guides/replay-and-overrides.md</a></td></tr><tr><td><strong>Build a regression suite</strong></td><td>Cohorts, experiments, and the CI gate.</td><td><a href="../guides/regression-suite.md">../guides/regression-suite.md</a></td></tr><tr><td><strong>Write an evaluator</strong></td><td>From your domain expert's criteria to a versioned gate.</td><td><a href="../guides/write-an-evaluator.md">../guides/write-an-evaluator.md</a></td></tr><tr><td><strong>Agent skills</strong></td><td>Let your coding assistant run the loop with you.</td><td><a href="../agent-native/skills.md">../agent-native/skills.md</a></td></tr><tr><td><strong>Deploy Kitaru</strong></td><td>Self-host for your team.</td><td><a href="../deploy/README.md">../deploy/README.md</a></td></tr></tbody></table>
