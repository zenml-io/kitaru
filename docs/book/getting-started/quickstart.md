---
description: Wrap an agent, record a run, replay it unchanged, fork it with a cheaper model, and read the diff. One sitting.
icon: rocket
---

# Quickstart

You will take an agent you already have, wrap it so every run becomes a
[session](../concepts/agents-and-sessions.md), and then do the one thing
Kitaru is for: **replay** that recording. Reproduce it, fork it with one
thing changed, and read what the change did. One sitting, one ticket:

> Refund order #4821 — the card reader was double-charged.

You need a running server and a login — two commands, covered in
[Installation](installation.md). Already collecting traces in Langfuse or
elsewhere? You can skip wrapping entirely and start from
[Import your traces](import-your-traces.md) — everything from
[step 4](#write-the-evaluator) on works the same.

## 1. Register the agent

The agent is a support copilot; tell Kitaru it exists and how to start it:

```bash
kitaru agent register support-agent \
  --command "python support.py" \
  --description "Resolves support tickets"
export KITARU_AGENT_ID=<id from the output>
```

The `--command` is the agent's run spec — how a
[worker](../concepts/workers.md) will re-launch this exact agent when you
replay. The agent calls an OpenAI model, so give it a key too:

```bash
export OPENAI_API_KEY=sk-...
```

## 2. Wrap the agent and record a run

No decorators, no graph, no rewrite. Wrap the agent you already have with
`KitaruAgent` and run it:

```python
# support.py
import os
import uuid

from pydantic_ai import Agent
from kitaru.adapters.pydantic_ai import KitaruAgent

agent = Agent(
    "openai:gpt-5.4",
    name="support-agent",
    system_prompt="You resolve support tickets. Issue refunds when the customer was overcharged.",
)

@agent.tool_plain
def refund_payment(order_id: str) -> str:
    # Your real refund API. Returns a human-readable confirmation.
    return payments.refund(order_id)

support = KitaruAgent(agent, agent_id=uuid.UUID(os.environ["KITARU_AGENT_ID"]))

if __name__ == "__main__":
    result = support.run_sync(
        "Refund order #4821 — the card reader was double-charged."
    )
    print(result.output)
```

```bash
python support.py
```

That run left behind one session: the model requests, the
`refund_payment` call with its arguments and result, token usage, and
cost, recorded as the run progressed. This same script is also your replay
target — under a worker, the adapter feeds it the recorded inputs
automatically, so nothing about it changes.

## 3. Look at the recording

The Python client is async and reads its connection from the environment:

```python
# inspect.py
import asyncio
from kitaru.client import KitaruAPIClient
from kitaru.api_models.v1.session import SessionListParams

async def main() -> None:
    client = KitaruAPIClient.from_env()
    sessions = await client.sessions.list(SessionListParams(size=1))
    session = sessions.items[0]
    print(session.id, session.status, session.cost, session.tokens)

    nodes = await client.sessions.list_nodes(session.id, include_payloads=True)
    for node in nodes.items:
        print(f"{node.index:3} {node.node_type:12} {node.name}")

asyncio.run(main())
```

```text
  0 llm_call     support-agent
  1 tool_call    refund_payment
  2 llm_call     support-agent
```

The recording is the whole conversation between your agent and the world:
what the model saw, what the tool returned, what it cost.

<a id="write-the-evaluator"></a>

## 4. Write the evaluator

A replay is always scored, so define what "good" means before replaying.
Scaffold an [evaluator](../concepts/evaluators.md) — plain Python reading
the session:

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

## 5. Start a worker

Replays execute in **your** environment, not on the server. Open a second
terminal in the same directory and virtualenv:

```bash
kitaru worker start
```

Leave it running — it claims the replay you're about to create, re-runs
`support.py` as a subprocess, and runs your evaluator.

## 6. Replay it unchanged — the baseline

First, prove the recording is faithful. Re-run the session with nothing
changed and every tool call answered from the recording:

```python
# replay.py
import asyncio
from kitaru.client import KitaruAPIClient
from kitaru.api_models.v1.replay import ReplayCreateRequest
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig,
    HistoryConfig,
    ToolPolicy,
)

RECORDED_TOOLS = ToolPolicy(default=HistoryConfig(scope="baseline", on_miss="fail"))

async def main() -> None:
    client = KitaruAPIClient.from_env()
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

Your real code ran again — but `refund_payment` was answered from the
recording, so no card was touched, and `on_miss="fail"` guarantees
anything unrecorded stops the replay instead of reaching a live system.
**This is the discipline: if the unchanged replay doesn't hold up, stop —
nothing you fork from it can be trusted.** A faithful baseline is what
makes the next step mean something.

## 7. Fork it with one thing changed

Now ask the question you actually care about: *would the cheaper model
have handled this ticket?* Same replay, one override:

```python
from kitaru.api_models.v1.replay_config import ReplayOverride

    fork = await client.replays.create(
        ReplayCreateRequest(
            baseline_session_id=SESSION_ID,
            override=ReplayOverride(model={"openai:gpt-5.4": "openai:gpt-5-nano"}),
            evaluators=[EvaluatorConfig(evaluator="refund-check")],
            tool_policy=RECORDED_TOOLS,
        )
    )
```

The agent re-runs from the top on the cheaper model, against the same
recorded world. One run, one thing changed.

## 8. Read the diff

Each replay produced a new session (`origin: replay`), already scored.
Compare the fork against the baseline:

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

If `refund_issued` still passes and cost dropped, you just learned
something real about a model swap — from a production run, without touching
production.

## 9. Widen it into a regression suite

One replay tells you about one ticket. Freeze a week of real runs into a
[cohort](../concepts/cohorts.md), make the model swap an
[experiment](../concepts/experiments.md), and replay the whole population —
that's [Build a regression suite from production](../guides/regression-suite.md),
and it's the loop that keeps a fixed failure fixed.

## Where to go next

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Import your traces</strong></td><td>Your existing Langfuse history, as replayable sessions.</td><td><a href="import-your-traces.md">import-your-traces.md</a></td></tr><tr><td><strong>Replay a failure and fork it</strong></td><td>Overrides, tool policies, and reading a comparison.</td><td><a href="../guides/replay-and-overrides.md">../guides/replay-and-overrides.md</a></td></tr><tr><td><strong>Build a regression suite</strong></td><td>Cohorts, experiments, and the CI gate.</td><td><a href="../guides/regression-suite.md">../guides/regression-suite.md</a></td></tr><tr><td><strong>Write an evaluator</strong></td><td>From your domain expert's criteria to a versioned gate.</td><td><a href="../guides/write-an-evaluator.md">../guides/write-an-evaluator.md</a></td></tr></tbody></table>
