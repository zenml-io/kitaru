---
description: Wrap any PydanticAI agent once with KitaruAgent. Every run records as a session, and the same wrapper executes replays.
icon: cube
---

# PydanticAI Adapter

The PydanticAI adapter records any [PydanticAI](https://ai.pydantic.dev) agent without changing its code. Wrap the agent once with `KitaruAgent` and every run lands as a [session](../concepts/agents-and-sessions.md) (model requests, tool calls, token usage, cost), and the same wrapper executes [replays](../concepts/replay.md) when a [worker](../concepts/workers.md) re-runs your script.

```python
import os
import uuid

from pydantic_ai import Agent
from kitaru_pydantic_ai import KitaruAgent

agent = Agent(
    "openai:gpt-5.4", name="support-agent", system_prompt="You resolve support tickets."
)

support = KitaruAgent(agent, agent_id=uuid.UUID(os.environ["KITARU_AGENT_ID"]))

result = support.run_sync("Refund order #4821.")
```

`KitaruAgent` is a transparent `WrapperAgent`: `run`, `run_sync`, `iter`, tools, output types, and capabilities all behave exactly as on the wrapped agent. The adapter ships as its own distribution. Install it alongside Kitaru in the agent's environment:

```bash
uv add kitaru-pydantic-ai
```

## Constructor

```python
KitaruAgent(
    agent,  # the PydanticAI agent to wrap
    agent_id=None,  # the registered Kitaru agent's UUID
    agent_version_id=None,  # optional: pin sessions to a version
    session_name=None,  # falls back to KITARU_SESSION_NAME
    batch_size=20,  # nodes per ingest batch
    use_task_inputs_as_prompt=True,  # project task inputs into prompt and history
)
```

Register the agent first (`kitaru agent register`) and hand its id to the wrapper, via `KITARU_AGENT_ID` in your own environment, as the examples do. When the script runs under a worker task (a replay), the id is optional: the adapter infers the agent from the task itself.

The connection is the client's, not the adapter's: server and credential resolve the same way as for `KitaruAPIClient`: `KITARU_API_URL` (or the stored server URL), then the task token a worker injects (`KITARU_API_TOKEN`), then `KITARU_API_KEY`, then stored `kitaru login` credentials. That's what makes the same script work on your laptop, in production, and under a worker without edits.

## What gets recorded

The adapter opens a session when a run starts and streams nodes as the run progresses, in batches of `batch_size`:

- one `llm_call` node per model request: requested and resolved model, the messages in and out, token usage, and cost;
- one `tool_call` node per tool invocation: name, arguments, result, and the cache key that lets replay answer the same call later;
- session rollups (cost, tokens, call counts) maintained server-side as nodes arrive.

If the process dies mid-run, the session is left `in_progress` with everything recorded so far; a partial recording of a crash is exactly the evidence you want.

## Replay mode

You never instantiate anything special for replay. When a worker runs your script as a replay's agent task, the environment tells the adapter what to do:

- `KITARU_TASK_ID` links the new session to the task (and through it, the replay and experiment).
- `KITARU_TASK_INPUTS` (or the task spec) carries the baseline's recorded inputs. By default, the adapter substitutes them for your script's prompt and projects any recorded message history into the conversation, which is why a hardcoded prompt in `__main__` is fine.
- `KITARU_REPLAY_ID` makes the adapter fetch the replay's override and tool policy: model swaps and `model_params` apply at the model-request boundary, and tool calls are answered per policy: `history` lookups against the recording, `static` cases, or live `passthrough`.

A `history` miss with `on_miss="fail"` raises `ToolPolicyMissError` inside the run, failing the task, which is the guarantee that nothing unrecorded slips through to a live system. A matched recorded failure raises `ToolPolicyError` with the stored error text and aborts the run unless application code catches it. Kitaru does not recreate the original exception class or a PydanticAI retry signal such as `ModelRetry`. Both error types are importable from the adapter package:

```python
from kitaru_pydantic_ai import ToolPolicyError, ToolPolicyMissError
```

### Keep a simulator's prompt

If task inputs identify a benchmark episode while your simulator builds the actual conversation, set `use_task_inputs_as_prompt=False`:

```python
support = KitaruAgent(agent, use_task_inputs_as_prompt=False)
result = await support.run(simulator_prompt, message_history=simulator_history)
```

The adapter keeps the prompt and message history you pass to the run. Task inputs still become the session inputs and root node inputs, so a benchmark task ID remains recorded without becoming the model's prompt. An explicit `replay.override.prompt` still replaces the caller's prompt. Model overrides and tool policies continue to apply.

## Attach a benchmark grade after the run

`KitaruAgent` completes the session before `run` or `run_sync` returns. Updating that session's outputs or metadata through `sessions.update` then returns HTTP 409 by design. If an external simulator produces a grade after the run, attach it as a manual evaluation instead.

Given the recorded `session_id`, numeric `grade`, and boolean `passed` from your benchmark harness, run this in your async function:

```python
from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import SessionEvaluationsRequest
from kitaru.client import KitaruAPIClient

async with KitaruAPIClient() as client:
    await client.sessions.create_evaluations(
        session_id,
        SessionEvaluationsRequest(
            evaluations=[
                EvaluationResult(name="benchmark_grade", score=grade, passed=passed)
            ]
        ),
    )
```

An agent task can attach this evaluation to its own completed session before the task process exits. Each evaluation name must be unique within the session. Manual evaluations have no `evaluator_version_id` or `task_id` and do not contribute to experiment aggregates.

For grades that contribute to experiment aggregates, compute the grade inside a PydanticAI `output_validator` and include it in the final output. The adapter records that output before completing the session. A [registered evaluator](../guides/write-an-evaluator.md) can then read the recorded grade and emit the metric linked to its evaluator version. An existing validator that waits for the episode to finish and returns graded JSON can keep doing so.

## Notes and limits

- **Multi-turn conversations:** by default, the recorded inputs preserve the conversation shape (prompt plus message history), and replay projects them back the same way. With `use_task_inputs_as_prompt=False`, your application supplies the conversation instead.
- **The `llm` tool policy** is not yet supported by this adapter; a replay that reaches one fails with `ToolPolicyError`. See [Tool policies](../guides/tool-policies.md).
- **Recording overhead** is one async client and batched node uploads per run, off the hot path of model calls. If the Kitaru server is unreachable your run fails fast at session creation rather than running unrecorded; treat server availability accordingly in production.
- **Alongside other tracing:** the adapter composes with PydanticAI's OpenTelemetry instrumentation, so recording to Kitaru and tracing to Langfuse from the same run works.
- **Import-first alternative:** the [PydanticAI returns agent example](https://github.com/zenml-io/kitaru/tree/main/examples/python/pydantic_ai_ticket_resolver) imports a checked-in Langfuse export of PydanticAI runs into Kitaru as replayable sessions.
