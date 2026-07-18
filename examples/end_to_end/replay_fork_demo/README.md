# Case-first replay with a PydanticAI support agent

This example starts with a reported production trace, imports it as a stored
Kitaru execution, and then submits native Kitaru executions to a registered
candidate agent as one durable replay experiment.

The distinction between the two execution sources matters:

- A Langfuse JSONL export can be imported, inspected, and scored as stored
  evidence.
- An imported trace does not contain Kitaru's executable checkpoint graph, so
  it cannot be replayed with framework-exact checkpoint reuse.
- The `replay` and `experiment` commands therefore accept only execution IDs
  from native Kitaru runs of this agent.

The registered agent pins a deterministic `completed-execution` protection.
Every replayed case must reach terminal completion and score `1.0` before the
suite can pass. The example deliberately does not guess at permission-policy
behavior from checkpoint IDs because the frozen evidence does not expose enough
tool-call detail to make that claim safely.

## Setup

From a fresh checkout:

```bash
uv sync --extra local --extra pydantic-ai --extra llm
cd examples/end_to_end/replay_fork_demo
uv run kitaru init
export OPENAI_API_KEY=sk-...
```

The production-shaped agent is a PydanticAI `Agent` with typed output and local
tools. `build_support_agent(...)` wraps it in a `KitaruAgent` with
`checkpoint_strategy="calls"`, so native runs record model and tool calls as
replayable checkpoints.

## 1. Import a reported Langfuse trace

The import API accepts a Langfuse observations JSONL export, not a
`langfuse://` URL. Start with a read-only dry run:

```bash
uv run python demo.py import-traces \
  trace_fixtures/support-traces.jsonl \
  --source-project-id <LANGFUSE_PROJECT_ID> \
  --trace-id 56cf81f1cb9e4b92994b17b81b041351
```

Omit `--trace-id` to plan every trace in the export. Review the selected trace
count, destination project, stack, artifact store, and per-trace outcomes.
Then repeat the same command with `--commit` to persist the full observation
inputs and outputs:

```bash
uv run python demo.py import-traces \
  trace_fixtures/support-traces.jsonl \
  --source-project-id <LANGFUSE_PROJECT_ID> \
  --trace-id 56cf81f1cb9e4b92994b17b81b041351 \
  --commit
```

The result contains an `execution_id` for each imported trace. Inspect one with:

```bash
uv run kitaru executions get <IMPORTED_EXECUTION_ID>
```

Imported executions preserve Langfuse provenance and recorded observations.
They can be passed to `client.executions.evaluate(...)` when a scorer can make
its decision from the frozen evidence Kitaru exposes. Do not pass an imported
execution ID to this example's replay commands.

## 2. Register the candidate

Select the candidate configuration and a human label:

```bash
export SUPPORT_AGENT_VARIANT=nano_trimmed_permissions
export SUPPORT_AGENT_VERSION=v2.3
uv run python demo.py register
```

The stable entrypoint is `evals.register:kagent`. Kitaru fingerprints its code,
configuration, worldview, and protection snapshot. Repeating registration with
the same identity and label reuses the existing AgentVersion. Changing or
removing the protection creates a different AgentVersion identity.

The replay commands also perform this idempotent registration in their own
process before submitting work. Running `register` separately is useful for
reviewing the resolved AgentVersion, but it is not hidden process state that
later commands depend on.

## 3. Replay one native case

Use an execution ID from a native Kitaru run of the support agent. Replay from
its first model-request checkpoint:

```bash
uv run python demo.py replay <NATIVE_EXECUTION_ID> \
  --at support_agent_model_request \
  --idempotency-key permissions-case-48211-v2
```

The command submits this request through the registered candidate:

```python
result = kagent.replay(
    [execution_id],
    at="support_agent_model_request",
    on_error="collect",
    uncovered_policy="fail",
    idempotency_key="permissions-case-48211-v2",
    repeats=3,
    wait=True,
    name=f"case-{execution_id}",
    suite_key=f"case-{execution_id}",
)
```

Kitaru validates the complete request before creating durable state. It then
records one experiment attempt, submits each repeat, writes experiment lineage
to every child, and verifies membership before finalizing the submission.

If the caller loses the response, retry the same logical request with the same
idempotency key. Kitaru recovers or returns the existing attempt instead of
creating a duplicate. Reusing the key with different inputs fails.

## 4. Replay several native cases as one experiment

Pass native execution IDs in the order that should be frozen:

```bash
uv run python demo.py experiment \
  <NATIVE_EXECUTION_ID_1> \
  <NATIVE_EXECUTION_ID_2> \
  --name support-agent-permissions-v2 \
  --at support_agent_model_request \
  --idempotency-key permissions-v2-attempt-1
```

Two targets with three repeats produce six intended child submissions. The
immutable specification retains target order, checkpoint coverage, repeat
count, registered candidate version, and replay inputs. Member execution bodies
remain on their execution records rather than being copied into the Agent
catalog.

Because the Agent has a pinned protection, this call waits for terminal child
evidence, scores every verified child, and freezes a separate `pass`, `fail`, or
`hold` verdict. Lifecycle `completed` and verdict `pass` are distinct facts.

## 5. Rerun the suite as a bounded regression gate

After registering the candidate code you want to test, rerun the frozen suite
with fewer repeats and explicit limits:

```bash
uv run python demo.py rerun support-agent-permissions-v2 \\
  --idempotency-key permissions-v2-attempt-2 \\
  --max-trials 3 \\
  --max-cost-usd 1.00 \\
  --max-incurred-tokens 100000 \\
  --max-duration-seconds 300
```

The command follows the same SDK path a pytest gate can use:

```python
from kitaru import RegressionLimits

result = kagent.replay(
    experiment="support-agent-permissions-v2",
    idempotency_key="permissions-v2-attempt-2",
    repeats=1,
    limits=RegressionLimits(
        max_trials=3,
        max_cost_usd=1.00,
        max_incurred_tokens=100_000,
        max_duration_seconds=300,
    ),
)
result.assert_pass()
```

`max_trials` is checked before Kitaru creates the new attempt. Cost, incurred
tokens, and elapsed time are checked between terminal trials. One model call
can therefore cross a cost or token ceiling before further submissions stop.
Missing required usage or a reached operational limit freezes a `hold` verdict,
not a false `pass`. Retrying the same idempotency key returns the same attempt
and cannot duplicate the suite spend.

If the assertion fails, its structured output includes the suite and attempt
IDs, objective and protection facts, completeness counts, frozen usage facts,
and the compare URL when available. The same fields appear under `regression`
in `result.to_json()`.

## 6. Read the experiment

The immediate result exposes the frozen specification, durable record,
submission rows, and member-run lookup:

```python
print(result.spec.experiment_id)
print(result.record.status)
print(result.verdict)
print(result.submission.summary.to_json())

page = result.runs.list(page=1, size=50)
```

Read the attempt later through the Agent catalog:

```python
from kitaru import KitaruClient

client = KitaruClient()
attempt = client.agents.experiments.get(result.spec.experiment_id)
member_page = attempt.runs.list(page=1, size=50)
```

Execution relationships project the stored experiment membership and replay
lineage:

```python
source = client.executions.get("<NATIVE_EXECUTION_ID>")

for attempt in source.experiments:
    print(attempt.experiment_id)

for replay in source.replays:
    print(replay.exec_id)

child = client.executions.get(result.submission.results[0].replay_exec_id)
print(child.original)
print(child.root)
```

Older executions without verified root metadata return `None` for `root`
instead of inferring ancestry from names or imported provenance.

## Fixture provenance

`trace_fixtures/support-traces.jsonl` is a checked-in Langfuse observations
export. The generator under `trace_fixtures/` can create a fresh export source
when maintaining the example, but it is not part of the user journey above.
See [`trace_fixtures/README.md`](trace_fixtures/README.md) for its credential and
export requirements.
