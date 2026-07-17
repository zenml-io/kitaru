# Case-first replay with a PydanticAI support agent

This example starts with a production report, inspects the reported trace, then
uses native Kitaru executions to prove a candidate change as one durable replay
experiment.

Stage 1 keeps an important boundary explicit:

- imported Langfuse traces are inspectable historical records;
- replay experiments accept native Kitaru executions whose checkpoint graph can
  run again;
- scoring and replaying imported traces are later-stage work.

## The agent

The production agent is a PydanticAI `Agent` with a typed result and eight
tools. PydanticAI runs the model loop and tool calls. Kitaru records each model
and tool call as a checkpoint through:

```python
# evals/register.py
kagent = build_support_agent(load_variant(VARIANT_NAME), name="support-agent")
```

`build_support_agent` returns a `KitaruAgent` configured with
`checkpoint_strategy="calls"`.

## 1. Register the candidate

Registering records the exact candidate code, configuration, and executable
without running the agent:

```bash
cd examples/end_to_end/replay_fork_demo
uv run kitaru init
export SUPPORT_AGENT_VARIANT=nano_trimmed_permissions
export SUPPORT_AGENT_VERSION=v2.3
uv run python demo.py register
```

The registered entrypoint is `evals.register:kagent`. The replay experiment is
submitted through this registered candidate version even when its source
execution came from an older registered version.

## 2. Inspect a reported Langfuse trace

Import one reported trace:

```bash
uv run python demo.py import-traces \
  langfuse://trace/8f3a91c2 \
  --name ticket-48211
```

Or import the checked-in production export:

```bash
uv run python demo.py import-traces \
  trace_fixtures/support-traces.jsonl \
  --format langfuse
```

The import preserves the recorded input, output, model calls, tool calls,
latency, cost, metadata, tags, and Langfuse provenance. Use `demo.py find` to
inspect matching imported records. Stage 1 does not replay those imported
records because they do not contain an executable native checkpoint graph.

## 3. Replay one native case

Take the execution ID from a native Kitaru run of this agent, then replay from
its first model-request checkpoint:

```bash
uv run python demo.py replay <NATIVE_EXECUTION_ID> \
  --at support_agent_model_request \
  --idempotency-key permissions-case-48211-v2
```

The command calls the registered agent API with every policy choice explicit:

```python
result = kagent.replay(
    [execution_id],
    at="support_agent_model_request",
    on_error="collect",
    uncovered_policy="fail",
    idempotency_key="permissions-case-48211-v2",
    repeats=3,
    wait=False,
    name=f"case-{execution_id}",
)
```

Kitaru validates the complete request before it creates durable state. It then
creates one experiment attempt, submits each repeat through the registered
candidate, writes the experiment tag and metadata to every child, verifies both
membership signals, and records a terminal submission status.

Retry the same logical request with the same idempotency key if the caller loses
the response. Kitaru returns the existing attempt instead of submitting a
duplicate. Reusing the key for different inputs fails.

## 4. Replay several native cases as one experiment

Pass explicit native execution IDs in the order that should be frozen:

```bash
uv run python demo.py experiment \
  <NATIVE_EXECUTION_ID_1> \
  <NATIVE_EXECUTION_ID_2> \
  --name support-agent-permissions-v2 \
  --at support_agent_model_request \
  --idempotency-key permissions-v2-attempt-1
```

One call creates one experiment. Three targets with three repeats produce nine
intended child submissions. The immutable specification retains target order,
checkpoint coverage, repeat count, registered candidate version, and replay
inputs. Member execution bodies remain on the execution records rather than
being copied into the Agent catalog.

`completed` means all intended children were submitted and both membership
signals were verified. It does not mean every child has finished running.
Individual executions remain authoritative for live and terminal run status.

## 5. Read the experiment

The replay result exposes the frozen specification, cached record, existing
replay rows, and a lazy member-run lookup:

```python
print(result.spec.experiment_id)
print(result.record.status)
print(result.submission.summary.to_json())

page = result.runs.list(page=1, size=50)
```

Read the same attempt later through the Agent catalog:

```python
from kitaru import KitaruClient

client = KitaruClient()

attempts = client.agents.experiments.list()
attempt = client.agents.experiments.get(result.spec.experiment_id)
member_page = attempt.runs.list(page=1, size=50)
```

Attempts are newest first. Exact experiment IDs always resolve. Suite keys and
human names resolve only when unambiguous.

Execution relationships are projections over the catalog, tags, metadata, and
recorded replay lineage:

```python
source = client.executions.get("<NATIVE_EXECUTION_ID>")

for attempt in source.experiments:
    print(attempt.experiment_id)

for replay in source.replays:
    print(replay.exec_id)

child = client.executions.get(result.submission.results[0].replay_exec_id)
original = child.original
root = child.root
```

Older executions without verified root metadata return `None` for `root`
rather than inferring ancestry from a name or imported provenance.

## Fixture generation

The helper under `trace_fixtures/` runs seeded scenarios through the same
PydanticAI agent and records fresh Langfuse traces:

```bash
uv run --with langfuse python -m trace_fixtures.generate \
  --set smoke \
  --generation-id kitaru-replay-example-20260717-final
```

Use it when preparing demo data. See
[`trace_fixtures/README.md`](trace_fixtures/README.md) for credentials and the
export workflow.
