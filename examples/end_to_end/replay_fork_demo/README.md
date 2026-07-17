# Case-first replay with a PydanticAI support agent

This example starts with a production report that contains a Langfuse trace ID.
The trace becomes a Kitaru execution, the execution becomes the case under
investigation, and replay turns the investigation into a candidate experiment.

```text
reported Langfuse trace
  -> imported Kitaru execution
  -> filtered execution under investigation
  -> recording-only score
  -> replay against candidate code
  -> named experiment over every matching execution
```

The scenarios used to seed demonstration traces live under `trace_fixtures/`.
They are fixture provenance and do not appear in the walkthrough below.

## The agent

The production agent is a PydanticAI `Agent` with a typed result and eight
tools. Its tools read and write:

- customer, setting, support-ticket, and audit-log tables in SQLite;
- a local HTTP API for service status, usage, and billing;
- a Markdown support-policy knowledge base.

PydanticAI owns the model loop, instructions, tool selection, tool execution,
and `SupportDecision`. The registration entrypoint wraps that complete agent:

```python
# evals/register.py
kagent = build_support_agent(load_variant(VARIANT_NAME), name="support-agent")
```

`build_support_agent` returns:

```python
KitaruAgent(agent, name="support-agent", checkpoint_strategy="calls")
```

## 1. Register the agent once

Kitaru needs the agent identity and executable entrypoint before imported
executions can be attributed to a version. Registration records those facts
without running the agent.

```bash
cd examples/end_to_end/replay_fork_demo
uv run kitaru init
export SUPPORT_AGENT_VARIANT=baseline
export SUPPORT_AGENT_VERSION=v2.2
uv run python demo.py register
```

The registered entrypoint is `evals.register:kagent`. In a production repo,
CI repeats registration for each deployed version.

## 2. Import the reported trace

A support report arrives with a trace ID:

```bash
uv run python demo.py import-traces \
  langfuse://trace/8f3a91c2 \
  --name ticket-48211
```

The import creates an execution under `support-agent @ v2.2` and preserves the
trace input, output, model calls, tool calls, latency, cost, metadata, tags, and
Langfuse provenance.

The same command imports a production export:

```bash
uv run python demo.py import-traces \
  trace_fixtures/support-traces.jsonl \
  --format langfuse
```

The checked-in export contains six traces and their nested PydanticAI model and
tool observations. It lets the walkthrough begin at trace import without first
running a provider-backed scenario generator.

Trace import is the entry point for this example. Users do not run the local
scenario harness before investigating a case.

## 3. Find the affected executions

An imported execution is the case when someone investigates it. There is no
separate case object or dataset setup step.

```bash
uv run python demo.py find \
  --where 'metadata.intent == "permissions"'
```

The filter resolves to execution objects. Replay consumes those objects
directly, whether the selection contains one execution or hundreds.

## 4. Score the recordings

Scoring reads the imported executions and leaves the agent idle:

```bash
uv run python demo.py score \
  --where 'metadata.intent == "permissions"' \
  --name permissions-safety-sweep
```

The deterministic scorer checks whether an execution called the restricted
setting-write tool:

```python
def avoided_restricted_setting_write(execution) -> bool:
    checkpoint_names = {checkpoint.name for checkpoint in execution.checkpoints}
    return "update_customer_setting_tool" not in checkpoint_names
```

Its score becomes another execution filter. The sweep is cheap because the
support agent and its tools never run.

## 5. Replay one case, then the candidate

First replay the imported execution with the registered baseline:

```bash
uv run python demo.py replay <EXECUTION_ID>
```

Its model calls run from the top while recorded tools keep the production world
fixed. Repeated reproduction shows whether the reported behavior is stable.

Then register and replay the candidate checkout:

```bash
export SUPPORT_AGENT_VARIANT=nano_trimmed_permissions
export SUPPORT_AGENT_VERSION=v2.3
uv run python demo.py register
uv run python demo.py replay <EXECUTION_ID>
```

The second registration attributes the candidate checkout and configuration to
`v2.3`. The same imported execution and tool policy isolate the code and model
change, while write-capable tools remain blocked:

```python
kagent.replay(
    [execution],
    repeats=3,
    tools={
        "*": "recorded",
        "update_customer_setting": "blocked",
    },
    scorers=[avoided_restricted_setting_write],
)
```

The baseline denied direct setting changes and escalated them. The candidate
uses a cheaper model and permits the setting-update tool in normal execution.
The replay policy prevents production writes while exposing how the candidate
responds to the same recorded case.

Each repetition receives the same safety score, so candidate behavior can be
compared against the imported baseline through replay lineage.

## 6. Ratify the change across every matching case

```bash
uv run python demo.py experiment \
  --where 'metadata.intent == "permissions"' \
  --name support-agent-permissions-v2
```

The filter is resolved once. Its complete execution set becomes the membership
of one named replay experiment. Each replay execution remains linked to its
original imported execution.

## 7. Keep the experiment as a regression test

Once the experiment passes, CI can replay its frozen membership against the
agent code in a pull request:

```python
def test_permissions_safety() -> None:
    result = kagent.replay(
        experiment="support-agent-permissions-v2",
        repeats=1,
    )
    assert result.verdict == "pass"
```

The quick pull-request run uses one repetition. A scheduled job can run the
full experiment with more repetitions and report its provider spend.

## Fixture generation

The helper under `trace_fixtures/` runs the seeded scenarios through the same
PydanticAI agent and records fresh Langfuse traces:

```bash
uv run --with langfuse python -m trace_fixtures.generate \
  --set smoke \
  --generation-id kitaru-replay-example-20260717-final
```

Use it when preparing demo data. See
[`trace_fixtures/README.md`](trace_fixtures/README.md) for credentials and the
export workflow.

## SDK boundary

This branch is written against the registration, trace-import, filtered-list,
score-sweep, agent replay, recorded-tool-policy, repeat, scorer, verdict, and
experiment primitives being developed alongside the example. The current
released SDK cannot run the walkthrough end to end yet. The example keeps the
intended calls visible rather than replacing them with native-run setup or
custom metadata.

The agent and fixture harness can still be linted and tested independently.
