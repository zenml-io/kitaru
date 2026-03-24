# Basic flow examples

This group collects the shortest, most "start here" Kitaru examples. Each one
introduces a single concept — start at the top and work down.

## Getting started

```bash
cd examples/basic_flow
uv pip install 'kitaru[local]'   # Install Kitaru with local runtime
kitaru init                  # Initialize a Kitaru project in this directory
python <module_name>.py      # Run any example
```

These examples use your current Kitaru connection context. If you want them to
run against a deployed Kitaru server, connect first with `kitaru login
<server>` and verify with `kitaru status`.

For the full catalog, see [../README.md](../README.md).

## Examples

| Example | Run | What it demonstrates | Test |
|---|---|---|---|
| [first_working_flow.py](first_working_flow.py) | `uv run examples/basic_flow/first_working_flow.py` | Smallest end-to-end `@flow` + `@checkpoint` example | [../../tests/test_phase5_example.py](../../tests/test_phase5_example.py) |
| [flow_with_logging.py](flow_with_logging.py) | `uv run examples/basic_flow/flow_with_logging.py` | `kitaru.log()` metadata at flow and checkpoint scope | [../../tests/test_phase7_logging_example.py](../../tests/test_phase7_logging_example.py) |
| [flow_with_artifacts.py](flow_with_artifacts.py) | `uv run examples/basic_flow/flow_with_artifacts.py` | Named artifact save/load across executions | [../../tests/test_phase8_artifacts_example.py](../../tests/test_phase8_artifacts_example.py) |
| [flow_with_configuration.py](flow_with_configuration.py) | `uv run examples/basic_flow/flow_with_configuration.py` | Runtime config defaults, overrides, and frozen execution specs | [../../tests/test_phase10_configuration_example.py](../../tests/test_phase10_configuration_example.py) |
| [flow_with_checkpoint_runtime.py](flow_with_checkpoint_runtime.py) | `uv run examples/basic_flow/flow_with_checkpoint_runtime.py` | Checkpoint runtime selection (`"isolated"`) with `.submit()` fan-out | — |

### `first_working_flow.py` — Durable execution in two decorators

The absolute minimum: mark a function with `@flow` and its steps with
`@checkpoint`. Every checkpoint output is persisted automatically — if the
flow crashes after `gather_sources`, a replay skips it and resumes from
`summarize` without re-running work.

```bash
python first_working_flow.py
```

### `flow_with_logging.py` — Structured metadata on executions and checkpoints

Shows `kitaru.log()` at two scopes: **flow-level** metadata (topic, stage)
attaches to the execution as a whole, while **checkpoint-level** metadata
(cost, tokens, latency, quality scores) attaches to individual steps. This
is how you track LLM usage, quality metrics, or any structured data you want
to query later.

```bash
python flow_with_logging.py
```

### `flow_with_artifacts.py` — Persist and reload data across executions

Demonstrates `kitaru.save()` and `kitaru.load()` for cross-execution data
sharing. A first flow produces research notes and saves extra context as a
named artifact. A second flow loads both the checkpoint output and the named
artifact from the first execution — no file paths, no external storage
setup.

```bash
python flow_with_artifacts.py
```

### `flow_with_configuration.py` — Runtime configuration and precedence

Shows how `kitaru.configure()` sets process-level defaults, `@flow(...)`
sets flow-level defaults, and `.run(..., retries=3)` provides invocation-time
overrides. The resolved configuration is frozen into a durable execution
spec — so you always know exactly what settings a past execution ran with.

```bash
python flow_with_configuration.py
```
