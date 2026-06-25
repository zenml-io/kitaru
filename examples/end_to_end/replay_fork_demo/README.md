# LangGraph replay/fork regression demo

This demo is the LangGraph/Langfuse version of the replay story. It starts from a recorded LangGraph trace, rebuilds that trace as a Kitaru flow, replays from a chosen graph node, and then runs an edited candidate from the same point.

The flow is:

```text
original recorded trace
  -> unchanged replay control
  -> edited candidate fork
  -> drift report
```

The unchanged replay is the control. If it cannot reproduce the recorded decision, the candidate comparison is not useful yet. If it does reproduce the recorded decision, the edited fork answers the release question: did this model, prompt, or config change alter the decision for this case?

Use this demo when you want to see how Kitaru can work with a LangGraph agent and Langfuse trace data. Use `../replay_overrides_demo/` when you want the simpler SDK/CLI override walkthrough.

## What Kitaru does here

Kitaru imports Langfuse observation rows from a LangGraph support copilot, rebuilds a Kitaru flow with one checkpoint per LangGraph node, and replays from a selected node.

During replay:

- checkpoints before the cut reuse their recorded node outputs,
- checkpoints at and after the cut run live,
- the edited fork runs the same live tail with candidate settings,
- the HTML report compares the original trace, unchanged replay, and edited fork.

This is not LangGraph-native time travel. It is a Kitaru replay flow reconstructed from a trace that contains enough node-level state to restart safely from the selected graph node.

## What this demo uses

- A bundled typed LangGraph support copilot in `reference_agent/`.
- Langfuse observation rows from `reference_agent/fixtures/langfuse_rich_observations.jsonl`.
- `KitaruAdapter`, which wraps the compiled LangGraph graph.
- A rehydration helper in `utils.py`, because this reference agent uses typed state objects rather than plain JSON dictionaries.

A JSON-native LangGraph agent would need less demo-specific code. The adapter can replay plain JSON state directly.

## Run from the bundled fixture

The fixture path reads Langfuse observation rows from disk, so it does not fetch a trace from Langfuse. The `replay`, `fork`, and `run-all` commands still re-execute the live LangGraph tail from the cut, so they need the model credentials used by the reference agent.

```bash
cd examples/end_to_end/replay_fork_demo

export TRACE_ID=trace-replay-fork-rich-baseline
export TRACE_FILE=reference_agent/fixtures/langfuse_rich_observations.jsonl

uv run python demo.py import-trace "$TRACE_FILE" --trace-id "$TRACE_ID"
uv run python demo.py replay "$TRACE_FILE" --trace-id "$TRACE_ID"
uv run python demo.py fork "$TRACE_FILE" --trace-id "$TRACE_ID"
uv run python demo.py run-all "$TRACE_FILE" --trace-id "$TRACE_ID"
```

What the commands mean:

| command | what it checks |
|---|---|
| `import-trace` | Reads the bundled Langfuse rows and checks that the trace has enough node outputs for replay. This command does not call OpenAI or Langfuse. |
| `replay` | Compares the original recorded trace with an unchanged replay. |
| `fork` | Compares original trace -> unchanged replay -> edited candidate fork, then writes `replay_vs_fork.html`. |
| `run-all` | Runs import or trace generation, replay, candidate fork, and HTML report creation. |

## Generated HTML report

`replay_vs_fork.html` is a three-way report. It shows:

1. the original recorded trace,
2. the unchanged replay control,
3. the edited candidate fork.

Read the first comparison before trusting the second one. If the unchanged replay differs from the original trace, fix reproduction first. If it matches, the fork comparison shows what the candidate changed.

## Creating a fresh trace

To create a new live trace, configure OpenAI and Langfuse credentials for the reference agent:

```bash
cd examples/end_to_end/replay_fork_demo
set -a && . ./.env && set +a
uv run python demo.py create-trace
```

Then import and replay the printed trace id:

```bash
uv run python demo.py import-trace langfuse:<TRACE_ID>
uv run python demo.py run-all langfuse:<TRACE_ID>
```

## Relationship to `reference_agent/README.md`

This README explains the replay/fork workflow. `reference_agent/README.md` explains the bundled LangGraph support copilot and fixture data.
