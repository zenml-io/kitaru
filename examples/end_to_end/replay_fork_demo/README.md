# LangGraph replay/fork demo

This demo shows how to replay and fork a recorded **LangGraph** agent run with Kitaru.

The concrete story is:

```text
original recorded trace
  → unchanged replay / reproduction
    → edited fork
```

Kitaru imports a recorded Langfuse trace, rebuilds a Kitaru flow with one checkpoint per LangGraph node, runs an unchanged replay from a chosen node, then runs an edited fork from the same point.

## What this demo uses

- A bundled typed reference agent in `reference_agent/`.
- Langfuse observation rows from `reference_agent/fixtures/langfuse_rich_observations.jsonl`.
- `KitaruAdapter`, which wraps the compiled LangGraph graph.
- A rehydration helper in `utils.py`, because this reference agent uses typed state objects rather than plain JSON dictionaries.

A JSON-native LangGraph agent would need less demo-specific code: the adapter can replay plain JSON state directly.

## Run from the offline fixture

The fixture path works without Langfuse or OpenAI calls, assuming the fixture file is already present.

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
| `import-trace` | Reads the Langfuse rows and verifies that the trace has enough node outputs for replay. |
| `replay` | Compares original recorded trace → unchanged replay. |
| `fork` | Compares original recorded trace → unchanged replay → edited fork, then writes `replay_vs_fork.html`. |
| `run-all` | Runs the full path from import/generate through replay, fork, and HTML report. |

## Generated HTML report

`replay_vs_fork.html` is a three-way report. It shows:

1. the original recorded trace,
2. the unchanged replay / reproduction,
3. the edited fork.

The first comparison tells you whether reproduction worked. If the unchanged replay does not match the original trace, the fork comparison is not trustworthy yet. The second comparison tells you whether the model/prompt edit changed the decision after reproduction succeeded.

## Creating a fresh trace

To create a new live trace, you need OpenAI and Langfuse credentials configured for the reference agent:

```bash
cd examples/end_to_end/replay_fork_demo
set -a && . ./.env && set +a
uv run python demo.py create-trace
```

Then import the printed trace id:

```bash
uv run python demo.py import-trace langfuse:<TRACE_ID>
uv run python demo.py run-all langfuse:<TRACE_ID>
```

## Relationship to `reference_agent/README.md`

- This root README documents replaying and forking recorded traces with Kitaru.
- `reference_agent/README.md` documents the reference agent itself and how the trace fixture is generated.

Start here when you want to understand replay/fork behavior. Go to the nested README when you want to inspect or regenerate the source traces.
