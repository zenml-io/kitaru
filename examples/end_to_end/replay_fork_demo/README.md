# LangGraph replay/fork regression demo

This demo shows how Kitaru can take a recorded **LangGraph** agent trace, replay it from a chosen graph node, then run an edited candidate from the same point and compare what changed.

The concrete story is:

```text
original recorded trace
  → unchanged replay / reproduction control
    → edited candidate fork
      → drift report
```

The unchanged replay is the control run. If it cannot reproduce the recorded decision, the candidate comparison is not trustworthy yet. Once the control run matches, the edited fork answers the useful release question: “Did this model/prompt/config change alter the decision for this case?”

## What Kitaru does here

Kitaru imports Langfuse observation rows from a LangGraph support copilot, rebuilds a Kitaru flow with one checkpoint per LangGraph node, and replays from a selected node:

- checkpoints before the cut reuse their recorded node outputs,
- checkpoints at and after the cut run live,
- the edited fork runs the same live tail with candidate settings,
- the HTML report compares original trace → unchanged replay → edited fork.

This is not LangGraph-native time travel. It is a Kitaru replay flow reconstructed from a trace that contains enough node-level state to restart safely from the selected graph node.

## What this demo uses

- A bundled typed LangGraph support copilot in `reference_agent/`.
- Langfuse observation rows from `reference_agent/fixtures/langfuse_rich_observations.jsonl`.
- `KitaruAdapter`, which wraps the compiled LangGraph graph.
- A rehydration helper in `utils.py`, because this reference agent uses typed state objects rather than plain JSON dictionaries.

A JSON-native LangGraph agent would need less demo-specific code: the adapter can replay plain JSON state directly.

## Run from the bundled fixture

The fixture path reads Langfuse observation rows from disk, so it does not need to fetch a trace from Langfuse. The `replay`, `fork`, and `run-all` commands still re-execute the live LangGraph tail from the cut, so they need the model credentials used by the reference agent.

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
| `import-trace` | Reads the bundled Langfuse rows and verifies that the trace has enough node outputs for replay. This command does not call OpenAI or Langfuse. |
| `replay` | Compares original recorded trace → unchanged replay. |
| `fork` | Compares original recorded trace → unchanged replay → edited candidate fork, then writes `replay_vs_fork.html`. |
| `run-all` | Runs the full path from import/generate through replay, candidate fork, and HTML report. |

## Generated HTML report

`replay_vs_fork.html` is a three-way report. It shows:

1. the original recorded trace,
2. the unchanged replay / reproduction control,
3. the edited candidate fork.

The first comparison tells you whether reproduction worked. If the unchanged replay does not match the original trace, the fork comparison is not trustworthy yet. The second comparison tells you whether the model/prompt edit changed the decision after reproduction succeeded.

## Creating a fresh trace

To create a new live trace, you need OpenAI and Langfuse credentials configured for the reference agent:

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

- This root README documents replaying and forking recorded traces with Kitaru.
- `reference_agent/README.md` documents the bundled LangGraph support copilot used to create the traces.

Start here when you want to understand replay/fork behavior. Go to the nested README when you want to inspect the source agent and fixture data.
