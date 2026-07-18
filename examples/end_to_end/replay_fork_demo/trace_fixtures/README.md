# Trace fixture generation

This directory supports the one-time setup for the replay example. The public
walkthrough starts in `../demo.py` with a trace import.

`generate.py` runs the PydanticAI support agent through the seeded scenarios
and records the resulting production-shaped traces in Langfuse. It is useful
when the example needs a fresh set of traces or a checked-in Langfuse export.

```bash
export OPENAI_API_KEY=sk-...
export LANGFUSE_PUBLIC_KEY=pk-lf-...
export LANGFUSE_SECRET_KEY=sk-lf-...
export LANGFUSE_HOST=https://cloud.langfuse.com

cd examples/end_to_end/replay_fork_demo
uv run --with langfuse python -m trace_fixtures.generate \
  --set smoke \
  --generation-id kitaru-replay-example-20260717-final
```

The command prints one `langfuse://trace/<id>` URI per scenario so maintainers
can find the generated traces in Langfuse. Kitaru does not import those URIs
directly. Export the selected traces from Langfuse as an observations JSONL file,
then pass that file and the Langfuse project ID to `demo.py import-traces`.

`support-traces.jsonl` is the checked-in export for that generation ID. It
contains 46 observations across six traces, including the root span, PydanticAI
agent span, model generations, and tool calls. When refreshing it, export all
observations for the generated trace IDs rather than exporting only root
observations.

These scenarios, local services, and generation commands are fixture
provenance. Users investigating production behavior do not run them.
