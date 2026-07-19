# Trace fixture generation

This directory supports the one-time setup for the replay example. The public
walkthrough starts in `../demo.py` with a trace import.

`generate.py` runs the frozen `baseline` PydanticAI support agent through the
seeded scenarios and records the resulting production-shaped traces in
Langfuse. That variant maps to the immutable `v2.2-json-text-imported` source
label; the command rejects other variants instead of stamping them with that
label. It is useful when the example needs a fresh set of traces or a checked-in
Langfuse export.

```bash
export OPENAI_API_KEY=sk-...
export LANGFUSE_PUBLIC_KEY=pk-lf-...
export LANGFUSE_SECRET_KEY=sk-lf-...
export LANGFUSE_HOST=https://cloud.langfuse.com

cd examples/end_to_end/replay_fork_demo
uv run --with langfuse python -m trace_fixtures.generate \
  --set smoke \
  --variant baseline \
  --generation-id kitaru-replay-example-json-text-v1
```

The command prints one `langfuse://trace/<id>` URI per scenario. Pass a URI
directly to `demo.py import-traces` for a read-only preview, or export several
traces as observations JSONL when you want one ordered batch. JSONL imports also
need the Langfuse project ID.

`imported-support-cases.jsonl` is the current small walkthrough fixture. Its
source label, baseline variant, callable-tool schemas, and validated JSON-text
output contract are frozen together.

`support-traces.jsonl` is the larger historical export for generation
`kitaru-replay-example-20260717-final`. It contains 46 observations across six
traces, including the root span, PydanticAI agent span, model generations, and
tool calls. It is not the source fixture registered by the current walkthrough.
When refreshing an exported scenario corpus, export all observations for the
generated trace IDs rather than exporting only root observations.

These scenarios, local services, and generation commands are fixture
provenance. Users investigating production behavior do not run them.
