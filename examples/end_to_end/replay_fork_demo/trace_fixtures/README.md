# Trace fixture generation

This directory supports the one-time setup for the replay example. The public
walkthrough imports the selected trace through `kitaru import langfuse` before
using `demo.py` for candidate replay and inspection.

`generate.py` runs the frozen `baseline` PydanticAI support agent through the
seeded scenarios and records the resulting production-shaped traces in
Langfuse. That variant maps to the immutable
`v2.3-structured-escalation-imported` source label; the command rejects other
variants instead of stamping them with that label. It is useful when the example
needs a fresh set of traces or a checked-in Langfuse export.

```bash
export OPENAI_API_KEY=sk-...
export LANGFUSE_PUBLIC_KEY=pk-lf-...
export LANGFUSE_SECRET_KEY=sk-lf-...
export LANGFUSE_HOST=https://cloud.langfuse.com

cd examples/end_to_end/replay_fork_demo
for scenario in account_setting_change_request service_status_question; do
  uv run --with langfuse python -m trace_fixtures.generate \
    --scenario "$scenario" \
    --variant baseline \
    --generation-id kitaru-replay-example-structured-escalation-v1
done
```

Each command prints one `langfuse://trace/<id>` URI. Pass a URI directly to
`kitaru import langfuse` for a read-only preview. To generate ordered JSONL from
raw live observations, fetch each printed trace and select the `AGENT` observation
named
`support-agent` whose input contains `messages` and `tools`, and write the raw
account-setting observation followed by the raw service-status observation.
Keep the observation id, trace id, timestamps, input, output, source version,
and metadata unchanged. JSONL imports also need the Langfuse project ID.

`imported-support-cases.jsonl` is the current small walkthrough fixture. Its
source label, baseline variant, callable-tool schemas, and validated JSON-text
output contract are frozen together. The checked-in rows carry the Langfuse
generation identifier `kitaru-replay-example-json-text-v1` and
`fixture_contract_revision=structured-escalation-derived-v1`. The revision
marker identifies the structured escalation call and audit result as derived
production-shaped fixture data, not raw live observations. Keep the marker on
derived rows. To generate raw live observations, run the command above and
export the two raw `support-agent` observations in the documented order.

`support-traces.jsonl` is a separate larger exported corpus for generation
`kitaru-replay-example-20260717-final`. It contains 46 observations across six
traces, including the root span, PydanticAI agent span, model generations, and
tool calls. It is not the source fixture registered by the current walkthrough.
For an exported scenario corpus, export all observations for the generated
trace IDs rather than only the root observations.

These scenarios, local services, and generation commands are fixture
provenance. Users investigating production behavior do not run them.
