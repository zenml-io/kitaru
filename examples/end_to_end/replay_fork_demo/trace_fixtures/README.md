# Trace fixture maintenance

The public walkthrough starts from `imported-support-cases.jsonl`. Generating
agent traffic and exporting Langfuse observations are one-time fixture
maintenance tasks, not part of the case-first journey.

`generate.py` runs the frozen `baseline` PydanticAI support agent through the
seeded scenarios. It records production-shaped traces in Langfuse with source
label `v2.3-structured-escalation-imported`. The account-setting scenario uses
the example's SQLite customer database, local knowledge base, and escalation
write. The status scenario calls the local HTTP service.

```bash
export OPENAI_API_KEY=sk-...
export LANGFUSE_PUBLIC_KEY=pk-lf-...
export LANGFUSE_SECRET_KEY=sk-lf-...
export LANGFUSE_HOST=https://cloud.langfuse.com

cd examples/end_to_end/replay_fork_demo
for scenario in \
  account_setting_change_request \
  service_status_question \
  refund_policy_explanation \
  usage_spike_complaint \
  outage_with_ticket_request; do
  uv run --with langfuse python -m trace_fixtures.generate \
    --scenario "$scenario" \
    --variant baseline \
    --generation-id kitaru-replay-example-20260720-regenerated
done
```

Each command prints a `langfuse://trace/<id>` URI. Export the selected traces
in walkthrough order:

```bash
uv run --with langfuse python -m trace_fixtures.export \
  --trace-id 390972667ef147cbbbd6db2b30e8ad1b \
  --trace-id 00cbb102c7844e00aeb0149e8deea83b \
  --trace-id 40860e9cee9d4d71b6a6f82208af1a75 \
  --trace-id 50b5ad259d5c4c61bdfe2f593c8f8495 \
  --trace-id 88b77b16089946ee966d2ae55e0921d7
```

The exporter writes two artifacts:

- `raw-imported-support-cases.jsonl` contains all 35 observations returned by
  Langfuse. It preserves the root spans, PydanticAI agent spans, model
  generations, tool calls, timestamps, costs, and metadata. Public-key metadata
  is removed.
- `imported-support-cases.jsonl` contains five replay-ready rows. The
  exporter selects the final model generation, converts PydanticAI message
  parts to Kitaru's imported-message contract, and retains the live trace ID,
  tool schemas, tool arguments, tool results, final output, and source stamps.

The replay rows carry
`fixture_generation_id=kitaru-replay-example-20260720-regenerated` and
`fixture_contract_revision=pydantic-ai-final-generation-v1`. The revision marks
the deterministic conversion from the raw export. Kitaru can import the raw
observation stream, but the current importer reports ambiguous message order
for replay. The derived fixture is the checked-in entry point while that SDK
surface evolves.

`support-traces.jsonl` is an older six-trace corpus for generation
`kitaru-replay-example-20260717-final`. It remains as broader importer test data
and is not used by the walkthrough.
