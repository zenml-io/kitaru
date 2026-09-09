# Kitaru Mastra importer

Import selected Mastra trace exports as Kitaru sessions. This is a development package, not a server-default importer. The optional history-only replay mode targets the context contract in [#1050](https://github.com/zenml-io/kitaru/issues/1050), integrated in this branch and validated against the adapter built from this checkout. Release this mode only with that context-capable adapter. Default imports preserve raw invocation inputs and report `metadata.mastra.replay.eligible: false`.

## Accepted export

Save the JSON response from Mastra's **full** `GET /observability/traces/{traceId}` API, or serialize the corresponding storage `getTrace({traceId})` result. The verified contract is Mastra core **1.51.0**:

```json
{"traceId": "...", "spans": []}
```

The example shows only the envelope; an actual export must contain its root and descendant spans. The importer also accepts a JSON array of these responses so several selected invocations can be imported together. That array is an importer convenience, not a Mastra server response format. No network calls or continuous ingestion occur during parsing.

Use full trace responses. `getTraceLight`, trace-list summaries, OpenTelemetry payloads, and raw observability exporter events use different or incomplete formats. They must not be substituted for `getTrace`. An export with no root, missing parents, duplicate span IDs, invalid timestamps, or malformed mapped values produces an isolated `ImportFailure`; valid neighboring traces continue.

The [versioned synthetic fixture](../../tests/importers/fixtures/mastra/1.51.0/README.md) was generated through real `Agent.generate` calls and `MastraStorageExporter`, using a local deterministic model and no model service. It contains two related invocations, model calls, and a tool whose schema changes the execution arguments.

## Development use

From a Kitaru checkout, sync the plugin workspace and call the parser:

```bash
uv sync --project plugins --frozen --all-packages
uv run --project plugins python - <<'PY'
from pathlib import Path
from kitaru_mastra_importer import parse

for item in parse(Path("mastra-traces.json").read_bytes(), {}):
    print(item.model_dump_json())
PY
```

A development worker can also run the self-contained parser script. Choose your intended local or development server explicitly:

```bash
kitaru importer register mastra-export \
  --server http://localhost:8000 \
  --provider mastra \
  --script plugins/packages/mastra-importer/src/kitaru_mastra_importer/importer.py \
  --entrypoint parse

kitaru session import mastra-traces.json \
  --server http://localhost:8000 \
  --importer mastra-export@latest \
  --agent your-agent@latest \
  --wait
```

Registration creates server state and requires a running worker for import execution. The package is not registered automatically, and this development workflow does not require publishing a wheel.

## Sessions, identity, and ordering

Each trace becomes one session. An ordinary root `agent_run` represents one invocation. Separate invocations in the same conversation remain separate sessions, each with its original input and output; they are not combined into a new, synthetic invocation.

`metadata.mastra.conversation_id` retains the root thread/conversation identifier, and `invocation_started_at` records its source start time. Sessions are emitted by start time, then source identity for ties. Span indexes follow the parent hierarchy with siblings ordered by start time, explicit step/chunk sequence when available, and span ID for remaining ties. Unresolved timestamp ties do not establish a causal order. Message arrays retain their original order. Thread metadata supports grouping the selected invocations but does not claim the export includes every turn in that conversation.

The default external session ID is the Mastra trace ID. Kitaru deduplicates repeated imports by provider and external ID. The optional parser parameter `source_namespace` distinguishes independent deployments that might reuse trace IDs; it hashes the namespace and trace ID together. Use the same namespace on every import from that deployment. Changing it deliberately creates different identities. Identical repeated trace responses within a payload are skipped; conflicting copies of the same trace ID are rejected rather than importing an arbitrary copy. A re-export of an already imported, unfinished trace is still a duplicate, not an update to the existing session.

## Data fidelity

- Root `input` and `output` become session inputs and outputs in default mode. History-only mode stores the original root input under `supplied_messages` alongside the recorded-context envelope. Every span retains its original input, output, attributes, IDs, source metadata, and timestamps.
- `model_inference` becomes a model-call node; tool and MCP-tool spans become tool nodes. Generation and step spans remain generic parent spans, so the generation loop is not counted as an extra model call. Missing inference spans are not synthesized. Tool names use the exported `entityId`, which Mastra sets to the callable registry name, including aliases.
- `model`, `responseModel`, `provider`, and `parameters` populate model fields. Empty response-model names fall back to the recorded requested model. Full message inputs remain structured; flattened step text is never used to reconstruct conversation history.
- Generation token totals are counted once. Step/inference counters already included in the same model generation are retained in attributes but excluded from normalized totals. Missing aggregate counters fall back to available step/inference counters individually. Independent model generations inside tools retain their own usage.
- `costContext.estimatedCost` populates cost only when `costUnit` explicitly says `USD`. Other or absent units remain in the original attributes. No prices are looked up or costs estimated by the importer. Missing usage and cost remain missing; detailed fields not represented by Kitaru's token model remain in attributes.
- Event spans with no end timestamp are completed point-in-time events. An unfinished ordinary span remains in progress. Source errors remain errors; the importer does not infer failure from wording in the output.

## Replay limits

Mastra exports the root invocation messages separately from the initial model messages in `model_generation.input.messages`. Those may differ because of recalled memory or processors. The importer preserves both and identifies the unique direct generation span in `metadata.mastra.replay.context_span_id`. It never reconstructs conversation history from flattened model-step text.

By default, inputs remain raw and replay eligibility is false. To prepare a known history-only memory invocation, pass `{"replay_context":"history-only"}` as importer parameters. This is an operator declaration: the export does not prove the source agent had no working, semantic, or observational memory, custom processors, or `prepareStep`. Do not use this mode for those configurations.

With that declaration, the importer emits the #1050 `mastra_conversation_context` version 1 envelope with `source: "recalled"`, full initial model messages, and the original invocation under `supplied_messages`. The source tag describes a memory-dependent snapshot including system, recalled, and supplied messages. A completed agent invocation with an identifiable thread and one full initial model context is marked complete and eligible. Missing or ambiguous context, missing invocation input, malformed messages, non-agent roots, or unfinished spans produce an incomplete envelope with reasons. The #1050 adapter rejects an incomplete envelope before calling the model. Default eligibility metadata alone is advisory, not a server-enforced replay prohibition.

Choose the mode before importing. Reimporting the same trace with different parameters does not update the stored session. To create a separate replay-ready import after a default import, use a new explicit `source_namespace`, then keep that namespace stable for deduplication.

The deterministic adapter probe checks full context restoration and recorded tool substitution with Python-generated cache keys. Its exported tool input is `{"value":"3"}`, while execution receives `{"value":3,"label":"defaulted"}`; the original raw arguments and unwrapped result are retained.

A [bounded live validation](../../tests/importers/fixtures/mastra/1.51.0/live/README.md) also passed: real OpenAI generation with native history-only memory, full export, candidate-wheel import through a clean worker and local server, duplicate import, and worker-driven replay through the #1050 adapter. Replay restored the recalled synthetic code despite different live instructions, made one real server history lookup, and executed no live tools. This verifies that specific configuration, not arbitrary Mastra memory or processor setups.

Licensed under Apache-2.0.
