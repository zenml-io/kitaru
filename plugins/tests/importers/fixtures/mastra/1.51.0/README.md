# Mastra 1.51.0 export fixture

`traces.json` contains two unmodified JSON serializations of Mastra storage `getTrace({traceId})` responses. The outer array collects selected responses for this importer; it is not a Mastra API response envelope. All content and identities are synthetic.

`generate.mjs` runs actual Mastra agents with `@mastra/core@1.51.0`, `@mastra/observability@1.16.0`, and `zod@4.3.6`. It exports through `MastraStorageExporter` into `InMemoryStore`. Its local model returns deterministic responses and supplied token counts. There are no model API calls. `package-lock.json` pins the generator's full dependency graph.

The first invocation calls `double` with `{"value":"3"}` and receives `{"doubled":6,"label":"defaulted"}`. The second supplies the first exchange as explicit conversation history. The agent has no live memory implementation; thread/resource options demonstrate exported conversation identity without external storage. `evidence.json` records the actual model prompts and the schema-coerced/defaulted tool execution input. IDs and timestamps other than explicitly assigned trace IDs vary between regenerations.

Reproduce in a scratch directory so regeneration does not overwrite the reviewed fixture:

```bash
mkdir -p /tmp/mastra-fixture-reproduction
cp plugins/tests/importers/fixtures/mastra/1.51.0/{generate.mjs,package.json,package-lock.json} /tmp/mastra-fixture-reproduction/
cd /tmp/mastra-fixture-reproduction
npm ci
node generate.mjs
```

Use Node 22, the Kitaru adapter's supported runtime. The generator also writes individual `trace-1.json` and `trace-2.json` API responses for inspection. See `provenance.json` for pinned primary-source contracts.

## Recorded-context adapter replay probe

`replay.mjs` requires the context-capable Mastra adapter from #1050 and loads compiled Kitaru and Mastra packages from the repository, or from the checkout selected by `KITARU_MASTRA_ADAPTER_ROOT`. Its HTTP stub accepts only `https://fixture.invalid`. It checks restoration of every recorded snapshot message, original system messages overriding deliberately different live instructions, prior-turn-dependent output, removal of live memory options, and history substitution using cache keys computed by Kitaru's Python implementation. The live tool raises if called.

From the repository root, with Node 22 and pnpm available:

```bash
pnpm install --frozen-lockfile
pnpm --filter @zenml-io/kitaru build
pnpm --filter @zenml-io/kitaru-mastra build
KITARU_MASTRA_REPLAY_TEST=1 uv run --project plugins pytest -q -c plugins/pyproject.toml plugins/tests/importers/test_mastra.py
```

The opt-in test builds its input from the real Python parser, writes it into a temporary directory, and runs `replay.mjs`. The default importer test suite needs neither Node nor a model service. The parser runs with `replay_context="history-only"` to produce the #1050 recorded-context envelope. This fixture uses explicit history with no advanced memory or input processors, so that declaration is supported by its generator. The probe verifies integration with the context-capable adapter using a stubbed Kitaru API; it does not start a live worker/server.

To test a separately built context-capable checkout, add `KITARU_MASTRA_ADAPTER_ROOT=/path/to/context-adapter-checkout` to the pytest command. The selected checkout must have its dependencies installed and both TypeScript packages built. The adapter root defaults to this repository; an older adapter without #1050 cannot restore these inputs.
