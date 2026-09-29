# MLflow 3.16.1 export fixture

`traces.json` is the output of `mlflow traces search --experiment-id <id> --output json` for five traces that `generate.py` records with MLflow 3.16.1 autologging. All content and identities are synthetic, and no model service is called: the OpenAI and Anthropic clients use in-process HTTP transports that return fixed responses and token counts.

| Trace | What it covers |
|---|---|
| `weather_agent` (two traces) | An `AGENT` span with two OpenAI `CHAT_MODEL` calls around a `TOOL` span, sharing `mlflow.trace.session` metadata, with a human feedback assessment on the second trace |
| `refund_agent` | An Anthropic `CHAT_MODEL` call with a top-level system prompt and no session metadata |
| `order_agent` | A `TOOL` span that raises, failing the trace with an `exception` event |
| `ChatOpenAI` | A LangChain chat model span wrapping the OpenAI span recorded for the same request, both carrying token usage and cost |

MLflow computed the `mlflow.llm.cost` span costs and the `mlflow.trace.tokenUsage` and `mlflow.trace.cost` trace totals from its bundled pricing data. The trace totals count each request once, which the tests compare against the importer's session totals.

The export is unmodified except that `generate.py` replaces the local fixture directory, Python environment, and home directory paths in exception stack traces with `<fixture-dir>`, `<python-env>`, and `<home>`. It also records the traces under a synthetic OS user and artifact location. Trace ids, span ids, and timestamps change between regenerations.

Regenerate from the repository root:

```bash
uv run plugins/tests/importers/fixtures/mlflow/3.16.1/generate.py
```

The traces go to a temporary SQLite store unless `MLFLOW_TRACKING_URI` points at a tracking server, which is how to record them into a running MLflow server for an API import. An optional argument writes `traces.json` to another directory instead of this one.
