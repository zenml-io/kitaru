# Kitaru MLflow importer

Import MLflow traces as Kitaru sessions. This package backs the built-in `kitaru/mlflow` importer and runs on a Kitaru worker, so traces are parsed in your environment.

Most users do not install or call this package directly. Export traces with the MLflow CLI, start a Kitaru worker, then select the built-in importer:

```bash
mlflow traces search --experiment-id 1 --output json > mlflow-traces.json
kitaru session import mlflow-traces.json \
  --importer kitaru/mlflow@latest \
  --agent support-agent@latest \
  --wait
```

The importer accepts an `mlflow traces search --output json` page, a single `Trace.to_json()` document, a JSON list of traces, or JSONL whose lines are any of those. Traces must include their spans.

Traces that share `mlflow.trace.session` metadata become one session with one turn per trace; a trace without it becomes its own session. Set `join_on` to a dotted path or JSON pointer inside each trace to group by another value, for example `--params '{"join_on":"info.trace_metadata.mlflow.trace.user"}'`.

Source identity uses `source_instance`, then the `experiment_id` parameter alias, then the experiment in the trace location. Keep it stable across imports to preserve deduplication. Traces stored in a Databricks Unity Catalog location carry no experiment id; supply `source_instance` for those.

The importer maps `LLM` and `CHAT_MODEL` spans to model calls and `TOOL` spans to tool calls, reading MLflow's normalized model, provider, token usage, and cost attributes. When a model span wraps another model span, such as a LangChain chat model around the OpenAI call it makes, only the innermost span counts as a model call, and a span keeps only the tokens and cost its descendants do not already account for, so session totals count each request once. Valid MLflow assessments are kept in session metadata.

## API import

The `api` extra fetches traces directly from an MLflow 3 tracking server. Configure `MLFLOW_TRACKING_URI`, plus `MLFLOW_TRACKING_TOKEN` or `MLFLOW_TRACKING_USERNAME` and `MLFLOW_TRACKING_PASSWORD` when the server requires authentication. A time-window query searches `experiment_ids`, defaulting to `MLFLOW_EXPERIMENT_ID`, and accepts an MLflow `filter_string`. A `trace_ids` query fetches those traces.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
