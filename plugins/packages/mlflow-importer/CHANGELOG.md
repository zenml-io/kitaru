# Changelog

## Unreleased

- Import MLflow's `cache_creation_input_tokens` token usage as `cache_write_input_tokens`.

### Release context

- Requires the unreleased `TokenUsage.cache_write_input_tokens` field in core. Replace `kitaru==0.27.3+dev` with the selected compatible published core version before releasing.

## 0.1.0 - 2026-10-08

- Add the MLflow trace importer for `mlflow traces search` and `Trace.to_json()` exports, grouping traces into sessions by `mlflow.trace.session` metadata or a `join_on` path.
- Fetch traces from an MLflow tracking server by trace id or time window through the `api` extra.
