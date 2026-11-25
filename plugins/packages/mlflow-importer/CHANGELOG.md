# Changelog

## Unreleased

## 0.2.0 - 2026-11-25

- Import MLflow's `cache_creation_input_tokens` token usage as `cache_write_input_tokens`.
- Require Kitaru 0.27.7 or later for `TokenUsage.cache_write_input_tokens`.

## 0.1.0 - 2026-10-08

- Add the MLflow trace importer for `mlflow traces search` and `Trace.to_json()` exports, grouping traces into sessions by `mlflow.trace.session` metadata or a `join_on` path.
- Fetch traces from an MLflow tracking server by trace id or time window through the `api` extra.
