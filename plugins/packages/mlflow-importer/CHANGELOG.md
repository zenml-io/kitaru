# Changelog

## 0.1.0 - 2026-10-08

- Add the MLflow trace importer for `mlflow traces search` and `Trace.to_json()` exports, grouping traces into sessions by `mlflow.trace.session` metadata or a `join_on` path.
- Fetch traces from an MLflow tracking server by trace id or time window through the `api` extra.
