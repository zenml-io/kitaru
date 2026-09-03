# Changelog

## Unreleased

- Fetch traces from the Phoenix API by trace id or time window, importing them oldest first, installed through the `adapter` extra.
- Fetch traces concurrently, bounded by the fetch query's `concurrency` key.
- Wait out a Phoenix rate limit and retry instead of failing the import task.

## 0.2.0

- Add the Arize Phoenix importer-backed adapter, installed through the `adapter` extra.
- Bound nested span paths to 64 levels and isolate malformed costs, token counts, embedded JSON, and invalid Unicode per trace.
- Ignore non-ASCII or oversized indexed message keys without discarding valid messages.

## 0.1.0

- Add the Arize Phoenix JSON and JSONL trace importer.
