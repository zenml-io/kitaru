# Changelog

## 0.3.0 - 2026-09-10

- Require Kitaru 0.26.0 or later for the `api` and `adapter` extras. File-only parsing retains support for Kitaru 0.24.0 or later.

- Prefix session external IDs with a validated project namespace. Accept `source_instance` and `project` import parameters, require project identity for file exports, and retain the selected API or adapter project in serialized traces. Reimports of sessions created with earlier bare trace IDs can create a second session.

- Remove the importer payload size cap. Uploads are bounded by the server blob limit only.
- Follow SDK cursor pagination to import all matching spans, including windows and traces larger than 1,000 spans.

- Fetch traces from the Phoenix API by trace id or time window, importing them oldest first, installed through the `api` extra.
- Fetch traces concurrently, bounded by the fetch query's `concurrency` key.
- Wait out a Phoenix rate limit and retry instead of failing the import task.

## 0.2.0

- Add the Arize Phoenix importer-backed adapter, installed through the `adapter` extra.
- Bound nested span paths to 64 levels and isolate malformed costs, token counts, embedded JSON, and invalid Unicode per trace.
- Ignore non-ASCII or oversized indexed message keys without discarding valid messages.

## 0.1.0

- Add the Arize Phoenix JSON and JSONL trace importer.
