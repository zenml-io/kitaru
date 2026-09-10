# Changelog

## 0.3.0 - 2026-09-10

- Require Kitaru 0.26.0 or later for the `api` and `adapter` extras. File-only parsing retains support for Kitaru 0.24.0 or later.

- Support Langfuse SDK 4.15.2's renamed observation model field while retaining support for earlier SDK versions.
- Standardize source identity precedence as `source_instance`, then `project_id`, then embedded project identity; trim strings, reject other types, and reject embedded conflicts even with overrides.
- Require an explicit source identity when an export has no project ID instead of deriving one from the filename; accept `project_id` as an alias for `source_instance` and include a CLI remedy in the error.
- Remove the importer payload size cap. Uploads are bounded by the server blob limit only.
- Preserve observations outside the selection window when importing a trace that starts inside the window.

- Fetch traces directly from the Langfuse API by trace id or time window, through the `api` extra.
- Import traces oldest first and grouped by session, fixing later traces in a session being dropped as duplicates.
- Fetch traces concurrently, bounded by the fetch query's `concurrency` key.
- Read observations through the bulk observations endpoint instead of per-trace fetches, and wait out Langfuse rate limits during API imports.

## 0.2.0

- Add the Langfuse importer-backed adapter, installed through the `adapter` extra.
- Isolate invalid numeric values, model fields, and unserializable payloads without discarding unrelated sessions; preserve explicit zero costs.
- Bound observation depth and nested tool JSON scanning, validate before tool-link inference, and use iterative tree flattening.

## 0.1.1

- Resolve legacy ingestion updates independently of JSONL row order and preserve explicit zero token counts.

## 0.1.0

- Promote the Langfuse importer, including inferred tool-call links and normalized tool outputs, to stable.

## 0.1.0rc3

- Nest matched tool calls under requesting LLM calls, retain their source parent as a secondary link, decode JSON tool-call arguments, and select structured final-answer text.

## 0.1.0rc2

- Align the Langfuse importer release candidate with Kitaru 0.22.0rc9.

## 0.1.0rc1

- Add causal links from tool spans to the model calls that requested them, with an option to disable inference.

## 0.1.0rc0

- Initial release candidate for the Langfuse trace importer.
