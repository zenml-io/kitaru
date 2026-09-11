# Changelog

## Unreleased

- Emit `reasoning_selectors` pointing at visible reasoning in node outputs instead of extracted `reasoning` text.

## 0.3.0 - 2026-09-10

- Require Kitaru 0.26.0 or later for the `api` and `adapter` extras. File-only parsing retains support for Kitaru 0.24.0 or later.

- Standardize source identity precedence as `source_instance`, then `project_id`, then embedded project identity; trim strings, reject other types, and reject embedded conflicts even with overrides. Remove filename-derived identity.
- Remove the importer payload size cap. Uploads are bounded by the server blob limit only.
- Use supported Braintrust root filtering and cursor pagination so window imports succeed and complete traces are fetched beyond the first page.
- Fetch traces directly from the Braintrust API through the `api` extra.
- Import API-fetched traces oldest first in a single payload, so traces sharing a session are grouped instead of dropped as duplicates.
- Fetch traces concurrently, bounded by the fetch query's `concurrency` key.
- Wait out a Braintrust rate limit and retry instead of failing the import task.

## 0.2.0

- Add the Braintrust importer-backed adapter, installed through the `adapter` extra.
- Contain malformed numeric, nested payload, graph, and Unicode failures while preserving unrelated sessions.
- Bound parent validation and nested tool scanning; make trace identity and grouping independent of record order.

## 0.1.0

- First stable release of the Braintrust trace importer.

## 0.1.0rc0

- Initial release candidate for the Braintrust trace importer.
