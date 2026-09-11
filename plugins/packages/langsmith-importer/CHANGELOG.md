# Changelog

## Unreleased

- Emit `reasoning_selectors` pointing at visible reasoning in node outputs instead of extracted `reasoning` text.

### Release context

- Requires the unreleased session node `reasoning_selectors` field in core from [PR #1078](https://github.com/zenml-io/kitaru/pull/1078). Replace `kitaru==0.26.0+dev` with the selected compatible published core version before releasing.

## 0.3.0 - 2026-09-10

- Require Kitaru 0.26.0 or later for the `api` and `adapter` extras. File-only parsing retains support for Kitaru 0.24.0 or later.

- Standardize source identity precedence as `source_instance`, then `project_name`, then embedded project identity; trim strings, reject other types, and reject embedded conflicts even with overrides.
- Accept `project_name` as an alias for `source_instance` and include an actionable import parameter example when project identity is missing.
- Remove the importer payload size cap. Uploads are bounded by the server blob limit only.
- Fetch traces from the LangSmith API by trace id or time window through the `api` extra.
- Import traces oldest first and group them by thread instead of dropping later traces of a thread as duplicates.
- Fetch traces concurrently, bounded by the fetch query's `concurrency` key.
- Wait out a LangSmith rate limit and retry instead of failing the import task.

## 0.2.0

- Add the LangSmith importer-backed adapter, installed through the `adapter` extra.
- Contain malformed numeric, nested payload, graph, and Unicode failures while preserving unrelated sessions.
- Bound parent validation and nested tool scanning; make trace identity and grouping independent of record order.

## 0.1.0

- First stable release of the LangSmith trace importer.

## 0.1.0rc0

- Initial release candidate for the LangSmith trace importer.
