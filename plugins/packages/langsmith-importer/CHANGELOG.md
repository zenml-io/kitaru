# Changelog

## Unreleased

- Fetch traces from the LangSmith API by trace id or time window through the `adapter` extra.
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
