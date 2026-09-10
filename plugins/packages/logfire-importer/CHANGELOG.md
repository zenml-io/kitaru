# Changelog

## 0.3.0 - 2026-09-10

- Require Kitaru 0.26.0 or later for the `api` and `adapter` extras. File-only parsing retains support for Kitaru 0.24.0 or later.

- Require a stable project identity instead of falling back to `logfire`; normalize string identities and reject invalid identity types.

- Remove the importer payload size cap. Uploads are bounded by the server blob limit only.
- Fail API imports visibly when a successful HTTP response contains a query stream error, including after partial results.

- Support Logfire 5.x for the `adapter` extra in addition to the existing 4.35+ line.
- Fetch traces from the Logfire Query API by trace id or time window through the `api` extra, importing traces oldest first and grouped by session, fetched concurrently bounded by the fetch query's `concurrency` key.
- Wait out a Logfire rate limit and retry instead of failing the import task.

## 0.2.0

- Add the Logfire importer-backed adapter, installed through the `adapter` extra.
- Validate parent chains with memoized depths, limiting nested span paths to 64 levels.
- Isolate malformed costs, token counts, embedded JSON, and invalid Unicode at the existing trace or grouped-session boundaries.

## 0.1.1

- Keep same-named sessions from different Logfire projects separate.

## 0.1.0

- First stable release of the Logfire records-query importer.

## 0.1.0rc0

- Add the Logfire records-query importer.
