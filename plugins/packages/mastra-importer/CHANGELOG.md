# Changelog

## Unreleased

## 0.3.0 - 2026-11-25

- Import Mastra's `inputDetails.cacheWrite` usage as `cache_write_input_tokens`.
- Require Kitaru 0.27.7 or later for `TokenUsage.cache_write_input_tokens`.

## 0.2.0 - 2026-09-21

- Emit `reasoning_selectors` pointing at visible reasoning in the span output instead of extracted `reasoning` text.
- Report one conflict failure for inconsistent copies of a trace regardless of export order.
- Identify imported nodes and their parents by external ID. Require Kitaru 0.27.0 or later for the new node contract.

## 0.1.0 - 2026-09-10

- Add the Mastra trace importer.
