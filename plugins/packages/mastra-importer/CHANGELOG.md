# Changelog

## Unreleased

- Import Mastra's `inputDetails.cacheWrite` usage as `cache_write_input_tokens`.

### Release context

- Requires the unreleased `TokenUsage.cache_write_input_tokens` field in core. Replace `kitaru==0.27.3+dev` with the selected compatible published core version before releasing.

## 0.2.0 - 2026-09-21

- Emit `reasoning_selectors` pointing at visible reasoning in the span output instead of extracted `reasoning` text.
- Report one conflict failure for inconsistent copies of a trace regardless of export order.
- Identify imported nodes and their parents by external ID. Require Kitaru 0.27.0 or later for the new node contract.

## 0.1.0 - 2026-09-10

- Add the Mastra trace importer.
