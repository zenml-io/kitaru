# Changelog

## Unreleased

- Import the `input_cache_write` usage category as `cache_write_input_tokens`. Input tokens still include it.

### Release context

- Requires the unreleased `TokenUsage.cache_write_input_tokens` field in core. Replace `kitaru==0.27.3+dev` with the selected compatible published core version before releasing.

## 0.1.0 - 2026-10-08

- Add file and API imports for finished ElevenLabs Agents conversations.
- Preserve transcript events, matched tool requests and results, source analysis, usage, billing metadata, and authenticated recording references.
- Represent transcript events as spans without reconstructing underlying model requests or adding a replay runtime.

### Release context

Requires Kitaru `0.27.0` or later for the existing imported-node and API-fetch contracts. Kitaru `0.27.2` adds this distribution to the worker first-party package inventory so package installation can bypass a stale dependency cutoff. The importer remains outside the server-default catalog.
