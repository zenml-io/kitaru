# Changelog

## Unreleased

## 0.2.0 - 2026-11-25

- Import the `input_cache_write` usage category as `cache_write_input_tokens`. Input tokens still include it.
- Require Kitaru 0.27.7 or later for `TokenUsage.cache_write_input_tokens`.

## 0.1.0 - 2026-10-08

- Add file and API imports for finished ElevenLabs Agents conversations.
- Preserve transcript events, matched tool requests and results, source analysis, usage, billing metadata, and authenticated recording references.
- Represent transcript events as spans without reconstructing underlying model requests or adding a replay runtime.

### Release context

Requires Kitaru `0.27.0` or later for the existing imported-node and API-fetch contracts. Kitaru `0.27.2` adds this distribution to the worker first-party package inventory so package installation can bypass a stale dependency cutoff. The importer remains outside the server-default catalog.
