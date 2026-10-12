# Changelog

## 0.1.0 - 2026-10-08

- Add file and API imports for finished ElevenLabs Agents conversations.
- Preserve transcript events, matched tool requests and results, source analysis, usage, billing metadata, and authenticated recording references.
- Represent transcript events as spans without reconstructing underlying model requests or adding a replay runtime.

### Release context

Requires Kitaru `0.27.0` or later for the existing imported-node and API-fetch contracts. Kitaru `0.27.2` adds this distribution to the worker first-party package inventory so package installation can bypass a stale dependency cutoff. The importer remains outside the server-default catalog.
