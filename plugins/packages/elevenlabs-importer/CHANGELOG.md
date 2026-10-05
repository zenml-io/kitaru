# Changelog

## Unreleased

- Add file and API imports for finished ElevenLabs Agents conversations.
- Preserve transcript events, matched tool requests and results, source analysis, usage, billing metadata, and authenticated recording references.
- Represent transcript events as spans without reconstructing underlying model requests or adding a replay runtime.

### Release context

Initial package version: `0.1.0`. Requires Kitaru `0.27.0` or later for the existing imported-node and API-fetch contracts. No new core data contract is required. The worker first-party package inventory also gains this distribution so package installation can bypass a stale dependency cutoff; that inventory update will ship in the next applicable core release. This package is intended for the next applicable plugin release and is not a server-default importer. Publication is pending.
