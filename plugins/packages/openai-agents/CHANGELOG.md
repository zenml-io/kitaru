# Changelog

## 0.3.0 - 2026-09-21

- Record nodes and parent relationships with provider or synthetic external IDs, and retain concrete start times for materialized model, tool, and handoff nodes. Require Kitaru 0.27.0 or later for the new node contract.

## 0.2.0

- Add named history tool policies for replaying recorded direct function-tool results.
- Record direct function-tool inputs as canonical JSON arguments so new sessions can supply history matches.
- Replay completed `null` results and raise recorded tool failures without executing the live tool.
- Support OpenAI Agents SDK 0.20, 0.21, and 0.22.

## 0.1.0

- First stable release of the OpenAI Agents recording adapter.

## 0.1.0rc0

- Initial release candidate for the OpenAI Agents recording adapter.
