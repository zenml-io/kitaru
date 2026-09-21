# Changelog

## 0.2.0 - 2026-09-21

- Fixed capture byte-budget accounting for enum values while retaining bounded unwrapping.
- Record nodes and parent relationships with external IDs instead of positional indexes. Require Kitaru 0.27.0 or later for the new node contract.

## 0.1.2

- Mark captured mapping-key coercion and collisions as non-replayable, preserve nested tuples in tool results, and reject malformed stored outcomes without executing the live tool.

## 0.1.1

- History replay now distinguishes genuine misses from matched tool calls, replays completed native tool outcomes, and raises matched failures with their stored errors without executing the live tool.

## 0.1.0

- First stable release of the LangGraph recording and replay adapter.

## 0.1.0rc0

- Initial release candidate for the LangGraph recording and replay adapter.
