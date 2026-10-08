# Changelog

## 0.3.0 - 2026-10-08

- Serialize baseline history lookups per cache key so identical tool calls dispatched in parallel replay successive recorded occurrences instead of all replaying the first one.
- Record the requested and served model, provider, token usage, and estimated cost on model-call nodes.
- Preserve LangGraph callback names on recorded chain spans and document upstream middleware input omissions.
- Record tool attempts rejected by middleware, including Deep Agents parallel same-file mutations, while preserving native error results and avoiding duplicate execution or substitution records.
- Clarify that Deep Agents built-in middleware runs before Kitaru replay middleware.

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
