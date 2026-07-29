# v2 implementation notes

Decisions made during implementation that the spec left open, and issues that
required solving beyond what the documents describe.

## Foundations

- `FrozenModel` moved from `server/base.py` to the top-level
  `src/kitaru/base.py` as the spec requires. `server/base.py` re-exports it,
  so existing server imports keep working.
- `JsonValue` is specified as "Any, recursively finite". Implemented as an
  annotated `Any` with a `BeforeValidator` that walks dicts, lists, and
  tuples and rejects non-finite floats.
- `compute_tool_cache_key` hashes the tool name and the canonical JSON dump
  of the inputs (sorted keys, compact separators) with a NUL byte between
  the two, so a tool name that is a prefix of another cannot collide with
  shifted input bytes.
- `packaging` added to the server extra for plugin requirement validation.
