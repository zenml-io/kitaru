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

## API models

- `AgentCapabilities.tools` and `.mcp_servers` are untyped in the spec table.
  Typed as `list[str]` matching `skills`.
- `RunSpec.command` and `TaskRunSpec.command` are typed `str`, not
  `list[str]`, since worker.md runs the command via `sh -c`.
- `StaticCase.match` is `JsonValue | None` and `StaticCase.result` is
  `JsonValue`, the spec leaves both untyped.
- `EvaluationResult`'s positional routing is a single optional positional
  parameter routed by type in `__init__`. `model_validate` bypasses
  `__init__`, so wire parsing is unaffected.
- `ImportFailure` and `ImportStats` extend `ResponseModel`, they are read
  back from task results rather than supplied by clients.
- The evaluation name rule is implemented locally in
  `api_models/v1/evaluation.py`, since `api_models` cannot import the server
  domain's `Name` type.
- `job.py` imports `TaskKind` and `TaskStatus` from `task.py` for
  `JobTasksListParams`, an import the spec's cross-file list omits.
