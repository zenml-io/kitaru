# src/kitaru/CLAUDE.md

Guidance for working inside the Kitaru Python SDK package. The root `CLAUDE.md`
covers project-wide conventions; this file covers what only matters when you are
editing SDK, CLI, or MCP source.

## Analytics instrumentation

Kitaru collects anonymous usage analytics for users who have opted in (via ZenML's global analytics setting). When adding new features, discuss analytics coverage with the core team during planning to decide what (if anything) should be tracked.

- **Event registry:** all event names live in the `AnalyticsEvent` enum in `src/kitaru/analytics.py`. Add new events there — never use raw strings.
- **Privacy by design:** track only non-sensitive metadata (event names, boolean flags, enum values, counts). Never include user content, file paths, prompts, model outputs, secret values, or positional CLI arguments. The CLI command tracker uses an allowlist of known multi-word commands (`_MULTI_TOKEN_COMMANDS`) to avoid leaking positional args.
- **Three instrumentation surfaces:**
  - **CLI** (`src/kitaru/cli.py` + `src/kitaru/_cli/`): entry-point tracking in `cli()`, per-command feature events in subcommand handlers.
  - **MCP** (`src/kitaru/mcp/server.py`): `@tracked_mcp_tool` decorator wraps each tool with automatic success/failure tracking.
  - **Core SDK** (`src/kitaru/`): `track(AnalyticsEvent.X, {...})` calls at key lifecycle points (flow submit/terminal, wait, LLM calls, artifact save/load, replay, etc.).
- **Graceful degradation:** all `track()` calls silently fail if analytics is unavailable. Never let a tracking failure break user-facing functionality.
- **Source tagging:** each entry point calls `set_source()` (cli, mcp, python) so events can be segmented by surface without leaking specifics.

When expanding the CLI, MCP, or SDK surface, check whether the new feature needs a tracking event in `AnalyticsEvent` and whether the event is wired into the appropriate surface (CLI handler, `@tracked_mcp_tool`, or SDK lifecycle point). If multi-word CLI commands are added, update `_MULTI_TOKEN_COMMANDS` in `cli.py` to avoid leaking positional arguments into analytics.
