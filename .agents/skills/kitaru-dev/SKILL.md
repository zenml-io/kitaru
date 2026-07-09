---
name: kitaru-dev
description: Use for Kitaru commands, CLI, analytics, PRs.
---

# Kitaru Development, CLI, and PR Workflow

Use this when you need the command catalog beyond the daily loop in the root
`AGENTS.md`, or when adding CLI commands, analytics events, or PR descriptions.

## Python Workflows

- `uv sync`: install and sync dependencies
- `uv sync --extra local`: install with local ZenML runtime components
- `uv run kitaru init`: required in a fresh `git worktree`; it creates the
  `.kitaru/` project marker that ZenML's dynamic pipeline resolver needs to
  re-import example modules by dotted path
- `just check`: all checks: format, lint, typecheck, typos, YAML, actions lint, links
- `just fix`: auto-fix formatting, lint issues, and YAML
- `just test`: full pytest suite
- `just test tests/test_file.py::test_name`: one targeted test
- `just lint`: lint only
- `just typecheck`: type check only
- `just typos`: typo check only
- `just format-check`: check formatting without modifying files
- `just yaml-check`: check YAML formatting
- `just actions-lint`: lint GitHub Actions workflows; requires `actionlint`
- `just zizmor`: audit GitHub Actions workflow security with `zizmor`
- `just audit`: audit Python dependencies with `pip-audit` and the documented ignore list
- `just links`: check markdown links offline; requires `lychee`
- `just links-external`: check links including external URLs; slow
- `just example-coverage-audit`: validate `examples/example-coverage.yaml`
  against public example docs, referenced tests/smoke/provider metadata, and
  explicit waivers for `missing`, `planned`, or `manual_only` coverage
- `just build`: build wheel and sdist locally

When merging `develop` into a feature branch and resolving `pyproject.toml` or
`uv.lock`, do not assume a broad `uv lock` preserves recent dependency-security
fixes. Check recent commits touching `uv.lock` or `.github/pip-audit-ignored.txt`,
use targeted commands such as `uv lock --upgrade-package <package>` when a
package was intentionally bumped, and run `just audit` before pushing.

## Docs Workflows

These require Node 22+ and pnpm.

- `just generate-docs`: generate CLI reference, changelog, and SDK reference docs
- `just docs`: preview docs locally at `localhost:3000`
- `just docs-build`: build docs static export
- `just docs-validate`: validate the static export as served under `/docs`

## CLI Structure

The `kitaru` console script is defined in `pyproject.toml` under
`[project.scripts]`. `src/kitaru/cli.py` is the thin facade / entrypoint.
Command implementations live in `src/kitaru/_cli/`.

Add new subcommands in the appropriate `src/kitaru/_cli/_*.py` module and
register them on the shared Cyclopts app there. When testing CLI commands,
always pass an explicit arg list, such as `app(["--help"])`, not bare `app()`.
Successful invocations raise `SystemExit(0)`.

## JSON Output Contract

Agent-facing commands should keep the shared `--output json` / `-o json`
contract consistent:

- single-item commands emit `{command, item}`
- list commands emit `{command, items, count}`
- `kitaru executions logs --follow --output json` emits JSONL event objects
  instead of one final document

Document login consistently: bare `kitaru login` starts the local server, while
`kitaru login <server>` is the remote-login path. Local server support requires
the `kitaru[local]` extra.

## Diagnostics and Cleanup

`kitaru info` shows a multi-section diagnostic overview: connection, config
provenance, connection sources, and system info. Use `--all` for a full dump
including installed packages and environment type. Use `--file debug.json` or
`.yaml` to export diagnostics to a file. Environment variable secrets are masked.

`kitaru clean project|global|all` resets Kitaru state. `project` removes
`.kitaru/`, `global` removes the global config directory with auto-backup and
local server teardown, and `all` does both. Use `--dry-run` to preview and
`--force` when model registry aliases exist for `global` or `all`. The `clean`
command is bootstrap-safe: it works even when the store is broken.

## Analytics

Kitaru collects anonymous usage analytics for opted-in users. When adding new
features, discuss analytics coverage with the core team to decide what should
be tracked.

- Add every event name to the `AnalyticsEvent` enum in `src/kitaru/analytics.py`.
- Track only non-sensitive metadata: event names, boolean flags, enum values,
  and counts.
- Never include user content, file paths, prompts, or secret values.
- CLI feature events use `track()` in subcommand handlers under `src/kitaru/_cli/`.
- MCP feature events use `@tracked_mcp_tool` in `src/kitaru/mcp/server.py`.
- Core SDK lifecycle events use `track(AnalyticsEvent.X, {...})` in the relevant module.
- All `track()` calls must fail silently.

If a CLI command is multi-word, such as `clean project`, add it to
`_MULTI_TOKEN_COMMANDS` in `cli.py`.

## Pull Requests

Use a clear human-readable title. Never include a `[Codex] ` prefix. Include:

- what changed
- why it was needed
- key implementation decisions
- reviewer focus areas

Link related issues when applicable.

Every PR description should include a `Reviewer Notes` H2 or H3 section. Treat
that section as a narrative guide for a human reviewer, not a file-by-file
checklist:

- Start with the story of the change: where important behavior now happens,
  what used to go wrong, and what would break if the implementation is wrong.
- Point reviewers toward genuinely tricky or high-risk areas. Mention files
  only when the file name helps the story, and explain what to inspect there.
- Include a concrete `Reproduction` subsection either inside Reviewer Notes or
  immediately after it.
- Prefer an example, CLI flow, or UI path that proves the behavior end to end.
- Do not use a standalone `Verification` section as a substitute for
  reproduction when it only says `just check`, `just test`, or `/simplify`.
  Those commands are useful local hygiene, but they do not show the reviewer
  how to see the feature or bug fix.
- If local hygiene commands are worth mentioning, keep them as a short `Local
  checks run` note after the reproduction steps.
