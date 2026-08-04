---
name: kitaru-dev
description: Use for Kitaru commands, CLI, analytics, PRs.
---

# Kitaru Development, CLI, and PR Workflow

Use this when you need the command catalog beyond the daily loop in the root
`AGENTS.md`, or when adding CLI commands, analytics events, or PR descriptions.

## Python Workflows

- `uv sync`: install and sync dependencies
- `uv sync --extra mcp`: include the optional native MCP server
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
- `just mcp-schema-check`: verify the public MCP registry budgets and committed snapshots
- `just mcp-wheel-smoke`: verify clean base and `[mcp]` installs from the single wheel under `dist/`

When merging `develop` into a feature branch and resolving `pyproject.toml` or
`uv.lock`, do not assume a broad `uv lock` preserves recent dependency-security
fixes. Check recent commits touching `uv.lock` or `.github/pip-audit-ignored.txt`,
use targeted commands such as `uv lock --upgrade-package <package>` when a
package was intentionally bumped, and run `just audit` before pushing.

## Docs Workflows

These require Node 22+ and pnpm.

- `just generate-docs`: generate changelog and SDK reference docs
- `just docs`: preview docs locally at `localhost:3000`
- `just docs-build`: build docs static export
- `just docs-validate`: validate the static export as served under `/docs`

## Native MCP Server

The native v2 server is installed with `kitaru[mcp]` and started with `kitaru-mcp`. It defaults to `read-only` mode with 2 tools; `standard` advertises 5 and `destructive` advertises 7. Treat a mode change or schema snapshot change as a public API and security review.

Run `uv run --extra mcp python scripts/report_mcp_schema.py --check` after changing MCP models, registry declarations, descriptions, annotations, or SDK versions. Build the wheel and run `just mcp-wheel-smoke` after entrypoint, packaging, or optional-import changes.

## CLI Structure

The `kitaru` console script is defined in `pyproject.toml` under `[project.scripts]`. `src/kitaru/cli/__init__.py` is the lazy entry point, and command implementations live in ordinary modules under `src/kitaru/cli/`.

Add new subcommands in the appropriate `src/kitaru/cli/*.py` module and register them on the shared Cyclopts app. When testing CLI commands, always pass an explicit argument list to `main([...])`; successful invocations return exit code 0.

## JSON Output Contract

Agent-facing commands use the version-1 structured contract. Success documents include `schema_version`, `command`, `ok`, `warnings`, `links`, and `next_actions`, plus `item` for one result or `items`, `count`, and `page` for a list. Structured errors are one JSON object on stderr with a stable error kind and exit code.

Document login consistently: `kitaru login SERVER` targets the full managed or self-hosted instance URL, while `kitaru login --local` targets an already-running server at `http://localhost:8000`; it never starts a local server.

## Diagnostics and Cleanup

`kitaru status` shows the selected server, provenance, credential state, compatibility, and live-worker count. `kitaru info` adds local package, Python, and platform details. `kitaru doctor` runs independent local, server, authentication, and tooling checks without stopping after the first failure. These commands never print secret values.

## Analytics

Kitaru collects anonymous usage analytics for opted-in users. When adding new
features, discuss analytics coverage with the core team to decide what should
be tracked.

- Add every event name to the `AnalyticsEvent` enum in `src/kitaru/analytics.py`.
- Track only non-sensitive metadata: event names, boolean flags, enum values,
  and counts.
- Never include user content, file paths, prompts, or secret values.
- CLI feature events use `track()` in subcommand handlers under `src/kitaru/cli/`.
- Native MCP calls are attributed through `AnalyticsSource.MCP` on the shared API client; do not add argument/body telemetry or a separate tool event without a reviewed privacy contract.
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
