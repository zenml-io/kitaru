# Changelog fragments

Each pull request with a user-facing change adds one file to this directory instead of editing `CHANGELOG.md`. The release-preparation PR moves every fragment into a new release section with `uv run python scripts/changelog_fragments.py build --version <version>` and deletes the fragment files.

Name the file `<pr-number>.<section>.md`, where `<section>` is one of `added`, `changed`, `deprecated`, `removed`, `fixed`, or `security`. The file holds one or more Markdown list items, each on a single line, exactly as they should appear under that section:

```md
- Added `kitaru agent delete AGENT --force`, which soft-deletes an agent.
```

Run `uv run python scripts/changelog_fragments.py check` to validate the fragments and `uv run python scripts/changelog_fragments.py build --version <version> --draft` to preview the rendered release section.
