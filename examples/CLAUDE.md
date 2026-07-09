# examples/CLAUDE.md

Guidance for working on runnable Kitaru examples.

## Example READMEs are public-facing, educational content

`examples/**/README.md` files are written for users learning Kitaru — not
internal documentation. Their audience is a developer encountering the example
for the first time and trying to understand what Kitaru does, what the example
demonstrates, and how to run it themselves. Keep them focused on concepts,
primitives in use, and the flow of the example.

Do **not** add sections like "Testing" (how maintainers run the example's test
suite), internal CI setup notes, contributor-only credential instructions for
stubbed/mocked test runs, or any content that only makes sense to someone
working on Kitaru itself. Those details belong in `tests/` docstrings, internal
contributor docs, or PR descriptions.

A good rule of thumb: if the section wouldn't help a brand-new user understand
Kitaru, it doesn't belong in an example README.

Adding, removing, or renaming an example also means updating
`examples/example-coverage.yaml` — see the root `CLAUDE.md` under
"Commits and PRs".
