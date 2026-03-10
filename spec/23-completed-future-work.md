# Work Already completed from 21 and 22

## Product: API and developer experience

How users write Kitaru code — import style, invocation patterns, language support.

### Flow invocation API — RESOLVED

Direct call syntax (`my_agent("input")`) removed — `__call__` now raises a friendly `KitaruUsageError`. `.start()` removed entirely. `.run()` is the canonical verb. `.deploy()` remains as semantic sugar for `.run(..., stack=...)`.

Two invocation patterns:

```python
# Handle-based — returns a FlowHandle
handle = my_agent.run("Build a CLI tool")

# Deploy — signals remote/deployment intent
handle = my_agent.deploy("Build a CLI tool", stack="aws-sandbox")

# Block until complete
result = my_agent.run("Build a CLI tool").wait()
```

### Import style — RESOLVED

Canonical style: `from kitaru import flow, checkpoint` for decorators. `import kitaru` for runtime helpers (`kitaru.log()`, `kitaru.wait()`, etc.). Both `@flow` and `@kitaru.flow` work mechanically, but docs/examples use the direct import form.

**Terminology alignment — DONE.** All error messages, docstrings, test assertions, skill files, docs pages, and spec chapters (02–20) updated to use canonical `@flow` / `@checkpoint` style. Runtime helpers remain namespaced (`kitaru.log()`, `kitaru.wait()`, etc.).

### Python version support — RESOLVED

Original floor was 3.12+ for MVP. Audit completed: no PEP 695 `type` statements or other 3.12-only syntax found in Kitaru source. The natural floor is 3.11 (due to `tomllib` and `enum.StrEnum`, both 3.11+). Modern type annotations (`list[str]`, `X | None`) are 3.9+/3.10+ respectively, so no issue. Minimum lowered to 3.11, CI matrix updated to include 3.11 test lanes.

## Docs

### ~~Fix sidebar double nesting~~ — FIXED

Removed duplicate separator labels from the root `meta.json` and stopped listing `index` as an explicit child page in folder `meta.json` files (both manual and in generation scripts). Each section now appears once in the sidebar.

### ~~Code snippet contrast~~ — FIXED

Switched Shiki themes to `github-light` + `github-dark` and forced code blocks to use the dark variant via `var(--shiki-dark)`.
