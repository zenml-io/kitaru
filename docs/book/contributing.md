---
description: How to contribute to Kitaru.
icon: code-pull-request
---

# Contributing

We welcome contributions to Kitaru! For full guidelines, see
[CONTRIBUTING.md](https://github.com/zenml-io/kitaru/blob/develop/CONTRIBUTING.md)
in the repository.

## Quick Start

```bash
git clone https://github.com/zenml-io/kitaru.git
cd kitaru
uv sync
just check   # Run all checks
just test    # Run tests
```

## Key Details

* **Default branch:** `develop` — all PRs target this branch
* **Checks:** `just check` runs formatting, linting, type checking, typos, and YAML validation
* **Docs:** These pages live in `docs/book/` (GitBook source, plain Markdown) — edit the `.md` files and register new pages in `docs/book/toc.md`
* **Outside contributors:** If you cannot open a PR directly, open an issue and
  link to any branch you already prepared. Maintainers may invite issue authors
  as repository collaborators when direct PR access would help review.

## Links

* [GitHub Repository](https://github.com/zenml-io/kitaru)
* [Issue Tracker](https://github.com/zenml-io/kitaru/issues)
