---
description: How to contribute to Kitaru.
icon: code-pull-request
---

# Contributing

We welcome contributions to Kitaru! For full guidelines, see [CONTRIBUTING.md](https://github.com/zenml-io/kitaru/blob/develop/CONTRIBUTING.md) in the repository.

## Quick Start

```bash
git clone https://github.com/zenml-io/kitaru.git
cd kitaru
uv sync
just check   # Run all checks
just test    # Run tests
```

## Key Details

- **Default branch:** `develop`; all PRs target this branch
- **Checks:** `just check` runs formatting, linting, type checking, typos, and YAML validation
- **Docs:** These pages live in `docs/book/` (GitBook source, plain Markdown). Edit the `.md` files and register new pages in `docs/book/toc.md`
- **Outside contributors:** Direct PRs are limited to collaborators. Comment on an existing issue or open a new one before you write code; once a maintainer agrees on the approach, they will add you as a collaborator so you can open the PR. Small fixes like typos: just open an issue and we'll make the change.

## Links

- [GitHub Repository](https://github.com/zenml-io/kitaru)
- [Issue Tracker](https://github.com/zenml-io/kitaru/issues)
