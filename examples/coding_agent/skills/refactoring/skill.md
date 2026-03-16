---
name: refactoring
description: Safe refactoring patterns
keywords: [refactor, clean, rename, extract, simplify]
---

Make one logical change per edit — don't mix refactoring with new features.

- Preserve all existing tests and run them after each change
- Prefer renaming over deleting + recreating (preserves git history)
- Extract functions only when the same logic appears 3+ times
- Use `git_diff` after each step to verify only intended changes were made
- If a refactor touches more than ~5 files, break it into smaller commits
- Keep the public API stable — rename internals freely, but deprecate
  public names before removing them
