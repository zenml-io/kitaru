---
name: testing
description: Test-writing guidance
keywords: [test, spec, coverage, tdd]
---

Write tests for every change you make.

- Use the project's existing test framework — check for `pytest.ini`,
  `setup.cfg`, or `pyproject.toml [tool.pytest]` to discover conventions
- Run only the relevant test file or class, not the full suite
- If tests fail, fix the code, not the tests (unless the test itself is wrong)
- Prefer small, focused test functions over large parametrized matrices
- Name tests after the behavior they verify, not the function they call
