# Execution management examples

These examples show the "control plane" side of Kitaru: inspecting runs,
handling waits, and resuming execution.

```bash
uv run examples/execution_management/<module_name>.py
```

For the full catalog, see [../README.md](../README.md).

| Example | Run | What it demonstrates | Test |
|---|---|---|---|
| [client_execution_management.py](client_execution_management.py) | `uv run examples/execution_management/client_execution_management.py` | Browse executions and artifacts with `KitaruClient` | [../../tests/test_phase11_client_example.py](../../tests/test_phase11_client_example.py) |
| [wait_and_resume.py](wait_and_resume.py) | `uv run examples/execution_management/wait_and_resume.py` | `kitaru.wait()` with inline local prompt or fallback CLI input/resume | [../../tests/test_phase15_wait_example.py](../../tests/test_phase15_wait_example.py) |

Install once before running these:

```bash
uv sync --extra local
```
