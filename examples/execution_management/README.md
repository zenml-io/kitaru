# Execution management examples

These examples show the “control plane” side of Kitaru: inspecting runs,
handling waits, and resuming execution.

The implementations live here, but the public commands stay flat:

```bash
uv run -m examples.<module_name>
```

For the full catalog, see [../README.md](../README.md).

| Example | Run | What it demonstrates | Test |
|---|---|---|---|
| [client_execution_management.py](client_execution_management.py) | `uv run -m examples.client_execution_management` | Browse executions and artifacts with `KitaruClient` | [../../tests/test_phase11_client_example.py](../../tests/test_phase11_client_example.py) |
| [wait_and_resume.py](wait_and_resume.py) | `uv run -m examples.wait_and_resume` | Pause with `kitaru.wait()`, then provide input and resume | [../../tests/test_phase15_wait_example.py](../../tests/test_phase15_wait_example.py) |

Install once before running these:

```bash
uv sync --extra local
```
