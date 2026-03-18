# Basic flow examples

This group collects the shortest, most "start here" Kitaru examples.

```bash
uv run examples/basic_flow/<module_name>.py
```

For the full catalog, see [../README.md](../README.md).

| Example | Run | What it demonstrates | Test |
|---|---|---|---|
| [first_working_flow.py](first_working_flow.py) | `uv run examples/basic_flow/first_working_flow.py` | Smallest end-to-end `@flow` + `@checkpoint` example | [../../tests/test_phase5_example.py](../../tests/test_phase5_example.py) |
| [flow_with_logging.py](flow_with_logging.py) | `uv run examples/basic_flow/flow_with_logging.py` | `kitaru.log()` metadata at flow and checkpoint scope | [../../tests/test_phase7_logging_example.py](../../tests/test_phase7_logging_example.py) |
| [flow_with_artifacts.py](flow_with_artifacts.py) | `uv run examples/basic_flow/flow_with_artifacts.py` | Named artifact save/load across executions | [../../tests/test_phase8_artifacts_example.py](../../tests/test_phase8_artifacts_example.py) |
| [flow_with_configuration.py](flow_with_configuration.py) | `uv run examples/basic_flow/flow_with_configuration.py` | Runtime config defaults, overrides, and frozen execution specs | [../../tests/test_phase10_configuration_example.py](../../tests/test_phase10_configuration_example.py) |

Install once before running these:

```bash
uv sync --extra local
```
