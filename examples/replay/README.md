# Replay example

This group focuses on Kitaru's replay model: keep earlier durable work and rerun
only the suffix you care about.

```bash
uv run examples/replay/replay_with_overrides.py
```

For the full catalog, see [../README.md](../README.md).

| Example | Run | What it demonstrates | Test |
|---|---|---|---|
| [replay_with_overrides.py](replay_with_overrides.py) | `uv run examples/replay/replay_with_overrides.py` | Replay from a checkpoint boundary with targeted `checkpoint.*` overrides | [../../tests/test_phase16_replay_example.py](../../tests/test_phase16_replay_example.py) |

Install once before running it:

```bash
uv sync --extra local
```
