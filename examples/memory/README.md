# Memory example

This group demonstrates Kitaru's durable memory surface end to end: seeding
memory outside a flow, reading and updating it inside a flow body, and
inspecting multiple scopes explicitly with `KitaruClient.memories`.

```bash
uv sync --extra local
uv run examples/memory/flow_with_memory.py
```

This example uses your current Kitaru connection context. If you want it to use
a deployed Kitaru server, connect first with `uv run kitaru login ...` (or
`kitaru login ...`) and verify with `kitaru status`.

For the full catalog, see [../README.md](../README.md).

| Example | What it demonstrates | Test |
|---|---|---|
| [flow_with_memory.py](flow_with_memory.py) | Outside-flow seeding, in-flow `kitaru.memory` usage, and explicit-scope inspection with `KitaruClient.memories` | [../../tests/test_phase20_memory_example.py](../../tests/test_phase20_memory_example.py) |

For the broader feature overview, see
[Use Memory](https://kitaru.ai/docs/guides/memory).
