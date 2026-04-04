# Memory example

This group demonstrates Kitaru's durable memory surface end to end: seeding
memory outside a flow, reading and updating it inside a flow body, inspecting
multiple scopes explicitly with `KitaruClient.memories`, and running post-flow
memory maintenance (multi-key compaction, purge, and audit log inspection).

```bash
uv sync --extra local
uv run examples/memory/flow_with_memory.py
```

The compaction phase uses `kitaru.llm()` under the hood, so you need a
configured default model to run the full example:

```bash
kitaru model register default --model openai/gpt-5-nano
```

This example uses your current Kitaru connection context. If you want it to use
a deployed Kitaru server, connect first with `uv run kitaru login ...` (or
`kitaru login ...`) and verify with `kitaru status`.

For the full catalog, see [../README.md](../README.md).

| Example | What it demonstrates | Test |
|---|---|---|
| [flow_with_memory.py](flow_with_memory.py) | Outside-flow seeding, in-flow `kitaru.memory` usage, explicit-scope inspection with `KitaruClient.memories`, and post-run maintenance (multi-key compact, purge, audit log) | [../../tests/test_phase20_memory_example.py](../../tests/test_phase20_memory_example.py) |

Single-key compaction now defaults to compacting the current value of one key.
Use `source_mode="history"` when you explicitly want to summarize that key's
full non-deleted version history instead.

For the broader feature overview, see
[Use Memory](https://kitaru.ai/docs/guides/memory).
