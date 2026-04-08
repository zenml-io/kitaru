# Repo Memory Walkthrough

This example demonstrates Kitaru's durable memory surface end to end: seeding
memory outside a flow, reading and updating it inside a flow body, inspecting
multiple scopes via `KitaruClient.memories`, showing detached post-run writes
into an execution scope, and running post-flow memory maintenance (multi-key
compaction, purge, and audit log inspection).

## Quick start

```bash
uv sync --extra local
uv run examples/memory/flow_with_memory.py
```

The example **auto-detects** whether a model is configured. If one is available,
it runs the full walkthrough including LLM-powered compaction, purge, and audit
log. If not, those sections are skipped with guidance on how to enable them.

To configure a model for the maintenance demo:

```bash
kitaru model register default --model openai/gpt-5-nano
```

## Surfaces

| Surface | Command | Requires model | Best for |
|---|---|---|---|
| Python (default) | `uv run examples/memory/flow_with_memory.py` | Auto-detects | First-time run, full walkthrough |
| Python (runtime only) | `... --skip-maintenance` | No | Runtime clip, flow-body story |
| CLI follow-up | `kitaru memory ...` | Yes for compact | Admin/maintenance clip |
| MCP follow-up | `kitaru_memory_*` tools | Yes for compact | Assistant clip |

## Output options

- `--output text` (default): narrated walkthrough output
- `--output json`: machine-readable structured snapshot (same data the tests consume)
- `--skip-maintenance`: force-skip compact/purge/audit even when a model is available
- `--namespace-scope SCOPE`: override the namespace scope (default: `repo_docs`)
- `--topic TOPIC`: override the topic string (default: `release_notes`)
- `--model MODEL`: override the model alias for compaction (default: `default`)

## Connection context

This example uses your current Kitaru connection context. If you want it to use
a deployed Kitaru server, connect first with `kitaru login ...` and verify with
`kitaru status`.

## Demo recordings

See [DEMO_PLAYBOOK.md](DEMO_PLAYBOOK.md) for recording recipes, clip
instructions, and a safe/unsafe claims reference.

## Testing

| Example | What it demonstrates | Test |
|---|---|---|
| [flow_with_memory.py](flow_with_memory.py) | Outside-flow seeding, in-flow `kitaru.memory` usage, detached post-run execution-scope writes, explicit-scope inspection with `KitaruClient.memories`, and post-run maintenance (multi-key compact, purge, audit log) | [../../tests/test_phase20_memory_example.py](../../tests/test_phase20_memory_example.py) |

Single-key compaction defaults to compacting the current value of one key.
Use `source_mode="history"` when you explicitly want to summarize that key's
full non-deleted version history instead.

The example also shows an important execution-memory distinction:

- `scope=<execution_id>` tells you which execution bucket a memory entry belongs to
- `execution_id` on the entry tells you whether that particular version was actually produced during a live run

That means a detached post-run write can still belong to one execution scope
without claiming that the execution itself physically wrote the version.

For the broader feature overview, see
[Use Memory](https://kitaru.ai/docs/guides/memory).
