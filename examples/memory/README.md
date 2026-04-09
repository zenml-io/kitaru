# Memory Walkthrough

This example demonstrates Kitaru's durable memory evolving **checkpoint by
checkpoint** across all three scopes:

- **namespace scope**: seeded before the flow, read and updated during it
- **execution scope**: tracking per-run progress within the flow body
- **flow scope**: accumulating cross-run summaries

The flow interleaves memory writes between checkpoints so that each checkpoint
boundary has a different memory state visible — ideal for UI panels that show
"what memory was available at this checkpoint."

Also covers detached post-run writes into an execution scope via `KitaruClient`
and post-flow memory maintenance (multi-key compaction, purge, and audit log).

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
| [flow_with_memory.py](flow_with_memory.py) | Memory evolving checkpoint-by-checkpoint across namespace, execution, and flow scopes; detached post-run execution-scope writes; `KitaruClient.memories` inspection; and post-run maintenance (compact, purge, audit log) | [../../tests/test_phase20_memory_example.py](../../tests/test_phase20_memory_example.py) |

Single-key compaction defaults to compacting the current value of one key.
Use `source_mode="history"` when you explicitly want to summarize that key's
full non-deleted version history instead.

The example shows two kinds of execution-scope writes:

- **In-flow writes** (`progress/phase`, `progress/items_processed`): written
  between checkpoints during the flow, so the UI can show them evolving
- **Post-flow writes** (`execution/notes`): detached annotations added after
  the flow completes, demonstrating that `scope=<execution_id>` is the bucket
  while `execution_id` on the entry tracks whether it was physically produced
  during a live run

For the broader feature overview, see
[Use Memory](https://kitaru.ai/docs/guides/memory).
