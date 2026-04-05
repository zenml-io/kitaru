# Demo playbook — Repo Memory Walkthrough

Operator-facing guide for recording demos, clips, and live walkthroughs of
Kitaru's memory feature.

## Prerequisites

**Basic run (seeding + flow + inspection):**
Only a Kitaru connection context is needed. No model required.

**Full run with maintenance (compact / purge / audit log):**
A configured model is needed for compaction:

```bash
kitaru model register default --model openai/gpt-5-nano
```

**Deterministic recordings:**
Rerunning the walkthrough on the same scope accumulates history and changes
counts. For repeatable recordings, use a timestamped scope:

```bash
DEMO_SCOPE="repo_docs_$(date +%s)"
uv run examples/memory/flow_with_memory.py --namespace-scope "$DEMO_SCOPE"
```

Or start from a clean project / fresh server state.

## Long-form demo

Run with a model configured for the full experience:

```bash
uv run examples/memory/flow_with_memory.py
```

What to point at in the output:

- **Seeding:** keys seeded outside a flow — detached provenance
- **Flow execution:** topic count increment, flow summary, soft-delete in flow body
- **Maintenance:** compact writes a summary, purge trims history, audit log tracks both

Safe claims this supports:

- Durable, versioned, artifact-backed memory
- One memory system across Python, client, CLI, and MCP
- In-flow writes preserve execution provenance
- Compact and purge are separate control-plane operations
- Soft deletes preserve full history

## Short clip recipes

### Runtime-only clip (~45 seconds)

Shows seeding, flow usage, and inspection without needing a model:

```bash
uv run examples/memory/flow_with_memory.py --skip-maintenance
```

### CLI maintenance clip

After running the example, demonstrate maintenance from the CLI:

```bash
DEMO_SCOPE=repo_docs  # or your timestamped scope

kitaru memory list --scope "$DEMO_SCOPE"
kitaru memory compact --scope "$DEMO_SCOPE" --key conventions/test_runner
kitaru memory purge conventions/test_runner --scope "$DEMO_SCOPE" --keep 1
kitaru memory compaction-log --scope "$DEMO_SCOPE"
```

### MCP assistant clip

Use the same scope/key names with MCP tools from an assistant:

1. `kitaru_memory_list(scope="repo_docs")`
2. `kitaru_memory_get(key="conventions/test_runner", scope="repo_docs")`
3. `kitaru_memory_compact(scope="repo_docs", key="conventions/test_runner")`
4. `kitaru_memory_purge(key="conventions/test_runner", scope="repo_docs", keep=1)`
5. `kitaru_memory_compaction_log(scope="repo_docs")`

## Safe and unsafe claims

| Safe to claim | Unsafe to claim |
|---|---|
| Durable, versioned, artifact-backed memory | Replay-safe memory |
| One memory system across Python, client, CLI, and MCP | Memory inside checkpoints |
| In-flow writes preserve execution provenance | Repo Memory Agent is shipped |
| Compact and purge are separate control-plane operations | Compaction deletes source entries |
| Soft deletes preserve full history | Every entry links to a checkpoint |
| Compaction audit log tracks all maintenance | Rollback or fork from memory history |
