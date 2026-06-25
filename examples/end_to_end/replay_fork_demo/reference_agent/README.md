# Reference LangGraph support copilot

This directory contains the fictional support copilot used by the parent `replay_fork_demo`.

The parent demo uses this agent for a replay/regression workflow:

```text
recorded Langfuse trace
  -> Kitaru import and validation
  -> unchanged replay from a LangGraph node
  -> edited candidate fork from the same node
```

This README explains the agent. To run replay and produce the HTML comparison report, start with `../README.md` and `../demo.py`.

## What the agent does

The agent receives a support request, asks an OpenAI model which local tools to call, executes those tools, summarizes the evidence, and returns a structured support decision.

A typical run looks like this:

1. A user asks for something, such as "exports are failing; open a ticket if there is an outage."
2. The model chooses local tools: customer lookup, service status, usage, billing, or knowledge-base search.
3. The LangGraph graph executes the requested tools. Some tools only read. Other tools write local SQLite rows, such as support tickets or audit-log entries.
4. Guardrails can block forbidden tools, dry-run writes, or calls beyond `max_tool_calls` before local state changes.
5. The model summarizes evidence such as `customer_id`, `account_tier`, `permission_role`, `incident_id`, and knowledge document ids.
6. The model returns a `SupportDecision` with policy label, risk status, required action, summary, evidence ids, and tool names.
7. Langfuse records the run, including scenario and variant metadata.

## Files

```text
examples/end_to_end/replay_fork_demo/reference_agent/
  agent.py                      # OpenAI tool selection, summarization, and structured decisions
  graph.py                      # LangGraph state machine
  config.py                     # Pydantic models and YAML loading
  db.py                         # SQLite reset, reads, writes, and audit log
  mock_api.py                   # localhost HTTP API
  tools.py                      # local tool registry
  knowledge.py                  # Markdown search
  scenarios.yaml                # seeded support requests
  variants/*.yaml               # baseline and candidate variants
  fixtures/seed_data.json       # deterministic local state
  fixtures/langfuse_export.jsonl
  fixtures/langfuse_rich_observations.jsonl
  fixtures/trace_generation_manifest.json
  knowledge_base/*.md
```

## Variants

- `baseline`: `gpt-5-mini`, full permission prompt, normal tool budget.
- `nano_trimmed_permissions`: `gpt-5-nano`, weakened permission prompt and policy. This variant can request the dangerous local setting-update tool, so permission-sensitive traces can show a visible regression when the model chooses it.
- `mini_tool_budget_2`: `gpt-5-mini`, full permission prompt, but `max_tool_calls=2`. This can miss evidence when the model asks for more than two local tools.

## Local validation and bundled fixtures

Most replay/fork development uses `fixtures/langfuse_rich_observations.jsonl`. Import validation reads that file without OpenAI or Langfuse credentials. Replay and fork commands still run the live LangGraph tail, so they need the model credentials used by the reference agent.

From the parent directory:

```bash
cd examples/end_to_end/replay_fork_demo
export TRACE_ID=trace-replay-fork-rich-baseline
export TRACE_FILE=reference_agent/fixtures/langfuse_rich_observations.jsonl
uv run python demo.py import-trace "$TRACE_FILE" --trace-id "$TRACE_ID"
uv run python demo.py run-all "$TRACE_FILE" --trace-id "$TRACE_ID"
```

The deterministic pytest coverage avoids OpenAI and Langfuse by using test doubles.

## Live trace generation

Live trace generation is handled by the parent demo command:

```bash
cd examples/end_to_end/replay_fork_demo
export OPENAI_API_KEY="sk-..."
export LANGFUSE_PUBLIC_KEY="pk-lf-..."
export LANGFUSE_SECRET_KEY="sk-lf-..."
export LANGFUSE_BASE_URL="https://cloud.langfuse.com"
uv run python demo.py create-trace
```

The command prints a trace id. You can then import and replay it:

```bash
uv run python demo.py import-trace langfuse:<TRACE_ID>
uv run python demo.py run-all langfuse:<TRACE_ID>
```

## Fixture files

`fixtures/langfuse_rich_observations.jsonl` is the fixture used by the replay/fork demo. It includes node-level LangGraph observation rows and state deltas, so Kitaru can reconstruct the graph as replayable checkpoints.

`fixtures/langfuse_export.jsonl` is a shallower export used by tests to prove that incomplete traces fail clearly instead of replaying with missing state.
