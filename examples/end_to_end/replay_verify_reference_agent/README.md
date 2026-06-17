# Replay Verify reference agent

This example is a small, fictional B2B SaaS support copilot used to generate Langfuse traces for Replay Verify exploration.

The example is intentionally local except for two live calls:

1. OpenAI provides the LLM summary and final structured decision.
2. Langfuse receives the trace.

Everything else is local and deterministic: SQLite state, a localhost HTTP API, Markdown knowledge-base search, seeded scenarios, and variant YAML files.

## What it demonstrates

The agent receives a support request, gathers evidence, summarizes the evidence with a real LLM call, and asks the LLM for a structured final decision.

The concrete story looks like this:

1. A customer asks for something: for example, “exports are failing; open a ticket if there is an outage.”
2. The graph calls local tools: customer lookup, service status, usage, billing, or knowledge-base search.
3. Some tools only read. Other tools write local SQLite rows, such as support tickets or audit-log entries.
4. The LLM summarizes the evidence and keeps important facts such as `customer_id`, `account_tier`, `permission_role`, `incident_id`, and knowledge document ids.
5. The LLM returns a `SupportDecision` with policy label, risk status, required action, summary, evidence ids, and tool names.
6. Langfuse records the run, including scenario and variant metadata.

Stage 1 stops there. It does **not** import traces into cases, validate cases, compare baseline and candidate runs, calculate metrics, generate reports, or produce a CI verdict.

## Files

```text
examples/end_to_end/replay_verify_reference_agent/
  agent.py                      # OpenAI summarization and structured decision calls
  graph.py                      # LangGraph state machine
  config.py                     # Pydantic models and YAML loading
  db.py                         # SQLite reset, reads, writes, and audit log
  mock_api.py                   # localhost HTTP API
  tools.py                      # local tool registry
  knowledge.py                  # Markdown search
  scenarios.yaml                # seeded support requests
  variants/*.yaml               # baseline and candidate variants
  generate_traces.py            # live OpenAI + Langfuse trace generation
  fixtures/seed_data.json       # deterministic local state
  fixtures/langfuse_export.jsonl
  fixtures/trace_generation_manifest.json
  knowledge_base/*.md
```

## Variants

- `baseline`: `gpt-5-mini`, full permission prompt, normal tool budget.
- `nano_trimmed_permissions`: `gpt-5-nano`, weakened permission prompt and policy. This variant intentionally performs a dangerous local setting update in permission-sensitive scenarios so the trace shows a visible regression.
- `mini_tool_budget_2`: `gpt-5-mini`, full permission prompt, but `max_tool_calls=2`. This intentionally misses evidence in scenarios that need lookup + API + knowledge-base search.

## Local validation without credentials

This command checks scenario and variant files without calling OpenAI or Langfuse:

```bash
uv run --extra langgraph-openai \
  examples/end_to_end/replay_verify_reference_agent/generate_traces.py \
  --validate-only
```

The deterministic pytest coverage also avoids OpenAI and Langfuse.

## Live trace generation

Trace generation is intentionally fail-closed. It refuses to run unless all four environment variables are set:

```bash
export OPENAI_API_KEY="sk-..."
export LANGFUSE_PUBLIC_KEY="pk-lf-..."
export LANGFUSE_SECRET_KEY="sk-lf-..."
export LANGFUSE_BASE_URL="https://cloud.langfuse.com"
```

Then run:

```bash
uv run --extra langgraph-openai --with langfuse \
  examples/end_to_end/replay_verify_reference_agent/generate_traces.py \
  --variants baseline,nano_trimmed_permissions,mini_tool_budget_2 \
  --scenario-set smoke
```

The script prints one line per scenario/variant run:

```text
<trace_id> | <variant> | <scenario_id> | <required_action> | writes=<audit row count>
```

In Langfuse, filter by tags:

- `kitaru`
- `replay-verify`
- `reference-agent`
- `stage-1`

Each trace also includes metadata:

- `scenario_id`
- `case_id`
- `variant_name`
- `agent_version`
- `model`
- `prompt_profile`
- `tool_policy_name`
- `fixture_generation_run_id`

The instrumentation follows Langfuse’s LangChain/LangGraph callback pattern: create a `CallbackHandler`, pass it through graph/model invocation config, and flush/shut down the client before the short-lived script exits.

## Fixture files

`fixtures/langfuse_export.jsonl` and `fixtures/trace_generation_manifest.json` are offline copies of live traced runs. Later stages can use them without OpenAI or Langfuse credentials.

If those files contain an empty placeholder, no credentialed trace generation has been run in this checkout yet. Run `generate_traces.py` with the live environment variables above to replace them with real trace ids and local run outputs.
