# Replay Verify reference agent

This example is a small, fictional B2B SaaS support copilot. It exists for two connected demos:

1. **Trace generation:** run a real LLM-driven LangGraph agent, let OpenAI choose local tools, and send traces to Langfuse.
2. **Fork demo:** run the same support-agent shape inside a Kitaru flow, fork a LangGraph checkpoint, change the active variant, resume from that checkpoint, and produce a local HTML report plus a Kitaru HTML artifact.

The important idea is concrete:

> The baseline run gathers customer and policy evidence. Kitaru then forks the LangGraph run after tool collection, swaps in a weaker candidate variant, resumes downstream, and shows that the final behavior changed without rerunning the earlier tool collection.

Everything that could mutate customer state is local: SQLite rows, a localhost HTTP API, Markdown knowledge-base search, seeded scenarios, and variant YAML files.

## What this example shows

The reference agent receives a support request and works through a small support workflow:

1. A customer asks for something: for example, “please change this restricted account setting.”
2. The agent gathers local evidence: customer lookup, policy search, service status, usage, billing, or audit-log writes.
3. Guardrails can block risky or over-budget tool requests before local state changes.
4. The agent summarizes evidence and returns a structured `SupportDecision` with policy label, risk status, required action, summary, evidence ids, and tool names.
5. Kitaru can capture the LangGraph run at graph-call level, or at model/tool-call level when calls pass through Kitaru’s calls-mode instrumentation.
6. The fork demo selects the checkpoint just before `summarize_evidence`, updates the `variant` state value, and asks LangGraph to resume from that forked checkpoint.

The fork demo is intentionally repeatable. Its model-shaped calls are deterministic so the video/report is stable. The trace-generation path still uses real OpenAI and Langfuse.

## Files

```text
examples/end_to_end/replay_verify_reference_agent/
  agent.py                      # OpenAI tool selection plus calls-mode wrappers
  graph.py                      # LangGraph state machine
  config.py                     # Pydantic models and YAML loading
  db.py                         # SQLite reset, reads, writes, and audit log
  mock_api.py                   # localhost HTTP API
  tools.py                      # local tool registry
  knowledge.py                  # Markdown search
  scenarios.yaml                # seeded support requests
  variants/*.yaml               # baseline and candidate variants
  generate_traces.py            # live OpenAI + Langfuse trace generation
  fork_demo.py                  # Kitaru flow that runs baseline + forked candidate
  fork_report.py                # HTML report renderer for the fork demo
  fixtures/seed_data.json       # deterministic local state
  fixtures/langfuse_export.jsonl
  fixtures/trace_generation_manifest.json
  knowledge_base/*.md
  reports/*.html                # generated locally; not committed
```

## Variants

- `baseline`: `gpt-5-mini`, full permission prompt, normal tool budget.
- `nano_trimmed_permissions`: `gpt-5-nano`, weakened permission prompt and policy. In the trace-generation path this variant can request riskier local actions. In the deterministic fork demo it changes the downstream decision after the fork point.
- `mini_tool_budget_2`: `gpt-5-mini`, full permission prompt, but `max_tool_calls=2`. This can miss evidence when the model asks for more than two local tools.

## One-time setup in a fresh worktree

Kitaru examples need a project marker in fresh worktrees:

```bash
uv run --python 3.12 kitaru init
```

If you want to generate live Langfuse traces, set these variables too:

```bash
export OPENAI_API_KEY="sk-..."
export LANGFUSE_PUBLIC_KEY="pk-lf-..."
export LANGFUSE_SECRET_KEY="sk-lf-..."
export LANGFUSE_BASE_URL="https://cloud.langfuse.com"
```

The fork demo itself does not need live provider credentials, because it uses deterministic local model-shaped calls for repeatability.

## Demo 1: generate real Langfuse traces

First, validate that the local scenario and variant files load:

```bash
uv run --python 3.12 --extra langgraph-openai \
  examples/end_to_end/replay_verify_reference_agent/generate_traces.py \
  --validate-only
```

Then generate live traces:

```bash
uv run --python 3.12 --extra langgraph-openai --with langfuse \
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

Each trace includes metadata such as `scenario_id`, `case_id`, `variant_name`, `agent_version`, `model`, `prompt_profile`, `tool_policy_name`, and `tool_selection_mode=llm_tool_calling`.

## Demo 2: run the Kitaru fork demo

Run the default graph-call version:

```bash
uv run --python 3.12 --extra local --extra langgraph-openai \
  examples/end_to_end/replay_verify_reference_agent/fork_demo.py \
  --scenario account_setting_change_request \
  --candidate nano_trimmed_permissions
```

The script prints:

```text
Kitaru execution id: <id>
HTML report: examples/end_to_end/replay_verify_reference_agent/reports/fork-demo.html
Report artifact: fork_demo_report
Report check: local HTML file exists
```

Open the local report:

```bash
open examples/end_to_end/replay_verify_reference_agent/reports/fork-demo.html
```

What to point at in the report:

1. **Public API call:** the report shows `kitaru.fork(fork_runner, ...)`.
2. **Adapter call:** `kitaru.fork(...)` delegates to `KitaruGraphRunner.fork(...)`.
3. **Checkpoint selection:** Kitaru asks for the LangGraph checkpoint where `next == ("summarize_evidence",)`.
4. **State update:** the fork changes the `variant` value from `baseline` to `nano_trimmed_permissions`.
5. **Resume:** LangGraph creates a new fork checkpoint and resumes downstream from that point.
6. **Behavior diff:** the baseline keeps the restricted account-setting request in review/escalation mode; the candidate fork becomes more permissive downstream.
7. **No tool-collection rerun:** the report shows whether the earlier tool-collection step reran. For the intended demo it should say it did not rerun.
8. **Kitaru artifact:** the same HTML is published as a Kitaru artifact named `fork_demo_report` on the printed execution id.

Plain-language story for the video:

> We ran the baseline support agent. It gathered customer and policy evidence, then reached the summary step. Kitaru forked that LangGraph checkpoint, changed the active candidate variant, and resumed. The earlier local tool calls were not repeated, but the downstream decision changed. That gives us a visible “candidate behavior changed after this fork point” report.

## Demo 3: run calls-mode for a better visual story

Graph-call mode records the outer `graph.invoke(...)` call. Calls-mode also records individual model/tool-shaped calls when they pass through Kitaru instrumentation.

Run the calls-mode version with a separate report path:

```bash
uv run --python 3.12 --extra local --extra langgraph-openai \
  examples/end_to_end/replay_verify_reference_agent/fork_demo.py \
  --scenario account_setting_change_request \
  --candidate nano_trimmed_permissions \
  --checkpoint-strategy calls \
  --report-path examples/end_to_end/replay_verify_reference_agent/reports/fork-demo-calls.html
```

Open it:

```bash
open examples/end_to_end/replay_verify_reference_agent/reports/fork-demo-calls.html
```

What changes in this version:

- The fork behavior is the same: baseline first, fork at the post-tool checkpoint, candidate variant downstream.
- Kitaru also captures model/tool-shaped events from the deterministic wrappers in `agent.py`.
- The report shows baseline and forked model/tool event counts.
- In the Kitaru execution, calls-mode checkpoints have names shaped like `model_call__...`, `tool_call__...`, and `langgraph_summary__...`.

This is the more visual version when you want to show individual tool/model checkpoints rather than one large graph-call checkpoint.

## What is real, and what is deliberately limited

Real in this demo:

- `kitaru.fork(...)` is a public dispatcher in Kitaru.
- `KitaruGraphRunner.fork(...)` is the LangGraph adapter implementation.
- LangGraph owns checkpoint history lookup, state update, fork checkpoint creation, and resume.
- Kitaru labels the run, captures execution metadata, and publishes the HTML report as an artifact.
- The local support tools really read/write SQLite and call a localhost HTTP API.
- The live trace-generation path really calls OpenAI and Langfuse when credentials are present.

Deliberately limited:

- The fork demo uses LangGraph’s in-memory checkpointer, not a Kitaru checkpointer.
- Kitaru does not yet claim arbitrary imported Langfuse traces can become executable LangGraph checkpoints.
- The deterministic fork demo changes the downstream variant after tool collection; it is not a full generic “change anything anywhere in any agent” product.
- Calls-mode only shows individual model/tool events when those calls pass through Kitaru’s instrumentation wrappers.

## Fixture files

`fixtures/langfuse_export.jsonl` and `fixtures/trace_generation_manifest.json` are offline copies of live traced runs. Later stages can use them without OpenAI or Langfuse credentials.

If those files contain an empty placeholder, no credentialed trace generation has been run in this checkout yet. Run `generate_traces.py` with the live environment variables above to replace them with real trace ids and local run outputs.
