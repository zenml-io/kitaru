# OpenAI research bot

This example ports the OpenAI research-bot investigation to Kitaru. It runs a small multi-stage workflow:

1. **Planner** — turns your question into a focused web-search plan.
2. **Search agents** — runs one OpenAI Agents SDK search agent per planned search.
3. **Writer** — turns the search summaries into a Markdown report.

The default model is `gpt-5-nano` to keep the example cheap enough to try. If you want a smarter and pricier run, pass `--model gpt-5-mini` or override a single stage with `--planner-model`, `--search-model`, or `--writer-model`.

## Quick start

```bash
cd examples/end_to_end/openai_research_bot
uv sync --extra local --extra openai-agents
uv run kitaru init
export OPENAI_API_KEY='sk-...'

uv run python research_bot.py "What should a small AI startup know about durable agents?" --max-searches 2
```

For a fuller run, omit `--max-searches 2`; the default is 5 searches. The CLI clamps the search budget to 1-10 so an accidental prompt cannot fan out into a very expensive example run.

The flow disables Kitaru's ordinary same-input cache on purpose. A fresh run of this example should perform fresh web searches, because web results are date-sensitive. Replay still reuses completed checkpoints from the source execution; that is the durability behavior demonstrated in the drill below.

## Useful flags

```bash
uv run python research_bot.py "AI agent durability" \
  --max-searches 4 \
  --model gpt-5-nano \
  --writer-model gpt-5-mini
```

| Flag | What it changes |
| --- | --- |
| `query` | The research question. |
| `--max-searches` | Maximum planned searches, clamped to 1-10. |
| `--model` | Default model for planner, search, writer, and the local web-search helper. Defaults to `gpt-5-nano`. |
| `--planner-model` | Only changes the planning agent. |
| `--search-model` | Only changes the search-summary agent and, by default, the search helper. |
| `--writer-model` | Only changes the final report writer. |
| `--search-tool-model` | Only changes the OpenAI Responses API model used inside the local `search_web` tool. |
| `--fail-on-search-error` | Exit non-zero if any search agent run fails. Useful for smoke tests. |
| `--secret-name` | Remote-stack secret name that contains `OPENAI_API_KEY`. Defaults to `openai-research-bot-keys`. |

## What happens in Kitaru

Picture the flow like this:

```text
@flow openai_research_bot
  ├── research_planner OpenAI run
  ├── normalize_search_plan              ← produces research_plan artifact
  ├── search_01                          ← submitted checkpoint
  ├── search_02                          ← submitted checkpoint
  ├── publish_search_summaries           ← produces search_summaries artifact
  ├── durability_drill_gate              ← optional intentional failure point
  ├── research_writer OpenAI run
  └── publish_report                     ← produces final_report + metadata artifacts
```

The planner and writer run at flow scope through `KitaruRunner` with `checkpoint_strategy="runner_call"`. The planned searches fan out with `run_search_item.submit(...)`, so each search is its own durable checkpoint and the writer only starts after the submitted search futures are collected.

## What to look for in the UI

Open the Kitaru UI and inspect the execution trace:

- `research_plan` shows the planner's exact list of searches.
- `search_summaries` shows the complete list passed to the writer, including any failed searches.
- `durability_drill` records the optional retry drill gate.
- `final_report` is the Markdown report you probably want to read first.
- `research_report_metadata` records the query, models, and search counts.

## Why replay helps

Imagine the run like a three-act play:

1. The planner writes the search script.
2. Search agents gather evidence.
3. The writer turns the evidence into the final report.

If the writer crashes after the searches finish, Kitaru does not have to pay for the planner and searches again. On replay, it can reuse the completed planner runner checkpoint and submitted search checkpoints, then continue from the broken part. That is the concrete benefit: fewer duplicate model calls, less wasted money, and a clearer place to debug.

## Try the durability drill

You can ask the example to fail after the search stage finishes:

```bash
export KITARU_RESEARCH_BOT_FAIL_AFTER_SEARCHES=1
uv run python research_bot.py "What should a small AI startup know about durable agents?" --max-searches 2
```

The run should fail at `durability_drill_gate`, after `research_plan`, the submitted search checkpoints, and `search_summaries` have already completed.

Copy the failed execution ID from the terminal or UI, then unset the flag and replay from the failure checkpoint:

```bash
unset KITARU_RESEARCH_BOT_FAIL_AFTER_SEARCHES
uv run kitaru executions replay <EXECUTION_ID> --from durability_drill_gate
```

`replay` creates a new execution from the failed one and asks Kitaru to reuse completed upstream checkpoints. `retry` tries to restart the same failed execution, which is not always available on server-backed stacks after a run has concluded. `resume` is for paused executions that are waiting for input.

On replay, Kitaru should reuse the completed planner/search outputs and continue from `durability_drill_gate` into the writer and final report.

## Why this example uses `runner_call`

This example is meant to show a clean end-to-end research workflow. The planner and writer are captured as durable runner checkpoints, while planned searches use submitted Kitaru checkpoints for fan-out. The script prints the named `final_report` artifact so the CLI stays simple even when multiple durable steps are terminal in the execution graph.

For a smaller example that compares `checkpoint_strategy="calls"` with `checkpoint_strategy="runner_call"`, see `examples/integrations/openai_agents_agent/`.

## Local and remote credentials

For local runs, use your shell:

```bash
export OPENAI_API_KEY='sk-...'
```

For remote stacks, store the key in a Kitaru secret:

```bash
kitaru secrets set openai-research-bot-keys --OPENAI_API_KEY=sk-...
```

When the active stack is remote, `research_bot.py` adds `secret_environment_from=["openai-research-bot-keys"]` to the run image. Only the secret name is sent with the run. The key value should not appear in flow parameters, logs, or artifacts.

## Why this example uses a local search tool

The original investigation used the OpenAI Agents SDK hosted `WebSearchTool`. This Kitaru port intentionally uses a local `@function_tool` named `search_web`, and that function calls the OpenAI Responses API with the `web_search` tool.

That choice makes the checkpoint story clearer for readers today: Kitaru can show local function tools cleanly, while hosted tools are currently less visible in the adapter trace.

## Known limitations

- This is a real web-search workload, so results can vary by date and by what sources the API finds.
- The example catches individual search failures and asks the writer to treat them as missing evidence, not as facts. Use `--fail-on-search-error` when you want the whole run to stop instead.
- It does not add memory or human approval yet. The point of this example is durable multi-stage OpenAI Agents execution.
