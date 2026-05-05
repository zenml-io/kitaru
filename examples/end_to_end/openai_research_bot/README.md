# OpenAI research bot

This example ports the OpenAI research-bot investigation to Kitaru. It runs a small multi-agent workflow:

1. **Planner** — turns your question into a focused web-search plan.
2. **Parallel searches** — runs each planned search as its own Kitaru checkpoint.
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

## Useful flags

```bash
uv run python research_bot.py "AI agent durability" \
  --max-searches 4 \
  --model gpt-5-nano \
  --writer-model gpt-5-mini \
  --strategy calls
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
| `--strategy` | OpenAI adapter checkpoint strategy for planner/writer: `calls` or `runner_call`. Defaults to `calls`. |
| `--compare-runner-call` | Runs a second pass with `runner_call` so you can compare checkpoint shapes. This costs more. |
| `--fail-on-search-error` | Exit non-zero if any parallel search checkpoint fails. Useful for smoke tests. |
| `--secret-name` | Remote-stack secret name that contains `OPENAI_API_KEY`. Defaults to `openai-research-bot-keys`. |

## What happens in Kitaru

Picture the flow like this:

```text
@flow openai_research_bot
  ├── planner OpenAI run                 ← call-level checkpoints when --strategy=calls
  ├── publish_research_plan              ← produces research_plan artifact
  ├── search_01_<query_slug>             ← one durable checkpoint per search
  ├── search_02_<query_slug>             ← runs in parallel with the other searches
  ├── publish_search_summaries           ← produces search_summaries artifact
  ├── durability_drill_gate              ← optional intentional failure point
  ├── writer OpenAI run                  ← call-level checkpoints when --strategy=calls
  └── publish_report                     ← produces final_report + metadata artifacts
```

The search stage is the most important teaching piece. Each search is like a separate sealed envelope. If search 3 fails, searches 1 and 2 do not need to be thrown away. Kitaru has already saved them.

## What to look for in the UI

Open the Kitaru UI and inspect the execution trace:

- `research_plan` shows the planner's exact list of searches.
- Each `search_XX_...` checkpoint shows one search task and its result.
- `search_summaries` shows the complete list passed to the writer, including any failed searches.
- `durability_drill` records the optional retry drill gate.
- `final_report` is the Markdown report you probably want to read first.
- `research_report_metadata` records the query, models, checkpoint strategy, and search counts.

## Why replay helps

Imagine the run like a three-act play:

1. The planner writes the search script.
2. Several search actors gather evidence at the same time.
3. The writer turns the evidence into the final report.

If the writer crashes after the searches finish, Kitaru does not have to pay for the planner and searches again. On replay, it can reuse those completed checkpoints and continue from the broken part. That is the concrete benefit: fewer duplicate model calls, less wasted money, and a clearer place to debug.

## Try the durability drill

You can ask the example to fail after the parallel searches finish:

```bash
export KITARU_RESEARCH_BOT_FAIL_AFTER_SEARCHES=1
uv run python research_bot.py "What should a small AI startup know about durable agents?" --max-searches 2
```

The run should fail at `durability_drill_gate`, after `research_plan`, both `search_XX_...` checkpoints, and `search_summaries` have already completed.

Copy the failed execution ID from the terminal or UI, then unset the flag and retry the same execution:

```bash
unset KITARU_RESEARCH_BOT_FAIL_AFTER_SEARCHES
uv run kitaru executions retry <EXECUTION_ID>
```

`retry` is the right command for failed executions. `resume` is for paused executions that are waiting for input. On retry, Kitaru should reuse the completed planner/search checkpoints and continue from the failure gate into the writer and final report.

## `calls` vs `runner_call`

The default is:

```bash
--strategy calls
```

That means the planner and writer run from flow scope, and the OpenAI adapter can show inner model/tool calls as child checkpoints.

The parallel search checkpoints use `checkpoint_strategy="runner_call"` internally on purpose. They are already inside Kitaru checkpoints, so opening more call-level checkpoints inside them would be nesting checkpoints inside checkpoints. Instead, each search is one durable unit.

If you pass:

```bash
--strategy runner_call
```

planner and writer become coarser: each whole OpenAI run is one checkpoint. That is less detailed, but sometimes easier to scan.

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
