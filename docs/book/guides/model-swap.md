---
description: Compare models on your agent's own recorded calls for quality, cost, speed, and openness, and choose the right one.
icon: shuffle
---

# Choose the right model for your agent

The right model for an agent depends on what you need: better answers, lower cost, faster replies, or a model you can run anywhere instead of being locked to one provider. The `kitaru/model-swap` analyzer compares candidate models on your agent's own recorded traffic: an open model on a provider, a local model on Ollama, a cheaper or newer closed model, and the model you use today. It rebuilds each recorded model call from its stored messages, sends it to every candidate, and compares their answers. It runs on your worker with your keys, so your traces and credentials stay on your side.

The comparison shows where the candidates behave like your current model and where they differ. You then judge the differences, which decides quality, and confirm the finalists with a full [replay](replay-and-overrides.md) when your agent supports it.

This is step-level replay: Kitaru does not re-run your agent. Imported sessions store their model calls as `llm_call` nodes with their messages, answer, token usage, and cost, and some importers also store the tool definitions offered to the model (see [Track cost and model usage](llm-calls.md)). Each candidate sees the messages the recorded model saw at that step, except recorded reasoning parts, which are dropped. The comparison therefore works for any trace whose messages use a [supported shape](#supported-trace-shapes), including importer-backed agents, which cannot replay with a model override.

You get a **Best for** summary (closest to today, cheapest, fastest, best open model) and, per candidate:

- **Agreement.** Whether it called the same tool with the same arguments as your current model, or gave the same text answer, plus per-argument agreement for each tool.
- **Provider failures.** Timeouts, rate limits, HTTP errors, empty replies, tool-call parse errors, and refusals, counted apart from disagreement, because they say more about the provider setup than about the model.
- **Speed and tokens.** Median and 95th-percentile latency, and input, cached-input, output, and reasoning tokens per call.
- **Cost.** Cost per call, projected per session and per month, explained by price per token and tokens used, with the number of retries shown next to it. Cost per call averages only the calls the candidate answered, weighted by how often each node name and model occurs in the import; failed attempts are not priced.
- **Openness.** Whether the model's weights are open, as you declare it.
- **Coverage.** How many recorded calls were seen, sampled, replayable, and skipped, and why.

## Before you start

You need a Kitaru server, a worker that claims analyzer tasks, and an import. [Importing sessions](importing-sessions.md) covers the agent and import setup, and each trace source has its own import guide.

```bash
uv add "kitaru[cli,worker]"
uv run kitaru login --local
uv run kitaru worker start --claim importer --claim analyzer
```

The examples below use the quickstart traces in `examples/python/pydantic_ai_ticket_resolver/traces/langfuse-traces.jsonl` (10 sessions, 33 model calls by `gpt-5-nano`). From that directory, register the agent as its README describes, then import:

```bash
uv run kitaru session import traces/langfuse-traces.jsonl \
  --importer kitaru/langfuse@latest \
  --agent returns-resolver@1 \
  --params '{"source_instance":"demo"}' \
  --media-type application/x-ndjson \
  --wait
```

Note the `import_id` in the output; the analysis and the report use it. If you already imported these traces, for example while following the quickstart, reuse that import instead of importing again: `uv run kitaru import list` shows existing imports and their ids.

## Choose candidates

Each candidate is any endpoint that speaks the OpenAI chat-completions API:

```json
{
  "name": "qwen3-local",
  "base_url": "http://localhost:11434/v1",
  "model": "qwen3:8b",
  "price": {"input_per_mtok": "0", "output_per_mtok": "0"}
}
```

| Field | Meaning |
|---|---|
| `name` | Label in the report. Must be unique. |
| `base_url` | Endpoint base URL, ending before `/chat/completions`. |
| `model` | Model name sent in each request. |
| `api_key_env` | Name of the environment variable holding the API key. Omit it for endpoints without authentication, such as a local Ollama. |
| `reference` | Mark the model your agent uses today. At most one candidate can be the reference. It is compared like any other candidate, and its agreement with its own recording shows how much answers vary from run to run, the noise floor for every other candidate. |
| `open_weights` | `true` for open-weight models you could run with any provider or on your own hardware. Shown in the comparison and used for **Best open model**. |
| `price` | `input_per_mtok`, `output_per_mtok`, and optional `cached_input_per_mtok`, in USD per million tokens. When omitted, the price comes from the bundled [genai-prices](https://github.com/pydantic/genai-prices) table, and the report names the table version. |
| `price_provider` | Provider id for that lookup, such as `openai`, when the base URL does not identify the provider. |
| `provider_settings` | Per-provider request settings, described below. |

Include your current model as the reference. Without it, you cannot tell a candidate's disagreement from your current model's own variation.

A good first set covers the choices you care about: one open model on a provider, one local model if you run Ollama, one cheaper tier and one stronger model from your current provider.

### Keys

API keys never go in the analyzer parameters. Each candidate names the variable its key lives in, and the worker supplies the value. Put the keys in a provider connection for the `model-swap` provider, and mark it the default so the analyzer picks it up without naming it:

```bash
uv run kitaru connection create model-swap-keys \
  --provider model-swap \
  --set-secret OPENAI_API_KEY=sk-... \
  --set-secret OPENROUTER_API_KEY=sk-or-... \
  --default
```

Alternatively, set the variables in the worker's environment. See [Provider connections](provider-connections.md) for how values are merged.

`api_key_env` cannot name a `KITARU_*` variable, so a candidate cannot send the worker's own Kitaru credentials to an endpoint. A key is sent only over `https`, unless the host is `localhost`, `127.0.0.1`, or `::1`, such as a local Ollama. `base_url` must not contain credentials; put the key in the variable `api_key_env` names instead.

Any analyzer can read the worker's environment, not just model-swap. Run model-swap on a worker whose environment holds only the keys you intend to share with the candidate endpoints.

### Provider settings

The same open model can behave very differently at different providers, and a wrong tool-call format can make almost every call fail. `provider_settings` adjusts each candidate:

| Setting | Default | Meaning |
|---|---|---|
| `tool_call_format` | `native` | Where tool calls come back. `native` reads the `tool_calls` field. `hermes` parses `<tool_call>{...}</tool_call>` blocks from the text, for endpoints served without a tool-call parser. `json_content` parses the whole text as one `{"name", "arguments"}` object. |
| `reasoning_effort` | not sent | Sent as `reasoning_effort`. |
| `max_tokens` | not sent | Output token limit per call. |
| `max_tokens_field` | `max_completion_tokens` | Request field for `max_tokens`; some servers only accept `max_tokens`. |
| `temperature` | not sent | Sampling temperature. |
| `extra_body` | `{}` | Extra fields merged into every request. |
| `timeout_seconds` | `120` | Per-request timeout. |
| `max_retries` | `1` | Retries after a timeout, rate limit, or server error. Failed attempts are not priced; retries are counted and shown next to cost. |

When a candidate's failure rate is high, the report's recommendation says to fix the provider setup first. Failures are never counted as disagreement.

## Estimate first

A dry run sends nothing. It reports how many calls would be sent to each candidate and the estimated spend, based on the recorded token counts:

```bash
uv run kitaru import analyze IMPORT_ID \
  --analyzer kitaru/model-swap@latest \
  --analyzer-params 'kitaru/model-swap@latest={
    "dry_run": true,
    "candidates": [
      {"name": "gpt-5-nano", "base_url": "https://api.openai.com/v1",
       "model": "gpt-5-nano", "api_key_env": "OPENAI_API_KEY", "reference": true},
      {"name": "qwen3-local", "base_url": "http://localhost:11434/v1",
       "model": "qwen3:8b", "price": {"input_per_mtok": "0", "output_per_mtok": "0"}}
    ]
  }' \
  --wait
uv run kitaru insight report --import-id IMPORT_ID --out estimate.html
```

## Run the comparison

Drop `dry_run` and set a spend cap. Before each call, the analyzer reserves a high estimate of its cost: four times the recorded output, at least 2048 tokens, and never more than the candidate's `max_tokens`. A call whose reservation does not fit waits for calls in flight to settle, and is not sent if it still cannot fit; the report counts the calls it did not send. Reservations are estimates, so a reply far longer than reserved can push spend slightly past the cap; set `max_tokens` on each candidate to bound every reply. A call cut off by the time budget is charged at its reservation, because the provider has usually billed it already. Replace the `...` placeholder with the same candidates as the dry run; the command does not run as written:

```bash
uv run kitaru import analyze IMPORT_ID \
  --analyzer kitaru/model-swap@latest \
  --analyzer-params 'kitaru/model-swap@latest={
    "max_spend_usd": "2",
    "candidates": [ ... ]
  }' \
  --wait
uv run kitaru insight report --import-id IMPORT_ID --out report.html
```

A spend cap needs a price for every candidate; the analyzer refuses to start when one is unknown, rather than spending without a limit. You can also select the analyzer on the import itself with `kitaru session import ... --analyzer kitaru/model-swap@latest --analyzer-params ...`.

### Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `candidates` | required | One to eight candidates. |
| `dry_run` | `false` | Estimate without sending. |
| `max_spend_usd` | none | Spend cap across all candidates and the judge. |
| `max_calls` | `100` | Recorded calls to sample, spread evenly across node names and models. |
| `tool_schemas` | `{}` | Tool name to the JSON schema of its parameters, for tools the trace did not record. |
| `tool_descriptions` | `{}` | Optional tool descriptions to go with `tool_schemas`. |
| `use_recorded_tools` | `true` | Use tool definitions recorded on each call. |
| `infer_missing_schemas` | `false` | Infer a loose schema from recorded arguments when no schema exists. |
| `sessions_per_month` | derived | Traffic for monthly projections. Derived from the import's time span when it covers at least a day. |
| `judge` | none | A candidate-shaped model that rules whether differing answers or arguments are equivalent. |
| `concurrency` | `4` | Requests in flight. |
| `time_budget_seconds` | `600` | Counted from the start of the analyzer task, including fetching sessions. When it ends, the analyzer stops fetching sessions and starting new calls, and cuts off calls still running, so the analysis returns results before the analyzer task timeout. The report counts sessions that were not fetched. |
| `max_disagreements` | `200` | Disagreements kept in the report. |

## Tool definitions

A model that chooses a tool can only be replayed fairly when the candidate sees the same tool definitions the recorded model saw. The analyzer takes them, in order, from:

1. **The recorded call.** The Langfuse, Logfire, and Phoenix importers store the tool definitions a trace carries, and the MLflow and LangSmith importers keep the raw request, so its `tools` are used when the request recorded them. The quickstart export carries all seven tool schemas, so every call is replayable without extra input.
2. **`tool_schemas`.** For traces without tool definitions, pass the schemas from your agent's code. A coding agent can read the code and produce them, and so can the framework itself. For a Pydantic AI agent, run it once against a `FunctionModel` and read `info.function_tools` and `info.output_tools`.
3. **Inferred schemas,** only with `infer_missing_schemas`. A loose schema built from the arguments the recorded calls used. Calls replayed this way are marked as inferred and left out of the headline numbers.

Supplied `tool_schemas` are offered on calls that used a tool or recorded tool definitions, and fill in any tool the call did not record. Calls with no sign of tools get none, because an import can also hold routers and summarizers that never had tools; a text-only step of a tool-using agent that recorded no tool definitions is therefore replayed without tools. Without any of these sources, calls that used a tool are skipped as `missing_tool_schema`, and the report says how many.

Agents recorded by a native adapter do not store tool definitions on their model calls today, so they need `tool_schemas` too.

## Read the report

`kitaru insight report` writes one self-contained HTML file with no external resources, so you can open it in a browser or hand it to your coding agent. It shows, in order:

- **Best for.** The closest match to today, then the cheapest and the fastest among candidates whose agreement rate is within 10 points of that closest match, plus the best open-weight model. The closest match can be your current model itself when it is a candidate, since it is compared like any other. Candidates failing more than 20% of calls are left out until their provider setup is fixed.
- **Compare.** Tool choice, exact match, judged equivalence, failure rate, cost per call and month, latency, and open weights for every candidate, with a one-line summary of each. Cost per call averages answered calls, weighted by how often each node name and model occurs; when a candidate failed every call of one kind, its cost is marked partial and it is not named cheapest.
- **Why the cost differs.** Prices, tokens per call, reasoning tokens, retries, and where each price came from.
- **Provider failures, agreement by recorded tool** (with per-argument agreement), and **coverage**.
- **Disagreements.** Grouped by what the recorded model did, each with both answers and a link to the session in the dashboard.

How to read the agreement numbers:

- **Tool choice** is the share of tool-using calls where the candidate picked the same tool. Arguments that hold free text, such as a customer reply, almost never match exactly, so tool choice and per-argument agreement on structured fields (an action, an amount) are usually the useful signals. **Agreement** is the strict share of calls with the same tool and arguments, or the same text.
- **Matching today is not the same as being right.** Agreement measures how closely a candidate behaves like your current model, which tells you where they differ, not which one is better. Review the disagreements to decide which answers are right; a candidate that differs can be the better choice.
- **Your current model sets the noise floor.** If it picks the same tool as its own recording 90% of the time, a candidate at 88% is within noise. When it rarely agrees with itself, the summaries say so.
- **Reasoning is not replayed.** Recorded reasoning (thinking) parts are not sent to candidates, so a candidate sees less context than the recorded model did on later steps. The report counts the affected calls.

After review, confirm the finalists end to end with a [replay experiment](replay-and-overrides.md) on the reviewed sessions when your agent was recorded through an adapter that supports model overrides.

Each run stores its own insights. The report renders the most recent model-swap run of the import, followed by the import's other insights. A card from an earlier run can still appear under **Other insights** when that run stored it under a name the latest run no longer uses. Session links use the dashboard URL the server reports when the analysis runs; they are left out when the server reports none. The full report is also available as JSON in the `kitaru.model_swap/v1` metadata of the `model-swap-summary` insight, for scripts and coding agents.

## Supported trace shapes

Messages are rebuilt from two shapes: OTel GenAI messages with typed parts (Langfuse and Logfire exports of Pydantic AI, Phoenix `gen_ai.*` spans) and OpenAI chat messages (MLflow, Braintrust, LangSmith OpenAI wrappers, most Kitaru JSONL files). Anything else is skipped and counted rather than guessed, including Anthropic-format messages, LangChain-serialized messages, images and other non-text content, failed recorded calls, calls recorded without inputs, and calls whose tool calls or results have no ids (skipped as `missing_tool_call_id`). When a recording keeps the system prompt as OTel GenAI `gen_ai.system_instructions` in the span attributes rather than in the messages, it is replayed as the system message; calls with no system prompt at all are counted in a report note.

## Limits

The analysis runs inside one analyzer task, which times out after 15 minutes without partial results. `time_budget_seconds` keeps it inside that window, so keep it comfortably under 15 minutes, leaving time to store the results; for many calls or slow candidates, lower `max_calls` or raise `concurrency`. The analyzer needs network access from the worker to every candidate endpoint, so a local Ollama candidate requires a worker on the same machine.
