---
description: Send your agent's recorded model calls to other models and see what would change, and what it would cost, before you switch.
icon: shuffle
---

# Find out what changes if you switch models

Your agent runs on one model in production, and you want to know what happens if its calls move to another one: an open model on a provider, a local model on Ollama, or a cheaper closed tier. The `kitaru/model-swap` analyzer answers that from your own traces. It sends each recorded model call, exactly as it was recorded, to the candidate models you name and compares their answers with the recording. It runs on your worker with your keys, so your traces and credentials stay on your side.

This is step-level replay: Kitaru does not re-run your agent. Every imported session already stores its model calls as `llm_call` nodes with their messages, tool definitions, answer, token usage, and cost (see [Track cost and model usage](llm-calls.md)). Each candidate sees the same messages the recorded model saw at that step, so the comparison works for agents imported from any supported trace source, including importer-backed agents, which cannot replay with a model override.

You get, per candidate:

- **Agreement.** Whether it called the same tool with the same arguments, or gave the same text answer, plus per-argument agreement for each tool.
- **Provider failures.** Timeouts, rate limits, HTTP errors, empty replies, tool-call parse errors, and refusals, counted apart from disagreement, because they say more about the provider setup than about the model.
- **Latency and tokens.** Input, cached-input, output, and reasoning tokens per call.
- **Cost.** Cost per call, projected per session and per month, split into price per token, tokens used, and retries.
- **Coverage.** How many recorded calls were seen, sampled, replayed, and skipped, and why.

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

Note the `import_id` in the output; the analysis and the report use it.

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
| `reference` | Mark one candidate as the production model. Its agreement with its own recording shows how much the model varies from run to run, which is the noise floor for every other candidate. |
| `price` | `input_per_mtok`, `output_per_mtok`, and optional `cached_input_per_mtok`, in USD per million tokens. When omitted, the price comes from the bundled [genai-prices](https://github.com/pydantic/genai-prices) table, and the report names the table version. |
| `price_provider` | Provider id for that lookup, such as `openai`, when the base URL does not identify the provider. |
| `provider_settings` | Per-provider request settings, described below. |

Include the production model as the reference. Without it, you cannot tell a candidate's disagreement from the production model's own variation.

A good first set is one open model on a provider, one local model if you run Ollama, and one cheaper tier of your current provider.

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
| `max_retries` | `1` | Retries after a timeout, rate limit, or server error. Retries count toward cost and are reported. |

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

Drop `dry_run` and set a spend cap. The analyzer stops sending calls before its estimated spend would pass `max_spend_usd`, and the report counts the calls it did not send:

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
| `time_budget_seconds` | `600` | Stop sending calls after this long, so the analysis returns results before the analyzer task timeout. |
| `max_disagreements` | `200` | Disagreements kept in the report. |

## Tool definitions

A model that chooses a tool can only be replayed fairly when the candidate sees the same tool definitions the recorded model saw. The analyzer takes them, in order, from:

1. **The recorded call.** The Langfuse, Logfire, and Phoenix importers store the tool definitions a trace carries, and the MLflow and LangSmith importers keep the request's `tools`. The quickstart export carries all seven tool schemas, so every call is replayable without extra input.
2. **`tool_schemas`.** For traces without tool definitions, pass the schemas from your agent's code. A coding agent can read the code and produce them, and so can the framework itself. For a Pydantic AI agent, run it once against a `FunctionModel` and read `info.function_tools` and `info.output_tools`.
3. **Inferred schemas,** only with `infer_missing_schemas`. A loose schema built from the arguments the recorded calls used. Calls replayed this way are marked as inferred and left out of the headline numbers.

Without any of these, calls that used a tool are skipped as `missing_tool_schema`, and the report says how many.

Agents recorded by a native adapter do not store tool definitions on their model calls today, so they need `tool_schemas` too.

## Read the report

`kitaru insight report` writes one self-contained HTML file with no external resources, so you can open it in a browser or hand it to your coding agent. It shows, in order:

- **Candidates.** Agreement, tool choice, judged equivalence, failure rate, cost per call, session and month, and latency.
- **Recommendation.** A rule-based verdict per candidate, read against the reference.
- **Why the cost differs.** Prices, tokens per call, reasoning tokens, retries, and where each price came from.
- **Provider failures, agreement by recorded tool** (with per-argument agreement), and **coverage**.
- **Disagreements.** Grouped by what the recorded model did, each with both answers and a link to the session in the dashboard.

How to read the agreement numbers:

- **Tool choice** is the share of tool-using calls where the candidate picked the same tool. Arguments that hold free text, such as a customer reply, almost never match exactly, so tool choice and per-argument agreement on structured fields (an action, an amount) are usually the useful signals. **Agreement** is the strict share of calls with the same tool and arguments, or the same text.
- **The reference sets the bar.** If the production model picks the same tool as its own recording 90% of the time, a candidate at 88% is within noise. When the reference itself rarely agrees, the recommendation says so.
- **Reasoning is not replayed.** Recorded reasoning (thinking) parts are not sent to candidates, so a candidate sees less context than the recorded model did on later steps. The report counts the affected calls.

Each run stores its own insights. The report renders the most recent model-swap run of the import, followed by the import's other insights.

## Supported trace shapes

Messages are rebuilt from two shapes: OTel GenAI messages with typed parts (Langfuse and Logfire exports of Pydantic AI, Phoenix `gen_ai.*` spans) and OpenAI chat messages (MLflow, Braintrust, LangSmith OpenAI wrappers, most Kitaru JSONL files). Anything else is skipped and counted rather than guessed, including Anthropic-format messages, LangChain-serialized messages, images and other non-text content, failed recorded calls, and calls recorded without inputs.

## Limits

The analysis runs inside one analyzer task, which times out after 15 minutes without partial results. `time_budget_seconds` keeps it inside that window; for many calls or slow candidates, lower `max_calls` or raise `concurrency`. The analyzer needs network access from the worker to every candidate endpoint, so a local Ollama candidate requires a worker on the same machine.
