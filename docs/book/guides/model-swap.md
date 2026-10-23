---
description: Compare models on your agent's own recorded calls for quality, cost, speed, and openness, and choose the right one.
icon: shuffle
---

# Choose the right model for your agent

The right model for an agent depends on what you need: better answers, lower cost, faster replies, or a model you can run anywhere instead of being locked to one provider. The `kitaru/model-swap` analyzer compares candidate models on your agent's own recorded traffic: an open model on a provider, a local model on Ollama, a cheaper or newer closed model, and the model you use today. It rebuilds each recorded model call from its stored messages, sends it to every candidate, and compares their answers. It runs on your worker with your keys: the analysis and your credentials stay on the worker, but replaying a call sends its recorded messages, including the system prompt and tool results, and its tool definitions to the candidate providers you choose, and to the judge when you set one. Use candidates where sending that trace content is acceptable.

The comparison shows where the candidates behave like your current model and where they differ. You then judge the differences, which decides quality, and confirm the finalists with a full [replay](replay-and-overrides.md) when your agent supports it.

This is step-level replay: Kitaru does not re-run your agent. Imported sessions store their model calls as `llm_call` nodes with their messages, answer, token usage, and cost, and some importers also store the tool definitions offered to the model (see [Track cost and model usage](llm-calls.md)). Each candidate sees the messages the recorded model saw at that step, except recorded reasoning parts, which are dropped. The comparison therefore works for any trace whose messages use a [supported shape](#supported-trace-shapes), including importer-backed agents, which cannot replay with a model override.

You get a **Best for** summary (closest to today, cheapest, fastest, best open model, and the finalists worth a full replay) and, per candidate:

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

Each candidate is an endpoint that speaks the OpenAI chat-completions or Responses API:

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
| `base_url` | Endpoint base URL, ending before `/chat/completions` or `/responses`. |
| `model` | Model name sent in each request. |
| `api_key_env` | Name of the environment variable holding the API key. Omit it for endpoints without authentication, such as a local Ollama. |
| `reference` | Mark the model your agent uses today. At most one candidate can be the reference. It is compared like any other candidate, and its agreement with its own recording shows how much answers vary from run to run, the noise floor for every other candidate. |
| `open_weights` | `true` for open-weight models you could run with any provider or on your own hardware. Shown in the comparison and used for **Best open model**. |
| `origin` | Where the model comes from and runs: `lab`, `lab_country`, `host`, `host_region`, as you declare them. Shown in the comparison and used by `excluded_origins`. |
| `price` | `input_per_mtok`, `output_per_mtok`, and optional `cached_input_per_mtok`, in USD per million tokens. When omitted, the price comes from the bundled [genai-prices](https://github.com/pydantic/genai-prices) table, and the report names the table version. |
| `price_provider` | Provider id for that lookup, such as `openai`, when the base URL does not identify the provider. |
| `provider_settings` | Per-provider request settings, described below. |
| `system_prompt` | A system prompt sent instead of the recorded one, to try a cleaned-up or rewritten prompt. It replaces every recorded system and developer message, including any per-call context the agent put there. The report notes which candidates use their own prompt, since their numbers measure the prompt and the model together. The reference cannot set one. |
| `system_prompt_nodes` | Node names whose calls get `system_prompt`; empty (the default) means every call. Set it when the agent's nodes (a router, a sub-agent, a summarizer) have different prompts. |

Include your current model as the reference. Without it, you cannot tell a candidate's disagreement from your current model's own variation.

A good first set covers the choices you care about: one open model on a provider, one local model if you run Ollama, one cheaper tier and one stronger model from your current provider.

A candidate is a setting, not only a model: the same model can appear twice with a different `reasoning_effort` (under `provider_settings`) or `system_prompt`, each under its own `name`. A cheaper effort level or a cleaned-up prompt on today's model is often the cheapest change worth trying.

Always add the model your agent uses today as the `reference`. Without it the report has no noise floor, says so in its notes, and cannot tell a real difference from run-to-run variation.

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
| `api` | `chat_completions` | Request shape. `responses` posts to OpenAI's `/responses` instead of `/chat/completions`. Use it when an OpenAI model rejects function tools with reasoning at `/chat/completions`, including GPT-5.6 Sol and Luna. Tool calls then come back natively. |
| `tool_call_format` | `native` | Where tool calls come back. `native` reads the `tool_calls` field. `hermes` parses `<tool_call>{...}</tool_call>` blocks from the text, for endpoints served without a tool-call parser. `json_content` parses the whole text as one `{"name", "arguments"}` object. |
| `reasoning_effort` | not sent | Sent as `reasoning_effort` (as `reasoning.effort` with the Responses API). |
| `max_tokens` | not sent | Output token limit per call. |
| `max_tokens_field` | `max_completion_tokens` | Request field for `max_tokens`; some servers only accept `max_tokens`. |
| `temperature` | not sent | Sampling temperature. |
| `extra_body` | `{}` | Extra fields merged into every request. Fields the analyzer sets itself are rejected: `model`, `messages`, `input`, `instructions`, `tools`, the recorded request controls (`tool_choice`, `parallel_tool_calls`, `response_format`, and `text.format` for the Responses API), the token limits (`max_tokens`, `max_completion_tokens`, `max_output_tokens`), `n`, and `stream`. Use `max_tokens` for the output limit, so the spend cap reserves what is actually sent. |
| `timeout_seconds` | `120` | Per-request timeout. |
| `max_retries` | `1` | Retries after a timeout, rate limit, or server error. Error responses are not priced, but timed-out attempts count against the spend cap at their reservation; retries are counted and shown next to cost. |

For example, a GPT-5.6 candidate using tools with medium reasoning can set `"provider_settings": {"api": "responses", "reasoning_effort": "medium"}`. Preserve the reasoning effort you intend to compare when selecting the API.

When a candidate's failure rate is high, the report's recommendation says to fix the provider setup first. Failures are never counted as disagreement. A reply cut off by the output token limit is its own failure, `truncated`, so a low `max_tokens` shows up as a setup problem rather than as the model answering differently.

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

Drop `dry_run` and set a spend cap. Before each call, the analyzer reserves its cost. For a candidate with `max_tokens`, the reservation is the full `max_tokens` output, so the cap holds; the analyzer refuses to start when a single call's reservation is already above the cap. Without `max_tokens`, the reservation is an estimate (four times the recorded output, at least 2048 tokens), so a reply far longer than that can push spend slightly past the cap. A call whose reservation does not fit waits for calls in flight to settle, and is not sent if it still cannot fit or the time budget ends first; the report counts the calls it did not send. A call cut off by the time budget, including one waiting between retries, is charged at its reservation, because the provider has usually billed it already. The same goes for work the provider may have billed without reporting usage: a timeout or broken connection after the request was sent, or a reply without usage, is charged its full reservation rather than counted as free, and the report notes how many there were. Each retry must reserve room for its own attempt, so a retry the cap cannot fit is not sent. A chat judge reserves its own `max_tokens`, so set one on the judge to keep its reservation small. Such charges appear in the run's total spend but not in a candidate's own spend, and a reply whose usage is unknown has no cost in the per-call figures. Replace the `...` placeholder with the same candidates as the dry run; the command does not run as written:

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
| `candidates` | none | Up to eight OpenAI-compatible candidates. Give candidates, decision candidates, or both. |
| `decision_candidates` | none | Up to four decision models that answer the decision parts of each call; see [Find calls a decision model could make](#find-calls-a-decision-model-could-make). |
| `dry_run` | `false` | Estimate without sending. |
| `max_spend_usd` | none | Spend cap across candidates, decision candidates, and the judge. Decision models need an explicit `price`. |
| `max_calls` | `100` | Recorded calls to sample, spread evenly across node names and models. |
| `tool_schemas` | `{}` | Tool name to the JSON schema of its parameters, for tools the trace did not record. |
| `tool_descriptions` | `{}` | Optional tool descriptions to go with `tool_schemas`. |
| `use_recorded_tools` | `true` | Use tool definitions recorded on each call. |
| `infer_missing_schemas` | `false` | Infer a loose schema from recorded arguments when no schema exists. |
| `sessions_per_month` | derived | Traffic for monthly projections. Derived from the import's time span when it covers at least a day. |
| `judge` | none | A judge that rules whether differing answers or arguments are equivalent: a decision model (see [Judge the differences quickly](#judge-the-differences-quickly)) or a candidate-shaped chat model. |
| `concurrency` | `4` | Requests in flight. |
| `time_budget_seconds` | `600` | Counted from the start of the analyzer task, including fetching sessions. When it ends, the analyzer stops fetching sessions and starting new calls, and cuts off calls still running, so the analysis returns results before the analyzer task timeout. The report counts sessions that were not fetched. |
| `max_disagreements` | `200` | Disagreements kept in the report. |
| `min_mix_calls` | `20` | Answered calls a candidate needs on a step before the best mix can move that step to it. |
| `repeats` | `1` | Times each call is sent to each candidate (up to 5). Above 1, the report shows how often any and all attempts matched; agreement still comes from the first attempt. |
| `excluded_origins` | none | Origins a candidate must not have, by `lab`, `lab_country`, `host`, or `host_region`, for example `{"lab_country": ["CN"]}`. Excluded candidates are still compared but never recommended. |

## Judge the differences quickly

Every disagreement still needs a human verdict to say which answer is right, and agents whose answers are free text can produce many. A judge reads each disagreement where a candidate called the same tool with different arguments, or gave a different text answer, and rules whether the two answers are equivalent. Different tool choices are not judged. Its ruling is shown next to strict agreement and orders the disagreements for review within each group: answers it found different first, then unsure or not judged ones, then likely equivalent ones. It never replaces your verdict, and it does not choose which disagreements the report keeps when `max_disagreements` cuts the list.

A decision model suits this job: it answers a fixed yes-or-no question with a probability instead of generating text, so it is fast and charges only for input. Three services are supported:

| `kind` | Service | Defaults |
|---|---|---|
| `jev` | TypeSafe Jev | `https://api.typesafe.ai`, key in `TYPESAFE_API_KEY`, model `jev-latest`. Access may require early access from TypeSafe. |
| `clef` | Cloudflare Clef on Workers AI, with the same request shape | No defaults for the URL or key: set `base_url` to `https://api.cloudflare.com/client/v4/accounts/ACCOUNT_ID/ai/run/@cf/cloudflare/clef` and `api_key_env` (for example `CLOUDFLARE_API_TOKEN`). Model `clef`, or `clef-flash`. |
| `openai_decisions` | OpenAI Decisions API (public beta) | `https://api.openai.com/v1`, key in `OPENAI_API_KEY`, model `gpt-6-luna`. |

```json
"judge": {
  "kind": "jev",
  "equivalent_at": 0.8,
  "price": {"input_per_mtok": "0.042", "output_per_mtok": "0"}
}
```

The judge returns the probability that the answers are equivalent. At or above `equivalent_at` (above 0.5, up to 1; default `0.8`) the ruling is "equivalent", at or below one minus it "different", and in between "unsure". The report shows the probability next to each disagreement and counts the rulings per candidate, along with judge requests that failed (for example a timeout, a refusal, or an unreadable answer); a failed request leaves that ruling open. `base_url`, `model`, `api_key_env`, and `timeout_seconds` override the defaults. When you set your own `base_url`, also name `api_key_env`, so a key is never sent to a custom URL without you choosing it; the same URL and key rules apply as for candidates.

The judge's key must be in the analyzer's provider connection or the worker environment (see [Keys](#keys)); a missing key stops the analysis before any call is sent. Judge requests count against `max_spend_usd` and `time_budget_seconds` like candidate calls, but the dry-run estimate covers candidates and decision candidates only, so leave headroom in the cap for the judge. Decision services are not in the bundled price table, so pass `price` (check the service's current pricing) when you set `max_spend_usd`. When a service does not report token usage, the request is charged at its estimate.

The judge sees the two answers as answer A and answer B and is never told which one is today's; which comes first varies from call to call. After the run, up to 20 of its rulings are asked again with the answers in the other order, and the report shows how many flipped, how many became unsure, and how many could not be asked because the time or spend budget ran out (`judge_check`). With at least 5 rechecked rulings, a judge that flips a fifth or more is flagged in the notes as weak evidence, and the notes also say when no recheck could run. Use a judge that is neither a candidate nor today's model: a model may favor answers like its own, and the notes say so when a chat judge runs the same model as a candidate or as the recording (provider prefixes such as `openai/` are ignored).

Decision models trail large models on judgments that need several reasoning steps, and text inside a recorded answer can sway them, so read their rulings as a way to order your review, not as verdicts. A candidate-shaped chat model can still be the judge instead: give `name`, `base_url`, `model`, and `api_key_env` without `kind`.

## Find calls a decision model could make

Many calls inside an agent are decisions rather than writing: which tool to call next, whether to escalate, which category or action applies. A decision model such as TypeSafe Jev, Cloudflare Clef, or OpenAI's Decisions API answers that kind of question with a probability, quickly and for input tokens only, but it cannot write text or invent values. Every report classifies each sampled call, without calling any service:

- **Decision:** the model chose one tool, and every argument it set (ignoring null ones) takes one of a fixed set of values in the tool's schema: an enum or a boolean, including optional and referenced ones. The call could move off an LLM.
- **Partial:** the choice of tool, or of answering in text, is a decision, and so are any such arguments, but at least one argument is free-form (a string, a number, or a value outside its enum) or the answer is text, and that part needs a generative model.
- **Generative:** there are no tools to choose from, the call made several tool calls at once, the recorded tool is not among the offered ones, the call offers too many tools, or its tool schemas were inferred from recorded arguments and so cannot say what the model chose from.

The report shows how many sampled calls, and what share of their recorded cost, fall in each class, and the main reasons calls are not fully decisions.

To try decision models on those calls, add them as `decision_candidates`:

```json
"decision_candidates": [
  {"name": "jev", "kind": "jev", "price": {"input_per_mtok": "0.042", "output_per_mtok": "0"}}
]
```

Each takes the same `kind`, `base_url`, `model`, `api_key_env`, `price`, and `timeout_seconds` as a decision judge (see [Judge the differences quickly](#judge-the-differences-quickly)), plus a `name`; its key goes in the provider connection like any other (see [Keys](#keys)). For every decision or partial call, the analyzer asks, in one request, which tool to call (or to answer in text) and which value each enum or boolean argument should take. It compares the answers with what the recorded model chose and reports, per decision candidate: how often it picked the same tool, how often all of its decisions matched, agreement per argument, failures, calls not run, cost per call for the decision parts, and latency. Savings are compared only on calls that are decisions throughout (cost per decision call next to the recorded model's cost on the same calls), because partial calls still need an LLM for the rest. You can run decision candidates without any `candidates`.

**What leaves the worker.** A decision candidate receives each call's rebuilt conversation, including the system prompt and tool results, plus the tool names and descriptions, argument names, and enum values. A decision judge receives the two answers it compares. Use decision models only where sending that trace content to the service is acceptable.

Agreement with the recorded model's choices is not proof that either is right; review disagreements as for any candidate. Moving a partial call means changing the agent: decide with the decision model, then generate the rest with an LLM. Decision candidate requests run before candidate calls, share `max_spend_usd` and `time_budget_seconds` with them, and are included in the dry-run estimate; give each a `price` when you set a spend cap.

## Mix models, set provenance rules, and read noise

**Best mix.** Many teams end up with a hybrid: an inexpensive model for most steps and a strong one where it matters. The report's **Best mix** table works per step, and names each step by what comes just before the call: the first turn, a user message, or the tool whose result the model is reading ("after lookup_order"). That is what a router can see before it picks a model; the tool the recorded model ended up calling is only known afterwards. Picking the best of many candidates on many steps finds some fits by luck, so the sessions are split in two by session id: each step is chosen on one half and kept only if the pick also holds on the other, held-out half. For each step it picks the cheapest allowed candidate that answered at least `min_mix_calls` (20) of the step's calls, failed on no more than a fifth of the step's calls it was sent, and whose rate of taking the same kind of action there (the same tools, or a text answer where the recording answered in text) on the first half is within 10 points of your current model's on the same calls, and then checks that its held-out rate is within 10 points of your current model's held-out rate (or of the first-half bar when there is no reference). Each step shows its held-out rate and `held_out` status: `held`, `failed` (the pick fell more than 10 points below, or failed on more than a fifth of the held-out calls, and the step stays on today's model), `too_few` (fewer than a quarter of `min_mix_calls` held-out calls, so the pick stays but is unconfirmed), or `no_split` (the step appears in only one half, so it was judged on all its calls and not checked). The mix also reports how many sessions fell in each half; with only a few sessions, most picks cannot be checked. When every pick fails on held-out sessions, the table still appears so you can see that fits were found but did not replicate. Steps with no candidate that fits, or too few calls to tell, stay on today's model, and the reference candidate is never picked, since it is today's model run again. The text match is deliberately coarse: any two text answers count as the same kind of action, so it is not semantic agreement, and a candidate that answers "I do not know" where the recording answered "yes" still matches. Review the disagreements, or use a judge, for what text answers say. A candidate's overall failure rate can hide a step where most of its calls fail, so each step counts attempted, answered, failed, and not-run calls per candidate, and the table names candidates kept off a step for failing too often. Today's cost comes from the reference's replay where the reference answered the step: recorded calls often hit a warm prompt cache that replayed single calls do not, so the recording would make today's model look cheaper than it is. Elsewhere it falls back to the recorded cost; each step says which, and a mix of both is labeled as not like for like. Sampling takes an even number of calls from each node name and model, so shares and costs are weighted by how much recorded traffic each sampled call stands for; a mix that picks one candidate everywhere costs what that candidate costs on its own. The table shows each step's share of calls and the combined cost per call and per month against today's, and **Best for** gains a "Best mix" row when at least one step can move. A mix is a shortlist: confirm it with a full replay before routing traffic.

**Small samples.** The Compare table shows each rate with its 95% interval. With a few dozen calls, candidates a few points apart are usually indistinguishable; sample more calls before reading a small gap as a difference.

**Provenance.** Price and openness are not the only constraints: some teams cannot use models from certain labs or hosted in certain regions. Declare each candidate's `origin` (`lab`, `lab_country`, `host`, `host_region`) and list what is not allowed in `excluded_origins`. Excluded candidates are still compared, so you can see what you give up, but they are never named in **Best for** or **Best mix**. Origins are what you declare; Kitaru does not verify them.

**Noise and repeats.** One answer per call is a noisy read on harder steps. With `repeats` set to 2 or more, each call is sent to each candidate that many times; agreement, the judge, and the disagreements still use the first attempt, and the comparison adds how often any attempt, and every attempt, matched the recording. Repeats multiply calls and spend, and the dry run shows that.

**Cost per task.** Per-session and monthly costs assume each candidate takes the same steps as the recording. A model that is cheaper per token but needs more turns, retries, or tool calls to finish a task costs more per task than these numbers show; the report says so, and leads with tokens per call (input, output, and reasoning) so token-hungry models stand out. A full [replay](replay-and-overrides.md) of the agent measures cost per task.

## Tool definitions

A model that chooses a tool can only be replayed fairly when the candidate sees the same tool definitions the recorded model saw. The analyzer takes them, in order, from:

1. **The recorded call.** The Langfuse, Logfire, and Phoenix importers store the tool definitions a trace carries, and the MLflow and LangSmith importers keep the raw request, so its `tools` are used when the request recorded them. The quickstart export carries all seven tool schemas, so every call is replayable without extra input.
2. **`tool_schemas`.** For traces without tool definitions, pass the schemas from your agent's code. A coding agent can read the code and produce them, and so can the framework itself. For a Pydantic AI agent, run it once against a `FunctionModel` and read `info.function_tools` and `info.output_tools`.
3. **Inferred schemas,** only with `infer_missing_schemas`. A loose schema built from the arguments the recorded calls used. Calls replayed this way are marked as inferred and left out of the headline numbers.

Supplied `tool_schemas` are offered on calls that used a tool or recorded tool definitions, and fill in any tool the call did not record. Calls with no sign of tools get none, because an import can also hold routers and summarizers that never had tools; a text-only step of a tool-using agent that recorded no tool definitions is therefore replayed without tools. Without any of these sources, calls that used a tool are skipped as `missing_tool_schema`, and the report says how many.

Agents recorded by a native adapter do not store tool definitions on their model calls today, so they need `tool_schemas` too.

## Read the report

`kitaru insight report` writes one self-contained HTML file with no external resources, so you can open it in a browser or hand it to your coding agent. It shows, in order:

- **Best for.** The report presents itself as stage 1 of 2: it compares single recorded calls, so it rules candidates out but does not pick a winner; replay the candidates worth a full replay, graded on outcomes, to decide. Within it: the candidate closest to today, then the cheapest and the fastest among comparable candidates, plus the best open-weight model. A candidate is comparable when its agreement rate is within 10 points of how often your current model matches its own recording, with both rates measured on the calls the two answered (each row shows them, for example "61% vs today's model's own 70%"); without a reference, the bar is the closest candidate's rate. Only candidates that answered at least 80% of their calls are ranked, and each pick shows how many calls it answered; a candidate that answered fewer, for example because the spend cap or time budget ran out, is listed under **Needs more measurement** instead of being ranked or ruled out, and a reference that answered too few sets no bar. Your current model is the baseline, never a pick: the cheapest and fastest picks must also be cheaper or faster than today's model. A row with no qualifying candidate says **No pick** and why, for example "No candidate within 10 points of today's model costs less than it", naming the cheapest candidate outside the band. When no candidate comes within 10 points, or today's model matches itself less than half the time, the cheapest and fastest rows name no pick, since step agreement cannot vouch for an equivalent; decide with full replays. Every report ends with **Worth a full replay**: every candidate except clear failures (those matching today's model less than half as often as the closest candidate), plus the cheapest candidate, to confirm end to end with your evaluator before switching. Step agreement ranks a cheap model low when it reaches the same outcome by a different route, so the screen only drops what clearly fails; whether a candidate keeps your outcomes only shows in full runs. Candidates failing more than 20% of calls are left out until their provider setup is fixed.
- **Compare.** Tool choice, exact match, judged equivalence, failure rate, cost per call and month, latency, and open weights for every candidate, with a one-line summary of each. Cost per call averages answered calls, weighted by how often each node name and model occurs; when a candidate has no answered call of some kind (for example because it failed them all), its per-call and monthly cost and its bar in the cost chart are marked partial, and it is not named cheapest.
- **Why the cost differs.** Prices, tokens per call, reasoning tokens, retries, and where each price came from.
- **Provider failures, agreement by recorded tool** (with per-argument agreement), and **coverage**.
- **Disagreements.** Grouped by what the recorded model did, each with both answers and a link to the session in the dashboard.

How to read the agreement numbers:

- **Tool choice** is the share of tool-using calls where the candidate picked the same tool. Arguments that hold free text, such as a customer reply, almost never match exactly, so tool choice and per-argument agreement on structured fields (an action, an amount) are usually the useful signals. **Agreement** is the strict share of calls with the same tool and arguments, or the same text.
- **Matching today is not the same as being right.** Agreement measures how closely a candidate behaves like your current model, which tells you where they differ, not which one is better. Review the disagreements to decide which answers are right; a candidate that differs can be the better choice.
- **Your current model sets the noise floor.** If it picks the same tool as its own recording 90% of the time, a candidate at 88% is within noise. When it rarely agrees with itself, the summaries say so. Its self-agreement measures more than run-to-run noise when the replay differs from the recorded call: the reference may resolve to a different model snapshot than an alias named in the recording, settings the recording did not keep (such as reasoning effort or temperature) are sent as you configure them, and recorded reasoning is dropped. Match the reference's `provider_settings` to how the agent called the model, and read low self-agreement as a sign that the replay differs, not only as noise.
- **Reasoning is not replayed.** Recorded reasoning (thinking) parts are not sent to candidates, so a candidate sees less context than the recorded model did on later steps. The report counts the affected calls.

After review, confirm the finalists end to end with a [replay experiment](replay-and-overrides.md) on the reviewed sessions when your agent was recorded through an adapter that supports model overrides.

Each run stores its own insights. The report renders the most recent model-swap run of the import, followed by the import's other insights. A card from an earlier run can still appear under **Other insights** when that run stored it under a name the latest run no longer uses. Session links use the dashboard URL the server reports when the analysis runs; they are left out when the server reports none. The full report is also available as JSON in the `kitaru.model_swap/v1` metadata of the `model-swap-summary` insight, for scripts and coding agents.

## Supported trace shapes

Messages are rebuilt from two shapes: OTel GenAI messages with typed parts (Langfuse and Logfire exports of Pydantic AI, Phoenix `gen_ai.*` spans) and OpenAI chat messages (MLflow, Braintrust, LangSmith OpenAI wrappers, most Kitaru JSONL files). Anything else is skipped and counted rather than guessed, including Anthropic-format messages, LangChain-serialized messages, images and other non-text content, failed recorded calls, calls recorded without inputs, and calls whose tool calls or results have no ids (skipped as `missing_tool_call_id`). IDs longer than 64 characters are shortened consistently in rebuilt requests, keeping calls paired with their results and leaving the recording unchanged. When a recording keeps the system prompt as OTel GenAI `gen_ai.system_instructions` in the span attributes rather than in the messages, it is replayed as the system message. Calls without a system or developer message are counted in a report note; the first user message may carry the instructions. A recorded `tool_choice`, `parallel_tool_calls`, `response_format`, and function `strict` flag are sent with the call in both the chat-completions and Responses API shapes, since they constrain what the model may answer. A Pydantic AI call recorded with `allow_text_output: false` in its `model_request_parameters` (kept by the Langfuse, Logfire, and Phoenix importers) is sent with `tool_choice: required`, as Pydantic AI sent it, unless the recording names another `tool_choice`. A call whose control has another form (such as legacy `functions`, or a `tool_choice` naming a tool that is not offered) is skipped as `unsupported_request_control`, and a call offering a tool that is not a function definition (for example an OpenAI `custom` tool or a provider's built-in tool) is skipped as `unsupported_tool` rather than replayed with fewer tools. Functions recorded without parameters are offered with an empty schema. A recorded answer stored as a string is read as a response only when it is a recognized response shape; any other string, including plain text that happens to be valid JSON, is the answer's text.

## Limits

The analysis runs inside one analyzer task, which times out after 15 minutes without partial results. `time_budget_seconds` keeps it inside that window, so keep it comfortably under 15 minutes, leaving time to store the results; for many calls or slow candidates, lower `max_calls` or raise `concurrency`. The analyzer needs network access from the worker to every candidate endpoint, so a local Ollama candidate requires a worker on the same machine.
