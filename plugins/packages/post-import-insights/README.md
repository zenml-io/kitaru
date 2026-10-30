# Kitaru post-import insights

Generate evidence-backed insight cards from imported Kitaru sessions. This distribution provides two independently selectable analyzers. Both profile every selected session and build deterministic candidates; the OpenAI analyzer then uses bounded analyst and editor calls to select candidates and write their descriptions.

| Catalog name | Entrypoint in `kitaru_post_import_insights.analyzer` | Required configuration |
|---|---|---|
| `kitaru/post-import-insights` | `analyze_post_import_sessions` | None |
| `kitaru/openai-post-import-insights` | `analyze_openai_post_import_sessions` | OpenAI connection supplying `OPENAI_API_KEY`; defaults to `gpt-5.6-luna` with low reasoning effort and accepts an optional `model` override |

Both entrypoints accept a list of session UUIDs for one agent and import, fetch each session with its nodes through `KitaruAPIClient`, and return `list[InsightInput]` for the task runner to persist. Each fetched trace is processed and released before the next fetch. Select either analyzer or both for an import. Each invocation persists its own cards; matching candidate names do not replace another analyzer's results.

The API client uses `KITARU_API_URL` and `KITARU_API_TOKEN`. Both analyzers accept `agent_name` for display context. The deterministic analyzer does not accept a `model` parameter or call OpenAI. The OpenAI analyzer uses `gpt-5.6-luna` with low reasoning effort when its `model` parameter is omitted; callers can override that parameter with a compatible model available to their OpenAI project. It fails when its credential is missing.

Install `kitaru-post-import-insights` for deterministic analysis, or `kitaru-post-import-insights[openai]` for OpenAI generation. The default catalog selects the corresponding requirement for each analyzer. The package does not depend on Langfuse or send insight-generation telemetry to it.

## Recorded input and activity

Language cards detect literal markers in selected text. A recorded `user` role does not prove that text came from a customer: some harnesses put agent policy in user-role messages. Importers that know the customer input should set `input_text_selector` on both the session and each relevant LLM node. For example, `/messages/1/content` selects the second message's content in a `messages` array. Choose selectors for the actual trace structure and preserve later customer turns; setting only the session selector leaves node input histories available to the profiler.

Tool activity counts separate tool nodes when present in a session. Otherwise, it counts explicit `tool_call` parts in assistant output messages, accepting a message, a message list, or a `messages` container. When this fallback contributes requests, the distribution is labeled as requests and states that execution is unverified. Input histories are not counted, and incomplete request scans are excluded. Tool error, retry, and result analysis still requires separate tool nodes.

The model-mix card counts recorded model identifiers. It prefers the served model field and falls back to the requested model field; different identifiers can refer to the same model.

## Cost, time, token, and tool-path cards

Both analyzers also compute these cards in the same single pass over every session. Each card appears only when its threshold is met, and its caveat says which sessions or nodes lacked the field it needs.

| Card | What it measures | Shown when |
|---|---|---|
| Cost concentration | The share of recorded cost carried by the costliest 10% of sessions with a recorded session cost | At least 20 sessions have a cost, and the costliest 10% carry at least 30% of it |
| Where time goes | For each tool name and model, the union of its node intervals in each session, clipped to the session's start and end, so overlapping parallel calls count once | At least 5 sessions have a positive duration and a timed tool or model node, and one component covers at least 25% of all recorded time |
| Context growth | Sessions whose last model call has at least 3 times the input of the first, and at least 4,000 tokens | At least 3 sessions grow, and they are at least 10% of sessions with two or more sized model calls |
| Oversized model inputs | Model calls with at least 5 times the median input of the agent's calls, and at least 10,000 tokens | At least 20 calls have an input count, and at least one call is oversized |
| Prompt cache use | The share of input tokens read from cache, over calls that report both input and cache-read counts; and system prompts, resolved through `system_prompt_selector`, that change between consecutive calls to the same model | At least 10 calls report cache counts, and either cache reads cover less than half of input, or the system prompt changes in at least 3 sessions and 10% of comparable sessions |
| Tool paths | Each session's tool names in order, with consecutive repeats collapsed; the chart shows the five most common paths and combines the rest | At least 10 sessions have tool nodes, and they take at least two paths |

Sources disagree on whether `input_tokens` includes cache reads: OpenAI-style usage counts them inside input tokens, while Anthropic-style usage, including the Claude Agent SDK adapter, reports them separately. Kitaru stores both as recorded. When a call reports more cache reads than input tokens, its source must count them separately, so every call to that model in the same session is read that way and its input size is input tokens plus cache reads. Elsewhere the convention is unknown: input size is input tokens alone, and the prompt cache card states the cache share as a range between the two readings.

System prompts are compared with the previous call to the same model in the session, and only the shared-prefix length is kept; prompt text never enters facts, evidence, or metadata. Cards with node evidence keep at most ten locators so that a copied prompt with 200 contributing sessions stays within its length limit.

## Release context

The exact `kitaru==0.25.0+dev` dependency requires the unreleased analyzer contract in [PR #988](https://github.com/zenml-io/kitaru/pull/988): analyzer tasks pass session UUIDs, and the plugin fetches normalized sessions through the SDK. Replace the development pin with the compatible published core release floor during release preparation. This package must not be published while the development pin remains.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.

## Investigation button labels

New cards persist `cta_label` beside `investigation_prompt` in `metadata["kitaru.insights/v1"]`. Both analyzers use a reviewed finding-type catalog, such as "Investigate tool retries" and "Investigate session duration", with "Investigate this finding" for unknown finding types. Labels describe the investigation topic, use `Investigate [topic]`, and stay within 40 characters. No additional model requests are made.

The field is optional for historical records and accepts nonblank strings up to 40 characters. Consumers should render it as text and fall back to "Investigate this finding" when the label is missing or unusable. The existing `investigation_prompt` remains unchanged. A missing prompt should disable the action. Frontend wiring is separate from this backend contract. Existing stored labels are not rewritten. Older strict metadata readers reject the new field, so update any independently deployed readers before enabling new writers; retain reader compatibility on rollback.
