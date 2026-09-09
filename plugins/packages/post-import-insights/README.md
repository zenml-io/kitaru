# Kitaru post-import insights

Generate evidence-backed insight cards from imported Kitaru sessions. This distribution provides two independently selectable analyzers. Both profile every selected session and build deterministic candidates; the OpenAI analyzer then uses bounded analyst and editor calls to select candidates and write their descriptions.

| Catalog name | Entrypoint in `kitaru_post_import_insights.analyzer` | Required configuration |
|---|---|---|
| `kitaru/post-import-insights` | `analyze_post_import_sessions` | None |
| `kitaru/openai-post-import-insights` | `analyze_openai_post_import_sessions` | OpenAI connection supplying `OPENAI_API_KEY`; defaults to `gpt-5.6-luna` with low reasoning effort and accepts an optional `model` override |

Both entrypoints accept a list of session UUIDs for one agent and import, fetch each session with its nodes through `KitaruAPIClient`, and return `list[InsightInput]` for the task runner to persist. Each fetched trace is processed and released before the next fetch. Select either analyzer or both for an import. Each invocation persists its own cards; matching candidate names do not replace another analyzer's results.

The API client uses `KITARU_API_URL` and `KITARU_API_TOKEN`. Both analyzers accept `agent_name` for display context. The deterministic analyzer does not accept a `model` parameter or call OpenAI. The OpenAI analyzer uses `gpt-5.6-luna` with low reasoning effort when its `model` parameter is omitted; callers can override that parameter with a compatible model available to their OpenAI project. It fails when its credential is missing.

Install `kitaru-post-import-insights` for deterministic analysis, or `kitaru-post-import-insights[openai]` for OpenAI generation. The default catalog selects the corresponding requirement for each analyzer. The package does not depend on Langfuse or send insight-generation telemetry to it.

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
