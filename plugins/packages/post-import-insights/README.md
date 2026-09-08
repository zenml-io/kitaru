# Kitaru post-import insights

Generate evidence-backed insight cards from imported Kitaru sessions. This independent analyzer package profiles every selected session, builds deterministic candidates, and optionally uses OpenAI to select and edit the resulting cards.

The analyzer entrypoint is `kitaru_post_import_insights.analyzer:analyze_post_import_sessions`. It accepts a list of session UUIDs for one agent and import, fetches each session with its nodes through `KitaruAPIClient`, and returns `list[InsightInput]` for the task runner to persist. Each fetched trace is processed and released before the next fetch.

The API client uses `KITARU_API_URL` and `KITARU_API_TOKEN`. Optional analyzer parameters are `agent_name` for display context, `model` for OpenAI generation, and `observe` for metadata-only Langfuse telemetry. Without a model, generation is deterministic. Model-backed generation requires `OPENAI_API_KEY`. Telemetry uses the dedicated `KITARU_INSIGHTS_LANGFUSE_PUBLIC_KEY`, `KITARU_INSIGHTS_LANGFUSE_SECRET_KEY`, and `KITARU_INSIGHTS_LANGFUSE_BASE_URL` settings and is best effort.

## Release context

The exact `kitaru==0.25.0+dev` dependency requires the unreleased analyzer contract in [PR #988](https://github.com/zenml-io/kitaru/pull/988): analyzer tasks pass session UUIDs, and the plugin fetches normalized sessions through the SDK. Replace the development pin with the compatible published core release floor during release preparation. This package must not be published while the development pin remains.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
