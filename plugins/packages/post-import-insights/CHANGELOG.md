# Changelog

## Unreleased

- Extract post-import insight generation into an independent analyzer distribution.
- Fetch sessions sequentially from the IDs supplied by analyzer tasks, preserving bounded trace memory and complete import coverage.
- Make OpenAI generation available through the optional `openai` extra, selected by the OpenAI analyzer's default package requirement.
- Remove Langfuse telemetry and its dependency from insight generation.
- Register separate deterministic and OpenAI analyzers so callers can select either or both for an import. Require a model and OpenAI credentials for the OpenAI analyzer.

### Release context

- Requires the unreleased core analyzer task contract in [PR #988](https://github.com/zenml-io/kitaru/pull/988), which passes session UUIDs. Replace `kitaru==0.25.0+dev` with the selected compatible published core version before releasing `0.1.0`.
