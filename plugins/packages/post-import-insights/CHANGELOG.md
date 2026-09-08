# Changelog

## Unreleased

- Rewrite the copied investigation prompt as a finding-specific briefing: it opens with ordered setup steps (install the `kitaru` CLI, `kitaru login` against the recorded server, `kitaru setup` when the `kitaru-investigation` skill is missing) and hands the procedure to that skill; names the server, agent, and import; states what is odd with the finding's own counts, where to look first, a concrete cohort boundary, one hypothesis, and what a confirmed hypothesis looks like per family (trajectory, tool health, language, outcome, activity, timing, model mix); and moves the untrusted JSON evidence to the end. The analyzer now records the server URL and looks up the agent name when the task did not supply one, and the default per-card contributing-session cap drops from 250 to 200 so a full briefing still fits the 16,000-character prompt bound. The analysis version is bumped because the briefing text is part of each candidate's content hash.
- Validate editor copy per card and keep the model's wording for every card that passes; page framing is deterministic and no longer requested from the model.
- Allow card copy to restate numbers present in the card's facts or chart with their unit, measurable comparatives, and word quantities; keep rejecting invented numbers, causal claims, subjective quality judgments, and negated or unsupported outcome claims.
- Ask the editor for a fresh description per card instead of an echo of the deterministic text.
- Fail the OpenAI analyzer task when a model request fails or times out instead of returning deterministic cards.
- Pluralize the model-mix title.
- Bind a card number to a chart label only when the label is adjacent to it, so "runner at 39 and command at 16" is read as written; allow "accounts for" when it states a share rather than a cause; mask each clause of the deterministic caveat separately so a lightly paraphrased caveat still passes; and stop validating the analyst rationale, which is never persisted or shown.
- Name the failing exception class in the fallback reason so a failed OpenAI analyzer task says why the model call failed.
- Tell the analyst that at most one candidate per family is kept.
- Extract post-import insight generation into an independent analyzer distribution.
- Fetch sessions sequentially from the IDs supplied by analyzer tasks, preserving bounded trace memory and complete import coverage.
- Make OpenAI generation available through the optional `openai` extra, selected by the OpenAI analyzer's default package requirement.
- Remove Langfuse telemetry and its dependency from insight generation.
- Register separate deterministic and OpenAI analyzers so callers can select either or both for an import. Require a model and OpenAI credentials for the OpenAI analyzer.

### Release context

- Requires the unreleased core analyzer task contract in [PR #988](https://github.com/zenml-io/kitaru/pull/988), which passes session UUIDs. Replace `kitaru==0.25.0+dev` with the selected compatible published core version before releasing `0.1.0`.
