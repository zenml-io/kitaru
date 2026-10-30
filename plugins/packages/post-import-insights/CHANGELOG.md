# Changelog

## Unreleased

- Add cost, time, token, and tool-path cards to both analyzers: the share of recorded cost carried by the costliest 10% of sessions; the tool or model covering most recorded time, measured as the union of its node intervals so parallel calls count once; sessions whose model input grows at least three times and model calls with oversized input; the share of input tokens read from a prompt cache, with system prompts that change between calls to the same model; and the most common and one-off tool-call paths. Each card appears only above its documented threshold, keeps exact aggregates in bounded memory that spills to temporary storage, and reports in its caveat the sessions or calls that lacked the field it needs.
- Read a model call's input as input tokens plus cache reads when a call to the same model in that session reports more cache reads than input tokens, since that source counts them separately; where no call settles the convention, state the cache-read share as a range between the two readings.
- Let editor copy restate a number with a unit that the card's chart unit or fact names declare, such as tokens on a card with token facts.
- Bump the analysis version to `2026-10-30.1` because new candidates change each profile's content hash. Existing candidates are unchanged; the deterministic analyzer ranks the new cards after outcome and language cards and before the activity, duration, and model-mix distributions, so those can drop out of a full six-card page.

## 0.2.0 - 2026-10-30

- Count explicit tool requests in LLM output messages when a session has no separate tool nodes, label the fallback as requests, and exclude incomplete request scans from the distribution.
- Describe correction signals as literal text markers and call out that recorded user-role messages may contain agent policy or harness instructions.
- Label the model-mix card as recorded model identifiers without normalizing aliases or changing counts.

## 0.1.2 - 2026-10-08

- Simplify internal return syntax without changing insight generation.

## 0.1.1 - 2026-09-21

- Consume nodes in the server's materialized order and remove assumptions about positional node indexes, retaining compatibility with Kitaru 0.26.0 and later.

## 0.1.0 - 2026-09-10

- Describe insight actions with "Investigate [topic]" labels, such as "Investigate tool retries", and use "Investigate this finding" for unknown finding types.

- Include up to five highest-valued session examples from each nonuniform distribution's highest occupied bin in copied investigation prompts, preserving their priority through reference limits and stating whether the highlighted list is complete.
- Persist short, finding-specific copy button labels alongside investigation prompts in both analyzer modes.

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
- Register separate deterministic and OpenAI analyzers so callers can select either or both for an import. The OpenAI analyzer defaults to `gpt-5.6-luna` with low reasoning effort; callers can override its `model` parameter and must provide OpenAI credentials.

### Release context

- Requires Kitaru 0.26.0 or later for the analyzer task contract in [PR #988](https://github.com/zenml-io/kitaru/pull/988), which passes session UUIDs. Publish Kitaru 0.26.0 before this package.
