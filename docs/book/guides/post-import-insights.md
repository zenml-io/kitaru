---
description: Generate insights after an import on a local, self-hosted, or managed Kitaru server.
icon: chart-pie
---

# Post-import insights

Select post-import analyzers to find patterns in normalized sessions, such as failed tool calls, repeated calls, and outcome distributions. They store insight cards with supporting session references and investigation prompts. They run on your worker, including with a local or self-hosted server.

The independently versioned `kitaru-post-import-insights` package supplies three analyzers: `kitaru/post-import-insights` uses deterministic checks without a model or API key; `kitaru/openai-post-import-insights` uses OpenAI to select findings and customize their wording; `kitaru/openai-session-findings` uses OpenAI to read the sessions themselves and verify each problem it finds on every session. The server registers all three package entrypoints; the worker installs the package when executing an analysis task. Imports run only the analyzers explicitly selected by the caller.

## Set up a worker and import

Start with a connected Kitaru installation and a registered agent. See [Installation](../getting-started/installation.md) for a local server or an existing team server, and [Importing sessions](importing-sessions.md) for the trace format and agent setup. Keep the server and worker on the same Kitaru version.

In a project environment, install the CLI and worker and connect locally:

```bash
uv add "kitaru[cli,worker]"
uv run kitaru login --local
```

Start a worker in one terminal. These claims allow imports and their follow-up analysis without claiming agent replays:

```bash
uv run kitaru worker start --claim importer --claim analyzer
```

A worker started without `--claim` also accepts analyzer tasks. If an existing worker only claims importers or evaluators, add the `analyzer` claim; otherwise the import can finish parsing while its analysis task waits for a worker.

In another terminal, import your file for an existing agent, replacing `customer-service@latest` with your agent version:

```bash
uv run kitaru session import sessions.jsonl \
  --importer kitaru/kitaru-jsonl@latest \
  --agent customer-service@latest \
  --analyzer kitaru/post-import-insights@latest \
  --wait
```

The built-in analyzers are already registered, but you must select them with `--analyzer`. Imports with no completed or failed sessions do not launch analysis. An analyzer failure fails the import job but does not undo the imported sessions or insights already produced by another analyzer; the import record retains its parsing counts.

## Read the results

```bash
uv run kitaru insight list --agent customer-service --output json
uv run kitaru insight get <insight-id> --output json
```

Insight metadata contains the analysis coverage, source import, supporting references, investigation prompt, and a `check_first` caveat when the deterministic detector has one. The copied prompt is a briefing for your coding agent. It opens with setup steps (install the `kitaru` CLI, log in to the recorded server, run `kitaru setup` if the `kitaru-investigation` skill is missing) and tells the agent to follow that skill for the procedure. It then names the server, agent, and import, and states what is odd about this finding with its own counts, where to look first, a concrete cohort boundary, one hypothesis to test, and what a confirmed hypothesis would look like. The JSON evidence comes last: the exact description displayed on the card, the deterministic facts, chart, coverage, session IDs, and node references, plus the caveat when present, with the supplied session IDs labeled as either the full affected population or a retained subset with both counts. A detected pattern is a starting point, not proof of its cause.

SDK and REST consumers can read the same records through `client.insights` and `/api/v1/insights`. In MCP, `kitaru_session_import` starts the import workflow, and `kitaru_review_read` reads insights with `kind: "insight"`. See [Set up your coding agent](../agent-native/setup.md) for MCP configuration.

## Rerun an analyzer

An analyzer that failed, for example because the OpenAI analyzer was missing credentials, can be run again over the sessions an import already created. Pass the import id from the import receipt or `kitaru import list`:

```bash
uv run kitaru import analyze <import-id> \
  --analyzer kitaru/openai-post-import-insights@latest \
  --analyzer-params 'kitaru/openai-post-import-insights@latest={"model":"YOUR_MODEL"}' \
  --wait
```

This creates a new job holding one analysis task per selected analyzer, scoped to the same sessions. Re-importing the file instead would skip every session as a duplicate and give the analyzer nothing to read. An import with no completed or failed sessions is rejected. SDK and REST consumers use `client.imports.analyze(...)` and `POST /api/v1/imports/{import_id}/analyze`.

There is no command or MCP tool to run analyzers over arbitrary sessions outside an import. `kitaru insight create` stores a supplied insight. It does not run analysis.

Each generated insight stores its `import_id` directly, so task cleanup does not remove its import association. Deleting the import itself clears that reference. Deleting an analyzer version clears the insight's analyzer-version reference without deleting the insight.

## Run both analyzers

The OpenAI analyzer uses GPT-5.6 Luna (`gpt-5.6-luna`) with low reasoning effort by default.

To run OpenAI analysis, configure an OpenAI [provider connection](provider-connections.md) containing `OPENAI_API_KEY`, or supply that key in the worker's environment and configure its credential selector for `openai`. Setting it only in the importing terminal is not sufficient. To use a compatible model available to your OpenAI project instead, pass `--analyzer-params 'kitaru/openai-post-import-insights@latest={"model":"YOUR_MODEL"}'`. When you supply your own OpenAI key, model usage is billed to your account.

Select both analyzers:

```bash
uv run kitaru session import sessions.jsonl \
  --importer kitaru/kitaru-jsonl@latest \
  --agent customer-service@latest \
  --analyzer kitaru/post-import-insights@latest \
  --analyzer kitaru/openai-post-import-insights@latest \
  --wait
```

Both analyzers run independently and retain their results, even when findings overlap. The OpenAI analyzer requires credentials and uses Luna when no model parameter is provided; it does not switch to deterministic generation when credentials are missing. The model selects the cards and writes each card's eyebrow and description; titles, charts, counts, and caveats stay deterministic. A card's copy may restate only numbers that appear in that card's facts or chart, and must not add causes, outcomes, links, or markup. A card whose copy fails that check keeps the deterministic wording while the other cards keep the model's. If a model request fails or times out, the OpenAI analyzer task fails instead of returning deterministic cards. Without a connection or an eligible credential-equipped worker, its task stays queued; select only the deterministic analyzer if you want the import job to finish without OpenAI credentials.

The plugin package includes its model and observability dependencies. Model calls receive a bounded projection of computed candidates, facts, sanitized labels, and evidence references, not the complete raw traces. Deterministic code computes the counts and charts in both analyzers. OpenAI analysis can incur charges.

## Find and verify problems in sessions

`kitaru/openai-session-findings` reads what the agent actually did in each session and reports problems such as an action that contradicts its system prompt or the policy a tool returned, or a claim in a reply that no tool output supports. It runs in four steps:

1. A reader model reads the agent's system prompt and every imported session, and writes each problem it notices as a yes/no check about a single session.
2. The OpenAI Decisions API answers every check on every session. A check counts as holding in a session only when the answer's probability is at least 0.8.
3. A merge step groups the checks that held into findings, drops sessions where an earlier tool result explains what the agent did, and rates each finding's harm as high, medium, or low.
4. Code counts the sessions behind each finding, so the counts come from the per-session answers rather than from the reader's own estimate.

Each finding becomes one insight: the title names the problem, the description gives the verified count and the harm, and the text data holds one concrete example. The metadata under `kitaru.session-findings/v1` lists the IDs of the sessions where the finding was verified and the checks behind it. Findings are ordered most harmful first, at most ten per run.

The analyzer accepts three optional parameters:

| Parameter | Effect |
|---|---|
| `context` | Extra text about the agent, such as a summary of its code. The reader also writes a check for each risk it names, so a risk in the code is reported only when a session shows it happening. |
| `model` | Model for the reader and merge steps. Defaults to `gpt-5.6-luna`. |
| `decisions_model` | Model for the Decisions API. Defaults to `gpt-6-luna`. |

```bash
uv run kitaru session import sessions.jsonl \
  --importer kitaru/kitaru-jsonl@latest \
  --agent customer-service@latest \
  --analyzer kitaru/openai-session-findings@latest \
  --analyzer-params 'kitaru/openai-session-findings@latest={"context":"The refund tool does not check the approval limit."}' \
  --wait
```

It needs the same OpenAI connection as `kitaru/openai-post-import-insights`, and the OpenAI project must have access to the Decisions API. Without that access, or when any model request fails, the analysis task fails with the failing step and error type instead of returning partial findings.

Unlike the other two analyzers, this one sends session content to OpenAI: the system prompt, each user message, every tool call's inputs and outputs, and the agent's final reply, with each value cut to 2,000 characters. The reader and merge requests keep the newest sessions within about 400,000 characters of transcripts, while the Decisions API still checks every session. Do not select it for sessions whose content must not leave your deployment. It makes one Decisions API request per session plus two model requests, and the findings are model judgments that can differ between runs, so treat a finding as a lead to check in the sessions it lists.

## Coverage and large imports

Every analyzer receives session IDs through the same [analyzer contract](../concepts/analyzers.md). The post-import plugin fetches one complete session at a time and runs its deterministic checks across every imported session, including sessions marked in progress. It does not stop after a fixed number of sessions or nodes.

Evidence references, chart categories, and model input are bounded independently of the scan. Large category sets retain the leading categories and combine the remainder without dropping their counts. Payload traversal and text inspection have per-session limits, so an unusually large payload cannot consume the inspection budget for later sessions. Read the coverage and caveats before treating a text-dependent finding as exhaustive.

One exceptionally large session must still fit in worker memory because its nodes are loaded together. Exact high-cardinality counts and duplicate detection can use temporary disk storage, which is removed when analysis closes. Total runtime still grows with the import size and is subject to the server's analyzer task timeout. A timeout fails the task rather than reporting a completed partial scan.
