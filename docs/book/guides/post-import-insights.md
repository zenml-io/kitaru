---
description: Generate insights after an import on a local, self-hosted, or managed Kitaru server.
icon: chart-pie
---

# Post-import insights

Select post-import analyzers to find patterns in normalized sessions, such as failed tool calls, repeated calls, and outcome distributions. They store insight cards with supporting session references and investigation prompts. They run on your worker, including with a local or self-hosted server.

The independently versioned `kitaru-post-import-insights` package supplies two analyzers: `kitaru/post-import-insights` uses deterministic checks without a model or API key; `kitaru/openai-post-import-insights` uses OpenAI to select findings and customize their wording. The server registers both package entrypoints; the worker installs the package when executing an analysis task. Imports run only the analyzers explicitly selected by the caller.

## Set up a worker and import

Start with a connected Kitaru installation and a registered agent. See [Installation](../getting-started/installation.md) for a local Docker server or an existing team server, and [Importing sessions](importing-sessions.md) for the trace format and agent setup. Keep the server and worker on the same Kitaru version.

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

Insight metadata contains the analysis coverage, source import, supporting references, investigation prompt, and a `check_first` caveat when the deterministic detector has one. The evidence-specific prompt preserves the exact description displayed on the card alongside the deterministic facts, chart, coverage, session IDs, and node references. It includes the caveat when present and labels the supplied session IDs as either the full affected population or a retained subset with both counts. Use that evidence to choose sessions for an investigation or cohort before testing a change. A detected pattern is a starting point, not proof of its cause.

SDK and REST consumers can read the same records through `client.insights` and `/api/v1/insights`. In MCP, `kitaru_session_import` starts the import workflow, and `kitaru_review_read` reads insights with `kind: "insight"`. See [Set up your coding agent](../agent-native/setup.md) for MCP configuration.

There is no separate command or MCP tool to regenerate these cards over arbitrary existing sessions. `kitaru insight create` stores a supplied insight; it does not run analysis. Analyzers currently run as part of an import.

Each generated insight stores its `import_id` directly, so task cleanup does not remove its import association. Deleting the import itself clears that reference. Deleting an analyzer version clears the insight's analyzer-version reference without deleting the insight.

## Run both analyzers

The OpenAI analyzer uses GPT-5.6 Luna (`gpt-5.6-luna`) by default.

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

Both analyzers run independently and retain their results, even when findings overlap. The OpenAI analyzer requires credentials; it does not switch to deterministic generation when credentials are missing. Without a connection or an eligible credential-equipped worker, its task stays queued; select only the deterministic analyzer if you want the import job to finish without OpenAI credentials.

The plugin package includes its model and observability dependencies. Model calls receive a bounded projection of computed candidates, facts, sanitized labels, and evidence references, not the complete raw traces. Deterministic code computes the counts and charts in both analyzers. OpenAI analysis can incur charges.

## Coverage and large imports

Every analyzer receives session IDs through the same [analyzer contract](../concepts/analyzers.md). The post-import plugin fetches one complete session at a time and runs its deterministic checks across every imported session, including sessions marked in progress. It does not stop after a fixed number of sessions or nodes.

Evidence references, chart categories, and model input are bounded independently of the scan. Large category sets retain the leading categories and combine the remainder without dropping their counts. Payload traversal and text inspection have per-session limits, so an unusually large payload cannot consume the inspection budget for later sessions. Read the coverage and caveats before treating a text-dependent finding as exhaustive.

One exceptionally large session must still fit in worker memory because its nodes are loaded together. Exact high-cardinality counts and duplicate detection can use temporary disk storage, which is removed when analysis closes. Total runtime still grows with the import size and is subject to the server's analyzer task timeout. A timeout fails the task rather than reporting a completed partial scan.
