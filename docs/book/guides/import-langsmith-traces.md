---
description: "Turn LangSmith runs into replayable Kitaru sessions: the accepted export shapes, how threads group into sessions, dedup semantics, and what the importer cannot recover."
icon: hammer
---

# LangSmith

If your agent already reports to LangSmith, you do not need to re-instrument anything to start using Kitaru. Export the runs, import them, and each one lands as a [session](../concepts/agents-and-sessions.md) with `origin: imported`, the same object a live-recorded run produces. LangSmith stays your system of record; Kitaru takes a runnable copy of the runs you want to evaluate and replay.

[Import your traces](../getting-started/import-your-traces.md) covers the shortest path. This guide is the LangSmith contract: what the built-in importer accepts, how it decides where one session ends and the next begins, and where it tells you it lost fidelity.

## Export your runs

The importer reads **LangSmith run records**, one JSON object per run, in any of these shapes:

- **JSONL**, one run object per line. This is the shape bulk exports arrive in.
- **A JSON array** of run objects.
- **A run-query envelope**: a JSON object with the runs under a `runs` or `data` key. This is what the LangSmith runs-query API returns, so you can pipe its response straight to a file.
- **A single JSON object**, treated as a one-run export.

Payloads must be UTF-8 and 50 MiB or smaller (the importer's own cap, separate from the server's configurable blob limit). Export in slices as often as you like; [dedup](#dedup-one-session-per-project-and-thread) makes overlapping exports safe.

Each run record is read for the fields LangSmith already writes: `id`, `trace_id`, `parent_run_id`, `is_root`, `run_type`, `name`, `status`, `error`, `start_time` / `end_time`, `inputs`, `outputs`, `tags`, `extra.metadata`, `extra.invocation_params`, `serialized.kwargs`, `total_cost`, and token counts. Export whole traces rather than filtered subsets: a run whose parent is missing from the file still imports, but the session is marked partial.

{% hint style="info" %} `inputs`, `outputs`, `extra`, and `metadata` are commonly JSON-encoded strings in bulk exports. The importer decodes them, so you don't have to pre-process the file. {% endhint %}

## Import the export

Register the agent these runs belong to, if you have not, then start a [worker](../concepts/workers.md) in another terminal (`kitaru worker start`) and import:

```bash
kitaru agent register support-agent --command "python support.py"

kitaru session import langsmith-runs.jsonl \
  --importer kitaru/langsmith@latest \
  --agent support-agent@latest \
  --media-type application/x-ndjson \
  --tag imported-baseline --wait
```

The import is a job with one importer task. The export is uploaded as a blob; a worker claims the task and runs the parse **in your environment**, so the server never parses your run data. `--tag` labels the sessions once the import completes, which is why it requires `--wait`. The receipt reports sessions `created`, `skipped`, and `failed`, with failure samples.

`kitaru/langsmith` is one of the built-in importers, registered at server startup, so `@latest` always resolves and there is no importer code to write.

### Parameters

| Param | Meaning |
| --- | --- |
| `source_instance` | The LangSmith project the export came from. Optional when the runs carry `session_id`, `project_id`, `session_name`, or `project_name`; required when they don't. It anchors the sessions' external identity, so keep it stable across imports of the same project. |
| `join_on` | The path whose value groups traces into one session. Accepts a dotted path (`extra.metadata.thread_id`) or an RFC 6901 JSON Pointer (`/extra/metadata/thread_id`), resolved against each trace's root run. Omit it to use the defaults below. |

Pass them with `--params '{"source_instance": "my-project"}'`. `join_on` also has its own flag, `--join-on`, which accepts JSON Pointer syntax only (it must start with `/`) and cannot be combined with a `join_on` inside `--params`:

```bash
kitaru session import langsmith-runs.jsonl \
  --importer kitaru/langsmith@latest \
  --agent support-agent@latest \
  --join-on /extra/metadata/conversation_id \
  --media-type application/x-ndjson --wait
```

## Fetch traces from the LangSmith API

Skip the export and upload, and let the import task fetch runs from LangSmith directly:

```bash
kitaru session import \
  --importer kitaru/langsmith@latest \
  --agent support-agent@latest \
  --since 7d \
  --tag imported-baseline --wait
```

Omitting FILE and setting `--since` selects an API import: the worker calls the LangSmith API instead of parsing an uploaded payload. `--since` and `--until` accept an ISO 8601 timestamp or a relative duration (`7d`, `12h`, `30m`). `--trace-id` (repeatable) fetches exactly those trace ids instead of a time window. The same selection is a query object on the SDK and REST request:

| Query key | Meaning |
| --- | --- |
| `trace_ids` | LangSmith trace ids to fetch. When present, exactly those traces are fetched and the time window is ignored. |
| `since` | Timezone-aware ISO 8601 datetime, lower bound of trace start time. Required when `trace_ids` is absent. |
| `until` | Timezone-aware ISO 8601 datetime, upper bound of trace end time. Defaults to now. |
| `project_name` | LangSmith project to fetch from. Defaults to the SDK's tracer project, read from `LANGSMITH_PROJECT` (or `LANGCHAIN_PROJECT`) in the environment. |

Pass `project_name` through `--query '{"project_name": "my-project"}'`. The worker needs `kitaru-langsmith-importer[adapter]` installed (the default plugin catalog already installs it that way) and `LANGSMITH_API_KEY` in its environment, plus `LANGSMITH_ENDPOINT` for a self-hosted instance. Each fetched trace is parsed the same way an uploaded export would be, so the mapping, dedup, and limitations below apply the same way.

## What a LangSmith trace becomes

The mapping is one level deeper than a trace-per-session import, because a LangSmith thread is usually a multi-turn conversation spread over several traces:

| LangSmith | Kitaru |
| --- | --- |
| Project | The session's `source_instance`, half of its external identity |
| Thread (`thread_id`, `session_id`, or `conversation_id` in run metadata) | One **session**, holding every trace in the thread |
| Trace | One **turn** inside that session's inputs, in start-time order |
| Run with `run_type` `llm` or `chat_model` | An **llm_call** node |
| Run with `run_type` `tool` | A **tool_call** node, named after the run |
| Any other run type | A **span** node |
| `parent_run_id` | The node's parent, rebuilt as a tree per trace |

Without an explicit `join_on`, the importer looks for a thread value at `extra.metadata.thread_id`, `extra.metadata.session_id`, `extra.metadata.conversation_id`, and then the same three keys under a top-level `metadata`. If none is present, each trace becomes its own session and the session records the warning "No LangSmith thread metadata found; grouped by trace id".

Per node, the importer preserves timings, status and error, inputs and outputs, the requested and resolved model names, the model provider, model invocation parameters, token usage (input, output, and cached input, read from `extra.token_usage`, `outputs.llm_output.token_usage`, `outputs.usage_metadata`, or top-level `prompt_tokens` / `completion_tokens`), and `total_cost`. It also picks out the user prompt, the assistant's visible answer, the system prompt, and any visible model reasoning, so those render as text rather than as raw payload. LangSmith run type, status, and tags are kept as node attributes, and a bounded set of metadata keys (`thread_id`, `session_id`, `conversation_id`, `user_id`, `assistant_id`, `graph_id`, `langgraph_node`, `langgraph_checkpoint_ns`, `revision_id`, `environment`, and `reference_example_id`) is kept under `langsmith.*`.

At session level you get the thread's trace ids, the join paths used, the union of run tags and user ids, the turn count, and a `source_completeness` of `full` or `partial`. The importer also detects the agent framework (PydanticAI, LangGraph, OpenAI Agents, Google ADK, or the Claude Agent SDK) from run metadata when the evidence points to exactly one. Session status follows the latest trace's root run: failed if that run carries an error or a failure status, completed otherwise.

## Dedup: one session per project and thread

Every imported session records `imported_from: langsmith` plus an `external_id` of `<source_instance>:<thread>`. That pair is unique on the server, so re-importing an overlapping export **skips** what is already stored and reports it as `skipped`, not as an error. Nodes upsert by index within a session, so a re-parse restates each node's full content.

This is what makes "export the last 24 hours every night" safe. It also means the grouping key matters: if you change `source_instance` or `join_on` between imports of the same runs, the same thread lands as a second session rather than deduping against the first.

## Limitations

- **Only what the export contains.** Anything LangSmith did not record (intermediate state, code, environment) is not recoverable from the file.
- **Imported threads are frozen.** Once a thread is imported, later traces in the same thread are skipped by dedup rather than appended. Import a thread after it is finished, or scope `join_on` to something that closes.
- **Partial graphs import with a warning.** A trace with more than one root run, a run whose parent is missing from the export, or model output containing `tool_calls` with no corresponding tool runs all set `source_completeness: partial` and add a line to `normalization_warnings`. The session still imports.
- **A bad trace is isolated, not fatal.** A run with no trace id or run id, a trace with conflicting project identities or conflicting thread values, or a trace missing your chosen `join_on` value is reported as a failure and the rest of the file still imports. A malformed file (invalid JSON, non-UTF-8, empty, or over 50 MiB) fails the task as a whole.
- **Replay needs your code.** Imported sessions replay like recorded ones, but only if the agent version whose code produced the runs is registered with a run command. No trace export contains the code.

{% hint style="warning" %} Imported payloads contain whatever your runs contain: prompts, customer data, tool results. They are stored on your self-hosted server and parsed on your workers, but access and retention are yours to govern. {% endhint %}

## Next

Evaluate the history you imported with [Write an evaluator](write-an-evaluator.md), then freeze the sessions that matter into a cohort and put your next change to the test with [Build a regression suite from production](regression-suite.md).
