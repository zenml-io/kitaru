# Current MCP operations

Use the native Kitaru MCP server in `standard` mode. Every tool takes one top-level `request` object. List calls return one bounded page and do not follow cursors automatically.

## Available tools

| Tool | Use |
|---|---|
| `kitaru_registry_read` | Resolve agents, evaluator versions, cohort versions, and experiments. |
| `kitaru_activity_read` | Read sessions, nodes, evaluations, jobs, tasks, experiment runs, and replays. |
| `kitaru_review_read` | Read investigations, linked investigation sessions, and annotations. |
| `kitaru_cohorts_manage` | Create cohort parents and immutable versions. |
| `kitaru_experiments_manage` | Create experiments with exact evaluator selections. |
| `kitaru_session_import` | Import from an existing server-side payload blob. Local file upload remains a CLI operation. |
| `kitaru_review_manage` | Create and update investigations, answers, and manual annotations. |
| `kitaru_workflow_start` | Start bounded evaluation batches and experiment runs. |
| `kitaru_evaluators_manage` | Manage evaluator resources supported by the live schema. |

Read the live tool schema before the first request. Use exact UUIDs and immutable version numbers after discovery.

## Baseline reads

Resolve an exact agent and version with `kitaru_registry_read`. List baseline sessions through `kitaru_activity_read` using an exact `agent_version_id` plus source constraints such as `origin` and `tag`. Cache the bounded session IDs.

Read session nodes with payloads only when the review requires them and payload exposure is acceptable:

```json
{"request":{"operation":"list_children","kind":"session_nodes","parent_id":"SESSION_ID","size":100,"include_payloads":true}}
```

## Start and poll evaluations

Resolve exact evaluator IDs and version numbers, then use `kitaru_workflow_start`:

```json
{"request":{"operation":"evaluation","session_ids":["SESSION_ID_1","SESSION_ID_2"],"evaluators":[{"evaluator_id":"EVALUATOR_ID","version":1}]}}
```

The start returns immediately. Poll its job with `kitaru_activity_read`. Inspect job tasks when it fails. Fetch evaluation results for the exact session and evaluator-version IDs. Sort bounded numeric scores locally.

## Investigations and annotations

Create the investigation and its fixed question set with `kitaru_review_manage`. Each selected session may include a curated view whose items point to exact node selectors. Use `kitaru_review_read` to list linked sessions and recover progress.

Persist each answer with `kitaru_review_manage` using the investigation-session ID and question key. Attach the narrowest useful selector. Mark a linked session completed after every question is answered. List annotations filtered by investigation ID before resuming or deriving cohorts.

The CLI examples and persistence rules are in [current-investigations.md](current-investigations.md).

## Cohorts

Create a cohort parent only after its behavioral rule and membership are approved. Create an immutable version from exact reviewed session IDs, then verify membership through `kitaru_activity_read`. Create separate target and control cohorts when the experiment needs behavior to change in one set and remain stable in another.

## Experiments and runs

Create an experiment with exact evaluator selections through `kitaru_experiments_manage`. Set the replay tool policy explicitly. Start one run per cohort version with `kitaru_workflow_start`:

```json
{"request":{"operation":"experiment_run","experiment_id":"EXPERIMENT_ID","cohort_version_id":"COHORT_VERSION_ID","agent_version_id":"AGENT_VERSION_ID","evaluate_baselines":true}}
```

Poll the experiment run and inspect its child jobs through `kitaru_activity_read`. List replays by exact experiment-run ID, then join each baseline and result session to its evaluator results.

## Retry discipline

- Read after a dropped mutation response before retrying.
- Treat one schema or capability failure as evidence that the affected resource family needs its structured CLI fallback.
- Do not probe alternate argument shapes after the live schema rejects one valid documented request.
- Cache exact identities and resolve them again only after a mutation that could change them.
