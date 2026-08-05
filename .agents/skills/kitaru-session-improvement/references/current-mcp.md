# Current MCP operations

Use the native Kitaru MCP server in `standard` mode. Every tool takes one top-level `request` object. List calls return one bounded page and never follow cursors automatically.

## Surface selection

| Tool | Use |
|---|---|
| `kitaru_registry_read` | Resolve agents, agent versions, evaluators, evaluator versions, cohorts, cohort versions, and experiments. |
| `kitaru_activity_read` | Read sessions, evaluations, jobs, tasks, experiment runs, replays, and session nodes. |
| `kitaru_cohorts_manage` | Create cohort parents and immutable versions. |
| `kitaru_experiments_manage` | Create experiments with exact evaluator selections. |
| `kitaru_session_import` | Import from an existing server-side payload blob. Local file upload remains a CLI operation. |

Standard mode does not expose tools for starting session evaluations or experiment runs. Use the CLI operations in [current-cli.md](current-cli.md) for both.

## Registry reads

Get an exact parent by case-sensitive name:

```json
{"request":{"operation":"get","kind":"agent","reference":"returns-resolver"}}
```

List versions, then get the selected exact version:

```json
{"request":{"operation":"list_versions","kind":"agent","parent_reference":"returns-resolver","size":20,"sort":"created:asc"}}
```

```json
{"request":{"operation":"get_version","kind":"agent","parent_reference":"returns-resolver","version":1}}
```

Use the same operations with `kind=evaluator` or `kind=cohort`. Experiments are unversioned parents and use `get` with `kind=experiment`.

## Baseline sessions

List the canonical baseline by exact agent-version ID, origin, and tag:

```json
{"request":{"operation":"list","kind":"session","filter":{"and":[{"field":"agent_version_id","op":"eq","value":"BASELINE_AGENT_VERSION_ID"},{"field":"origin","op":"eq","value":"imported"},{"field":"tag","op":"eq","value":"returns-baseline"}]},"size":20,"sort":"created:asc"}}
```

## Poll deterministic evaluations

Start the bounded evaluation through the CLI. Ten sessions times three evaluators yields 30 pairs. Poll the returned job ID through MCP:

Poll the returned job ID:

```json
{"request":{"operation":"get","kind":"job","id":"JOB_ID"}}
```

Inspect failed tasks:

```json
{"request":{"operation":"list_children","kind":"job_tasks","parent_id":"JOB_ID","size":100,"sort":"created:asc","include_payloads":false}}
```

## Fetch exact evaluator results

Fetch one evaluator's results for the bounded session set. Replace every placeholder and repeat for each evaluator version:

```json
{"request":{"operation":"list","kind":"evaluation","filter":{"and":[{"field":"session_id","op":"in","value":["SESSION_ID_1","SESSION_ID_2"]},{"field":"evaluator_version_id","op":"eq","value":"EVALUATOR_VERSION_ID"}]},"size":20,"sort":"created:desc"}}
```

Sort returned numeric `score` values locally. Do not pass `score` as a server sort field.

## Create and verify an interviewed cohort version

Create the target parent only after the user approves the investigation brief and membership:

```json
{"request":{"operation":"create","agent_id":"AGENT_ID","name":"unsafe-refund-baseline","description":"Baseline sessions that refunded despite an approval or risk rule requiring escalation.","metadata":{}}}
```

Create an immutable membership snapshot from its receipt:

```json
{"request":{"operation":"create_version","cohort_id":"COHORT_ID","add_session_ids":["TICKET_004_SESSION_ID","TICKET_007_SESSION_ID"],"remove_session_ids":[],"display_version":"reviewed-policy-risks"}}
```

Repeat the two calls for `safe-refund-control` with the approved tickets 001, 009, and 010. Cohort names and memberships come from the interview and trace evidence. Do not derive them from evaluator ranking alone.

Verify membership through activity read:

```json
{"request":{"operation":"list","kind":"session","filter":{"field":"cohort_version_id","op":"eq","value":"COHORT_VERSION_ID"},"size":20,"sort":"created:asc"}}
```

## Create an experiment and inspect its runs

Create only after resolving exact evaluator parent IDs and version numbers:

```json
{"request":{"operation":"create","name":"improve-returns-policy","description":"Replay policy-risk and valid-refund cohorts with strict refund approval rules.","evaluators":[{"evaluator_id":"POLICY_EVALUATOR_ID","version":1,"params":{}},{"evaluator_id":"COST_EVALUATOR_ID","version":1,"params":{}},{"evaluator_id":"LATENCY_EVALUATOR_ID","version":1,"params":{}},{"evaluator_id":"TOOLS_EVALUATOR_ID","version":1,"params":{}}]}}
```

Start one run per cohort version through the CLI command in [current-cli.md](current-cli.md).

Poll the returned experiment-run ID and inspect its jobs when it fails:

```json
{"request":{"operation":"get","kind":"experiment_run","id":"EXPERIMENT_RUN_ID"}}
```

```json
{"request":{"operation":"list_children","kind":"experiment_run_jobs","parent_id":"EXPERIMENT_RUN_ID","size":100,"sort":"created:asc","include_payloads":false}}
```

List the run's replays:

```json
{"request":{"operation":"list","kind":"replay","filter":{"field":"experiment_run_id","op":"eq","value":"EXPERIMENT_RUN_ID"},"size":20,"sort":"created:asc"}}
```

Read the baseline and result session IDs from each replay. Get those sessions and their exact evaluation results. Read nodes only when payload exposure is acceptable:

```json
{"request":{"operation":"list_children","kind":"session_nodes","parent_id":"SESSION_ID","size":100,"sort":"index:asc","include_payloads":true}}
```

## Retry discipline

- Read after a dropped cohort, experiment, or import response before deciding whether to retry.
- Treat one response-validation or capability failure as evidence that the affected resource family is incompatible with the connected server. Use the structured CLI fallback for that family for the rest of the task.
- Do not probe alternate MCP argument shapes after the live schema and one valid request disagree.
- Cache every resolved parent, version, session, cohort, evaluator, and experiment ID. Resolve it again only when a mutation could have changed it.
