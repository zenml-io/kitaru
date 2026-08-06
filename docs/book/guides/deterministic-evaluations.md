---
description: Run Kitaru's offline deterministic evaluators over recorded and imported sessions
icon: gauge-high
---

# Deterministic Evaluations

The Kitaru repository includes ten deterministic evaluator plugins for recorded and imported sessions. They read stored session evidence and return repeatable diagnostics or policy results. They do not run the agent, call a model provider, replay a session, invoke a live tool, or read an external service.

An administrator must seed the repository plugin definitions explicitly:

```bash
uv run python scripts/seed_default_plugins.py
```

The command uses the configured Kitaru workspace, preserves the three existing evaluators from `plugins/evaluators/basic.py`, and prints the created or current version of every seeded plugin. The ten bundles below share `plugins/evaluators/deterministic.py`; a fresh workspace creates version 1.

You also start each evaluation explicitly. Importing, recording, or seeding a session does not start an evaluation Job automatically.

## Start with the descriptive bundles

Use these five bundles first when you are investigating unfamiliar traces:

| Evaluator | What it reports |
|---|---|
| `session-diagnostics@1` | Session terminality, node ordering, parent linkage, chronology, payload coverage, counts, duration, resource coverage, and malformed negative resource values. |
| `trajectory-signals@1` | Exact adjacent tool-call repetition, exact retry after a recorded failure, and bounded short tool-name cycles. |
| `tool-health@1` | Recorded tool failures, null or empty results, error/status inconsistencies, and adjacent failures of the same tool. |
| `timing-profile@1` | Wall-clock duration, node timing coverage, slowest recorded nodes, invalid intervals, and overlapping intervals. |
| `llm-call-signals@1` | Recorded LLM failures, null or empty results, exact adjacent repeated inputs, requested/served model mismatches, and metadata coverage. |

These results are descriptive. They leave `passed` unset because a repeated call, a slow span, or a failure marker is not by itself a judgment about agent quality or correctness.

Run the first pass from the CLI:

```bash
kitaru session evaluate "$SESSION_ID" \
  --evaluator session-diagnostics@1 \
  --evaluator trajectory-signals@1 \
  --evaluator tool-health@1 \
  --evaluator timing-profile@1 \
  --evaluator llm-call-signals@1 \
  --wait
```

Without `--wait`, the command returns the created Job immediately. Inspect it with `kitaru job get JOB_ID --tasks`, or read stored results with `kitaru evaluation list` and `kitaru evaluation get EVALUATION_ID`.

## Add a configured rule when you have a real policy

The other five bundles produce pass or fail verdicts only for rules you supply:

| Evaluator | Parameters |
|---|---|
| `output-contract@1` | `expected` for exact non-null JSON equality; `required_paths` for RFC 6901 JSON Pointer presence; `type_requirements` mapping pointers to `null`, `boolean`, `number`, `integer`, `string`, `array`, or `object`. Supply at least one rule. |
| `resource-budget@1` | One or more inclusive non-negative ceilings: `max_duration_seconds`, `max_cost`, `max_total_tokens`, `max_nodes`, `max_llm_calls`, or `max_tool_calls`. Node and call-count ceilings must be integers. |
| `tool-policy@1` | One or more of `required_tools`, `forbidden_tools`, or `max_calls_per_tool`. Tool names are exact and case-sensitive. |
| `model-policy@1` | One or more of `allowed_models`, `allowed_providers`, or `require_requested_model_match`. Recorded names are exact and case-sensitive. |
| `workflow-conformance@1` | Required `expected_tools` plus `mode`: `exact_order`, `in_order`, `contains_all`, or `exact_set`. |

For example, apply recorded resource ceilings and a tool policy:

```bash
kitaru session evaluate "$SESSION_ID" \
  --evaluator resource-budget@1 \
  --evaluator-params 'resource-budget@1={"max_duration_seconds":120,"max_tool_calls":20,"max_total_tokens":50000}' \
  --evaluator tool-policy@1 \
  --evaluator-params 'tool-policy@1={"required_tools":["search"],"forbidden_tools":["delete_account"]}' \
  --wait
```

Configured rules use conservative evidence semantics:

- A recorded violation can fail immediately.
- A rule passes only when the session is terminal and all evidence required by that rule is present and consistent.
- Insufficient evidence leaves `passed` unset. This is a HOLD, not a pass.
- Invalid configuration fails that evaluator task. Sibling evaluator tasks continue under the existing evaluation Job behavior.

## Understand result evidence

Every bundle emits `input_sha256` and `config_sha256`. The input hash covers the materialized session and node fields used across the deterministic catalog. The configuration hash covers normalized parameters for that evaluator. Use both values to tell whether two attempts analyzed the same fetched evidence with the same configuration.

Finding results use compact JSON in `value`:

```json
{
  "evidence": [{"indexes": [3, 4], "tool_name": "search"}],
  "total": 1,
  "truncated": false
}
```

Evidence uses node indexes or index windows. Most bundles retain at most 20 findings while keeping the full `total` and a `truncated` flag. `timing-profile` accepts `evidence_limit` from 1 to 100.

If any structured result value would exceed 64,000 UTF-8 bytes, Kitaru retains its SHA-256 hash and original byte count instead of allowing the evaluator task to exceed the worker result limit. Verdicts are computed from the full evidence before this result encoding limit is applied.

The exact-output rule retains SHA-256 hashes of the compared values instead of copying potentially large payloads into the evaluation result. The `passed` field still reflects exact canonical JSON equality.

The short-cycle detector examines tool-name cycles with periods from two through five and requires at least three repetitions. No cycle result means no cycle within those bounds, not that the trajectory contains no other repetition.

## Run through the Python SDK

Use the existing evaluation request and pin the version reported by the seed command. This example uses a fresh workspace, where the version is 1:

```python
import uuid

from kitaru.api_models.v1.evaluation import EvaluationBatchCreateRequest
from kitaru.api_models.v1.replay_config import EvaluatorConfig
from kitaru.client.api_client import KitaruAPIClient


async def start_diagnostics(session_ids: list[uuid.UUID]) -> str:
    async with KitaruAPIClient() as client:
        job = await client.evaluations.create(
            EvaluationBatchCreateRequest(
                input_session_ids=session_ids,
                evaluators=[
                    EvaluatorConfig(evaluator="session-diagnostics", version=1),
                    EvaluatorConfig(evaluator="trajectory-signals", version=1),
                    EvaluatorConfig(evaluator="tool-health", version=1),
                    EvaluatorConfig(evaluator="timing-profile", version=1),
                    EvaluatorConfig(evaluator="llm-call-signals", version=1),
                ],
            )
        )
        return str(job.id)
```

## Run through MCP

Start `kitaru-mcp` in `standard` mode. Discover each evaluator parent and exact version with `kitaru_registry_read`, then pass their IDs to `kitaru_workflow_start`:

```json
{
  "request": {
    "operation": "evaluation",
    "session_ids": ["00000000-0000-0000-0000-000000000001"],
    "evaluators": [
      {
        "evaluator_id": "00000000-0000-0000-0000-000000000010",
        "version": 1,
        "params": {}
      }
    ]
  }
}
```

The tool returns the submitted Job immediately. Read the Job with `kitaru_activity_read`, then use `list_children` with `kind: "job_tasks"` to inspect evaluator task results. See [MCP Server](../agent-native/mcp-server.md) for capability modes and request envelopes.

## Respect the batch limit

One request may contain at most 100 distinct session/evaluator pairs. The server calculates this as `number of sessions × number of selected evaluators`.

- The five-bundle descriptive first pass supports up to 20 sessions per request.
- Selecting all ten deterministic bundles supports up to 10 sessions per request.
- A two-bundle policy pass supports up to 50 sessions per request.

For larger sets, split the session IDs into chunks that satisfy the formula and submit one normal evaluation Job per chunk. With the CLI, write each chunk to a separate UTF-8 sessions file and use `--sessions-file`. With the SDK or MCP, submit the same evaluator selection once per chunk. This is caller-side batching; Kitaru does not create one aggregate verdict across the Jobs.

## Current evidence limits

The worker fetches the session and its nodes when an attempt runs. These reads are separate and the underlying records can change, so a retry can observe a later or internally mixed materialization. The hashes expose that difference, but they do not create an immutable snapshot. Repeatability also depends on a compatible Kitaru and Python worker runtime.

The current session view cannot distinguish an absent normalized output from an explicit JSON null. An observed null session output is therefore unavailable to `output-contract`, including when the expected value is null. A null tool result has the same ambiguity and is reported as a diagnostic rather than an integrity verdict.

The evaluator bundles see Kitaru's canonical session and node models. They cannot inspect raw importer events that typed ingestion rejected, unknown raw event kinds, or provider fields that were not retained. They also do not classify rate limits, context exhaustion, timeouts, or malformed external responses unless the canonical record exposes enough direct evidence for the specific result.

`timing-profile` and the call-count results report values for one session. They do not label cohort-relative duration or tool-call outliers. Establishing an outlier requires a frozen comparison cohort, a declared statistic, and calibrated thresholds; a high count alone is not an agent-quality failure.

## Versioning

All ten evaluators share one repository source file. The seed script creates a new immutable version when those source bytes change and leaves the current version unchanged when the bytes and entrypoint match. Use the version reported by the seed command and pin it when you need a stable contract. Re-executing an older version is deterministic only when the fetched materialized view and worker runtime are also equivalent.

Seeded evaluators are ordinary workspace plugins created through the public API. Kitaru does not register them at server startup or give them special read-only behavior.
