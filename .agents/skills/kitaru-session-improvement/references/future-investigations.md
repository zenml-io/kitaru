# Future investigation and annotation path

The current example stores machine results as evaluations and freezes selected sessions into cohorts. Human or agent review findings remain in the conversation because Kitaru does not yet expose persistent investigation annotations in this workflow.

The refined proposal adds three concepts:

- `annotation`: a durable freeform or structured verdict attached to a required session and an optional session node;
- `investigation`: an agent-scoped, bounded review with a fixed ordered question set and lifecycle status;
- `investigation_session`: an ordered session worklist with per-session status and a disposable summarized view whose elements retain node IDs.

Prepare current evidence for that future model without requiring it:

1. Keep every conclusion tied to an exact `session_id`.
2. Include a `session_node_id` when a claim concerns one LLM call, tool call, subagent call, or span.
3. Use stable question keys for repeated per-session judgments, such as `policy_correct`, `cost_driver`, `candidate_better`, and `regression_found`.
4. Keep the answer as one JSON-compatible `value`, such as a boolean, label, rating, or text note.
5. Keep investigation provenance separate from annotation identity so standalone annotations remain possible.
6. Build cohort membership from reviewed verdicts after the investigation instead of linking an investigation directly to a cohort.

Do not send these records to a metadata field as a workaround. When the annotation API exists, write session-level answers with a null node anchor and node-specific evidence with the exact node ID. Preserve annotations across investigation cancellation and re-imports according to the future server contract.

Source: [Kitaru Investigation & Annotation Data Model, refined proposal](https://app.notion.com/p/3b3f8dff253881ada592e8a293337e70), fetched August 5, 2026.
