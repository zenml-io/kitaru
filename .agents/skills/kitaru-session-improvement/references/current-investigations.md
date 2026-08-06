# Current investigation and annotation operations

An investigation is an agent-scoped, bounded review with a fixed ordered question set. Its linked sessions have their own pending, completed, or skipped status. Answers are annotations attached to an investigation session and question key.

## Select the review set

Select sessions from evidence before asking the user to handle Kitaru resources. Use the stated improvement goal, built-in evaluator results, outputs, errors, and trace patterns to include likely failures, nearby successes, and counterexamples. Keep the sample bounded.

Questions are fixed when the investigation is created. Use stable machine-readable keys and plain-language prompts. Include an outcome judgment and expected behavior for every session. Add one goal-specific question only when it helps distinguish the emerging rule.

## Curate evidence views

Each session view contains a summary and items with a label, description, and selectors. Every item must point to real evidence. Use exact node IDs and the narrowest useful payload location. Do not create decorative view items with an empty selector list.

A selector can identify a node and optionally its `input`, `output`, `error`, or `metadata` part, a JSON Pointer path, or a text span. Prefer node-level evidence over copying payloads into an annotation value.

## Create through the CLI

```bash
uv run kitaru investigation create REVIEW_NAME \
  --agent AGENT_REFERENCE \
  --description "WHY_THIS_REVIEW_EXISTS" \
  --question 'outcome=Is this outcome acceptable, problematic, or uncertain, and why?' \
  --question 'expected=What should the agent have done in this case?' \
  --session SESSION_ID \
  --session-view 'SESSION_ID={"summary":"CASE_SUMMARY","items":[{"label":"Decision evidence","description":"The call that determined the outcome.","selectors":[{"node_id":"NODE_ID","part":"output"}]}]}'
```

List the linked sessions to resolve each investigation-session ID:

```bash
uv run kitaru investigation session list INVESTIGATION_ID --size 20
```

## Persist each answer

```bash
uv run kitaru annotation create \
  --investigation-session INVESTIGATION_SESSION_ID \
  --question-key outcome \
  --selector '{"node_id":"NODE_ID","part":"output"}' \
  --value '{"judgment":"problematic","reason":"BUSINESS_REASON"}'
```

Use a session-level answer without `--selector` when the judgment concerns the complete run. Mark the session complete after every fixed question has an answer:

```bash
uv run kitaru investigation session complete INVESTIGATION_ID SESSION_ID
```

## Resume and derive cohorts

Read the investigation, linked-session list, and its annotations before asking another question. Filter annotations by investigation ID:

```bash
uv run kitaru annotation list \
  --filter '{"field":"investigation_id","op":"eq","value":"INVESTIGATION_ID"}' \
  --size 100
```

Derive cohort membership from reviewed answers and trace evidence after the investigation. An investigation does not automatically create a cohort. Investigation-wide shipping criteria remain in the approved behavior brief because the current model stores answers per linked session.

Deleting an investigation also deletes its linked review state and answers. Never delete one as a retry strategy.
