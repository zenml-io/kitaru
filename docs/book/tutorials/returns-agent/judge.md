---
description: Create an investigation and store human judgments against exact trace evidence.
icon: scale-balanced
---

# 2. Judge the selected behavior

**Observe → Judge → Define → Replay → Compare**

The traces prove what happened, but they do not contain the conclusion that a decision was acceptable or problematic. This phase stores human judgments separately from the raw evidence.

## Plan the review before writing

For every selected session, prepare one distinct question and optional highlights:

| Field | Requirement |
| --- | --- |
| Session | The exact session UUID and its position in the review. |
| Selection reason | The evidence-based reason for including it. |
| Question | One concise, session-specific question that requires human judgment. |
| Highlights | Exact nodes or fields that help answer the question without revealing a conclusion. |

The question and highlight descriptions appear beside the trace in the frontend, so they must make sense without this tutorial or your terminal history.

## Create a fixed investigation

An [**investigation**](../../concepts/investigations.md) stores an ordered review worklist and the questions asked about each session. The following shape uses two sessions; repeat the arguments for your complete selected worklist:

```bash
SESSION_A="YOUR_FIRST_SESSION_UUID"
SESSION_B="YOUR_SECOND_SESSION_UUID"
NODE_A="A_RELEVANT_NODE_UUID"
NODE_B="A_RELEVANT_NODE_UUID"
QUESTION_A="WRITE_A_QUESTION_FROM_SESSION_A_EVIDENCE"
QUESTION_B="WRITE_A_DIFFERENT_QUESTION_FROM_SESSION_B_EVIDENCE"
HIGHLIGHTS_A="[{\"selector\":{\"node_id\":\"$NODE_A\"},\"description\":\"DESCRIBE_WHY_THIS_NODE_IS_RELEVANT\"}]"
HIGHLIGHTS_B="[{\"selector\":{\"node_id\":\"$NODE_B\"},\"description\":\"DESCRIBE_WHY_THIS_NODE_IS_RELEVANT\"}]"

uv run kitaru investigation create returns-discovery \
  --agent returns-resolver \
  --description "Open review of diverse imported returns sessions." \
  --session "$SESSION_A" \
  --session-question "$SESSION_A:observation=$QUESTION_A" \
  --session-highlights "$SESSION_A:observation=$HIGHLIGHTS_A" \
  --session "$SESSION_B" \
  --session-question "$SESSION_B:observation=$QUESTION_B" \
  --session-highlights "$SESSION_B:observation=$HIGHLIGHTS_B"
```

The investigation links to existing sessions; it does not copy or modify their traces. Save the returned investigation UUID and inspect its ordered queue:

```bash
INVESTIGATION_ID="YOUR_INVESTIGATION_UUID"

uv run kitaru investigation session list \
  "$INVESTIGATION_ID" \
  --size 20
```

Three IDs now have different jobs:

| ID | What it identifies |
| --- | --- |
| Session UUID | The recorded agent run. |
| Node UUID | One event inside that run. |
| Investigation-session UUID | That session's place, question, and review state inside this investigation. |

This separation lets one session participate in different investigations without mixing their questions or answers.

## Review in the frontend

Open the agent's **Investigations** page in the workspace selected by `kitaru status`. For a local workspace, open [http://localhost:8000](http://localhost:8000). The frontend presents each fixed question beside its highlighted trace evidence. Answer the question and choose a whole-session verdict:

- `acceptable`
- `problematic`
- `uncertain`

The answer and verdict have different meanings. An **annotation** stores the substance of the answer and can point to exact evidence. The verdict classifies the complete session. `uncertain` is appropriate when the trace does not contain enough evidence for a complete judgment.

## Or store an annotation with the CLI

An annotation selector can target the entire node, a field inside it, or a character range inside a string. Start with the whole evidence node you inspected:

```bash
INVESTIGATION_SESSION_ID="YOUR_INVESTIGATION_SESSION_UUID"
EVIDENCE_NODE_ID="YOUR_EVIDENCE_NODE_UUID"

uv run kitaru annotation create \
  --investigation-session "$INVESTIGATION_SESSION_ID" \
  --question-key observation \
  --selector "{\"node_id\":\"$EVIDENCE_NODE_ID\"}" \
  --value '"Write your own observation here."'
```

When only one field is evidence, add an RFC 6901 JSON pointer such as `"path":"/outputs/message"`. Add a `span` with `start` and `end` offsets only when a specific character range inside that string supports the answer. Omit the selector when the judgment depends on the complete session.

Store the whole-session verdict separately:

```bash
REVIEWED_SESSION_ID="THE_RECORDED_SESSION_UUID_FOR_THIS_REVIEW_ITEM"

uv run kitaru investigation session verdict \
  "$INVESTIGATION_ID" \
  "$REVIEWED_SESSION_ID" \
  problematic
```

Replace `problematic` with the verdict supported by your review. Do not set a verdict merely to complete the workflow.

## Confirm the persisted review

After reviewing the complete worklist, inspect both answer and verdict coverage:

```bash
uv run kitaru investigation get "$INVESTIGATION_ID"

uv run kitaru annotation list \
  --filter "{\"field\":\"investigation_id\",\"op\":\"eq\",\"value\":\"$INVESTIGATION_ID\"}" \
  --size 100
```

Complete the investigation only when you accept the current evidence boundary:

```bash
uv run kitaru investigation update \
  "$INVESTIGATION_ID" \
  --status completed
```

The investigation status describes the review process. It does not claim that an agent problem has been fixed or that the reviewed sample represents all traffic.

## Checkpoint

You now have:

- a fixed `returns-discovery` review worklist;
- one neutral, trace-specific question per selected session;
- persisted annotations linked to relevant evidence;
- explicit whole-session verdicts where the evidence supported them; and
- an accepted boundary around what the review did and did not establish.

No agent or model has run. Continue to [3. Define one behavior to test](define.md).
