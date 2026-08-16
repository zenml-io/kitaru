# Investigate and improve a PydanticAI agent with Kitaru

This example uses an autonomous returns agent to demonstrate the complete Kitaru evidence loop. You import real PydanticAI traces from Langfuse, inspect behavior, record human judgments, freeze reviewed sessions into a cohort, evaluate a candidate, and compare replay evidence.

All customers, orders, shipments, and actions are synthetic. Refund and replacement tools modify one isolated in-memory store.

You can follow either route after setup:

- **Recommended:** install the Kitaru agent skills and give your coding agent one prompt.
- **Manual:** use the Kitaru CLI to perform the same operations yourself.

Neither route supplies an answer key. Review the session evidence before you define a problem, assign a verdict, choose cohort members, or write an evaluator.

Run all commands from `examples/pydantic_ai_ticket_resolver`.

## What you will create

The walkthrough produces durable Kitaru objects instead of a one-time report:

| Object | Purpose |
| --- | --- |
| Agent version | Identifies the exact registered command and capabilities used for a run. |
| Imported session | Preserves the Langfuse trace, model calls, tool calls, outputs, cost, tokens, and source identity. |
| Evaluation | Records one deterministic or custom measurement against a session. |
| Investigation | Stores an ordered review worklist and fixed questions for each session. |
| Annotation | Stores a human answer and can point to an exact node, JSON field, or character range. |
| Verdict | Records the human judgment for the complete session: `acceptable`, `problematic`, or `uncertain`. |
| Cohort version | Freezes reviewed session membership so later comparisons use the same population. |
| Evaluator version | Pins the code and parameters that turn an accepted behavior into repeatable measurements. |
| Experiment run | Replays one candidate across a cohort and keeps every completed, failed, canceled, and missing case visible. |

## Prepare the example

### 1. Install the locked environment

```bash
uv sync
```

### 2. Connect to a workspace

Start and select a CLI-managed local workspace:

```bash
uv run kitaru login --local
uv run kitaru status
```

Or select a remote workspace:

```bash
uv run kitaru login https://your-kitaru-workspace.example.com
uv run kitaru status
```

Confirm the official Langfuse importer and the built-in evaluator catalog:

```bash
uv run kitaru importer list
uv run kitaru evaluator list
```

### 3. Register the recorded baseline

The public PydanticAI entrypoint is `agent.py`. One invocation resolves one support email with these tools:

- `lookup_order`
- `get_return_policy`
- `check_shipping`
- `issue_refund`
- `create_replacement`
- `escalate_to_human`

Register the baseline:

```bash
uv run kitaru agent register \
  returns-resolver \
  --command "python -m examples.pydantic_ai_ticket_resolver.agent" \
  --description "Resolve one synthetic returns or delivery request, execute one mock action, and draft the customer reply." \
  --display-version baseline-v1 \
  --working-dir ../.. \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
```

This command creates the agent and its first immutable version. The receipt shows two UUIDs:

- `Parent ID` identifies the agent across all versions.
- `Version ID` identifies the exact registered version.

The commands below use `returns-resolver@1`, so you do not need to copy either UUID. If a command or API asks for an agent-version ID, use `Version ID`.

### 4. Start a worker

Imports and deterministic evaluations do not need an OpenAI key. Replays use `openai:gpt-5-nano`, make paid OpenAI API calls, and require `OPENAI_API_KEY`.

For this local walkthrough, open a second terminal in this directory. Export the key in that shell, then start the worker:

```bash
export OPENAI_API_KEY="your-openai-key"
uv run kitaru worker start --name returns-example-worker
```

You can also use a secret manager that injects `OPENAI_API_KEY` into the worker process. For a deployed worker, configure the environment in your deployment system or attach a [Kitaru secret](../../docs/book/deploy/secrets.md) to the agent version.

The worker runs in the foreground. The `starting: {...}` message means that it is ready and waiting for tasks. Leave this terminal open and run the remaining commands in your first terminal. Press Ctrl-C to stop the worker.

Confirm that Kitaru can see it:

```bash
uv run kitaru worker list
```

### 5. Import the Langfuse sessions

```bash
uv run kitaru session import \
  traces/langfuse-traces.jsonl \
  --importer kitaru/langfuse@latest \
  --agent returns-resolver@1 \
  --tag returns-baseline \
  --params '{"source_instance":"canonical-returns-example"}' \
  --media-type application/x-ndjson \
  --wait
```

Verify the imported population:

```bash
uv run kitaru session list \
  --tag returns-baseline \
  --origin imported \
  --size 20
```

## Recommended route: use the Kitaru skills

Install the Kitaru skills with the cross-host Agent Skills installer:

```bash
npx skills add zenml-io/kitaru-skills
```

Configure the native Kitaru MCP server in `standard` mode. This mode lets the coding agent read sessions and create investigations, annotations, cohorts, evaluators, evaluations, experiments, and runs. It does not expose destructive operations.

Find the executable:

```bash
uv run which kitaru-mcp
```

Add it to your coding agent's MCP configuration. Replace the command and server values:

```json
{
  "mcpServers": {
    "kitaru": {
      "command": "/absolute/path/to/.venv/bin/kitaru-mcp",
      "args": [
        "--mode",
        "standard",
        "--server",
        "https://your-kitaru-workspace.example.com"
      ]
    }
  }
}
```

Use `http://localhost:8000` for a local workspace. Restart the coding-agent session after you add the MCP server.

Give the coding agent this prompt:

```text
Use the kitaru-investigation skill to investigate the PydanticAI returns agent
registered as returns-resolver. Its imported sessions have the tag
returns-baseline. Assume I am new to Kitaru, explain each concept when it becomes
useful, and show me the recorded evidence before asking for a judgment. Do not
use fixture implementation details or test-only expected outcomes as an answer
key. Ask before creating or changing Kitaru resources. If we agree on a change
to test, continue with kitaru-replay-experiment and ask before changing code or
starting paid model calls.
```

The two named skills divide the work cleanly:

- `kitaru-investigation` discovers behavior, stores human evidence, confirms a cohort, and selects or creates an evaluator.
- `kitaru-replay-experiment` tests one accepted candidate against exact cohort and evaluator versions.

The skills will pause for human judgments and consequential writes. This is part of the evidence model. An assistant suggestion never becomes a human annotation or verdict without your confirmation.

## Manual route: operate the evidence loop yourself

The manual route uses the same durable objects. Do not copy identifiers from this document. Select sessions from the evidence in your own workspace.

### 1. Survey the population

Run low-cost deterministic evaluators before you decide what is wrong:

```bash
uv run kitaru session evaluate \
  --tag returns-baseline \
  --evaluator kitaru/session-diagnostics@latest \
  --evaluator kitaru/tool-health@latest \
  --evaluator kitaru/trajectory-signals@latest \
  --evaluator kitaru/llm-call-signals@latest \
  --evaluator kitaru/cost@latest \
  --evaluator kitaru/timing-profile@latest \
  --wait
```

Inspect the resulting measurements:

```bash
uv run kitaru evaluation list --size 100
```

Print a compact session inventory:

```bash
uv run kitaru --output json session list \
  --tag returns-baseline \
  --origin imported \
  --size 20 \
  | jq -r '.items[] | [.id, .name, .status, .outputs.action, .cost, .llm_call_count, .tool_call_count] | @tsv'
```

Choose a bounded worklist. Include different outcomes and tool paths, operational outliers, and at least one random session. Do not label a session from summary fields alone.

### 2. Inspect complete traces

Set one selected session ID and inspect all nodes with payloads:

```bash
SESSION_ID="YOUR_SESSION_UUID"

uv run kitaru session nodes \
  "$SESSION_ID" \
  --include-payloads \
  --size 100
```

Record the node IDs that contain useful evidence. A useful highlight can identify a model response, tool input, tool result, or final output.

Inspect each selected trace before you write its question. Give each session a distinct question about a concrete decision, tool interaction, inconsistency, operational signal, or missing piece of evidence visible in that trace. Keep the question neutral. Do not assume an expected outcome or repeat generic wording. The question and each highlight description must make sense in the frontend without this walkthrough as context.

Before you create the investigation, review the complete plan:

| Field | Requirement |
| --- | --- |
| Session | Exact session ID and review position. |
| Selection reason | Evidence-based reason for including this session. |
| Question | One concise, session-specific question that requires human judgment. |
| Highlights | Exact nodes or fields that help answer the question without stating the conclusion. |

### 3. Create an investigation

Create the complete fixed worklist in one command. Repeat `--session`, `--session-question`, and optional `--session-highlights` for every selected session:

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

Save the returned investigation ID. List its ordered review queue:

```bash
INVESTIGATION_ID="YOUR_INVESTIGATION_UUID"

uv run kitaru investigation session list \
  "$INVESTIGATION_ID" \
  --size 20
```

Each linked record has its own investigation-session ID. Use that ID when you answer a question.

Open the investigation from the agent's **Investigations** page. The frontend presents each question beside its highlighted trace evidence. Complete the question and verdict for each session there. The persisted answers and verdicts are available to the coding agent and CLI after you finish.

### 4. Store a precise human annotation

An annotation selector can target three levels:

- `node_id` identifies the exact trace node.
- `path` is an RFC 6901 JSON pointer into the node or session response.
- `span` identifies a character range inside the string resolved by `path`.

Store your observation against an exact field and character range:

```bash
INVESTIGATION_SESSION_ID="YOUR_INVESTIGATION_SESSION_UUID"
EVIDENCE_NODE_ID="YOUR_EVIDENCE_NODE_UUID"

uv run kitaru annotation create \
  --investigation-session "$INVESTIGATION_SESSION_ID" \
  --question-key observation \
  --selector "{\"node_id\":\"$EVIDENCE_NODE_ID\",\"path\":\"/outputs/message\",\"span\":{\"start\":0,\"end\":40}}" \
  --value '"Write your own observation here."'
```

Use a node-only selector when the whole node is evidence. Omit the selector when the judgment depends on the complete session.

Record the complete-session verdict separately:

```bash
uv run kitaru investigation session verdict \
  "$INVESTIGATION_ID" \
  "$SESSION_ID" \
  acceptable
```

Choose `acceptable`, `problematic`, or `uncertain` from your own review. Leave the verdict unset when you do not have a complete-session judgment.

Repeat the review for the bounded worklist. Then inspect answer and verdict coverage:

```bash
uv run kitaru investigation get "$INVESTIGATION_ID"

uv run kitaru annotation list \
  --filter "{\"field\":\"investigation_id\",\"op\":\"eq\",\"value\":\"$INVESTIGATION_ID\"}" \
  --size 100
```

Mark the investigation complete only after you accept the current evidence boundary:

```bash
uv run kitaru investigation update \
  "$INVESTIGATION_ID" \
  --status completed
```

### 5. Define one observable behavior

Use only persisted human observations and confirmed verdicts. Write one binary definition that answers these questions:

1. Under which observable conditions does the behavior matter?
2. Which recorded agent action passes?
3. Which recorded agent action fails?
4. Which external outcome evidence is required?
5. What happens when evidence is missing?
6. Which reviewed counterexamples limit the definition?

Keep agent behavior separate from tool or provider failures. Do not claim prevalence from this small adaptive sample.

### 6. Freeze the reviewed population

Before you create a cohort, list the exact included session IDs and the reviewed counterexamples. Confirm the membership, then create an immutable version:

```bash
uv run kitaru cohort create returns-regression \
  --agent returns-resolver \
  --description "Human-reviewed sessions for one accepted returns behavior." \
  --display-version initial-review \
  --session YOUR_REVIEWED_SESSION_UUID \
  --session YOUR_COUNTEREXAMPLE_SESSION_UUID
```

Verify the frozen population:

```bash
uv run kitaru cohort version get returns-regression@1
uv run kitaru session list --cohort returns-regression@1 --size 20
```

Create a new cohort version when membership changes. Existing versions remain unchanged.

### 7. Select or create an evaluator

Inspect the installed catalog first:

```bash
uv run kitaru evaluator list
```

Use an installed evaluator when it expresses the accepted behavior. Pin its exact version and parameters.

If no installed evaluator fits, create one narrow evaluator:

```bash
uv run kitaru evaluator scaffold returns-behavior --path evaluator.py
```

Replace the scaffolded `evaluator.py` with an implementation of the behavior that you accepted during review. For example, this evaluator checks that the final structured action matches the one accepted terminal tool call:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Evaluate consistency between an accepted action and the final output."""

from typing import Any

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session_node import NodeType
from kitaru.task.evaluator import SessionView

ACTION_BY_TOOL = {
    "issue_refund": "refund",
    "create_replacement": "replacement",
    "escalate_to_human": "escalate",
}


def _get_outputs(value: Any) -> dict[str, Any] | None:
    """Return final outputs from a native or imported session."""
    if isinstance(value, dict) and isinstance(value.get("turns"), list):
        turns = value["turns"]
        value = turns[-1].get("outputs") if turns else None
    return value if isinstance(value, dict) else None


def evaluate(session: SessionView) -> EvaluationResult:
    """Check that one accepted terminal tool matches the final action."""
    accepted_tools = [
        node.tool_name
        for node in session.nodes
        if node.node_type is NodeType.TOOL_CALL
        and node.tool_name in ACTION_BY_TOOL
        and isinstance(node.outputs, dict)
        and node.outputs.get("accepted") is True
    ]
    outputs = _get_outputs(session.session.outputs)

    if not accepted_tools or outputs is None:
        return EvaluationResult(
            name="terminal_action_consistency",
            value="unknown",
            passed=None,
            explanation="The trace does not contain enough recorded action evidence.",
        )

    if len(accepted_tools) != 1:
        return EvaluationResult(
            name="terminal_action_consistency",
            value="fail",
            passed=False,
            explanation=f"The trace contains {len(accepted_tools)} accepted actions.",
        )

    accepted_action = ACTION_BY_TOOL[accepted_tools[0]]
    reported_action = outputs.get("action")
    passed = reported_action == accepted_action
    return EvaluationResult(
        name="terminal_action_consistency",
        value="pass" if passed else "fail",
        passed=passed,
        explanation=(
            f"Accepted action: {accepted_action!r}; "
            f"reported action: {reported_action!r}."
        ),
    )
```

This example uses structured output and recorded tool results. It does not search the customer reply for words such as `refund`, and it does not map ticket IDs to expected answers. Adapt the rule to the behavior and missing-evidence policy that you confirmed during review.

Validate and register the implementation:

```bash
uv run kitaru evaluator test evaluator.py --entrypoint evaluate

uv run kitaru evaluator register \
  returns-behavior \
  --script evaluator.py \
  --entrypoint evaluate \
  --description "Evaluate the accepted returns behavior from recorded trace evidence." \
  --display-version initial-review
```

Run it against the exact cohort and compare its results with the human annotations and verdicts:

```bash
uv run kitaru session evaluate \
  --cohort returns-regression@1 \
  --evaluator returns-behavior@1 \
  --wait

uv run kitaru evaluation list --size 100
```

Report measured agreement, disagreements, missing evidence, and held-out coverage. Do not call the evaluator production-ready from a load test or a small reviewed sample.

### 8. Register one candidate

Make one bounded change to the agent after the investigation identifies a behavior worth changing. Register the changed working tree as a new agent version:

```bash
uv run kitaru agent version register \
  returns-resolver \
  --command "python -m examples.pydantic_ai_ticket_resolver.agent" \
  --description "Test one investigation-derived behavior change." \
  --display-version candidate-v1 \
  --working-dir ../.. \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
```

Record the exact candidate version and source revision. The run spec identifies the command but does not freeze a mutable working tree.

### 9. Create one bounded experiment

This example's tools change only an isolated in-memory store. The following explicit passthrough policy is safe for this synthetic agent. Use recorded history with `on_miss=fail` for real side-effecting tools unless live execution is deliberate and approved.

Create an experiment with one primary evaluator and two operational protections:

```bash
uv run kitaru experiment create returns-candidate \
  --agent returns-resolver \
  --description "Test one accepted behavior change against the reviewed cohort." \
  --tool-policy '{"default":{"type":"passthrough"},"tools":{}}' \
  --evaluator returns-behavior@1 \
  --evaluator kitaru/tool-health@latest \
  --evaluator kitaru/timing-profile@latest
```

Resolve the immutable cohort-version ID:

```bash
COHORT_VERSION_ID="$(
  uv run kitaru --output json cohort version get returns-regression@1 \
  | jq -r '.item.id'
)"
```

Start the experiment against the exact candidate version. `--evaluate-baselines` applies the same evaluator versions to both sides:

```bash
uv run kitaru experiment run start \
  returns-candidate \
  --cohort-version "$COHORT_VERSION_ID" \
  --agent returns-resolver@2 \
  --evaluate-baselines \
  --wait \
  --timeout 1800
```

### 10. Read the paired evidence

List runs and inspect the exact run receipt:

```bash
uv run kitaru experiment run list --size 20
uv run kitaru experiment run get YOUR_RUN_UUID
uv run kitaru experiment run jobs YOUR_RUN_UUID --size 100
```

Inspect replay sessions and their evaluations:

```bash
uv run kitaru session list \
  --agent returns-resolver \
  --origin replay \
  --size 20

uv run kitaru evaluation list --size 100
```

Compare each baseline with its replay. Include evaluator transitions, tool-path changes, cost, tokens, failures, canceled work, and missing results. Keep failed and missing cases visible instead of shrinking the denominator.

Use one evidence conclusion:

- `improved`
- `regressed`
- `trade-off`
- `inconclusive`

Kitaru supplies the evidence. You make the deployment decision.

## Stop the example

Stop the worker with `Ctrl-C`.

Disconnect from the selected workspace:

```bash
uv run kitaru logout
```

For a CLI-managed local workspace, logout stops its containers and retains the PostgreSQL volume. For a remote workspace, logout removes the stored credential without changing the deployment.
