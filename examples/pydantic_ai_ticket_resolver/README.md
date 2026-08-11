# Resolve returns tickets with Kitaru

This example runs an autonomous returns agent against ten synthetic customer emails. The agent investigates each order, checks the relevant policy or shipment, records a mock action, and drafts the reply. Langfuse captures the real PydanticAI executions, and Kitaru imports them as replayable sessions.

All customers, orders, shipments, and actions are synthetic. Refund and replacement tools only modify an in-memory store.

Run every command from `examples/pydantic_ai_ticket_resolver`.

Copy the local environment template before choosing either path:

```bash
cp .env.example .env
set -a; source .env; set +a
```

The second command exports the values from `.env` into the current terminal. Run it once in every terminal used for this example. It also clears credentials left by an earlier Kitaru task or server before loading values explicitly defined in `.env`.

The template points the CLI at `http://localhost:8000` and includes a local-only worker credential for the server's unauthenticated development mode. Model and Langfuse credentials are only required when regenerating the traces.

## Optional Step 0: Generate real traces

Add your OpenAI and Langfuse credentials to `.env`.

Generate ten baseline traces:

```bash
./generate.sh
```

The script makes real model calls and writes the Langfuse export to `traces/langfuse-traces.jsonl`. It does not connect to Kitaru or create Kitaru resources.

## Step 1: Start Kitaru locally

Start PostgreSQL, the Kitaru API, and the dashboard:

```bash
docker compose -f ../../docker-compose.yml up -d --build
```

Install the dependencies and connect the CLI:

```bash
uv sync --extra cli --extra worker --extra examples
uv pip install --editable '../../plugins/packages/pydantic-ai[openai]'
uv run --no-sync python ../../scripts/smoke_plugin_artifacts.py \
  --candidate-dir ../../plugins/candidate-wheels
export UV_FIND_LINKS="$(cd ../../plugins/candidate-wheels && pwd)"
uv run kitaru login --local
uv run kitaru status
```

The second command installs the standalone `kitaru-pydantic-ai` package and its OpenAI provider dependency into the repository environment. The next commands build and expose local wheels for the independently packaged importers and evaluators. Keep `UV_FIND_LINKS` set in every terminal that runs a worker until those exact package versions are available from the configured package index.

The server registers Kitaru's official importers and evaluators when it starts. Confirm that the `kitaru/langfuse` importer and the `kitaru/cost`, `kitaru/latency`, and `kitaru/tool-call-patterns` evaluators are available:

```bash
uv run kitaru importer list
uv run kitaru evaluator list
```

Open [http://localhost:8000](http://localhost:8000) to use the dashboard.

## Step 2: Register the baseline agent

The PydanticAI entrypoint is `agent.py`. Each invocation resolves one incoming email without a human turn.

- **Purpose:** investigate and resolve returns, refunds, and missing shipments.
- **Input:** one synthetic support email with a ticket ID, customer identity, subject, and body.
- **Output:** action, amount, reason, and customer reply.
- **State:** one isolated in-memory commerce store per invocation.
- **Tools:** `lookup_order`, `get_return_policy`, `check_shipping`, `issue_refund`, `create_replacement`, and `escalate_to_human`.
- **MCP servers:** none.
- **Skills:** none.

Register the baseline:

```bash
uv run kitaru agent register \
  returns-resolver \
  --command "python -m examples.pydantic_ai_ticket_resolver.agent" \
  --description "Resolve one synthetic returns or delivery ticket, execute one mock action, and draft the customer reply." \
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

Registration creates the `returns-resolver` agent and version `1`.

## Step 3: Start a worker

Open a second terminal in this directory and keep the worker active:

```bash
export UV_FIND_LINKS="$(cd ../../plugins/candidate-wheels && pwd)"
uv run kitaru worker start \
  --name returns-example-worker
```

Confirm that Kitaru can see it:

```bash
uv run kitaru worker list
```

## Step 4: Import the baseline sessions

Import the Langfuse traces under the exact baseline agent version:

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

The importer preserves the LLM calls, tool calls, tool results, final resolution, source trace IDs, and baseline agent version.

Check what the import produced:

```bash
uv run kitaru session list \
  --tag returns-baseline \
  --origin imported \
  --size 20
```

## Step 5: Find useful starting points

Run Kitaru's deterministic evaluators across the imported baseline:

```bash
uv run kitaru session evaluate \
  --tag returns-baseline \
  --evaluator kitaru/cost@latest \
  --evaluator kitaru/latency@latest \
  --evaluator kitaru/tool-call-patterns@latest \
  --wait
```

These evaluators make no model calls. Cost and latency show resource variation, while tool-call patterns expose repeated lookups and different investigation paths.

List the stored results:

```bash
uv run kitaru evaluation list --size 100
```

## Step 6: Record a review

The coding-agent walkthrough selects a diverse review set and conducts the full investigation through Kitaru's MCP server. This manual path records one representative judgment so you can see the underlying data.

Resolve ticket 004 and inspect its nodes:

```bash
TICKET_004_SESSION_ID="$(
  uv run kitaru --output json session list \
    --tag returns-baseline \
    --origin imported \
    --size 20 \
  | jq -r '.items[] | select((.inputs.turns[-1].inputs.ticket_id // .inputs.ticket_id) == "ticket-004") | .id'
)"

uv run kitaru session nodes \
  "$TICKET_004_SESSION_ID" \
  --include-payloads \
  --size 100
```

Choose the node that contains the accepted `issue_refund` result and store its ID:

```bash
TICKET_004_REFUND_NODE_ID="YOUR_REFUND_NODE_UUID"
```

Create a one-session investigation with a session question and a curated evidence view:

```bash
INVESTIGATION_ID="$(
  uv run kitaru --output json investigation create refund-policy-review \
    --agent returns-resolver \
    --description "Review whether risky refunds require human approval." \
    --session "$TICKET_004_SESSION_ID" \
    --session-question "$TICKET_004_SESSION_ID=Is this outcome acceptable, problematic, or uncertain, and what should the agent have done instead?" \
    --session-view "$TICKET_004_SESSION_ID={\"summary\":\"A \$280 refund exceeded the automatic approval threshold.\",\"items\":[{\"label\":\"Accepted refund\",\"description\":\"The terminal action that needs review.\",\"selectors\":[{\"node_id\":\"$TICKET_004_REFUND_NODE_ID\",\"part\":\"output\"}]}]}" \
  | jq -r '.item.id'
)"

INVESTIGATION_SESSION_ID="$(
  uv run kitaru --output json investigation session list \
    "$INVESTIGATION_ID" \
    --size 20 \
  | jq -r '.items[0].id'
)"
```

Store the support lead's answers and anchor the outcome judgment to the refund node:

```bash
uv run kitaru annotation create \
  --investigation-session "$INVESTIGATION_SESSION_ID" \
  --selector "{\"node_id\":\"$TICKET_004_REFUND_NODE_ID\",\"part\":\"output\"}" \
  --value '{"judgment":"problematic","reason":"The amount exceeds the automatic approval threshold."}'

uv run kitaru annotation create \
  --investigation-session "$INVESTIGATION_SESSION_ID" \
  --value '{"action":"escalate","reason":"Human approval is required before a refund."}'

uv run kitaru investigation session complete \
  "$INVESTIGATION_ID" \
  "$TICKET_004_SESSION_ID"
```

Kitaru now stores the session question, review progress, answers, and exact trace evidence. The guided path repeats this review over a diverse set that it selects from the sessions.

## Step 7: Decide what to improve

The broad evaluators describe the sessions, but they cannot decide what good behavior means for your business. Before creating a cohort, answer these questions:

1. Which agent outcome matters most?
2. Which observed behavior is unacceptable?
3. What should the agent have done instead?
4. Which successful cases must remain correct?
5. What evidence would be enough to use the candidate version?

For this example, the support lead gives these answers:

- Refunds above the automatic approval threshold must escalate.
- Refunds on orders with a risk flag must escalate.
- Valid refunds must remain refunds and cannot exceed the amount paid.
- Both risky cases must become correct, all control cases must remain correct, and no replay may fail.

The [coding-agent walkthrough](README_AGENT_GUIDED.md) shows how to conduct the same workflow through Kitaru's MCP server. It inspects the traces, asks these questions one at a time, proposes evidence-backed cohort membership, and carries the session IDs into Kitaru. After you approve the behavior brief, it creates and validates the evaluator used by the experiment.

The remaining commands show the same operations manually.

## Step 8: Create a policy evaluator

An evaluator is a Python function that applies the same check to every recorded or replayed session. Here, it turns the support lead's approved rule into one Boolean result named `policy_correct`.

The rubric is observable in the trace:

- **Pass:** the final action, accepted terminal tool call, and refund amount match the reviewed outcome.
- **Fail:** the action is wrong, a refund has the wrong amount, or conflicting accepted actions occurred. An escalation cannot pass if the agent already issued a refund.
- **Missing evidence:** fail the evaluation task with a clear error instead of guessing.

Ticket 001 is a passing example because the agent issues the reviewed $98 refund. Ticket 004 is a failing example because it issues $280 above the automatic approval threshold. Ticket 007 is a failing example because it accepts a refund despite an account risk flag.

The starter example does not contain `evaluator.py`. Create the scaffold after agreeing on the rubric:

```bash
uv run kitaru evaluator scaffold \
  returns-policy \
  --path evaluator.py
```

Replace the scaffold with this implementation:

<details>
<summary>Show evaluator.py</summary>

```python
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Evaluate whether a returns resolution follows the reviewed policy."""

import json
from decimal import Decimal, InvalidOperation
from typing import Any

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session_node import NodeType
from kitaru.task.evaluator import SessionView

REVIEWED_OUTCOMES = {
    "ticket-001": ("refund", Decimal("98.00")),
    "ticket-002": ("escalate", None),
    "ticket-003": ("escalate", None),
    "ticket-004": ("escalate", None),
    "ticket-005": ("escalate", None),
    "ticket-006": ("replacement", None),
    "ticket-007": ("escalate", None),
    "ticket-008": ("escalate", None),
    "ticket-009": ("refund", Decimal("80.00")),
    "ticket-010": ("refund", Decimal("98.00")),
}

ACTION_TO_TOOL = {
    "refund": "issue_refund",
    "replacement": "create_replacement",
    "escalate": "escalate_to_human",
}


def _get_latest_turn(value: Any, field: str) -> Any:
    """Unwrap one field from the latest imported turn when present."""
    if isinstance(value, dict) and isinstance(value.get("turns"), list):
        turns = value["turns"]
        if not turns:
            raise ValueError("The imported session has no turns.")
        return turns[-1].get(field)
    return value


def _get_resolution(value: Any) -> dict[str, Any]:
    """Read the native or imported resolution output."""
    value = _get_latest_turn(value, "outputs")
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict) or not isinstance(value.get("action"), str):
        raise ValueError("Session outputs do not contain a resolution action.")
    return value


def _get_amount(value: Any) -> Decimal | None:
    """Parse one optional money amount."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("The recorded refund amount is invalid.") from exc


def _get_accepted_terminal_actions(
    session: SessionView,
) -> list[tuple[str, Decimal | None]]:
    """Read accepted terminal actions from recorded tool nodes."""
    tool_nodes = [
        node for node in session.nodes if node.node_type is NodeType.TOOL_CALL
    ]
    if not tool_nodes:
        raise ValueError("Session nodes do not contain tool-call evidence.")

    terminal_tools = set(ACTION_TO_TOOL.values())
    actions: list[tuple[str, Decimal | None]] = []
    for node in tool_nodes:
        if node.tool_name not in terminal_tools:
            continue
        output = node.outputs
        if isinstance(output, str):
            output = json.loads(output)
        if isinstance(output, dict) and output.get("accepted") is True:
            actions.append((node.tool_name, _get_amount(output.get("amount"))))
    return actions


def evaluate(session: SessionView) -> EvaluationResult:
    """Pass when the reported and accepted actions match the reviewed outcome."""
    inputs = _get_latest_turn(session.session.inputs, "inputs")
    if not isinstance(inputs, dict) or not isinstance(inputs.get("ticket_id"), str):
        raise ValueError("Session inputs do not contain a ticket_id.")

    ticket_id = inputs["ticket_id"]
    if ticket_id not in REVIEWED_OUTCOMES:
        raise ValueError(f"No reviewed outcome exists for {ticket_id}.")

    expected_action, expected_amount = REVIEWED_OUTCOMES[ticket_id]
    resolution = _get_resolution(session.session.outputs)
    actual_action = resolution["action"]
    actual_amount = _get_amount(resolution.get("amount"))
    accepted_actions = _get_accepted_terminal_actions(session)
    expected_tool = ACTION_TO_TOOL[expected_action]
    expected_accepted = [(expected_tool, expected_amount)]

    passed = (
        actual_action == expected_action
        and (expected_amount is None or actual_amount == expected_amount)
        and accepted_actions == expected_accepted
    )
    return EvaluationResult(
        name="policy_correct",
        score=passed,
        passed=passed,
        explanation=(
            f"{ticket_id}: expected {expected_action} via {expected_tool}; "
            f"observed {actual_action} with accepted actions {accepted_actions}."
        ),
    )
```

</details>

Validate that the file loads and exposes the expected callable:

```bash
uv run kitaru evaluator test \
  evaluator.py \
  --entrypoint evaluate
```

This command validates loading and the function signature. It does not score a session. The server-side baseline evaluation in the next step checks the behavior against all ten recorded sessions.

Register its first immutable version:

```bash
uv run kitaru evaluator register \
  returns-policy \
  --script evaluator.py \
  --entrypoint evaluate \
  --description "Check whether the reported and accepted returns actions match the reviewed policy outcome." \
  --display-version 1.0
```

## Step 9: Score the baseline

Apply the policy evaluator to every imported baseline session:

```bash
uv run kitaru session evaluate \
  --tag returns-baseline \
  --evaluator returns-policy@1 \
  --wait
```

List the policy results:

```bash
uv run kitaru evaluation list \
  --filter '{"field":"name","op":"eq","value":"policy_correct"}' \
  --size 20
```

The checked-in baseline contains eight passes and two failures. Tickets 004 and 007 issue refunds where the reviewed policy requires escalation.

## Step 10: Create behavioral cohorts

List the baseline sessions with their ticket IDs and terminal actions:

```bash
uv run kitaru --output json session list \
  --tag returns-baseline \
  --origin imported \
  --size 20 \
  | jq -r '.items[] | [(.inputs.turns[-1].inputs.ticket_id // .inputs.ticket_id), .id, (.outputs.turns[-1].outputs.action // .outputs.action)] | @tsv'
```

Create the target cohort from tickets 004 and 007. Replace the placeholders with the listed session IDs:

```bash
uv run kitaru cohort create unsafe-refund-baseline \
  --agent returns-resolver \
  --description "Baseline sessions that refunded despite an approval or risk rule requiring escalation." \
  --session TICKET_004_SESSION_ID \
  --session TICKET_007_SESSION_ID
```

Create the control cohort from tickets 001, 009, and 010:

```bash
uv run kitaru cohort create safe-refund-control \
  --agent returns-resolver \
  --description "Valid refund sessions that must remain correct after the policy change." \
  --session TICKET_001_SESSION_ID \
  --session TICKET_009_SESSION_ID \
  --session TICKET_010_SESSION_ID
```

Confirm both immutable snapshots:

```bash
uv run kitaru session list --cohort unsafe-refund-baseline@1 --size 20
uv run kitaru session list --cohort safe-refund-control@1 --size 20
```

The target cohort captures the behavior that should change. The control cohort captures nearby behavior that must not regress.

## Step 11: Register an improved agent version

The baseline instructions tell the agent to assume that action tools enforce approval limits. The improved version removes that assumption and requires the agent to inspect risk flags, approval thresholds, final-sale rules, and return windows before calling `issue_refund`.

Register the same entrypoint with strict policy instructions enabled:

```bash
uv run kitaru agent version register \
  returns-resolver \
  --command "python -m examples.pydantic_ai_ticket_resolver.agent" \
  --description "Check approval and risk rules before issuing a refund." \
  --display-version strict-policy-v2 \
  --working-dir ../.. \
  --env RETURNS_POLICY_MODE=strict \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
```

This creates `returns-resolver@2`. The imported sessions remain attached to version 1.

## Step 12: Create the experiment

Create one reusable experiment that records the primary policy result plus the broad guardrails. Kitaru resolves each evaluator reference to an immutable version when it creates the experiment:

```bash
uv run kitaru experiment create \
  improve-returns-policy \
  --agent returns-resolver \
  --description "Replay policy-risk and valid-refund cohorts with strict refund approval rules." \
  --tool-policy '{"default":{"type":"passthrough"},"tools":{}}' \
  --evaluator returns-policy@1 \
  --evaluator kitaru/cost@latest \
  --evaluator kitaru/latency@latest \
  --evaluator kitaru/tool-call-patterns@latest
```

The mock commerce tools are safe to call again, so the experiment sets passthrough tool policy explicitly. Every replay receives a new isolated in-memory store.

## Step 13: Replay the target and control cohorts

Resolve the two cohort references to the UUIDs required by `experiment run start`:

```bash
TARGET_COHORT_VERSION_ID="$(
  uv run kitaru --output json \
    cohort version get unsafe-refund-baseline@1 \
  | jq -r '.item.id'
)"

CONTROL_COHORT_VERSION_ID="$(
  uv run kitaru --output json \
    cohort version get safe-refund-control@1 \
  | jq -r '.item.id'
)"
```

Replay the target cohort through agent version 2 and score both the imported baselines and replayed sessions:

```bash
uv run kitaru experiment run start \
  improve-returns-policy \
  --cohort-version "$TARGET_COHORT_VERSION_ID" \
  --agent returns-resolver@2 \
  --evaluate-baselines \
  --wait \
  --timeout 1800
```

Run the same experiment against the control cohort:

```bash
uv run kitaru experiment run start \
  improve-returns-policy \
  --cohort-version "$CONTROL_COHORT_VERSION_ID" \
  --agent returns-resolver@2 \
  --evaluate-baselines \
  --wait \
  --timeout 1800
```

Each baseline input now has a separate replayed session. Kitaru applies the same resolved policy and guardrail evaluator versions to both sides.

## Step 14: Compare the evidence

List the completed experiment runs:

```bash
uv run kitaru experiment run list --size 20
```

Each `experiment run start` receipt prints exact `get` and `jobs` commands in `next_actions`. Run those commands to inspect the replay and evaluator jobs. They have this form:

```bash
uv run kitaru experiment run get YOUR_RUN_UUID
uv run kitaru experiment run jobs YOUR_RUN_UUID --size 20
```

List the replayed sessions and policy evaluations:

```bash
uv run kitaru session list \
  --agent returns-resolver \
  --origin replay \
  --size 20

uv run kitaru evaluation list \
  --filter '{"field":"name","op":"eq","value":"policy_correct"}' \
  --size 100
```

Open [http://localhost:8000](http://localhost:8000) to compare each imported session with its replay, inspect the changed tool path, and review policy correctness, latency, and tool-call patterns together. The replay uses the same model as the checked-in baseline by default, so latency is comparable unless you override `BASELINE_MODEL`. Replay cost is marked unavailable because the PydanticAI adapter does not currently record provider cost.

The candidate succeeds when tickets 004 and 007 change from policy failure to pass, tickets 001, 009, and 010 remain passes, and every replay completes. Use the comparable latency and tool-path evidence as guardrails; do not interpret unavailable replay cost as zero cost. A failed replay remains useful evidence: inspect it, change the agent again, register another version, and rerun the same immutable experiment and cohort versions.

## Step 15: Stop the local server

After the walkthrough, stop the containers:

```bash
docker compose -f ../../docker-compose.yml down
```

The PostgreSQL volume retains the agents, sessions, evaluations, cohorts, and experiment runs.
