# Improve a TypeScript returns agent with Kitaru

This example runs the canonical ten-ticket returns story with TypeScript, AI SDK 7, and Kitaru's Vercel adapter. It records the baseline directly into Kitaru, turns a human policy judgment into an evaluator, and replays target and control cohorts through stricter instructions.

All customers, orders, shipments, and actions are synthetic. Every invocation gets a fresh in-memory store. Passthrough replay is safe here for that reason only; do not copy that policy to tools that contact payment, fulfillment, or support systems.

The default `MockLanguageModelV4` path is scripted. Its recordings use the requested model ID `openai/gpt-5-nano` and fixed synthetic token counts, so token and cost figures are scripted rather than measured. Its fixed outcomes prove the adapter, recording, evaluation, cohort, and replay workflow. They do not prove that prompting caused a real model to improve. The optional OpenAI path is paid, non-deterministic evidence to inspect and never promises the fixed result table below.

Run commands from `v2_examples/vercel_ai_ticket_resolver` unless a command says otherwise.

## 1. Install and start Kitaru

Use Node 22, Python 3.11 or newer, pnpm, uv, jq, Docker, and a source checkout of this repository. Python and uv are still required because Kitaru's current evaluator and worker ABI is Python, even though the agent itself is TypeScript.

Build the local TypeScript packages without registering this standalone example in the root workspace, then install and verify the example:

```bash
pnpm --dir ../.. --filter @zenml-io/kitaru build
pnpm --dir ../.. --filter @zenml-io/kitaru-vercel-ai build
pnpm --ignore-workspace install --frozen-lockfile
pnpm build
pnpm test
pnpm typecheck
pnpm lint
```

With PostgreSQL reachable on the configured Kitaru test database port, run the provider-free live API, evaluator, cohort, and replay proof:

```bash
pnpm test:e2e
```

Start PostgreSQL, the API, and dashboard, then install and connect the Python CLI and worker:

```bash
docker compose -f ../../docker-compose.yml up -d --build
uv sync --extra cli --extra worker --extra mcp
cp .env.example .env
set -a; source .env; set +a
uv run kitaru login --local
uv run kitaru status
```

Load `.env` in every terminal used for the example. It gives both the TypeScript adapter and Python tools the local API URL; the default values do not authorize a provider call.

## 2. Register the baseline and start a worker

Register the compiled TypeScript command. One invocation receives one rendered email string, calls only the six synthetic tools, and returns structured resolution JSON.

```bash
uv run kitaru agent register \
  returns-resolver \
  --command "node dist/main.js" \
  --description "Resolve one synthetic returns or delivery ticket." \
  --display-version baseline-v1 \
  --working-dir . \
  --env RETURNS_POLICY_MODE=baseline \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
```

Start a worker in a second terminal from this directory and leave it active:

```bash
set -a; source .env; set +a
uv run kitaru worker start --name vercel-returns-example-worker
```

## 3. Record the exact deterministic baseline

Resolve and export the registered identities, then record all ten tickets directly through `createKitaruGenerateText`:

```bash
export KITARU_AGENT_ID="$(
  uv run kitaru --output json agent get returns-resolver | jq -r '.item.id'
)"
export KITARU_AGENT_VERSION_ID="$(
  uv run kitaru --output json agent version get returns-resolver@1 | jq -r '.item.id'
)"
pnpm baseline
```

The runner writes the exact ticket-to-session map to `.state/baseline-sessions.json` after each completed ticket. It resumes missing tickets by default. `--fresh` archives the prior manifest under `.state/evidence-sets/` before creating another evidence set, so use it only when you intentionally want new evidence.

If a process stops after Kitaru writes a session ID but before the manifest commits it, the runner refuses to guess. Inspect the orphan named in `.state/attempts/<evidence-set-id>/<ticket-id>.session-id` with `uv run kitaru session get SESSION_ID`. Adopt it only after confirming that the remote session completed:

```bash
pnpm baseline -- --adopt ticket-004=SESSION_ID
```

The general recovery form is `--adopt ticket-id=session-id`. If the session failed or is still in progress, do not adopt it. After you inspect the exact remote session and confirm it failed, use `--retry ticket-id=session-id`. Retry archives the local orphan marker, records a new remote session, and never deletes remote state.

Only the rendered ticket ID, sender, subject, and body enter the recorded prompt. The fixture's `scenario` and `expected_action` fields are verification oracles and are not recorded inputs. Do not use them to decide annotations or cohort membership before investigating trace evidence.

Create a reusable exact-session file and inspect the recorded set:

```bash
jq -r '.sessions | to_entries[] | .value.session_id' \
  .state/baseline-sessions.json > .state/baseline-session-ids.txt
uv run kitaru session list \
  --agent returns-resolver \
  --origin recorded \
  --status completed \
  --size 20
```

## 4. Measure and review the baseline

Run the broad deterministic evaluators over only the manifest sessions:

```bash
uv run kitaru session evaluate \
  --sessions-file .state/baseline-session-ids.txt \
  --evaluator kitaru/cost@latest \
  --evaluator kitaru/latency@latest \
  --evaluator kitaru/tool-call-patterns@latest \
  --wait \
  --timeout 1800
uv run kitaru evaluation list --size 100
```

These signals describe cost, latency, and tool paths. They do not decide whether a refund is acceptable. Inspect ticket 004 as one representative policy judgment:

```bash
TICKET_004_SESSION_ID="$(
  jq -r '.sessions["ticket-004"].session_id' .state/baseline-sessions.json
)"
uv run kitaru session nodes \
  "$TICKET_004_SESSION_ID" \
  --include-payloads \
  --size 100
```

Create an investigation with a bounded evidence view, answer each question, anchor the outcome annotation to the completed accepted `issue_refund` node, and mark the session complete. Replace the node placeholder after inspecting the trace:

```bash
TICKET_004_REFUND_NODE_ID="YOUR_COMPLETED_REFUND_NODE_UUID"
INVESTIGATION_ID="$(
  uv run kitaru --output json investigation create refund-policy-review \
    --agent returns-resolver \
    --description "Review whether risky refunds require human approval." \
    --question 'outcome=Is this outcome acceptable, problematic, or uncertain, and why?' \
    --question 'expected=What should the agent have done?' \
    --session "$TICKET_004_SESSION_ID" \
    --session-view "$TICKET_004_SESSION_ID={\"summary\":\"A \$280 refund exceeded the automatic approval threshold.\",\"items\":[{\"label\":\"Accepted refund\",\"selectors\":[{\"node_id\":\"$TICKET_004_REFUND_NODE_ID\",\"part\":\"output\"}]}]}" \
  | jq -r '.item.id'
)"
INVESTIGATION_SESSION_ID="$(
  uv run kitaru --output json investigation session list \
    "$INVESTIGATION_ID" --size 20 | jq -r '.items[0].id'
)"
uv run kitaru annotation create \
  --investigation-session "$INVESTIGATION_SESSION_ID" \
  --question-key outcome \
  --selector "{\"node_id\":\"$TICKET_004_REFUND_NODE_ID\",\"part\":\"output\"}" \
  --value '{"judgment":"problematic","reason":"The amount exceeds the automatic approval threshold."}'
uv run kitaru annotation create \
  --investigation-session "$INVESTIGATION_SESSION_ID" \
  --question-key expected \
  --value '{"action":"escalate","reason":"Human approval is required."}'
uv run kitaru investigation session complete \
  "$INVESTIGATION_ID" "$TICKET_004_SESSION_ID"
```

Repeat the review for a diverse set before fixing the rule. The agreed behavior for this story is: refunds above the category's approval threshold escalate; orders with risk flags escalate; valid refunds remain refunds and never exceed the amount paid; missing evidence is an error rather than a guess.

## 5. Create and register the reviewed evaluator

The starter does not ship `evaluator.py`. Python is a walkthrough output because the current Kitaru evaluator ABI is Python. After the behavior brief is approved, create the file with the single source block below.

<!-- documented-evaluator:start -->
```python
"""Evaluate whether a returns resolution follows the reviewed policy."""

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
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


def _latest_turn_field(value: Any, field: str) -> Any:
    """Unwrap one field from the latest imported turn when present."""
    if isinstance(value, dict) and isinstance(value.get("turns"), list):
        turns = value["turns"]
        if not turns:
            raise ValueError("The imported session has no turns.")
        latest = turns[-1]
        if not isinstance(latest, dict):
            raise ValueError("The imported session has a malformed latest turn.")
        return latest.get(field)
    return value


def _get_ticket_id(value: Any) -> str:
    """Read a ticket ID from imported inputs or the Vercel prompt string."""
    value = _latest_turn_field(value, "inputs")
    if isinstance(value, dict) and isinstance(value.get("ticket_id"), str):
        return value["ticket_id"]
    if isinstance(value, str):
        match = re.search(r"(?m)^Ticket ID:\s*(ticket-\d+)\s*$", value)
        if match:
            return match.group(1)
    raise ValueError("Session inputs do not contain a ticket ID.")


def _get_resolution(value: Any) -> dict[str, Any]:
    """Read an imported resolution or parse Vercel session.outputs.text."""
    value = _latest_turn_field(value, "outputs")
    if isinstance(value, dict) and "text" in value:
        text = value["text"]
        if not isinstance(text, str):
            raise ValueError("Vercel session outputs.text must be a string.")
        try:
            value = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("Vercel outputs.text is not valid resolution JSON.") from exc
    elif isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("Session outputs are not valid resolution JSON.") from exc
    elif not (isinstance(value, dict) and isinstance(value.get("action"), str)):
        raise ValueError(
            "Vercel session outputs.text is missing and no imported resolution action exists."
        )
    if not isinstance(value, dict) or not isinstance(value.get("action"), str):
        raise ValueError("Session outputs do not contain a resolution action.")
    if value["action"] not in ACTION_TO_TOOL:
        raise ValueError(f"Unsupported resolution action: {value['action']!r}.")
    return value


def _get_amount(value: Any) -> Decimal | None:
    """Parse one optional money amount."""
    if value is None:
        return None
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("The recorded refund amount is invalid.") from exc
    if not amount.is_finite():
        raise ValueError("The recorded refund amount is invalid.")
    return amount


def _get_accepted_terminal_action(
    session: SessionView,
) -> tuple[str, Decimal | None]:
    """Require exactly one accepted, completed terminal tool-call node."""
    terminal_tools = set(ACTION_TO_TOOL.values())
    actions: list[tuple[str, Decimal | None]] = []
    for node in session.nodes:
        if (
            node.node_type is not NodeType.TOOL_CALL
            or node.status is not NodeStatus.COMPLETED
            or node.tool_name not in terminal_tools
        ):
            continue
        output = node.outputs
        if isinstance(output, str):
            try:
                output = json.loads(output)
            except json.JSONDecodeError as exc:
                raise ValueError("A terminal tool output is not valid JSON.") from exc
        if isinstance(output, dict) and output.get("accepted") is True:
            actions.append((node.tool_name, _get_amount(output.get("amount"))))
    if len(actions) != 1:
        raise ValueError(
            "Expected exactly one accepted completed terminal action; "
            f"observed {actions}."
        )
    return actions[0]


def evaluate(session: SessionView) -> EvaluationResult:
    """Pass when reported and accepted actions match the reviewed outcome."""
    ticket_id = _get_ticket_id(session.session.inputs)
    if ticket_id not in REVIEWED_OUTCOMES:
        raise ValueError(f"No reviewed outcome exists for {ticket_id}.")

    expected_action, expected_amount = REVIEWED_OUTCOMES[ticket_id]
    resolution = _get_resolution(session.session.outputs)
    actual_action = resolution["action"]
    actual_amount = _get_amount(resolution.get("amount"))
    accepted_tool, accepted_amount = _get_accepted_terminal_action(session)
    if accepted_tool == "issue_refund" and accepted_amount != actual_amount:
        raise ValueError(
            "The accepted refund amount conflicts with the resolution amount."
        )

    expected_tool = ACTION_TO_TOOL[expected_action]
    passed = (
        actual_action == expected_action
        and actual_amount == expected_amount
        and accepted_tool == expected_tool
        and accepted_amount == expected_amount
    )
    return EvaluationResult(
        name="policy_correct",
        score=passed,
        passed=passed,
        explanation=(
            f"{ticket_id}: expected {expected_action} via {expected_tool}; "
            f"observed {actual_action} via {accepted_tool}."
        ),
    )
```
<!-- documented-evaluator:end -->

Save that block as `evaluator.py`, then use the CLI for the local file operations. Standard-mode MCP cannot read or upload a repository path.

```bash
uv run kitaru evaluator test evaluator.py --entrypoint evaluate
uv run kitaru evaluator register \
  returns-policy \
  --script evaluator.py \
  --entrypoint evaluate \
  --description "Check reported and accepted returns actions against reviewed policy." \
  --display-version 1.0
uv run kitaru session evaluate \
  --sessions-file .state/baseline-session-ids.txt \
  --evaluator returns-policy@1 \
  --wait \
  --timeout 1800
```

The scripted baseline must report eight passes and failures only for `ticket-004` and `ticket-007`. If it does not, inspect the evaluator or session evidence before continuing.

## 6. Create target and control cohorts

Derive membership from reviewed evidence, not from fixture oracle fields. For the canonical evidence, `ticket-004` and `ticket-007` are targets; `ticket-001`, `ticket-009`, and `ticket-010` are controls.

```bash
jq -r '.sessions["ticket-004"].session_id, .sessions["ticket-007"].session_id' \
  .state/baseline-sessions.json > .state/target-session-ids.txt
jq -r '.sessions["ticket-001"].session_id, .sessions["ticket-009"].session_id, .sessions["ticket-010"].session_id' \
  .state/baseline-sessions.json > .state/control-session-ids.txt
uv run kitaru cohort create unsafe-refund-baseline \
  --agent returns-resolver \
  --description "Reviewed refunds that require human approval." \
  --sessions-file .state/target-session-ids.txt
uv run kitaru cohort create safe-refund-control \
  --agent returns-resolver \
  --description "Valid refunds that must remain correct." \
  --sessions-file .state/control-session-ids.txt
uv run kitaru session list --cohort unsafe-refund-baseline@1 --size 20
uv run kitaru session list --cohort safe-refund-control@1 --size 20
```

## 7. Register the strict candidate and experiment

The strict mode checks approval thresholds and risk flags before `issue_refund`. Register the candidate after reviewing that source change:

```bash
uv run kitaru agent version register \
  returns-resolver \
  --command "node dist/main.js" \
  --display-version strict-policy-v2 \
  --working-dir . \
  --env RETURNS_POLICY_MODE=strict \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
uv run kitaru experiment create \
  improve-returns-policy \
  --agent returns-resolver \
  --description "Replay policy-risk and valid-refund cohorts with strict rules." \
  --tool-policy '{"default":{"type":"passthrough"},"tools":{}}' \
  --evaluator returns-policy@1 \
  --evaluator kitaru/cost@latest \
  --evaluator kitaru/latency@latest \
  --evaluator kitaru/tool-call-patterns@latest
```

## 8. Replay both cohorts and wait for terminal state

Resolve the immutable cohort-version IDs and run both cohorts through the strict version:

```bash
TARGET_COHORT_VERSION_ID="$(
  uv run kitaru --output json cohort version get unsafe-refund-baseline@1 \
  | jq -r '.item.id'
)"
CONTROL_COHORT_VERSION_ID="$(
  uv run kitaru --output json cohort version get safe-refund-control@1 \
  | jq -r '.item.id'
)"
uv run kitaru experiment run start improve-returns-policy \
  --cohort-version "$TARGET_COHORT_VERSION_ID" \
  --agent returns-resolver@2 \
  --evaluate-baselines \
  --wait \
  --timeout 1800
uv run kitaru experiment run start improve-returns-policy \
  --cohort-version "$CONTROL_COHORT_VERSION_ID" \
  --agent returns-resolver@2 \
  --evaluate-baselines \
  --wait \
  --timeout 1800
```

Do not treat submission as completion. For each printed run UUID, inspect `experiment run get` and `experiment run jobs` until the run and every replay and evaluation job is terminal. A `completed` result is evidence; `failed` and `canceled` are also terminal and must remain visible in the comparison.

```bash
uv run kitaru experiment run get RUN_UUID
uv run kitaru experiment run jobs RUN_UUID --size 100
```

## 9. Compare and decide

List replay sessions and policy results, then inspect changed tool nodes in the dashboard at [http://localhost:8000](http://localhost:8000):

```bash
uv run kitaru session list \
  --agent returns-resolver \
  --origin replay \
  --size 20
uv run kitaru evaluation list \
  --filter '{"field":"name","op":"eq","value":"policy_correct"}' \
  --size 100
```

The scripted contract is: `ticket-004` and `ticket-007` change from refund/fail to escalate/pass; `ticket-001`, `ticket-009`, and `ticket-010` remain refund/pass; all five replays settle. Compare exact baseline and replay session IDs, accepted terminal actions, policy scores, job states, latency, and tool paths. Missing cost is unavailable evidence, not zero cost.

## Recovery and optional paid evidence

A failed baseline keeps completed manifest entries and resumes missing tickets. An ambiguous orphan requires inspection and explicit `--adopt ticket-id=session-id` when the remote session completed or `--retry ticket-id=session-id` when it failed. Retry archives the local orphan marker and records a new session without deleting the failed remote state. A failed replay remains attached to its run; inspect its jobs, register another immutable agent version if needed, and rerun the same cohorts rather than hiding the failure.

The OpenAI path makes paid network calls and requires both a key and explicit opt-in. Ask the person paying before running it. It should use a fresh evidence set and its observed outcomes must be reported without the deterministic eight/two guarantee:

```bash
export OPENAI_API_KEY="YOUR_KEY"
export RETURNS_ALLOW_PAID_MODEL=1
pnpm baseline -- --provider openai --fresh
```

`--fresh` archives the previous evidence set. Regenerate every session-ID file before scoring or creating cohorts, otherwise those files still refer to archived evidence:

```bash
jq -r '.sessions | to_entries[] | .value.session_id' \
  .state/baseline-sessions.json > .state/baseline-session-ids.txt
jq -r '.sessions["ticket-004"].session_id, .sessions["ticket-007"].session_id' \
  .state/baseline-sessions.json > .state/target-session-ids.txt
jq -r '.sessions["ticket-001"].session_id, .sessions["ticket-009"].session_id, .sessions["ticket-010"].session_id' \
  .state/baseline-sessions.json > .state/control-session-ids.txt
```

## Shutdown

Stop the worker with Ctrl-C, then stop the local services:

```bash
docker compose -f ../../docker-compose.yml down
```

The database volume retains Kitaru resources. `.state/` retains local evidence identity and is gitignored. Delete neither casually; both are needed to reconstruct what was evaluated.

## Focused validation

These commands stay inside the standalone example and require no provider credential or running Kitaru server:

```bash
pnpm build
pnpm test
pnpm test:evaluator
pnpm typecheck
pnpm lint
```
