# Improve a TypeScript returns agent with Kitaru

This example runs the canonical ten-ticket returns story with TypeScript, AI SDK 7, and Kitaru's Vercel adapter. It records the baseline directly into Kitaru, turns a human policy judgment into an evaluator, and replays target and control cohorts through stricter instructions.

All customers, orders, shipments, and actions are synthetic. Every invocation gets a fresh in-memory store. Passthrough replay is safe here for that reason only; do not copy that policy to tools that contact payment, fulfillment, or support systems.

The default `MockLanguageModelV4` path is scripted. Its recordings use the requested model ID `openai/gpt-5-nano` and fixed synthetic token counts. Token figures are scripted rather than measured, while cost remains unavailable because this example does not configure a price calculator. Its fixed outcomes prove the adapter, recording, evaluation, cohort, and replay workflow. They do not prove that prompting caused a real model to improve. The optional OpenAI path is paid, non-deterministic evidence to inspect and never promises the fixed result table below.

From the repository root, enter the standalone example directory before running the commands below unless a command says otherwise:

```bash
cd v2_examples/vercel_ai_ticket_resolver
```

## 1. Install and start Kitaru

Use Node 22, Python 3.11 or newer, pnpm, uv, jq, Docker, and a source checkout of this repository. Python and uv are still required because Kitaru's current evaluator and worker ABI is Python, even though the agent itself is TypeScript.

Build the local TypeScript packages without registering this standalone example in the root workspace, then install and verify the example:

```bash
pnpm --dir ../.. install --frozen-lockfile
pnpm --dir ../.. --filter @zenml-io/kitaru build
pnpm --dir ../.. --filter @zenml-io/kitaru-vercel-ai build
CI=true pnpm --ignore-workspace install --frozen-lockfile
pnpm build
pnpm test
pnpm typecheck
pnpm lint
```

`CI=true` keeps the standalone install non-interactive. Without it, pnpm refuses to remove a `node_modules` directory left by a root workspace install and aborts with `ERR_PNPM_ABORTED_REMOVE_MODULES_DIR_NO_TTY`.

With PostgreSQL reachable on the configured Kitaru test database port, run the provider-free live API, evaluator, cohort, and replay proof:

```bash
pnpm test:e2e
```

Start PostgreSQL and the API, then install and connect the Python CLI and worker. That compose stack serves the REST API only; this walkthrough is entirely CLI-driven and never opens a browser.

```bash
docker compose -f ../../docker-compose.yml up -d --build
uv sync --extra cli --extra worker --extra mcp
cp .env.example .env
set -a; source .env; set +a
uv run kitaru login http://localhost:8000
uv run kitaru status
```

Point the CLI at the running compose stack by URL. `kitaru login --local` provisions a second Kitaru deployment of its own and refuses to start while the compose stack holds port 8000.

Load `.env` in every terminal used for the example. It gives both the TypeScript adapter and Python tools the local API URL; the default values do not authorize a provider call.

## 2. Register the baseline and start a worker

Register the compiled TypeScript command. One invocation receives one rendered email string, calls only the six synthetic tools, and returns structured resolution JSON.

```bash
export RUN_SUFFIX="$(node --input-type=module -e 'console.log(crypto.randomUUID())')"
export AGENT_NAME="returns-resolver-${RUN_SUFFIX}"
export EVALUATOR_NAME="returns-policy-${RUN_SUFFIX}"
export INVESTIGATION_NAME="refund-policy-review-${RUN_SUFFIX}"
export TARGET_COHORT_NAME="unsafe-refund-baseline-${RUN_SUFFIX}"
export CONTROL_COHORT_NAME="safe-refund-control-${RUN_SUFFIX}"
export EXPERIMENT_NAME="improve-returns-policy-${RUN_SUFFIX}"
export WORKER_NAME="vercel-returns-example-worker-${RUN_SUFFIX}"
mkdir -p .state
{
  printf 'export AGENT_NAME="%s"\n' "$AGENT_NAME"
  printf 'export EVALUATOR_NAME="%s"\n' "$EVALUATOR_NAME"
  printf 'export INVESTIGATION_NAME="%s"\n' "$INVESTIGATION_NAME"
  printf 'export TARGET_COHORT_NAME="%s"\n' "$TARGET_COHORT_NAME"
  printf 'export CONTROL_COHORT_NAME="%s"\n' "$CONTROL_COHORT_NAME"
  printf 'export EXPERIMENT_NAME="%s"\n' "$EXPERIMENT_NAME"
  printf 'export WORKER_NAME="%s"\n' "$WORKER_NAME"
} > .state/run.env
uv run kitaru agent register \
  "$AGENT_NAME" \
  --command "node dist/main.js" \
  --description "Resolve one synthetic returns or delivery ticket." \
  --display-version baseline-v1 \
  --working-dir "$PWD" \
  --env RETURNS_POLICY_MODE=baseline \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
```

Run that command from this directory so `$PWD` stores the example's absolute path. A worker runs the command in the recorded directory, and a relative `--working-dir` would resolve against whichever worker claimed the job.

The generated resource names live in `.state/run.env`. Every later command assumes the primary shell still has `.env` and this file loaded. In any fresh shell, restore both before continuing:

```bash
set -a; source .env; set +a
source .state/run.env
```

Version 1 records its sessions from this shell in step 3, so its run spec needs no agent identity. Any version a worker executes does: the worker gives the process only the Kitaru API URL, a task token, the task ID and inputs, and the version's own `--env` pairs. Step 7 therefore registers the replayed strict version with `KITARU_AGENT_ID` in its run spec, using the ID exported in step 3.

Start a worker in a second terminal from this directory and leave it active:

```bash
set -a; source .env; set +a
source .state/run.env
uv run --frozen kitaru worker start --name "$WORKER_NAME"
```

Attach exactly one worker to this server while running the walkthrough. A worker claims any queued task the server offers it, so a second worker started for another agent or example can claim these replay jobs and run them in its own directory.

## 3. Record the exact deterministic baseline

Resolve and export the registered identities, then record all ten tickets directly through `createKitaruGenerateText`:

```bash
export KITARU_AGENT_ID="$(
  uv run kitaru --output json agent get "$AGENT_NAME" | jq -r '.item.id'
)"
export KITARU_AGENT_VERSION_ID="$(
  uv run kitaru --output json agent version get "${AGENT_NAME}@1" | jq -r '.item.id'
)"
{
  printf 'export KITARU_AGENT_ID="%s"\n' "$KITARU_AGENT_ID"
  printf 'export KITARU_AGENT_VERSION_ID="%s"\n' "$KITARU_AGENT_VERSION_ID"
} >> .state/run.env
pnpm baseline --fresh
```

The runner writes the exact ticket-to-session map to `.state/baseline-sessions.json` after each completed ticket. The walkthrough starts with `--fresh` because every invocation registers a uniquely named agent. That flag archives any prior manifest under `.state/evidence-sets/` before creating evidence for the new agent. If this recording is interrupted, resume its missing tickets with `pnpm baseline` and do not pass `--fresh` again.

If a process stops after Kitaru writes a session ID but before the manifest commits it, the runner refuses to guess. Inspect the orphan named in `.state/attempts/<evidence-set-id>/<ticket-id>.session-id` with `uv run kitaru session get SESSION_ID`. Adopt it only after confirming that the remote session completed:

```bash
pnpm baseline --adopt ticket-004=SESSION_ID
```

The general recovery form is `--adopt ticket-id=session-id`. If the session failed or is still in progress, do not adopt it. After you inspect the exact remote session and confirm it failed, use `--retry ticket-id=session-id`. Retry archives the local orphan marker, records a new remote session, and never deletes remote state.

Only the rendered ticket ID, sender, subject, and body enter the recorded prompt. The fixture's `scenario` and `expected_action` fields are verification oracles and are not recorded inputs. Do not use them to decide annotations or cohort membership before investigating trace evidence.

Create a reusable exact-session file and inspect the recorded set:

```bash
jq -r '.sessions | to_entries[] | .value.session_id' \
  .state/baseline-sessions.json > .state/baseline-session-ids.txt
uv run kitaru session list \
  --agent "$AGENT_NAME" \
  --origin recorded \
  --status completed \
  --size 20
```

## 4. Measure and review the baseline

A built-in evaluator runs as a `uv run --with kitaru-evaluator==VERSION` subprocess, and `uv` applies the `exclude-newer` cutoff configured in whichever project the worker runs from. This repository sets one, so for the first few days after a `kitaru-evaluator` release the pinned version is filtered out and every evaluation task fails with `No solution found`. Start the worker with the cutoff moved to now when that happens:

```bash
source .state/run.env
UV_EXCLUDE_NEWER="$(date -u +%Y-%m-%dT%H:%M:%SZ)" uv run --frozen kitaru worker start --name "$WORKER_NAME"
```

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

Create an investigation whose questions carry curated highlights, answer each question, anchor the outcome annotation to the completed accepted `issue_refund` node, and settle the session with a verdict. Replace the node placeholder after inspecting the trace:

```bash
TICKET_004_REFUND_NODE_ID="YOUR_COMPLETED_REFUND_NODE_UUID"
INVESTIGATION_ID="$(
  uv run kitaru --output json investigation create "$INVESTIGATION_NAME" \
    --agent "$AGENT_NAME" \
    --description "Review whether risky refunds require human approval." \
    --session "${TICKET_004_SESSION_ID}" \
    --session-question "${TICKET_004_SESSION_ID}:outcome=Is this outcome acceptable, problematic, or uncertain, and why?" \
    --session-question "${TICKET_004_SESSION_ID}:expected=What should the agent have done?" \
    --session-highlights "${TICKET_004_SESSION_ID}:outcome=[{\"selector\":{\"node_id\":\"${TICKET_004_REFUND_NODE_ID}\",\"path\":\"/outputs\"},\"description\":\"A \$280 refund exceeded the automatic approval threshold.\"}]" \
  | jq -r '.item.id'
)"
INVESTIGATION_SESSION_ID="$(
  uv run kitaru --output json investigation session list \
    "${INVESTIGATION_ID}" --size 20 | jq -r '.items[0].id'
)"
uv run kitaru annotation create \
  --investigation-session "${INVESTIGATION_SESSION_ID}" \
  --question-key outcome \
  --selector "{\"node_id\":\"${TICKET_004_REFUND_NODE_ID}\",\"path\":\"/outputs\"}" \
  --value '{"judgment":"problematic","reason":"The amount exceeds the automatic approval threshold."}'
uv run kitaru annotation create \
  --investigation-session "${INVESTIGATION_SESSION_ID}" \
  --question-key expected \
  --value '{"action":"escalate","reason":"Human approval is required."}'
uv run kitaru investigation session verdict \
  "${INVESTIGATION_ID}" "${TICKET_004_SESSION_ID}" problematic
```

Each `--session-question` and `--session-highlights` value starts with the session ID of a session already selected by `--session`, so the braces around `${TICKET_004_SESSION_ID}` are required: zsh reads `"$TICKET_004_SESSION_ID:outcome=..."` as a modifier expression and expands it to something else. A highlight and an annotation selector accept `node_id`, an RFC 6901 `path` into that node, and an optional `span`. The verdict is `acceptable`, `problematic`, or `uncertain`.

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
  "$EVALUATOR_NAME" \
  --script evaluator.py \
  --entrypoint evaluate \
  --description "Check reported and accepted returns actions against reviewed policy." \
  --display-version 1.0
uv run kitaru session evaluate \
  --sessions-file .state/baseline-session-ids.txt \
  --evaluator "${EVALUATOR_NAME}@1" \
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
uv run kitaru cohort create "$TARGET_COHORT_NAME" \
  --agent "$AGENT_NAME" \
  --description "Reviewed refunds that require human approval." \
  --sessions-file .state/target-session-ids.txt
uv run kitaru cohort create "$CONTROL_COHORT_NAME" \
  --agent "$AGENT_NAME" \
  --description "Valid refunds that must remain correct." \
  --sessions-file .state/control-session-ids.txt
uv run kitaru session list --cohort "${TARGET_COHORT_NAME}@1" --size 20
uv run kitaru session list --cohort "${CONTROL_COHORT_NAME}@1" --size 20
```

## 7. Register the strict candidate and experiment

The strict mode checks approval thresholds and risk flags before `issue_refund`. Register the candidate after reviewing that source change, from this directory and in the shell that exported `KITARU_AGENT_ID` in step 3. A worker runs this version, so its run spec carries the agent ID the adapter records replay sessions under:

```bash
uv run kitaru agent version register \
  "$AGENT_NAME" \
  --command "node dist/main.js" \
  --display-version strict-policy-v2 \
  --working-dir "$PWD" \
  --env RETURNS_POLICY_MODE=strict \
  --env KITARU_AGENT_ID="${KITARU_AGENT_ID}" \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
uv run kitaru experiment create \
  "$EXPERIMENT_NAME" \
  --agent "$AGENT_NAME" \
  --description "Replay policy-risk and valid-refund cohorts with strict rules." \
  --tool-policy '{"default":{"type":"passthrough"},"tools":{}}' \
  --evaluator "${EVALUATOR_NAME}@1" \
  --evaluator kitaru/cost@latest \
  --evaluator kitaru/latency@latest \
  --evaluator kitaru/tool-call-patterns@latest
```

## 8. Replay both cohorts and wait for terminal state

Resolve the immutable cohort-version IDs and run both cohorts through the strict version:

```bash
TARGET_COHORT_VERSION_ID="$(
  uv run kitaru --output json cohort version get "${TARGET_COHORT_NAME}@1" \
  | jq -r '.item.id'
)"
CONTROL_COHORT_VERSION_ID="$(
  uv run kitaru --output json cohort version get "${CONTROL_COHORT_NAME}@1" \
  | jq -r '.item.id'
)"
uv run kitaru experiment run start "$EXPERIMENT_NAME" \
  --cohort-version "$TARGET_COHORT_VERSION_ID" \
  --agent "${AGENT_NAME}@2" \
  --evaluate-baselines \
  --wait \
  --timeout 1800
uv run kitaru experiment run start "$EXPERIMENT_NAME" \
  --cohort-version "$CONTROL_COHORT_VERSION_ID" \
  --agent "${AGENT_NAME}@2" \
  --evaluate-baselines \
  --wait \
  --timeout 1800
```

Do not treat submission as completion. For each printed run UUID, inspect `experiment run get` and `experiment run jobs` until the run and every replay and evaluation job is terminal. A `completed` result is evidence; `failed` and `canceled` are also terminal and must remain visible in the comparison.

```bash
uv run kitaru experiment run get RUN_UUID
uv run kitaru experiment run jobs RUN_UUID --size 100
```

On a server that holds other work, the job listing can include jobs outside this run. The run-scoped counts are the `progress` fields of `experiment run get`, which must reach five settled replays across the two runs.

## 9. Compare and decide

List replay sessions and policy results, then inspect changed tool nodes with `kitaru session nodes`:

```bash
uv run kitaru session list \
  --agent "$AGENT_NAME" \
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
pnpm baseline --provider openai --fresh
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
