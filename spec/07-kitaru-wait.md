# 7. `kitaru.wait()` — Suspend for Input

## What it does

`kitaru.wait()` is the **suspension primitive** in Kitaru.

It pauses a running flow, persists enough state for the execution to continue later, releases compute, and resumes only when the required input arrives.

This is how Kitaru handles:

- human approval
- structured review input
- external webhook callbacks
- other validated resume inputs

From the user's perspective, `wait()` looks like a normal blocking Python call that returns a value. Under the hood, it behaves like a durable suspension point.

## Restriction

`wait()` is valid **only directly in flow execution**, not inside a checkpoint. This is a hard MVP restriction.

## Signature

```python
# Human approval (yes/no)
approved = kitaru.wait(schema=bool, question="Deploy to production?")

# Structured input (Pydantic model)
decision = kitaru.wait(schema=ReviewDecision, question="Review this draft")

# External system payload
payload = kitaru.wait(schema=dict, name="etl_completed")

# Simple gate (defaults to bool)
kitaru.wait(question="Ready to proceed?")
```

All parameters should use keyword arguments for clarity. `schema=` is the primary signature form.

## Parameters

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `schema` | `type` | `bool` | Expected input type. Can be any JSON-serializable shape — `bool`, `str`, `dict`, a Pydantic model, an `Enum` for multiple-choice UI generation, or other JSON-serializable types |
| `name` | `str` | `None` | Human-friendly label for the wait (the "name" of this wait point) |
| `question` | `str` | `None` | Human-readable question or instruction shown in dashboard, API, or CLI |
| `timeout` | `int` | `None` | **Active wait timeout only** — seconds to keep compute/resources alive before releasing them. See timeout semantics below |
| `metadata` | `dict` | `None` | Extra context visible in dashboard or API |

### Parameter naming rationale

- `schema` — clear intent, avoids Python keyword conflicts (not `type`, not `from`)
- `name` — the label, not the question
- `question` — the human-readable question/instruction

## What `wait()` means semantically

`wait()` is not just "sleep until something happens."

It is a **durable call boundary** that records:

- that the execution is waiting
- where the wait occurred in execution history
- what schema the input must satisfy
- what question and metadata were attached
- what input was eventually provided

Like checkpoints, waits participate in the execution journal and replay model.

A wait is therefore best thought of as:

- a durable suspension point on first execution
- a recorded input on later resume or replay

## Execution model: suspend, unwind, rerun

Like the rest of Kitaru, `wait()` does **not** restore a live Python stack or frame.

Instead, the model is:

1. the flow runs and reaches `wait()`
2. Kitaru records a durable wait call in the execution journal
3. the execution is marked `waiting`
4. the runtime exits the current run cleanly
5. later, input arrives through the dashboard, API, CLI, or webhook
6. the input is validated and stored
7. Kitaru starts a **rerun of the same execution from the top** (this is resume, not replay — same execution)
8. when rerun reaches the same `wait()`, Kitaru replays the recorded input instead of suspending again

That means `wait()` behaves like a normal function call in user code, while the runtime implements it as **suspend now, rerun later**.

## Timeout semantics

Wait timeout means **active wait / resource retention**, not total expiration of the wait.

- `timeout=60` means: keep compute/resources alive for 60 seconds while waiting for input
- if input arrives within the timeout, the flow continues immediately without needing to spin down and restart
- after the timeout, compute/resources are released — but the execution remains in `waiting` state
- the wait does **not** expire or fail after the timeout
- the execution can still be resumed later; it just needs to spin up fresh resources

**Indefinite paused waiting is acceptable.** A wait with no timeout, or a wait whose active timeout has passed, simply sits in `waiting` state consuming no resources until input arrives.

If a product later needs "approval expires after 24h" semantics, that is a different concept (business-level expiration) and should not overload the active wait timeout.

## Behavior

### On first execution

When a flow hits `kitaru.wait(...)` for the first time:

- Kitaru creates a durable wait record in the execution journal
- stores:
    - wait name and sequence position
    - schema
    - question
    - metadata
- marks the execution as `waiting`
- optionally keeps resources alive for the active timeout duration
- releases compute after the active timeout (or immediately if no timeout)
- exits the active run

### When input arrives

When input is provided:

- Kitaru validates it against `schema`
- records the validated input as the outcome of that wait
- schedules a resume run for the same execution

### On resume or replay

When the flow reruns from the top and reaches that same wait again:

- Kitaru does **not** suspend again
- it returns the previously recorded input immediately

So the code after `wait()` runs as if the function had simply returned the input synchronously.

## Replay semantics

Like checkpoints, waits replay **recorded outcomes**.

For `wait()`, the recorded outcome is usually the provided input.

That means on replay:

- waits before the replay point return their recorded input
- waits at or after the replay point may:
    - suspend again
    - use an override input
    - or execute according to the replay plan

This keeps replay deterministic and preserves normal Python control flow.

## Schema-driven UI

When `schema` is a Pydantic model, the dashboard can generate a structured input form from its JSON schema.

When `schema` is an `Enum`, the dashboard can present a multiple-choice selector — the enum values become the available choices.

This enables richer human-in-the-loop interactions without custom dashboard code.

## Examples

**Human approval gate:**

```python
@kitaru.flow
def deploy_agent(service: str, version: str) -> str:
    plan = plan_deployment(service, version)

    approved = kitaru.wait(
        schema=bool,
        name="approve_deploy",
        question=f"Approve deployment of {service} v{version}?",
        metadata={"plan": plan, "service": service},
    )

    if not approved:
        return f"Deployment of {service} v{version} cancelled"

    return execute_deployment(plan)
```

**Structured human input:**

```python
from pydantic import BaseModel

class EditFeedback(BaseModel):
    approved: bool
    notes: str = ""
    sections_to_revise: list[str] = []

@kitaru.flow
def writing_agent(topic: str) -> str:
    draft = write_draft(topic)

    feedback = kitaru.wait(
        schema=EditFeedback,
        name="review_draft",
        question=f"Review this draft about '{topic}'",
        metadata={"draft": draft},
    )

    if not feedback.approved:
        return revise_draft(draft, feedback.notes, feedback.sections_to_revise)

    return draft
```

**Multiple-choice input via Enum:**

```python
from enum import Enum

class Priority(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@kitaru.flow
def triage_agent(issue: str) -> str:
    analysis = analyze_issue(issue)

    priority = kitaru.wait(
        schema=Priority,
        name="set_priority",
        question=f"Set priority for: {issue}",
        metadata={"analysis": analysis},
    )

    return route_issue(issue, priority)
```

**External callback payload:**

```python
@kitaru.flow
def data_analysis_agent(query: str) -> str:
    job_id = trigger_etl(query)

    result = kitaru.wait(
        schema=dict,
        name="etl_completed",
        metadata={"job_id": job_id},
        timeout=3600,
    )

    return analyze_data(result["output_path"])
```

## Providing input

**Via CLI:**

```bash
kitaru executions input kr-a8f3c2 \
  --wait approve_deploy \
  --value true
```

**Via API (ZenML server endpoint):**

```bash
# All server URLs are ZenML server URLs — there is no separate Kitaru API
curl -X POST https://my-zenml-server.mycompany.com/api/v1/executions/kr-a8f3c2/input \
  -d '{"wait":"approve_deploy","value":true}'
```

**Via Python client:**

```python
client = KitaruClient()
client.executions.input(
    "kr-a8f3c2",
    wait="approve_deploy",
    value=True,
)
```

User-facing APIs may accept names for convenience, but internally resume should target a specific wait call instance.

## Validation behavior

The input provided to a wait should be validated before resume is accepted.

That means:

- `bool` waits only accept booleans
- `dict` waits accept JSON objects
- Pydantic model waits validate and parse structured input
- `Enum` waits accept only valid enum values
- invalid input should return a validation error and keep the execution in `waiting`

This is especially important for API and webhook resume paths.

## Dashboard behavior

A waiting execution should expose:

- current status: `waiting`
- the pending wait call
- question
- schema or expected input shape
- metadata context
- whether an active timeout exists and its remaining duration

Once input is provided, the wait should show:

- who or what provided input
- when it was provided
- the validated value

This is the core audit trail for human-in-the-loop and externally resumed workflows.

## Rules

- must be called inside a `@kitaru.flow`, **not** inside a checkpoint
- suspends the current execution instead of blocking an in-memory process forever
- returns a normal Python value when resumed
- participates in replay just like checkpoints do
- input should be validated before resume completes
- waits inside loops or branches are distinct durable calls, not just question strings

## MVP scope for `wait()`

For the MVP, `wait()` should stay conceptually narrow:

- one wait primitive
- one validation model
- one resume path semantically

Dashboard input, CLI input, API input, and webhook input are best thought of as **different clients of the same resume mechanism**, not fundamentally different runtime concepts.

This keeps the runtime smaller and cleaner.

## Future: event sources beyond webhook

For the MVP, all wait inputs arrive through the same mechanism: an external API call (from a human via the dashboard, CLI, Python client, or a webhook).

In the future, `wait()` may support richer event sources:

- **internal events** — e.g., waiting for another pipeline run to complete, triggered automatically by ZenML's internal event system
- **time-based events** — e.g., `wait(timeout_event=3600)` to resume after a delay (this would subsume the removed `sleep()` primitive)
- **third-party integrations** — e.g., waiting for a Slack reaction, a GitHub PR merge, or an external system callback

When these arrive, the syntax might expand with a `source=` or `event=` parameter. But for the MVP, all waits resolve through the same external-input API path.

## ZenML backend

Wait, pause, resume, and replay are all implemented by wrapping ZenML SDK and backend behavior. The implementation should look at the ZenML SDK and defer to / wrap its logic rather than reimplementing these capabilities from scratch. See the ZenML branch `feature/pause-pipeline-runs` for the existing wait/resume implementation.

**Branch status (March 2026):** `zenml.wait(...)` works and pauses in-progress runs. Resume works but differs by deployment:
- On Pro servers with snapshot execution, the run **auto-resumes** when input is provided
- On non-Pro servers or local orchestrators, the user must **manually resume** via a ZenML CLI command that already exists on the branch
- Wait resolution is currently **human input only**

## OSS vs Pro considerations

The full connected wait/resume experience — dashboard-triggered resume after compute is released — depends on Pro-backed server capabilities.

- **OSS / non-Pro / local orchestrator:** Resume is **manual**. After wait input is provided, the user must explicitly trigger a resume (e.g. via `kitaru executions resume` or the underlying ZenML CLI command). The run does not auto-continue.
- **Pro (remote orchestrator on snapshot-capable servers):** Resume is **automatic**. Once the wait condition is resolved (input provided), the run automatically resumes without user intervention.

As of March 2026, wait condition resolution is **human input only** — there are no webhook or automated triggers yet. Both resume paths (auto and manual) work on the `feature/pause-pipeline-runs` branch.

## Notes for MVP

- durable external resume generally requires connected or server-backed mode
- invalid input should leave the execution in `waiting`
- timeout is active wait / resource retention only — it does not expire or fail the wait
- waits need the same divergence detection as checkpoints, so historical input is never applied to the wrong wait site
