# 17. End-to-End Examples

This section shows how the primitives fit together in practice.

The examples are intentionally biased toward the durable execution model:

- flows orchestrate
- checkpoints define replay boundaries
- waits suspend and later resume
- `llm()` is a convenience wrapper
- artifacts and metadata make inspection and replay useful

Where relevant, examples are labeled as **MVP-friendly** patterns rather than broader future-platform patterns.

---

## 1. Simplest: One-Shot Summarizer

A single LLM call wrapped in a flow. This is the simplest useful Kitaru program.

```python
import kitaru

@kitaru.flow
def summarize(text: str) -> str:
    return kitaru.llm(
        f"Summarize this in 3 bullet points:\n\n{text}",
        model="fast",
        name="summary",
    )

result = summarize(long_article)
print(result)
```

What you get:

- one tracked execution
- one durable LLM call
- prompt and response artifacts
- token/cost/latency metadata
- replayability

---

## 2. Multi-Step Content Pipeline with Approval

This example shows:

- explicit checkpoints
- durable human review
- normal Python branching after a wait
- replay-friendly structure

```python
import kitaru
from pydantic import BaseModel

class ReviewDecision(BaseModel):
    approved: bool
    notes: str = ""

@kitaru.checkpoint(type="llm_call")
def research(topic: str) -> str:
    return kitaru.llm(
        f"Research {topic} thoroughly. Return detailed notes.",
        model="fast",
        name="research_notes",
    )

@kitaru.checkpoint(type="llm_call")
def write_draft(notes: str, topic: str) -> str:
    return kitaru.llm(
        f"Write a blog post about {topic} using these notes:\n\n{notes}",
        model="fast",
        name="draft",
    )

@kitaru.checkpoint(type="llm_call")
def revise_draft(draft: str, feedback: str) -> str:
    return kitaru.llm(
        f"Revise this draft based on feedback.\n\nDraft:\n{draft}\n\nFeedback:\n{feedback}",
        model="fast",
        name="revised_draft",
    )

@kitaru.flow
def content_pipeline(topic: str) -> str:
    notes = research(topic)
    draft = write_draft(notes, topic)

    review = kitaru.wait(
        schema=ReviewDecision,
        name="review_draft",
        prompt=f"Review draft for '{topic}'",
        metadata={"draft": draft},
    )

    if not review.approved:
        draft = revise_draft(draft, review.notes)

    kitaru.log(topic=topic, approved_first_try=review.approved)
    return draft
```

What you get:

- three durable checkpoints
- a wait that suspends cleanly and resumes by rerun (same execution)
- an audit trail for the review decision
- replay from `write_draft` or `review_draft`

---

## 3. Failure + Fallback with Deterministic Replay

This example shows why Kitaru must replay **failure outcomes**, not only successful outputs.

```python
import kitaru

@kitaru.checkpoint(retries=2)
def fetch_primary_data(query: str) -> str:
    raise ValueError("primary source unavailable")

@kitaru.checkpoint
def fetch_fallback_data(query: str) -> str:
    return f"fallback data for {query}"

@kitaru.flow
def resilient_lookup(query: str) -> str:
    try:
        return fetch_primary_data(query)
    except ValueError as e:
        kitaru.log(error=str(e), recovery="fallback")
        return fetch_fallback_data(query)
```

Why this matters:

- the first run takes the fallback path
- replay before the fallback must re-raise the recorded `ValueError`
- otherwise replay could take a different branch and stop being trustworthy

---

## 4. Cross-Execution Reuse with `load()`

This example shows how one execution can build on artifacts from another.

```python
import kitaru
from kitaru import KitaruClient

@kitaru.checkpoint(type="llm_call")
def research(topic: str) -> str:
    return kitaru.llm(f"Research {topic} thoroughly", model="fast")

@kitaru.flow
def first_pass(topic: str) -> str:
    return research(topic)

@kitaru.checkpoint(type="llm_call")
def follow_up_from_previous(prev_exec_id: str) -> str:
    notes = kitaru.load(prev_exec_id, "research")
    return kitaru.llm(
        f"Write a follow-up analysis based on these notes:\n\n{notes}",
        model="fast",
    )

@kitaru.flow
def second_pass(prev_exec_id: str) -> str:
    return follow_up_from_previous(prev_exec_id)

client = KitaruClient()
prev = client.executions.latest(flow="first_pass", status="completed")
result = second_pass(prev.exec_id)
```

What you get:

- explicit cross-execution linkage
- lineage that says the second execution depends on the first
- no hidden "latest" magic inside the runtime

---

## 5. Local Replay with Overrides

This example shows the MVP local replay story.

```python
client = KitaruClient()
prev = client.executions.latest(flow="content_pipeline", status="completed")

replayed = client.executions.replay(
    prev.exec_id,
    from_="write_draft",
    overrides={
        "flow.input.topic": "AI observability",
        "checkpoint.research": "Edited research notes from local debugging",
    },
)

print(replayed.exec_id)
```

Semantics:

- create a new execution
- reuse durable outcomes before `write_draft` unless overridden
- inject the edited research value
- re-run from the replay point forward

This is the core "edit one thing, rerun the rest" loop.

---

## 6. Side-Effect-Aware Pattern: Plan Then Commit

This example shows a safer structure for workflows that interact with external systems.

```python
import kitaru

@kitaru.checkpoint(type="llm_call")
def plan_pr(issue_text: str) -> dict:
    proposal = kitaru.llm(
        f"Plan the code changes for this issue:\n\n{issue_text}",
        model="fast",
    )
    return {"proposal": proposal, "idempotency_key": "issue-42-pr"}

@kitaru.checkpoint
def create_pr(plan: dict) -> str:
    # External system should use the idempotency key to avoid duplicates
    idempotency_key = plan["idempotency_key"]
    # github.create_pr(..., idempotency_key=idempotency_key)
    return "https://github.com/org/repo/pull/123"

@kitaru.flow
def coding_agent(issue_text: str) -> str:
    plan = plan_pr(issue_text)

    approved = kitaru.wait(
        schema=bool,
        name="approve_pr",
        prompt="Create the PR?",
        metadata={"proposal": plan["proposal"]},
    )
    if not approved:
        return "PR not created."

    return create_pr(plan)
```

Why this is a better pattern:

- planning is replay-safe
- the side-effecting step is isolated
- the external commit step can use an idempotency key

---

## 7. PydanticAI Adapter Pattern

This shows the intended MVP adapter shape.

```python
import kitaru
from pydantic_ai import Agent
from kitaru.adapters import pydantic_ai as kp

research_agent = kp.wrap(
    Agent("openai:gpt-4o", name="researcher")
)

@kitaru.checkpoint(type="llm_call")
def run_research(topic: str) -> str:
    result = research_agent.run_sync(f"Research {topic} thoroughly")
    return result.output

@kitaru.flow
def research_flow(topic: str) -> str:
    return run_research(topic)
```

Recommended mental model:

- the outer checkpoint is the replay boundary
- framework-internal model and tool activity becomes child events, artifacts, and metadata
- the adapter reduces rewrite, but does not replace Kitaru's runtime model

---

## 8. Minimal Wait / Resume Lifecycle

This example focuses only on the wait lifecycle. Resume is a **same-execution** operation.

```python
import kitaru

@kitaru.flow
def approval_flow() -> str:
    approved = kitaru.wait(
        schema=bool,
        name="approve_release",
        prompt="Approve this release?",
    )
    return "approved" if approved else "rejected"
```

Lifecycle:

1. run `approval_flow()`
2. execution enters `waiting`
3. provide input through dashboard, CLI, API, or webhook
4. the **same execution** reruns from the top
5. `kitaru.wait()` returns recorded input
6. flow finishes

That is the canonical wait/resume model in Kitaru. No new execution is created.

## 9. Claude Code / OpenClaw-Style Coding Agent

This example shows a more realistic coding-agent structure:

- durable planning
- code generation inside explicit checkpoints
- test execution as a checkpoint
- human review via `wait()`
- side-effect-aware PR creation
- replay from any meaningful step

The key design choice is that the agent loop is still orchestrated by a normal Kitaru flow. Kitaru does not provide the coding loop itself — it makes the loop durable.

```python
import kitaru
from pydantic import BaseModel

class CodeReview(BaseModel):
    approved: bool
    feedback: str = ""

class TestResult(BaseModel):
    passed: bool
    output: str
    failures: list[str] = []

MAX_REVIEW_ROUNDS = 3

@kitaru.checkpoint(type="llm_call")
def analyze_issue(issue_text: str) -> str:
    return kitaru.llm(
        f"""
Analyze this coding task and produce:
1. problem summary
2. likely files to change
3. implementation plan
4. test plan

Task:
{issue_text}
""",
        model="smart",
        name="issue_analysis",
    )

@kitaru.checkpoint(type="llm_call")
def inspect_codebase(repo_summary: str, issue_analysis: str) -> str:
    return kitaru.llm(
        f"""
Given this repository summary and issue analysis, identify the relevant code areas.

Repository summary:
{repo_summary}

Issue analysis:
{issue_analysis}
""",
        model="smart",
        name="codebase_context",
    )

@kitaru.checkpoint(type="llm_call")
def write_patch(issue_analysis: str, context: str, feedback: str | None = None) -> dict[str, str]:
    prompt = f"""
Generate a patch for this task.

Issue analysis:
{issue_analysis}

Codebase context:
{context}
"""
    if feedback:
        prompt += f"\n\nReview feedback to address:\n{feedback}\n"

    response = kitaru.llm(
        prompt,
        model="smart",
        name="code_patch",
    )

    kitaru.save("raw_patch_response", response, type="context", tags=["debug"])

    # Placeholder parser: in a real implementation this would parse file blocks
    patch = {
        "src/example.py": response,
    }
    kitaru.log(files_changed=len(patch))
    return patch

@kitaru.checkpoint(type="tool_call")
def run_tests(patch: dict[str, str]) -> TestResult:
    # In a real implementation:
    # 1. apply patch in a sandbox/worktree
    # 2. run the test command
    # 3. collect output
    kitaru.log(test_command="pytest -q")
    return TestResult(
        passed=True,
        output="47 tests passed.",
        failures=[],
    )

@kitaru.checkpoint(type="tool_call")
def create_pr(patch: dict[str, str], issue_analysis: str) -> str:
    pr_body = kitaru.llm(
        f"""
Write a concise pull request description for this change.

Issue analysis:
{issue_analysis}

Patch summary:
{list(patch.keys())}
""",
        model="fast",
        name="pr_description",
    )

    # Real implementation should use an external idempotency key
    kitaru.log(pr_description=pr_body, pr_url="https://github.com/org/repo/pull/123")
    return "https://github.com/org/repo/pull/123"

@kitaru.flow(retries=1)
def coding_agent(issue_text: str, repo_summary: str) -> str:
    analysis = analyze_issue(issue_text)
    context = inspect_codebase(repo_summary, analysis)

    plan_approved = kitaru.wait(
        schema=bool,
        name="approve_plan",
        prompt="Approve implementation plan?",
        metadata={
            "analysis": analysis,
            "context_preview": context[:1000],
        },
    )
    if not plan_approved:
        return "Implementation plan rejected."

    feedback: str | None = None

    for round_idx in range(MAX_REVIEW_ROUNDS):
        patch = write_patch(analysis, context, feedback)
        test_result = run_tests(patch)

        if not test_result.passed:
            feedback = f"Tests failed:\n{test_result.output}\nFailures: {test_result.failures}"
            kitaru.log(round=round_idx + 1, tests_passed=False)
            continue

        review = kitaru.wait(
            schema=CodeReview,
            name="code_review",
            prompt=f"Review code changes (round {round_idx + 1}/{MAX_REVIEW_ROUNDS})",
            metadata={
                "patch_files": list(patch.keys()),
                "test_output": test_result.output,
                "analysis": analysis,
            },
        )

        if review.approved:
            pr_url = create_pr(patch, analysis)
            kitaru.log(rounds=round_idx + 1, pr_url=pr_url)
            return pr_url

        feedback = review.feedback
        kitaru.log(round=round_idx + 1, review_approved=False)

    return f"Failed to get approval after {MAX_REVIEW_ROUNDS} rounds."
```

### Why this is a good Kitaru pattern

This structure works well because:

- **planning, patch generation, testing, and PR creation are explicit checkpoints**
- **human review is a real durable wait**
- **the control loop stays plain Python**
- **replay is meaningful**
    - replay from `write_patch` to regenerate code
    - replay from `run_tests` to rerun flaky tests
    - replay from `code_review` with a different review decision
- **external side effects are isolated**
    - PR creation is its own checkpoint
    - it can be guarded with an idempotency key in the real implementation

### Recommended mental model

A Claude Code / OpenClaw-style agent in Kitaru should usually look like:

- `@flow` for the overall coding loop
- `@checkpoint` for major durable work units
- `wait()` for review and approval boundaries
- framework/tool internals rendered as child events or artifacts where useful

That keeps the coding agent durable without requiring Kitaru itself to become the coding framework.
