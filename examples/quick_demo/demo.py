"""Tiny Kitaru demo: agent + evals + wait/resume + logging.

A mock "expense approval" workflow that showcases the core Kitaru primitives:

1. ``prepare_expense``  — format the raw request into a report.
2. ``policy_agent``     — a simple rule-based agent that recommends
                          APPROVE / REJECT with reasoning. Swap in
                          ``kitaru.llm(...)`` to make it a real LLM agent.
3. ``evaluate_agent``   — runs offline-style evals on the agent's output
                          (policy compliance, explanation quality,
                          confidence) and logs them as metadata.
4. ``kitaru.wait()``    — suspends for a human decision, seeded with the
                          agent's recommendation + eval scores.
5. ``record_decision``  — finalizes the decision and logs it.

Run locally and answer the prompt inline, or resume from another shell
using the CLI commands printed below.
"""

from pydantic import BaseModel

import kitaru
from kitaru import checkpoint, flow
from kitaru.runtime import _get_current_execution_id


class AgentReview(BaseModel):
    """Agent's recommendation with reasoning."""

    recommendation: str  # "APPROVE" or "REJECT"
    reasoning: str
    confidence: float  # 0.0 – 1.0


class EvalScores(BaseModel):
    """Offline eval scores for the agent's review."""

    policy_compliance: float  # 0.0 – 1.0
    explanation_quality: float  # 0.0 – 1.0
    confidence_calibration: float  # 0.0 – 1.0
    overall: float  # mean of the above


# --- Checkpoints --------------------------------------------------------


@checkpoint
def prepare_expense(employee: str, amount: float, reason: str) -> str:
    """Format a human-readable expense report."""
    return f"{employee} requests ${amount:.2f} for: {reason}"


@checkpoint
def policy_agent(report: str, amount: float, reason: str) -> AgentReview:
    """Mock policy-checking agent.

    Replace the rule-based logic below with ``kitaru.llm(...)`` to turn
    this into a real LLM agent — the rest of the flow stays the same.
    """
    reason_l = reason.lower()
    risky_keywords = ("alcohol", "entertainment", "gift")
    has_risky = any(k in reason_l for k in risky_keywords)

    if amount > 1000 or has_risky:
        return AgentReview(
            recommendation="REJECT",
            reasoning=(
                f"Amount ${amount:.2f} exceeds $1000 limit."
                if amount > 1000
                else f"Reason '{reason}' contains restricted category."
            ),
            confidence=0.9,
        )
    return AgentReview(
        recommendation="APPROVE",
        reasoning=f"Amount ${amount:.2f} under $1000 and reason is within policy.",
        confidence=0.8,
    )


@checkpoint
def evaluate_agent(review: AgentReview, amount: float) -> EvalScores:
    """Run offline evals on the agent's review and log the scores."""
    # Policy compliance: does the recommendation match the ground-truth rule?
    ground_truth_approve = amount <= 1000
    policy_compliance = (
        1.0
        if (review.recommendation == "APPROVE") == ground_truth_approve
        else 0.0
    )

    # Explanation quality: did the agent justify with specifics (amount/reason)?
    explanation_quality = min(1.0, len(review.reasoning) / 80)

    # Confidence calibration: penalize over-confident wrong answers.
    confidence_calibration = (
        review.confidence if policy_compliance == 1.0 else 1.0 - review.confidence
    )

    overall = (policy_compliance + explanation_quality + confidence_calibration) / 3
    scores = EvalScores(
        policy_compliance=policy_compliance,
        explanation_quality=round(explanation_quality, 2),
        confidence_calibration=round(confidence_calibration, 2),
        overall=round(overall, 2),
    )

    kitaru.log(
        eval_policy_compliance=scores.policy_compliance,
        eval_explanation_quality=scores.explanation_quality,
        eval_confidence_calibration=scores.confidence_calibration,
        eval_overall=scores.overall,
    )
    return scores


@checkpoint
def record_decision(
    report: str,
    review: AgentReview,
    scores: EvalScores,
    approved: bool,
) -> str:
    """Record the final decision and log it as execution metadata."""
    status = "APPROVED" if approved else "REJECTED"
    agreed_with_agent = (review.recommendation == "APPROVE") == approved
    kitaru.log(
        status=status,
        report=report,
        agent_recommendation=review.recommendation,
        human_agreed_with_agent=agreed_with_agent,
        eval_overall=scores.overall,
    )
    return f"[{status}] {report} (agent said {review.recommendation}, eval={scores.overall:.2f})"


# --- Flow ---------------------------------------------------------------


@flow
def expense_approval_flow(employee: str, amount: float, reason: str) -> str:
    """Draft expense → agent review → evals → human approval → record."""
    report = prepare_expense(employee, amount, reason)
    review_ref = policy_agent(report, amount, reason)
    scores_ref = evaluate_agent(review_ref, amount)

    # Materialize the artifact references so we can seed the wait prompt with
    # the agent's recommendation and eval score.
    review: AgentReview = review_ref.load()
    scores: EvalScores = scores_ref.load()

    exec_id = _get_current_execution_id()
    print("\nTo approve/reject remotely, run in another terminal:")
    print(f"  kitaru executions input {exec_id} --value true   # approve")
    print(f"  kitaru executions input {exec_id} --value false  # reject")
    print(f"  kitaru executions resume {exec_id}\n")

    question = (
        f"Approve expense for {employee} (${amount:.2f})?\n"
        f"  Agent: {review.recommendation} — {review.reasoning}\n"
        f"  Eval overall score: {scores.overall:.2f}"
    )

    approved = kitaru.wait(
        name="approve_expense",
        schema=bool,
        question=question,
        timeout=3600,
        metadata={
            "employee": employee,
            "amount": amount,
            "agent_recommendation": review.recommendation,
            "eval_overall": scores.overall,
        },
    )

    return record_decision(report, review_ref, scores_ref, approved)


def main() -> None:
    """Run the demo with a sample expense."""
    result = expense_approval_flow.run(
        employee="Ada Lovelace",
        amount=123.45,
        reason="Conference travel",
    ).wait()
    print(f"\nResult: {result}")


if __name__ == "__main__":
    main()
