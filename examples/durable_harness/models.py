"""Data models for the durable harness example."""

from typing import Literal

from pydantic import BaseModel, computed_field


class EvaluationReport(BaseModel):
    """Structured QA evaluation result."""

    passed: bool
    feedback: str
    criteria_met: int
    criteria_total: int


class ReviewDecision(BaseModel):
    """Human review decision after QA failure.

    Used as the schema for kitaru.wait().
    """

    action: Literal["approve", "revise", "abort"]
    feedback: str = ""


class HarnessResult(BaseModel):
    """Flow return value with code and execution metadata."""

    code: str
    spec: str
    rounds_completed: int
    outcome: Literal[
        "passed", "approved_by_user", "aborted_by_user", "max_rounds_exhausted"
    ]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def passed(self) -> bool:
        return self.outcome == "passed"
