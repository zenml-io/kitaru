"""Analysis helpers for the support-copilot demo.

Re-exports the metric, judge, drift, and decision-extraction helpers so callers
can keep using ``from utils import cost, diff_decisions, ...`` after the package
split. Cohort replay orchestration lives in ``utils.cohort``; the report model
in ``reporting.cohort_report``.
"""

from utils.metrics import (
    DriftReport,
    FieldChange,
    MetricDelta,
    QualityScore,
    ReplayRun,
    build_judge,
    cost,
    diff_decisions,
    execution_stats,
    latency,
    load_support_decision_from_execution,
    quality_judge,
    recent_exec_ids,
)

__all__ = [
    "DriftReport",
    "FieldChange",
    "MetricDelta",
    "QualityScore",
    "ReplayRun",
    "build_judge",
    "cost",
    "diff_decisions",
    "execution_stats",
    "latency",
    "load_support_decision_from_execution",
    "quality_judge",
    "recent_exec_ids",
]
