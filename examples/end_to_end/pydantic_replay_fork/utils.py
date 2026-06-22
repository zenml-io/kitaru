"""Shared utilities for the PydanticAI support-copilot demo.

Public surface
--------------
CUT           The fixed checkpoint name for the intermediate decide step.
Recipe        Captured edit-set (model / prompt_profile / at).
MetricDelta   Named metric comparison: baseline vs variant value.
QualityScore  Typed output for the LLM quality judge (score 1-5).
build_judge   Build a raw pydantic_ai.Agent that scores an answer 1-5.
quality_judge BYO metric: score baseline and variant with the judge.
cost          BYO metric (lower_is_better=True): display_cost_usd delta.
latency       BYO metric (lower_is_better=True): wall-clock latency delta.
decision_from_artifacts  Read SupportDecision dict from artifact store.
decision_of   Read the SupportDecision dict from an execution (unified).
DriftReport   Decision-field drift report (.has_fork_drift).
diff_decisions Compare two SupportDecision dicts and return a DriftReport.
"""
from __future__ import annotations

import dataclasses
import logging
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from pydantic_ai import Agent

from kitaru import KitaruClient

if TYPE_CHECKING:
    from support_copilot import RunHandle

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CUT constant
# ---------------------------------------------------------------------------

#: The fixed checkpoint name for the intermediate decide step.
CUT: str = "decide"


# ---------------------------------------------------------------------------
# Drift comparison — self-contained, no framework dependency
# ---------------------------------------------------------------------------

#: The decision fields that define "did the decision move". ``policy_label`` and
#: ``summary`` are free-text the model rewords every call, so they are excluded —
#: a faithful no-edit rerun should not register drift on wording alone.
DECISION_FIELDS = ("risk_status", "required_action")


@dataclasses.dataclass
class FieldChange:
    """One decision field compared across two runs."""

    field: str
    baseline_value: Any
    comparison_value: Any

    @property
    def matches(self) -> bool:
        return self.baseline_value == self.comparison_value


@dataclasses.dataclass
class DriftReport:
    """The decision-field differences between a baseline run and another run."""

    changes: list[FieldChange]

    @property
    def has_fork_drift(self) -> bool:
        """True when any decision field differs."""
        return any(not c.matches for c in self.changes)

    def __str__(self) -> str:
        diffs = [
            f"{c.field}: {c.baseline_value!r} -> {c.comparison_value!r}"
            for c in self.changes
            if not c.matches
        ]
        return "decision changed — " + "; ".join(diffs) if diffs else "no decision drift"


def diff_decisions(baseline: dict, other: dict) -> DriftReport:
    """Compare two SupportDecision dicts on the decision fields.

    Scoped to ``DECISION_FIELDS`` so ``has_fork_drift`` means the decision changed,
    not that the model reworded a label or summary.
    """
    return DriftReport([FieldChange(f, baseline.get(f), other.get(f)) for f in DECISION_FIELDS])


# ---------------------------------------------------------------------------
# Recipe — the captured edit-set for a replay
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class Recipe:
    """Captured edit-set for a replay.

    For ``rerun`` (no-edit): model=None, prompt_profile=None, at=CUT.
    For ``replay`` (with-edit): model and/or prompt_profile are set.
    """

    model: Any = None
    prompt_profile: str | None = None
    at: str = CUT

    def is_identity(self) -> bool:
        """True when no config changes are applied (rerun recipe)."""
        return self.model is None and self.prompt_profile is None

    def as_kwargs(self) -> dict:
        """Return kwargs suitable for ``agent.replay(..., **recipe.as_kwargs())``."""
        kw: dict = {"at": self.at}
        if self.model is not None:
            kw["model"] = self.model
        if self.prompt_profile is not None:
            kw["prompt_profile"] = self.prompt_profile
        return kw


# ---------------------------------------------------------------------------
# MetricDelta — one metric comparison result
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class MetricDelta:
    """Result of applying one BYO metric to a baseline/variant pair.

    Args:
        name: Human-readable metric name (e.g. "cost", "latency", "quality").
        baseline_value: The metric value for the baseline (rerun) handle.
        variant_value: The metric value for the variant (replay) handle.
        lower_is_better: True for cost/latency; False for quality score.
    """

    name: str
    baseline_value: float | None
    variant_value: float | None
    lower_is_better: bool

    @property
    def is_worse(self) -> bool:
        """True when the variant regressed relative to the baseline.

        - lower_is_better metrics: worse if variant > baseline.
        - higher_is_better metrics: worse if variant < baseline.

        Returns False when either value is None (no data = not worse).
        """
        if self.baseline_value is None or self.variant_value is None:
            return False
        if self.lower_is_better:
            return self.variant_value > self.baseline_value
        else:
            return self.variant_value < self.baseline_value


# ---------------------------------------------------------------------------
# LLM-judge
# ---------------------------------------------------------------------------

class QualityScore(BaseModel):
    """Typed output for the LLM quality judge (score 1-5)."""

    score: int = 3


def build_judge(model: Any) -> Agent:
    """Build a raw pydantic_ai.Agent that scores an answer 1-5.

    In tests, use ``TestModel(custom_output_args={"score": N})`` to make the
    judge deterministic.  In production, a real model evaluates the answer
    quality on a 5-point scale.

    Args:
        model: A PydanticAI-compatible model (real or ``TestModel``).

    Returns:
        A ``pydantic_ai.Agent`` with output_type ``QualityScore``.
    """
    return Agent(
        model,
        name="support_quality_judge",
        output_type=QualityScore,
        instructions=(
            "You are a quality evaluator for support-copilot answers. "
            "Given a support decision summary, rate the quality of the answer "
            "on a scale of 1 (poor) to 5 (excellent). "
            "Focus on clarity, correctness, and helpfulness."
        ),
    )


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def _extract_cost(client: KitaruClient, exec_id: str) -> float:
    """Return ``display_cost_usd`` from the execution's LLM usage summary.

    Falls back to 0.0 when the usage summary is absent (e.g. TestModel).
    """
    try:
        run = client.executions.get(exec_id)
        summary = run.llm_usage_summary
        if summary is None:
            return 0.0
        cost = summary.get("display_cost_usd")
        if cost is None:
            return 0.0
        return float(cost)
    except Exception:  # noqa: BLE001
        return 0.0


def _extract_latency_s(client: KitaruClient, exec_id: str) -> float | None:
    """Return wall-clock latency in seconds for *exec_id*.

    Returns None when either timestamp is absent.
    """
    try:
        run = client.executions.get(exec_id)
        started = run.started_at
        ended = run.ended_at
        if started is None or ended is None:
            return None
        delta = ended - started
        return max(0.0, delta.total_seconds())
    except Exception:  # noqa: BLE001
        return None


def decision_from_artifacts(client: KitaruClient, exec_id: str) -> dict:
    """Read the SupportDecision dict from the execution artifact store.

    Searches checkpoints in priority order: ``decide`` first, then
    ``finalize``.  Always reads from artifacts — never from an in-memory cache.

    Raises:
        RuntimeError: If the decision cannot be found via artifact lookup.
    """
    from agent import SupportDecision

    run = client.executions.get(exec_id)

    def _extract(val: Any) -> dict | None:
        if isinstance(val, dict) and "risk_status" in val:
            return val
        if isinstance(val, SupportDecision):
            return val.model_dump()
        model_dump = getattr(val, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump()
            if isinstance(dumped, dict) and "risk_status" in dumped:
                return dumped
        return None

    priority = [CUT, "finalize"]
    cp_by_name = {c.name: c for c in run.checkpoints}
    for cp_name in priority:
        cp = cp_by_name.get(cp_name)
        if cp is None:
            continue
        for art in cp.artifacts:
            if getattr(art, "direction", None) not in (None, "output"):
                continue
            try:
                val = art.load()
            except Exception:  # noqa: BLE001
                continue
            extracted = _extract(val)
            if extracted is not None:
                return extracted

    raise RuntimeError(
        f"Could not extract a SupportDecision from execution {exec_id!r}. "
        f"Searched checkpoints: {priority}. "
        f"Checkpoints present: {[c.name for c in run.checkpoints]}. "
        "Ensure the flow completed successfully and the 'decide' checkpoint "
        "produced a serializable SupportDecision dict as its artifact."
    )


def decision_of(client: KitaruClient, exec_id: str, cache: dict | None = None) -> dict:
    """Return the SupportDecision dict for *exec_id*.

    Lookup order:
    1. In-memory cache (populated at run() time) — if ``cache`` is provided.
    2. ``decide`` checkpoint artifact (preferred; produced by the CUT step).
    3. ``finalize`` checkpoint artifact (fallback; contains propagated fields).

    Args:
        client: A KitaruClient instance.
        exec_id: The execution ID to look up.
        cache: Optional dict mapping exec_id -> decision dict for fast lookup.

    Raises:
        RuntimeError: If the decision cannot be found via any path.
    """
    if cache is not None:
        cached = cache.get(exec_id)
        if cached is not None:
            return cached
    return decision_from_artifacts(client, exec_id)


# ---------------------------------------------------------------------------
# Built-in BYO metrics
# ---------------------------------------------------------------------------

def cost(baseline: "RunHandle", variant: "RunHandle") -> MetricDelta:
    """BYO metric: display_cost_usd (lower_is_better=True)."""
    client = KitaruClient()
    b = _extract_cost(client, baseline.exec_id)
    v = _extract_cost(client, variant.exec_id)
    return MetricDelta(name="cost", baseline_value=b, variant_value=v, lower_is_better=True)


def latency(baseline: "RunHandle", variant: "RunHandle") -> MetricDelta:
    """BYO metric: wall-clock latency in seconds (lower_is_better=True)."""
    client = KitaruClient()
    b = _extract_latency_s(client, baseline.exec_id)
    v = _extract_latency_s(client, variant.exec_id)
    return MetricDelta(name="latency", baseline_value=b, variant_value=v, lower_is_better=True)


def quality_judge(baseline: "RunHandle", variant: "RunHandle") -> MetricDelta:
    """BYO metric: LLM judge quality score (lower_is_better=False).

    Reads ``baseline.model`` — the model stored on the RunHandle at creation
    time.  Returns an empty (None) delta when no model is available.
    """
    model = getattr(baseline, "model", None)
    if model is None:
        return MetricDelta(name="quality", baseline_value=None, variant_value=None, lower_is_better=False)

    judge = build_judge(model)

    def _score(decision: dict) -> int | None:
        try:
            summary = (
                f"policy={decision.get('policy_label')} "
                f"risk={decision.get('risk_status')} "
                f"action={decision.get('required_action')} "
                f"summary={decision.get('summary', '')!r}"
            )
            return judge.run_sync(summary).output.score
        except Exception:  # noqa: BLE001
            return None

    b_score = _score(baseline.decision)
    v_score = _score(variant.decision)
    return MetricDelta(name="quality", baseline_value=b_score, variant_value=v_score, lower_is_better=False)
