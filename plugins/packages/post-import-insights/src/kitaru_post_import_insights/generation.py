#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Bounded provider-neutral analyst and editor pipeline."""

import asyncio
import re
import time
from typing import Generic, Protocol, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from kitaru.api_models.v1.insight import BinnedInsightData, CategoricalInsightData
from kitaru_post_import_insights.models import (
    MAX_INSIGHTS,
    EvidenceLocator,
    GenerationDiagnostics,
    GenerationMode,
    ProviderReceipt,
)
from kitaru_post_import_insights.profiling import (
    CandidateFinding,
    DeterministicFact,
    ProfilingResult,
)

# A numeric token is a number plus an optional unit, so the number parser can
# read the leading number of any token the numeric scan produces.
_NUMBER_PATTERN = r"-?\d+(?:[.,]\d+)*"
_NUMBER = re.compile(_NUMBER_PATTERN)
# A minus sign counts only when it does not follow a word character, so a
# range such as "3-6" reads as two positive numbers, not "3" and "-6".
_NUMERIC_TOKEN = re.compile(r"(?<![\w.])(?<![\w])-?\d+(?:[.,]\d+)*(?:%|[A-Za-z]+)?")
_UNIT_WORD = re.compile(r"\s*(%|[A-Za-z]+)")
_CURRENCY_PREFIX = re.compile(r"[$€£]\s*$")
_PERCENT_UNITS = {"%", "percent", "pct"}
# Written units that name something the profiler never measures.
_NON_COUNT_UNITS = {
    "x",
    "times",
    "dollar",
    "dollars",
    "cent",
    "cents",
    "usd",
    "eur",
    "euro",
    "euros",
    "gbp",
    "pound",
    "pounds",
    "byte",
    "bytes",
    "kb",
    "mb",
    "gb",
    "tb",
    "token",
    "tokens",
}
# Facts named as a statistic of the chart's values share the chart's unit.
_CHART_STATISTIC_FACTS = {
    "maximum",
    "minimum",
    "median",
    "mean",
    "average",
    "p50",
    "p90",
    "p95",
    "p99",
}
# Written time units normalized to seconds, so "500 ms" can ground on 0.5 s.
_TIME_UNIT_SECONDS = {
    "ms": 0.001,
    "millisecond": 0.001,
    "milliseconds": 0.001,
    "s": 1.0,
    "sec": 1.0,
    "secs": 1.0,
    "second": 1.0,
    "seconds": 1.0,
    "min": 60.0,
    "mins": 60.0,
    "minute": 60.0,
    "minutes": 60.0,
    "h": 3600.0,
    "hr": 3600.0,
    "hrs": 3600.0,
    "hour": 3600.0,
    "hours": 3600.0,
}
_QUANTITY_TOKEN = re.compile(
    r"\b(?:no|none|zero|one|two|three|four|five|six|seven|eight|nine|ten|"
    r"eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|"
    r"nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|"
    r"hundreds?|thousands?|millions?|billions?|trillions?|dozens?|"
    r"once|twice|thrice|all|every|each|both|half|halves|"
    r"double|doubled|doubles|doubling|triple|tripled|triples|tripling|"
    r"several|many|few|fewer|fewest|multiple|multiples|couple|couples|"
    r"majority|majorities|minority|minorities|numerous|handful|handfuls|"
    r"quarters?|thirds?|fourths?|fifths?|sixths?|sevenths?|eighths?|ninths?|"
    r"tenths?|fractions?|proportions?)\b",
    flags=re.IGNORECASE,
)
# Hostname-like editorial text is conservatively rejected, including dotted
# identifiers; deterministic chart labels are not checked as editorial copy.
_LINK = re.compile(
    r"(?:https?://|www\.|"
    r"(?<![\w.-])(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+"
    r"[a-z]{2,63}(?![\w-]|\.[a-z0-9]))",
    flags=re.IGNORECASE,
)
_MARKUP = re.compile(
    r"(?:<[^>]+>|\[[^\]]+\]\([^\)]+\)|```|`+[^`\n]+`+|"
    r"\*{1,3}(?=\S)[^*\n]+?(?<=\S)\*{1,3}|"
    r"(?<!\w)_{1,3}(?=\S)[^_\n]+?(?<=\S)_{1,3}(?!\w)|"
    r"~~(?=\S)[^~\n]+?(?<=\S)~~|"
    r"^\s{0,3}~{3,}.*$|"
    r"^\s{0,3}(?:(?:\*[ \t]*){3,}|(?:-[ \t]*){3,}|(?:_[ \t]*){3,})$|"
    r"^\s{0,3}=+[ \t]*$|"
    r"^\s{0,3}(?:#{1,6}|>|[-+*])\s)",
    re.MULTILINE,
)
_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_UNSUPPORTED_CLAIM = re.compile(
    r"\b(?:caus(?:e|es|ed|ing)|because|due to|"
    r"(?:result|results|resulted|resulting)\s+in|"
    r"(?:lead|leads|led|leading)\s+to|"
    r"drive|drives|drove|driving|driven|"
    r"(?:stems?|stemmed|stemming)\s+from|"
    r"produc(?:e|es|ed|ing)|creat(?:e|es|ed|ing)|"
    r"trigger(?:s|ed|ing)?|responsible\s+for|attributable\s+to|"
    r"(?:account|accounts|accounted|accounting)\s+for|"
    r"(?:contributes?|contributed|contributing)\s+to|"
    r"(?:give|gives|gave|given|giving)\s+rise\s+to|"
    r"(?:bring|brings|brought|bringing)\s+about|"
    r"(?:arise|arises|arose|arisen|arising|"
    r"originate|originates|originated|originating)\s+from|"
    r"explains?|explained|explaining|determines?|determined|determining|"
    r"improv(?:e|es|ed|ing|ements?)|outperform(?:s|ed|ing)?|"
    r"better|best|worse|worst|healthy|correct|incorrect)\b",
    flags=re.IGNORECASE,
)
_OUTCOME_TOKEN = re.compile(
    r"\b(?:fail(?:ed|ing|s)?|failures?|succeed(?:ed|ing|s)?|"
    r"success|successes|successful|successfully|pass(?:ed|es|ing)?|"
    r"resolv(?:e|es|ed|ing)|fix(?:es|ed|ing)?|"
    r"complete(?:d|s)?|"
    r"completion(?:s)?|completing|finish(?:ed|es|ing)?|done|"
    r"in[ -]progress|"
    r"timed?[ -]out|timeouts?|cancel(?:ed|led|ation|ations|ing|s)?|"
    r"abandon(?:ed|ing|s)?|abandonments?|errors?|errored)\b",
    flags=re.IGNORECASE,
)
_WORK_SUCCESS_TOKEN = re.compile(
    r"\b(?:work|works|worked|working)\s+(?:"
    r"as\s+(?:expected|intended|designed)|correctly|properly|normally)\b",
    flags=re.IGNORECASE,
)
_SESSION_OUTCOME_CLAIM = re.compile(
    r"(?:\b(?:failed|failing|successful|completed|in[ -]progress|timed?[ -]out|"
    r"cancel(?:ed|led)|abandoned)\s+(?:sessions?|runs?|traces?)\b|"
    r"\b(?:sessions?|runs?|traces?)\s+"
    r"(?:(?:were|are|have|had)\s+)?"
    r"(?:fail(?:ed|ing|ures?)?|succeed(?:ed|ing)?|"
    r"success|successes|successful|"
    r"pass(?:ed|ing)?|complete|completed|completing|completions?|"
    r"finish(?:ed|ing)|in[ -]progress|timed?[ -]out|timeouts?|"
    r"cancel(?:ed|led|ing|lations?)?|abandon(?:ed|ing|ments?)?|"
    r"errors?|work(?:ed|ing)?\s+(?:correctly|properly)))\b",
    flags=re.IGNORECASE,
)
_NEGATION_TOKEN = re.compile(
    r"\b(?:no|not|never|none|neither|nor|without|cannot|absent)\b|n['\u2019]t\b",
    flags=re.IGNORECASE,
)
_SESSION_STATUS_OUTCOMES = {
    "failed": "failure",
    "completed": "completion",
    "in_progress": "in_progress",
}
_NON_IDENTITY_LABEL_WORDS = {
    "a",
    "an",
    "the",
    "of",
    "session",
    "sessions",
    "call",
    "calls",
    "request",
    "requests",
    "response",
    "responses",
    "result",
    "results",
    "occurrence",
    "occurrences",
    "percent",
    "percentage",
    "percentages",
    "retry",
    "retries",
    "error",
    "errors",
    "failure",
    "failures",
    "completion",
    "completions",
    "item",
    "items",
    "run",
    "runs",
    "time",
    "times",
    "tool",
    "tools",
}

# A number followed by one of these nouns counts things, so it cannot borrow
# a duration such as a chart maximum that happens to have the same digits.
_COUNT_NOUNS = {
    word
    for word in _NON_IDENTITY_LABEL_WORDS
    if len(word) > 3 and word not in {"time", "times"}
} | {
    "label",
    "labels",
    "model",
    "models",
    "node",
    "nodes",
    "observation",
    "observations",
    "pair",
    "pairs",
    "turn",
    "turns",
}


class _GenerationModel(BaseModel):
    """Base for strict immutable provider-neutral generation values."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ModelGenerationConfig(_GenerationModel):
    """Credential-free hard limits for one model-backed run."""

    model: str = Field(min_length=1, max_length=255)
    total_timeout_seconds: float = Field(default=25.0, gt=0, le=120)
    analyst_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    editor_timeout_seconds: float = Field(default=12.0, gt=0, le=120)
    max_input_bytes: int = Field(default=512_000, ge=1_000, le=10_000_000)
    analyst_max_output_tokens: int = Field(default=1_000, ge=64, le=8_000)
    editor_max_output_tokens: int = Field(default=2_000, ge=64, le=8_000)


class CandidateProjection(_GenerationModel):
    """Bounded inert deterministic facts visible to a model stage."""

    id: str
    family: str
    rank: int
    deterministic_title: str
    deterministic_description: str
    detector_description: str
    caveat: str | None
    facts: list[DeterministicFact]
    chart_data: CategoricalInsightData | BinnedInsightData
    evidence_locators: list[EvidenceLocator]
    contributing_session_count: int = Field(ge=1)


class AnalystProjection(_GenerationModel):
    """Complete allowlisted input to the analyst."""

    content_hash: str
    candidates: list[CandidateProjection] = Field(min_length=1, max_length=100)


class AnalystPlan(_GenerationModel):
    """Provider-neutral candidate selection returned by the analyst."""

    selected_candidate_ids: list[str] = Field(min_length=1, max_length=MAX_INSIGHTS)
    recommended_candidate_id: str
    rationale: str = Field(min_length=1, max_length=1000)


class SelectedCandidateProjection(CandidateProjection):
    """One immutable selected candidate supplied to the editor."""

    position: int = Field(ge=0, lt=MAX_INSIGHTS)
    recommended: bool


class EditorialProjection(_GenerationModel):
    """Validated immutable analyst selection supplied to the editor."""

    content_hash: str
    recommendation_id: str
    candidates: list[SelectedCandidateProjection] = Field(
        min_length=1, max_length=MAX_INSIGHTS
    )


class EditorialCardCopy(_GenerationModel):
    """Model-authored explanatory copy for one selected candidate."""

    id: str
    eyebrow: str = Field(min_length=1, max_length=80)
    description: str = Field(min_length=1, max_length=1000)


class EditorialCardPlan(_GenerationModel):
    """Provider-neutral card copy returned by the editor."""

    insights: list[EditorialCardCopy] = Field(min_length=1, max_length=MAX_INSIGHTS)


DEFAULT_INTRO_EYEBROW = "What to look at first"
DEFAULT_INTRO_TITLE = "A few patterns are worth a closer look"
DEFAULT_INTRO_DESCRIPTION = (
    "These evidence-backed leads can guide your first investigation."
)
DEFAULT_RECOMMENDATION_TITLE = "Recommended next step"
DEFAULT_RECOMMENDATION_DESCRIPTION = (
    "Start with this pattern, define a focused cohort, and test a change."
)


class EditorialPlan(EditorialCardPlan):
    """Card copy combined with deterministic page framing."""

    intro_eyebrow: str = Field(
        default=DEFAULT_INTRO_EYEBROW, min_length=1, max_length=80
    )
    intro_title: str = Field(default=DEFAULT_INTRO_TITLE, min_length=1, max_length=255)
    intro_description: str = Field(
        default=DEFAULT_INTRO_DESCRIPTION, min_length=1, max_length=1000
    )
    recommendation_title: str = Field(
        default=DEFAULT_RECOMMENDATION_TITLE, min_length=1, max_length=255
    )
    recommendation_description: str = Field(
        default=DEFAULT_RECOMMENDATION_DESCRIPTION, min_length=1, max_length=1000
    )


T = TypeVar("T", bound=BaseModel)


class ModelStageResponse(_GenerationModel, Generic[T]):
    """Provider-neutral successful response and sanitized receipt."""

    value: T
    receipt: ProviderReceipt


class InsightModelGenerator(Protocol):
    """Perform the two settled model operations without provider leakage."""

    async def analyze(
        self,
        *,
        projection: AnalystProjection,
        config: ModelGenerationConfig,
        timeout_seconds: float,
    ) -> ModelStageResponse[AnalystPlan]:
        """Select and order deterministic candidates."""

    async def edit(
        self,
        *,
        projection: EditorialProjection,
        config: ModelGenerationConfig,
        timeout_seconds: float,
    ) -> ModelStageResponse[EditorialCardPlan]:
        """Write card copy for the validated selection without changing its facts."""


class ModelGenerationPlan(_GenerationModel):
    """Validated selection and copy ready for deterministic assembly."""

    selection: AnalystPlan
    editorial: EditorialPlan
    mode: GenerationMode
    diagnostics: GenerationDiagnostics


def _build_candidate_projection(candidate: CandidateFinding) -> CandidateProjection:
    return CandidateProjection(
        id=candidate.id,
        family=candidate.family,
        rank=candidate.rank,
        deterministic_title=candidate.title,
        deterministic_description=candidate.fallback_description,
        detector_description=candidate.eyebrow,
        caveat=candidate.caveat,
        facts=candidate.facts,
        chart_data=candidate.data,
        evidence_locators=candidate.evidence,
        contributing_session_count=candidate.coverage.contributing_sessions_available,
    )


def build_analyst_projection(profiling: ProfilingResult) -> AnalystProjection:
    """Build the only deterministic envelope the analyst may receive."""
    projection = AnalystProjection(
        content_hash=profiling.content_hash,
        candidates=[_build_candidate_projection(item) for item in profiling.candidates],
    )
    return projection


def validate_analyst_plan(
    plan: AnalystPlan, candidates: list[CandidateFinding]
) -> AnalystPlan:
    """Require one to six distinct known IDs and an in-selection recommendation."""
    selected = plan.selected_candidate_ids
    if len(selected) != len(set(selected)):
        raise ValueError("analyst candidate IDs must be unique")
    known = {candidate.id for candidate in candidates}
    if not set(selected).issubset(known):
        raise ValueError("analyst selected an unknown candidate ID")
    if plan.recommended_candidate_id not in selected:
        raise ValueError("analyst recommendation must be in the selection")
    if _CONTROL.search(plan.rationale) or _LINK.search(plan.rationale):
        raise ValueError("analyst rationale contains unsafe content")
    if _MARKUP.search(plan.rationale):
        raise ValueError("analyst rationale contains markup")
    return plan


def _diversify_analyst_plan(
    plan: AnalystPlan, candidates: list[CandidateFinding]
) -> AnalystPlan:
    """Keep one selected candidate per family and refill unused families."""
    by_id = {candidate.id: candidate for candidate in candidates}
    recommendation = by_id[plan.recommended_candidate_id]
    selected: list[str] = []
    families: set[str] = set()
    for candidate_id in plan.selected_candidate_ids:
        candidate = by_id[candidate_id]
        if candidate.family in families:
            continue
        selected.append(
            recommendation.id
            if candidate.family == recommendation.family
            else candidate_id
        )
        families.add(candidate.family)

    target_count = len(plan.selected_candidate_ids)
    refill_ids = deterministic_selection(candidates).selected_candidate_ids
    for candidate_id in refill_ids:
        if len(selected) == target_count:
            break
        candidate = by_id[candidate_id]
        if candidate.family in families:
            continue
        selected.append(candidate.id)
        families.add(candidate.family)

    return plan.model_copy(update={"selected_candidate_ids": selected})


def build_editorial_projection(
    profiling: ProfilingResult, selection: AnalystPlan
) -> EditorialProjection:
    """Freeze the validated selection before the editor runs."""
    candidates = {candidate.id: candidate for candidate in profiling.candidates}
    return EditorialProjection(
        content_hash=profiling.content_hash,
        recommendation_id=selection.recommended_candidate_id,
        candidates=[
            SelectedCandidateProjection(
                **_build_candidate_projection(candidates[candidate_id]).model_dump(),
                position=position,
                recommended=candidate_id == selection.recommended_candidate_id,
            )
            for position, candidate_id in enumerate(selection.selected_candidate_ids)
        ],
    )


def _validate_copy_safety(value: str) -> None:
    """Reject content that the frontend must not render as editorial copy."""
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        raise ValueError("editor copy must be valid UTF-8 text") from None
    if _CONTROL.search(value):
        raise ValueError("editor copy contains control characters")
    if _LINK.search(value):
        raise ValueError("editor copy contains a link")
    if _MARKUP.search(value):
        raise ValueError("editor copy contains markup")
    if _UNSUPPORTED_CLAIM.search(value):
        raise ValueError("editor copy contains an unsupported claim")


def _get_quantified_labels(candidate: CandidateFinding) -> set[str]:
    """Return quantity-bearing labels that also contain an identity token."""
    if not isinstance(candidate.data, CategoricalInsightData):
        return set()
    return {
        item.label
        for item in candidate.data.values
        if _is_identity_bearing_quantified_label(item.label)
    }


def _is_identity_bearing_quantified_label(label: str) -> bool:
    """Distinguish names containing quantities from labels that are quantities."""
    if not (_NUMERIC_TOKEN.search(label) or _QUANTITY_TOKEN.search(label)):
        return False
    without_quantities = _QUANTITY_TOKEN.sub("", label)
    without_quantities = _NUMERIC_TOKEN.sub("", without_quantities)
    remaining_words = {
        word.lower() for word in re.findall(r"[A-Za-z]+", without_quantities)
    }
    return bool(remaining_words - _NON_IDENTITY_LABEL_WORDS)


def _remove_known_labels(value: str, labels: set[str]) -> str:
    """Mask exact known labels so identity tokens are not treated as claims."""
    for label in sorted(labels, key=len, reverse=True):
        value = re.sub(
            rf"(?<![A-Za-z0-9]){re.escape(label)}(?![A-Za-z0-9])",
            "",
            value,
        )
    return value


def _get_outcome_categories(value: str) -> set[str]:
    """Normalize grammatical variants of outcome words into factual classes."""
    categories: set[str] = set()
    for match in _OUTCOME_TOKEN.finditer(value):
        token = match.group(0).lower()
        if token.startswith("fail"):
            categories.add("failure")
        elif token.startswith(("success", "succeed", "pass", "resolv", "fix")):
            categories.add("success")
        elif (
            token.startswith(("complete", "completion", "completing", "finish"))
            or token == "done"
        ):
            categories.add("completion")
        elif token.startswith("in"):
            categories.add("in_progress")
        elif token.startswith(("timed", "timeout")):
            categories.add("timeout")
        elif token.startswith("cancel"):
            categories.add("cancellation")
        elif token.startswith("abandon"):
            categories.add("abandonment")
        else:
            categories.add("error")
    if _WORK_SUCCESS_TOKEN.search(value):
        categories.add("success")
    return categories


def _has_outcome_wording(value: str) -> bool:
    """Return whether copy contains a bounded outcome assertion."""
    return bool(_OUTCOME_TOKEN.search(value) or _WORK_SUCCESS_TOKEN.search(value))


def _get_supported_outcome_categories(candidate: CandidateFinding) -> set[str]:
    """Return outcomes stated by the deterministic candidate evidence."""
    categories = set()
    if (
        candidate.id == "session-outcomes"
        and candidate.family == "outcome"
        and isinstance(candidate.data, CategoricalInsightData)
    ):
        categories.update(
            outcome
            for item in candidate.data.values
            if item.value > 0
            and (outcome := _SESSION_STATUS_OUTCOMES.get(item.label)) is not None
        )
    for value in (
        candidate.eyebrow,
        candidate.title,
        candidate.fallback_description,
    ):
        categories.update(_get_outcome_categories(value))
    return categories


def _get_supported_session_outcome_categories(
    candidate: CandidateFinding,
) -> set[str]:
    """Return observed session outcomes represented by a candidate."""
    if not (
        candidate.id == "session-outcomes"
        and candidate.family == "outcome"
        and isinstance(candidate.data, CategoricalInsightData)
    ):
        return set()
    return {
        outcome
        for item in candidate.data.values
        if item.value > 0
        and (outcome := _SESSION_STATUS_OUTCOMES.get(item.label)) is not None
    }


def _get_session_outcome_claims(value: str) -> set[str]:
    """Return outcome categories explicitly attributed to sessions."""
    return {
        outcome
        for match in _SESSION_OUTCOME_CLAIM.finditer(value)
        for outcome in _get_outcome_categories(match.group(0))
    }


def _has_negated_outcome(value: str) -> bool:
    """Reject grammatical negation in the same clause as an outcome term."""
    return any(
        _has_outcome_wording(clause) and _NEGATION_TOKEN.search(clause)
        for clause in re.split(r"[.;!?\n]+", value)
    )


def _parse_number(token: str) -> float | None:
    """Parse the leading number of a numeric token, tolerating separators."""
    match = _NUMBER.match(token)
    if match is None:
        return None
    raw = match.group(0)
    if re.fullmatch(r"\d{1,3}(?:,\d{3})+(?:\.\d+)?", raw):
        raw = raw.replace(",", "")
    else:
        raw = raw.replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def _classify_unit(unit: str | None) -> tuple[str, float]:
    """Map a fact name or chart unit to a quantity kind and a scale to seconds."""
    lowered = (unit or "").lower()
    if lowered in _PERCENT_UNITS or "percent" in lowered or "share" in lowered:
        return "percent", 1.0
    for word, scale in _TIME_UNIT_SECONDS.items():
        if lowered == word or lowered.endswith(f"_{word}"):
            return "seconds", scale
    if "duration" in lowered or "latency" in lowered:
        return "seconds", 1.0
    return "count", 1.0


def _get_grounded_numbers(candidate: CandidateFinding) -> dict[str, set[float]]:
    """Return every quantity the candidate states about itself, by kind.

    Kinds are ``count``, ``percent``, and ``seconds`` so that a count of three
    occurrences cannot ground "3 seconds" or "3%".
    """
    numbers: dict[str, set[float]] = {
        "count": set(),
        "percent": set(),
        "seconds": set(),
    }
    data = candidate.data
    chart_kind, chart_scale = _classify_unit(data.unit)
    for fact in candidate.facts:
        # A statistic of the chart's values, such as "maximum", is measured in
        # the chart's unit; every other fact keeps the unit its name declares.
        if fact.name.lower() in _CHART_STATISTIC_FACTS:
            kind, scale = chart_kind, chart_scale
        else:
            kind, scale = _classify_unit(fact.name)
        if isinstance(fact.value, str):
            values = [
                parsed
                for match in _NUMBER.finditer(fact.value)
                if (parsed := _parse_number(match.group(0))) is not None
            ]
        else:
            values = [float(fact.value)]
        numbers[kind].update(value * scale for value in values)
    kind, scale = chart_kind, chart_scale
    if isinstance(data, CategoricalInsightData):
        numbers[kind].update(float(item.value) * scale for item in data.values)
    else:
        for bin_ in data.bins:
            numbers["count"].add(float(bin_.count))
            numbers[kind].update(
                float(bound) * scale
                for bound in (bin_.lower_bound, bin_.upper_bound)
                if bound is not None
            )
    coverage = candidate.coverage
    numbers["count"].update(
        float(count)
        for count in (
            coverage.sessions_analyzed,
            coverage.affected_sessions,
            coverage.occurrences,
            coverage.contributing_sessions_available,
            len(candidate.contributing_session_ids),
        )
    )
    if coverage.sessions_analyzed:
        numbers["percent"].add(
            100.0 * coverage.affected_sessions / coverage.sessions_analyzed
        )
    return numbers


def _matches_grounded(value: float, decimals: int, grounded: set[float]) -> bool:
    return any(known == value or round(known, decimals) == value for known in grounded)


def _is_grounded_number(
    token: str,
    *,
    preceding: str,
    following: str,
    grounded: dict[str, set[float]],
    chart_unit: str | None,
) -> bool:
    """Accept a written number that equals or rounds a grounded quantity.

    A percent sign or time unit, attached to the token or as the next word,
    selects the kind the number must ground on; a bare number may ground on
    a count or a time value but never on a percentage. A currency prefix, a
    letter suffix outside the known units, or a unit word the profiler never
    measures rejects the number outright.
    """
    match = _NUMBER.match(token)
    if match is None:
        return False
    raw = match.group(0)
    value = _parse_number(raw)
    if value is None:
        return False
    decimals = len(raw.rsplit(".", 1)[1]) if "." in raw else 0
    if _CURRENCY_PREFIX.search(preceding):
        return False
    attached = token[match.end() :].lower()
    if attached and attached not in _PERCENT_UNITS | set(_TIME_UNIT_SECONDS):
        return False
    unit = attached
    if not unit and (next_word := _UNIT_WORD.match(following)):
        unit = next_word.group(1).lower()
    if unit in _NON_COUNT_UNITS and unit != (chart_unit or "").lower():
        return False
    if unit in _PERCENT_UNITS:
        return _matches_grounded(value, decimals, grounded["percent"])
    if unit in _COUNT_NOUNS:
        return _matches_grounded(value, decimals, grounded["count"])
    if unit in _TIME_UNIT_SECONDS:
        scaled = value * _TIME_UNIT_SECONDS[unit]
        # Compare at the written precision after scaling to seconds.
        scaled_decimals = decimals + max(
            0, -int(f"{_TIME_UNIT_SECONDS[unit]:e}".split("e")[1])
        )
        return _matches_grounded(scaled, scaled_decimals, grounded["seconds"])
    return _matches_grounded(value, decimals, grounded["count"]) or _matches_grounded(
        value, decimals, grounded["seconds"]
    )


def _negates_candidate_phrase(value: str, candidate: CandidateFinding) -> bool:
    """Return whether a sentence negates a quoted candidate outcome phrase."""
    for clause in re.split(r"[.;!?\n]+", value):
        masked = _remove_candidate_phrases(clause, candidate)
        if (
            masked != clause
            and _has_outcome_wording(clause)
            and _NEGATION_TOKEN.search(masked)
        ):
            return True
    return False


def _remove_candidate_phrases(value: str, candidate: CandidateFinding) -> str:
    """Mask clauses quoted from the candidate's own title, description, or caveat.

    The editor is asked to carry the caveat into its copy, so a quoted clause
    such as "is not the same as a failed session" is grounded wording rather
    than a claim the model made up.
    """
    clauses = {
        clause.strip().lower()
        for text in (candidate.title, candidate.fallback_description, candidate.caveat)
        if text
        for clause in re.split(r"[.;!?\n]+", text)
        if len(clause.strip()) >= 12
    }
    for clause in sorted(clauses, key=len, reverse=True):
        value = re.sub(re.escape(clause), " ", value, flags=re.IGNORECASE)
    return value


def _validate_card_copy(value: str, candidate: CandidateFinding) -> None:
    """Validate one card only against the deterministic candidate it explains."""
    _validate_copy_safety(value)
    remaining = _remove_known_labels(value, _get_quantified_labels(candidate))
    claims = _remove_candidate_phrases(value, candidate)
    if _has_negated_outcome(claims) or _negates_candidate_phrase(value, candidate):
        raise ValueError("editor card copy contains a negated outcome claim")
    grounded = _get_grounded_numbers(candidate)
    for match in _NUMERIC_TOKEN.finditer(remaining):
        if not _is_grounded_number(
            match.group(0),
            preceding=remaining[: match.start()],
            following=remaining[match.end() :],
            grounded=grounded,
            chart_unit=candidate.data.unit,
        ):
            raise ValueError(
                "editor card copy contains a numeric claim absent from the "
                "candidate facts"
            )
    if not _get_session_outcome_claims(claims).issubset(
        _get_supported_session_outcome_categories(candidate)
    ):
        raise ValueError(
            "editor card copy contains an unsupported outcome claim about sessions"
        )
    if not _get_outcome_categories(claims).issubset(
        _get_supported_outcome_categories(candidate)
    ):
        raise ValueError("editor card copy contains an unsupported outcome claim")


def _validate_selection_order(cards: EditorialCardPlan, selection: AnalystPlan) -> None:
    """Reject card copy that drops, adds, or reorders the selected candidates."""
    if [item.id for item in cards.insights] != selection.selected_candidate_ids:
        raise ValueError("editor must preserve selection membership and order")


def _validate_card(item: EditorialCardCopy, candidate: CandidateFinding) -> None:
    for value in (item.eyebrow, item.description):
        _validate_card_copy(value, candidate)


def _is_valid_card(item: EditorialCardCopy, candidate: CandidateFinding) -> bool:
    """Return whether both strings of one card pass candidate-grounded checks."""
    try:
        _validate_card(item, candidate)
    except ValueError:
        return False
    return True


def validate_editorial_plan(
    plan: EditorialCardPlan,
    selection: AnalystPlan,
    candidates: list[CandidateFinding],
) -> EditorialPlan:
    """Reject copy that changes selection or introduces unsupported claims."""
    _validate_selection_order(plan, selection)
    by_id = {item.id: item for item in candidates}
    for item in plan.insights:
        _validate_card(item, by_id[item.id])
    if isinstance(plan, EditorialPlan):
        return plan
    return EditorialPlan(**plan.model_dump())


def apply_editorial_copy(
    cards: EditorialCardPlan,
    selection: AnalystPlan,
    candidates: list[CandidateFinding],
) -> tuple[EditorialPlan, list[str]]:
    """Keep validated card copy and use deterministic copy for the rest.

    The selection fixes which cards exist and in what order. A selected card
    the editor skipped, or whose copy fails validation, uses deterministic
    copy; cards the editor added are ignored.

    Returns:
        Plan with deterministic page framing, and the IDs of the cards using
        deterministic copy.
    """
    by_id = {item.id: item for item in candidates}
    copy_by_id = {item.id: item for item in cards.insights}
    accepted: list[EditorialCardCopy] = []
    rejected: list[str] = []
    for candidate_id in selection.selected_candidate_ids:
        candidate = by_id[candidate_id]
        item = copy_by_id.get(candidate_id)
        if item is not None and _is_valid_card(item, candidate):
            accepted.append(item)
            continue
        rejected.append(candidate_id)
        accepted.append(
            EditorialCardCopy(
                id=candidate_id,
                eyebrow=candidate.eyebrow,
                description=candidate.fallback_description,
            )
        )
    return EditorialPlan(insights=accepted), rejected


def deterministic_selection(candidates: list[CandidateFinding]) -> AnalystPlan:
    """Select up to six stable, non-redundant candidates for fallback."""
    selected: list[CandidateFinding] = []
    families: set[str] = set()
    for candidate in sorted(candidates, key=lambda item: (item.rank, item.id)):
        if candidate.family in families:
            continue
        selected.append(candidate)
        families.add(candidate.family)
        if len(selected) == MAX_INSIGHTS:
            break
    if not selected:
        raise ValueError("cannot select from an empty candidate set")
    return AnalystPlan(
        selected_candidate_ids=[item.id for item in selected],
        recommended_candidate_id=selected[0].id,
        rationale="Stable deterministic fallback ordering.",
    )


def deterministic_editorial(
    selection: AnalystPlan, candidates: list[CandidateFinding]
) -> EditorialPlan:
    """Return stable page and card copy without a model request."""
    by_id = {candidate.id: candidate for candidate in candidates}
    return EditorialPlan(
        insights=[
            EditorialCardCopy(
                id=candidate_id,
                eyebrow=by_id[candidate_id].eyebrow,
                description=by_id[candidate_id].fallback_description,
            )
            for candidate_id in selection.selected_candidate_ids
        ],
    )


def _build_fallback(
    profiling: ProfilingResult,
    *,
    selection: AnalystPlan | None,
    receipts: list[ProviderReceipt],
    reason: str,
) -> ModelGenerationPlan:
    selected = selection or deterministic_selection(profiling.candidates)
    return ModelGenerationPlan(
        selection=selected,
        editorial=deterministic_editorial(selected, profiling.candidates),
        mode=GenerationMode.DETERMINISTIC_FALLBACK,
        diagnostics=GenerationDiagnostics(
            provider_receipts=receipts,
            warnings=["Model-backed generation was unavailable."],
            fallback_reason=reason,
        ),
    )


def generate_deterministic_plan(
    profiling: ProfilingResult,
) -> ModelGenerationPlan:
    """Produce a zero-request deterministic generation plan."""
    selection = deterministic_selection(profiling.candidates)
    return ModelGenerationPlan(
        selection=selection,
        editorial=deterministic_editorial(selection, profiling.candidates),
        mode=GenerationMode.DETERMINISTIC,
        diagnostics=GenerationDiagnostics(),
    )


async def generate_model_plan(
    profiling: ProfilingResult,
    *,
    generator: InsightModelGenerator,
    config: ModelGenerationConfig,
) -> ModelGenerationPlan:
    """Run at most one analyst and one editor request under one deadline."""
    if not profiling.candidates:
        raise ValueError("model generation requires at least one candidate")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + config.total_timeout_seconds
    receipts: list[ProviderReceipt] = []
    projection = build_analyst_projection(profiling)
    if len(projection.model_dump_json().encode()) > config.max_input_bytes:
        return _build_fallback(
            profiling,
            selection=None,
            receipts=receipts,
            reason="analyst_input_too_large",
        )
    analyst_started = time.monotonic()
    try:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError
        async with asyncio.timeout(min(config.analyst_timeout_seconds, remaining)):
            analyst_response = await generator.analyze(
                projection=projection,
                config=config,
                timeout_seconds=min(config.analyst_timeout_seconds, remaining),
            )
        if analyst_response.receipt.stage != "analyst":
            raise ValueError("analyst returned the wrong receipt stage")
        selection = _diversify_analyst_plan(
            validate_analyst_plan(analyst_response.value, profiling.candidates),
            profiling.candidates,
        )
        receipts.append(analyst_response.receipt)
    except TimeoutError:
        receipts.append(
            ProviderReceipt(
                stage="analyst",
                latency_ms=int((time.monotonic() - analyst_started) * 1000),
                outcome="timed_out",
            )
        )
        return _build_fallback(
            profiling,
            selection=None,
            receipts=receipts,
            reason="analyst_timed_out",
        )
    except Exception:
        receipts.append(
            ProviderReceipt(
                stage="analyst",
                latency_ms=int((time.monotonic() - analyst_started) * 1000),
                outcome="failed",
            )
        )
        return _build_fallback(
            profiling,
            selection=None,
            receipts=receipts,
            reason="analyst_failed",
        )

    editorial_projection = build_editorial_projection(profiling, selection)
    if len(editorial_projection.model_dump_json().encode()) > config.max_input_bytes:
        return _build_fallback(
            profiling,
            selection=selection,
            receipts=receipts,
            reason="editor_input_too_large",
        )
    editor_started = time.monotonic()
    try:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError
        async with asyncio.timeout(min(config.editor_timeout_seconds, remaining)):
            editor_response = await generator.edit(
                projection=editorial_projection,
                config=config,
                timeout_seconds=min(config.editor_timeout_seconds, remaining),
            )
        if editor_response.receipt.stage != "editor":
            raise ValueError("editor returned the wrong receipt stage")
    except TimeoutError:
        receipts.append(
            ProviderReceipt(
                stage="editor",
                latency_ms=int((time.monotonic() - editor_started) * 1000),
                outcome="timed_out",
            )
        )
        return _build_fallback(
            profiling,
            selection=selection,
            receipts=receipts,
            reason="editor_timed_out",
        )
    except Exception:
        receipts.append(
            ProviderReceipt(
                stage="editor",
                latency_ms=int((time.monotonic() - editor_started) * 1000),
                outcome="failed",
            )
        )
        return _build_fallback(
            profiling,
            selection=selection,
            receipts=receipts,
            reason="editor_failed",
        )

    receipts.append(editor_response.receipt)
    editorial, rejected_ids = apply_editorial_copy(
        editor_response.value, selection, profiling.candidates
    )
    return ModelGenerationPlan(
        selection=selection,
        editorial=editorial,
        mode=GenerationMode.MODEL_BACKED,
        diagnostics=GenerationDiagnostics(
            provider_receipts=receipts,
            warnings=[
                f"Card copy for {candidate_id} failed validation and uses "
                "deterministic text."
                for candidate_id in rejected_ids
            ],
        ),
    )
