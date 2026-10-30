#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Grounding and safety checks for editor card copy."""

import re

from kitaru.api_models.v1.insight import CategoricalInsightData
from kitaru_post_import_insights.profiling import CandidateFinding

# A numeric token is a number plus an optional unit, so the number parser can
# read the leading number of any token the numeric scan produces.
# Digits with optional separators, or a leading-dot decimal such as ".5".
_NUMBER_PATTERN = r"-?(?:\d+(?:[.,]\d+)*|\.\d+)"
_NUMBER = re.compile(_NUMBER_PATTERN)
# A token starts only where no word character or dot precedes it, so a range
# such as "3-6" reads as two positive numbers and "1332.029" is not rescanned
# from its fractional part; ".5" is still reached because the dot itself is
# where that token begins.
_NUMERIC_TOKEN = re.compile(rf"(?<![\w.]){_NUMBER_PATTERN}(?:%|[A-Za-z]+)?")
_UNIT_WORD = re.compile(r"\s*(%|[A-Za-z]+)")
_CURRENCY_PREFIX = re.compile(r"[$€£]\s*$")
_PERCENT_UNITS = {"%", "percent", "percentage", "pct", "share"}
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
    r"(?:account|accounts|accounted|accounting)\s+for\s+"
    r"(?!(?:(?:the|about|around|roughly|nearly|almost|over|under)\s+)*"
    r"(?:\d|most\b|half\b|larger|largest|smaller|smallest|bulk\b|"
    r"majority\b|rest\b|remaining|remainder))|"
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
    r"\b(?:no|not|never|none|zero|neither|nor|without|cannot|absent)\b|"
    r"n['\u2019]t\b",
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


# A chart label directly before or after a number, with at most one short
# connector word between them and no punctuation other than a colon, an
# equals sign, or a parenthesis.
_PRECEDING_LABEL = re.compile(
    r"(?<![\w./:-])([A-Za-z_][\w./:-]*)"
    r"(?:\s+(?:at|with|of|is|was|has|had|shows))?[\s:(=]*$"
)
_FOLLOWING_LABEL = re.compile(
    r"^[\s):,]*(?:(?P<connector>for|in|on|of|from|at|by|per|to)\s+)?"
    r"(?P<label>[A-Za-z_][\w./:-]*)"
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
        candidate.caveat or "",
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


def _get_label_values(candidate: CandidateFinding) -> dict[str, float]:
    """Map each categorical label, and its outcome category, to its own value."""
    if not isinstance(candidate.data, CategoricalInsightData):
        return {}
    values: dict[str, float] = {}
    for item in candidate.data.values:
        values[item.label.lower()] = float(item.value)
        outcome = _SESSION_STATUS_OUTCOMES.get(item.label)
        if outcome is not None:
            values[outcome] = float(item.value)
    return values


def _lookup_label_value(
    word: str, label_values: dict[str, float], *, outcomes: bool
) -> float | None:
    lowered = word.lower().strip("./:-")
    for key in (lowered, lowered.rstrip("s")):
        if key in label_values:
            return label_values[key]
    if outcomes:
        for outcome in _get_outcome_categories(word):
            if outcome in label_values:
                return label_values[outcome]
    return None


def _get_bound_label_value(
    *, preceding: str, following: str, label_values: dict[str, float]
) -> float | None:
    """Return the value a number must equal when a chart label is adjacent to it.

    "runner at 39" and "39 for runner" bind 39 to the runner's value. A label
    further away, as in "39 and runner at 16" or "runner, with 14 sessions",
    does not bind, because the number belongs to whatever label sits next to
    it, not to the nearest label mentioned.
    """
    before = _PRECEDING_LABEL.search(preceding)
    if before is not None:
        bound = _lookup_label_value(before.group(1), label_values, outcomes=False)
        if bound is not None:
            return bound
    after = _FOLLOWING_LABEL.match(following)
    if after is None:
        return None
    return _lookup_label_value(
        after.group("label"), label_values, outcomes=after.group("connector") is None
    )


def _is_grounded_number(
    token: str,
    *,
    preceding: str,
    following: str,
    grounded: dict[str, set[float]],
    chart_unit: str | None,
    label_values: dict[str, float],
) -> bool:
    """Accept a written number that equals or rounds a grounded quantity.

    A percent sign or time unit, attached to the token or as the next word,
    selects the kind the number must ground on; a bare number may ground on
    a count or a time value but never on a percentage. A currency prefix, a
    letter suffix outside the known units, or a unit word the profiler never
    measures rejects the number outright. A number followed by a chart label
    or its outcome word must equal that label's own value.
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
    if (
        not attached
        and (
            bound := _get_bound_label_value(
                preceding=preceding, following=following, label_values=label_values
            )
        )
        is not None
    ):
        return _matches_grounded(value, decimals, {bound})
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
        for clause in re.split(r"[.;!?\n,]+|\s(?:and|but)\s", text)
        if len(clause.strip()) >= 12
    }
    for clause in sorted(clauses, key=len, reverse=True):
        value = re.sub(re.escape(clause), " ", value, flags=re.IGNORECASE)
    return value


def validate_card_copy(value: str, candidate: CandidateFinding) -> None:
    """Validate one card only against the deterministic candidate it explains."""
    _validate_copy_safety(value)
    remaining = _remove_known_labels(value, _get_quantified_labels(candidate))
    claims = _remove_candidate_phrases(value, candidate)
    if _has_negated_outcome(claims) or _negates_candidate_phrase(value, candidate):
        raise ValueError("editor card copy contains a negated outcome claim")
    grounded = _get_grounded_numbers(candidate)
    label_values = _get_label_values(candidate)
    for match in _NUMERIC_TOKEN.finditer(remaining):
        if not _is_grounded_number(
            match.group(0),
            preceding=remaining[: match.start()],
            following=remaining[match.end() :],
            grounded=grounded,
            chart_unit=candidate.data.unit,
            label_values=label_values,
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
