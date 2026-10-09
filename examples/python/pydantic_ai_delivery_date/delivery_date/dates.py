"""Explicit dates supported by the fixture checks and customer validation."""

import re

_MONTH_NAMES = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)
_MONTH_NUMBERS = {
    name.lower(): number
    for number, full_name in enumerate(_MONTH_NAMES, start=1)
    for name in (full_name, full_name[:3])
} | {"sept": 9}
_MONTH_PATTERN = "(?:" + "|".join(_MONTH_NUMBERS) + r")\.?"
_NAMED_DATE_PATTERNS = (
    re.compile(
        rf"\b(?P<month>{_MONTH_PATTERN})\s+(?P<day>\d{{1,2}})(?:st|[nr]d|th)?(?:,\s*|\s+)(?P<year>\d{{4}})\b",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\b(?P<day>\d{{1,2}})(?:st|[nr]d|th)?\s+(?P<month>{_MONTH_PATTERN})(?:,\s*|\s+)(?P<year>\d{{4}})\b",
        re.IGNORECASE,
    ),
)


def extract_explicit_dates(text: str) -> set[str]:
    """Normalize ISO and English month-name dates with explicit days and years.

    Relative dates, spelled-out day numbers, and ambiguous numeric formats are
    excluded. Invalid calendar dates remain unmatched evidence, so the fixture
    checker can reject them rather than silently ignoring them.
    """
    dates = set(re.findall(r"\b\d{4}-\d{2}-\d{2}\b", text))
    for pattern in _NAMED_DATE_PATTERNS:
        for match in pattern.finditer(text):
            month = _MONTH_NUMBERS[match["month"].lower().rstrip(".")]
            dates.add(f"{int(match['year']):04d}-{month:02d}-{int(match['day']):02d}")
    return dates
