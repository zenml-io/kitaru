#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Small rate, interval and formatting helpers for model-swap reports."""

import math
from decimal import Decimal


def compute_weighted_mean(values: list[tuple[Decimal, Decimal]]) -> Decimal | None:
    """Mean of `(value, weight)` pairs, or None without any weight."""
    total = sum((w for _, w in values), Decimal(0))
    if not total:
        return None
    return sum((v * w for v, w in values), Decimal(0)) / total


def compute_rate(part: int, whole: int) -> float | None:
    """Return `part / whole` rounded to four places, or None for an empty whole."""
    return round(part / whole, 4) if whole else None


def compute_interval(part: int, whole: int, z: float = 1.96) -> list[float] | None:
    """Wilson 95% interval for a rate, so small samples read as uncertain."""
    if not whole:
        return None
    p = part / whole
    center = (p + z * z / (2 * whole)) / (1 + z * z / whole)
    half = (z * math.sqrt(p * (1 - p) / whole + z * z / (4 * whole * whole))) / (
        1 + z * z / whole
    )
    return [round(max(0.0, center - half), 4), round(min(1.0, center + half), 4)]


def format_yes_no(value: bool | None) -> str:
    """Format a flag that may be unknown."""
    return "unknown" if value is None else ("yes" if value else "no")


def format_pct(rate: float | None) -> str:
    """Format a rate as a whole percentage, or `n/a`."""
    return "n/a" if rate is None else f"{rate:.0%}"


def format_usd(value: Decimal | None) -> str | None:
    """Format a dollar amount to eight decimal places."""
    return None if value is None else format(value.quantize(Decimal("1e-8")), "f")


def format_decimal(value: Decimal | None) -> str | None:
    """Format a decimal as a plain string."""
    return None if value is None else str(value)


def round_rate(value: float) -> float:
    """Round a rate to three places."""
    return round(value, 3)


def compute_percentile(values: list[float], fraction: float) -> float:
    """Return the nearest-rank percentile of sorted values."""
    return values[min(len(values) - 1, int(fraction * len(values)))]
