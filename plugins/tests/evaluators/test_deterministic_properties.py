#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Property tests for deterministic evaluator pointer and decimal helpers."""

from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from kitaru_evaluator import deterministic as evaluators

_POINTER_TOKEN = st.text(max_size=20)
_JSON_VALUE = st.recursive(
    st.none()
    | st.booleans()
    | st.integers(min_value=-1_000, max_value=1_000)
    | st.text(max_size=20),
    lambda children: (
        st.lists(children, max_size=3)
        | st.dictionaries(st.text(max_size=8), children, max_size=3)
    ),
    max_leaves=8,
)


def _encode_pointer_part(part: str) -> str:
    """Encode one RFC 6901 reference token independently."""
    return part.replace("~", "~0").replace("/", "~1")


def _encode_pointer(parts: list[str]) -> str:
    """Encode reference tokens as an RFC 6901 JSON Pointer."""
    if not parts:
        return ""
    return "/" + "/".join(_encode_pointer_part(part) for part in parts)


@st.composite
def _build_planted_pointer(draw: st.DrawFn) -> tuple[Any, str, Any]:
    """Build a JSON value with a path to a generated leaf."""
    leaf = draw(_JSON_VALUE)
    steps = draw(
        st.lists(
            st.one_of(
                st.tuples(st.just("key"), _POINTER_TOKEN),
                st.tuples(st.just("index"), st.integers(min_value=0, max_value=5)),
            ),
            min_size=1,
            max_size=8,
        )
    )
    document = leaf
    for kind, value in reversed(steps):
        if kind == "key":
            assert isinstance(value, str)
            document = {value: document}
        else:
            assert isinstance(value, int)
            items: list[Any] = [None] * (value + 1)
            items[value] = document
            document = items
    return document, _encode_pointer([str(value) for _, value in steps]), leaf


@st.composite
def _build_finite_decimal(draw: st.DrawFn) -> Decimal:
    """Build a bounded finite decimal without parsing generated text."""
    coefficient = draw(st.integers(min_value=0, max_value=10**18 - 1))
    sign = draw(st.integers(min_value=0, max_value=1))
    exponent = draw(st.integers(min_value=-4_500, max_value=4_500))
    digits = tuple(int(digit) for digit in str(coefficient))
    return Decimal((sign, digits, exponent))


@given(parts=st.lists(_POINTER_TOKEN, max_size=8))
def test_pointer_tokens_round_trip(parts: list[str]) -> None:
    """Decode arbitrary escaped tokens, including root and empty keys."""
    assert evaluators._decode_pointer_parts(_encode_pointer(parts)) == parts


@given(case=_build_planted_pointer())
def test_pointer_resolves_generated_planted_value(
    case: tuple[Any, str, Any],
) -> None:
    """Resolve a generated path to the independently planted value."""
    document, pointer, leaf = case
    found, value = evaluators._resolve_pointer(document, pointer)
    assert found is True
    assert value == leaf


@given(case=_build_planted_pointer())
def test_pointer_reports_generated_missing_child(
    case: tuple[Any, str, Any],
) -> None:
    """Report a missing key, index, or scalar child without raising."""
    document, pointer, leaf = case
    if isinstance(leaf, dict):
        missing_part = "missing"
        while missing_part in leaf:
            missing_part += "_"
    elif isinstance(leaf, list):
        missing_part = str(len(leaf))
    else:
        missing_part = "child"

    missing_pointer = f"{pointer}/{_encode_pointer_part(missing_part)}"
    assert evaluators._resolve_pointer(document, missing_pointer) == (False, None)


@pytest.mark.parametrize("pointer", ["answer", "/a~", "/a~2b", "/a~xb"])
def test_pointer_rejects_invalid_syntax(pointer: str) -> None:
    """Reject malformed paths and escapes as configuration errors."""
    with pytest.raises(ValueError, match="JSON Pointer"):
        evaluators._resolve_pointer({}, pointer)


@pytest.mark.parametrize("part", ["00", "01", "-1", "+1", "\u0661", "\uff11", "2"])
def test_pointer_treats_invalid_or_missing_list_indexes_as_missing(part: str) -> None:
    """Keep valid pointer syntax distinct from list-index resolution."""
    assert evaluators._resolve_pointer(["zero", "one"], f"/{part}") == (
        False,
        None,
    )
    assert evaluators._resolve_pointer({part: "value"}, f"/{part}") == (
        True,
        "value",
    )


def test_pointer_decodes_escape_sequences_once() -> None:
    """Do not decode a tilde produced by an earlier escape."""
    assert evaluators._decode_pointer_parts("/~01") == ["~1"]


@given(
    values=st.lists(_build_finite_decimal(), max_size=20),
    precision=st.integers(min_value=1, max_value=50),
    data=st.data(),
)
def test_decimal_sum_matches_exact_rational_reference(
    values: list[Decimal], precision: int, data: st.DataObject
) -> None:
    """Sum decimals exactly under ordering and ambient-precision changes."""
    expected = sum((Fraction(value) for value in values), start=Fraction())
    reordered = data.draw(st.permutations(values))

    with localcontext() as context:
        context.prec = precision
        actual = evaluators.sum_decimals(values)
        reversed_sum = evaluators.sum_decimals(list(reversed(values)))
        reordered_sum = evaluators.sum_decimals(reordered)

    assert Fraction(actual) == expected
    assert Fraction(reversed_sum) == expected
    assert Fraction(reordered_sum) == expected


def test_decimal_sum_empty_cancellation_and_signed_zero() -> None:
    """Characterize empty input and the normalized representation of zero."""
    assert evaluators.sum_decimals([]) == Decimal(0)

    cancelled = evaluators.sum_decimals([Decimal("10.00"), Decimal("-10")])
    assert cancelled.as_tuple() == Decimal("0.00").as_tuple()

    signed_zero = evaluators.sum_decimals([Decimal("-0E-20")])
    assert signed_zero.as_tuple() == Decimal("0E-20").as_tuple()


def test_decimal_sum_handles_wide_finite_exponent_spread() -> None:
    """Sum finite values beyond Python's integer-string safety threshold."""
    values = [Decimal("1E+4300"), Decimal("1")]
    assert Fraction(evaluators.sum_decimals(values)) == sum(
        (Fraction(value) for value in values), start=Fraction()
    )


@pytest.mark.parametrize("value", ["NaN", "sNaN", "Infinity", "-Infinity"])
def test_decimal_sum_rejects_nonfinite_values(value: str) -> None:
    """Reject every nonfinite decimal even beside valid values."""
    with pytest.raises(ValueError, match="number must be finite"):
        evaluators.sum_decimals([Decimal("1.25"), Decimal(value)])
