"""Tests for adapter result identity canonicalization helpers."""

from typing import Literal

import pytest
from pydantic import BaseModel, ConfigDict, ValidationError

from kitaru.adapters._result_identity import canonicalize_result_model


class TargetResult(BaseModel):
    """Canonical result model imported by user code."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    status: Literal["completed"]
    final_output: str


class ForeignResult(BaseModel):
    """Same-shaped model from a different module/class identity."""

    model_config = ConfigDict(extra="forbid")

    __module__ = "src.kitaru.adapters.fake._types"

    schema_version: Literal[1] = 1
    status: Literal["completed"]
    final_output: str


def test_canonical_result_instance_is_returned_unchanged() -> None:
    result = TargetResult(status="completed", final_output="hello")

    canonical = canonicalize_result_model(result, TargetResult)

    assert canonical is result


def test_foreign_pydantic_result_is_rebuilt_as_canonical_model() -> None:
    foreign = ForeignResult(status="completed", final_output="hello")

    canonical = canonicalize_result_model(foreign, TargetResult)

    direct_validation_fails = False
    try:
        TargetResult.model_validate(foreign)
    except ValidationError:
        direct_validation_fails = True

    assert direct_validation_fails is True
    assert isinstance(canonical, TargetResult)
    assert not isinstance(canonical, ForeignResult)
    assert canonical.final_output == "hello"


def test_dict_payload_validates_directly() -> None:
    canonical = canonicalize_result_model(
        {"status": "completed", "final_output": "hello"},
        TargetResult,
    )

    assert isinstance(canonical, TargetResult)
    assert canonical.final_output == "hello"


def test_incompatible_payload_raises_validation_error() -> None:
    with pytest.raises(ValidationError):
        canonicalize_result_model(
            {"status": "completed", "final_output": "hello", "extra": "nope"},
            TargetResult,
        )
