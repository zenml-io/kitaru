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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for TypeSafe judge params validation."""

import pytest
from pydantic import ValidationError

from kitaru_typesafe_evaluator.params import JudgeParams, NoulQuestion

NOUL = {"type": "noul", "instructions": "Is `final_answer` polite?"}


def test_defaults() -> None:
    """Default to the outcome view, no include, no verdict, 0.8 band."""
    params = JudgeParams.model_validate({"questions": {"polite": NOUL}})
    question = params.questions["polite"]
    assert isinstance(question, NoulQuestion)
    assert params.state == "outcome"
    assert params.include is None
    assert params.model is None
    assert question.pass_when is None
    assert question.decisive_at == 0.8


def test_to_wire_drops_kitaru_only_fields() -> None:
    """Send jev only the fields it understands."""
    params = JudgeParams.model_validate(
        {"questions": {"polite": NOUL | {"pass_when": "yes", "decisive_at": 0.9}}}
    )
    assert params.questions["polite"].to_wire() == NOUL


def test_choice_to_wire_keeps_criteria() -> None:
    question = {
        "type": "choice",
        "instructions": "Which?",
        "criteria": {"a": "x", "b": None},
    }
    params = JudgeParams.model_validate(
        {"questions": {"kind": question | {"pass_when": ["a"]}}}
    )
    assert params.questions["kind"].to_wire() == question


@pytest.mark.parametrize(
    "bad",
    [
        {},
        {"questions": {}},
        {"questions": {"q": {"type": "essay", "instructions": "x"}}},
        {"questions": {"q": {"type": "noul"}}},
        {"questions": {"q": {"type": "noul", "instructions": ""}}},
        {"questions": {"q": NOUL | {"decisive_at": 0.5}}},
        {"questions": {"q": NOUL | {"decisive_at": 1.01}}},
        {"questions": {"q": NOUL | {"pass_when": "maybe"}}},
        {"questions": {"q": NOUL | {"typo_field": 1}}},
        {"questions": {"bad name!": NOUL}},
        {"questions": {"q": NOUL}, "state": "everything"},
        {"questions": {"q": NOUL}, "typo": 1},
        {
            "questions": {
                "q": {
                    "type": "choice",
                    "instructions": "x",
                    "criteria": {"only": "one"},
                }
            }
        },
        {
            "questions": {
                "q": {
                    "type": "choice",
                    "instructions": "x",
                    "criteria": {"a": "", "b": ""},
                    "pass_when": ["c"],
                }
            }
        },
        {
            "questions": {
                "q": {
                    "type": "choice",
                    "instructions": "x",
                    "criteria": {"a": "", "b": ""},
                    "pass_when": [],
                }
            }
        },
        {
            "questions": {
                "q": {"type": "score", "instructions": "x", "criteria": ["one"]}
            }
        },
        {
            "questions": {
                "q": {
                    "type": "score",
                    "instructions": "x",
                    "criteria": [str(i) for i in range(11)],
                }
            }
        },
        {
            "questions": {
                "q": {
                    "type": "score",
                    "instructions": "x",
                    "criteria": ["a", "b"],
                    "pass_when": "yes",
                }
            }
        },
    ],
)
def test_rejects_invalid_params(bad: dict) -> None:
    """Refuse a bad params block before anything is sent."""
    with pytest.raises(ValidationError):
        JudgeParams.model_validate(bad)
