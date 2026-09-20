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
"""Judge evaluator entrypoint."""

import os
from typing import Any, cast

from typesafe_sdk import (
    Question,
    TypeSafeAuthenticationError,
    TypeSafeBadRequestError,
    TypeSafeClient,
    TypeSafePermissionDeniedError,
)
from typesafe_sdk.constants import API_KEY_ENV

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.task.evaluator import SessionView
from kitaru_typesafe_evaluator.params import JudgeParams
from kitaru_typesafe_evaluator.results import build_result, build_unavailable_results
from kitaru_typesafe_evaluator.state import build_state, check_params_against_view

_TOO_LARGE = "max_tokens_exceeded"
_KEY_HELP = (
    f"Set {API_KEY_ENV} in the worker environment, or store it once with "
    "`kitaru connection create NAME --evaluator YOUR_EVALUATOR --default`."
)


def _build_client() -> TypeSafeClient:
    """Create the TypeSafe client from the task environment."""
    if not os.environ.get(API_KEY_ENV):
        raise RuntimeError(f"{API_KEY_ENV} is not set. {_KEY_HELP}")
    return TypeSafeClient()


def _is_too_large(error: TypeSafeBadRequestError) -> bool:
    """Check for jev's machine-readable size-limit tag."""
    detail = error.body.get("detail") if isinstance(error.body, dict) else None
    return isinstance(detail, dict) and detail.get("error_type") == _TOO_LARGE


def judge(session: SessionView, **params: Any) -> list[EvaluationResult]:
    """Ask jev the configured questions about one session."""
    config = JudgeParams.model_validate(params)
    check_params_against_view(config)
    state = build_state(session, config)
    # to_wire() dumps a validated pydantic model, so its dict shape always
    # matches one of the SDK's question TypedDicts; the cast just tells ty.
    questions = cast(
        "dict[str, Question]",
        {name: question.to_wire() for name, question in config.questions.items()},
    )
    try:
        with _build_client() as client:
            response = client.system_one(
                state=state, questions=questions, model=config.model
            )
    except TypeSafeBadRequestError as error:
        # Only the size tag describes this one session. Any other 400 is a
        # bug in the params or the request and would repeat on every session.
        if not _is_too_large(error):
            raise
        # A run already on the 'outcome' view has no smaller built-in view left
        # to fall back to, so only suggest narrowing it further with include.
        hint = (
            "Use the 'outcome' view or narrow it with include."
            if config.state == "full"
            else "Narrow it with include."
        )
        return build_unavailable_results(
            config.questions,
            f"The '{config.state}' state of this session is over jev's input "
            f"limit. {hint}",
        )
    except (TypeSafeAuthenticationError, TypeSafePermissionDeniedError) as error:
        raise RuntimeError(f"TypeSafe rejected {API_KEY_ENV}. {_KEY_HELP}") from error
    missing = [name for name in config.questions if name not in response.answers]
    if missing:
        raise RuntimeError(f"jev returned no answer for questions {missing}")
    return [
        build_result(name, question, response.answers[name], response.model)
        for name, question in config.questions.items()
    ]
