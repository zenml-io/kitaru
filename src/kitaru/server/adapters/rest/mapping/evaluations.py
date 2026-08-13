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
"""Evaluation DTO conversions."""

from kitaru.api_models.v1.evaluation import (
    EvaluationBatchCreateRequest,
    EvaluationListParams,
    EvaluationResponse,
    EvaluationResult,
)
from kitaru.api_models.v1.session import SessionEvaluationsRequest
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.adapters.rest.mapping.replay_config import evaluator_config_input
from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationWithEvaluator,
)
from kitaru.server.application.models.evaluation import (
    EvaluationFilter,
    EvaluationMerge,
)
from kitaru.server.application.models.job import EvaluationBatchCreate
from kitaru.server.domain.evaluation import Evaluation


def evaluation_list_params_to_filter(params: EvaluationListParams) -> EvaluationFilter:
    """Convert evaluation list params to the application filter.

    Args:
        params: Evaluation list params.

    Returns:
        Evaluation filter.
    """
    return EvaluationFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def evaluation_batch_create_to_command(
    body: EvaluationBatchCreateRequest,
) -> EvaluationBatchCreate:
    """Convert an evaluation batch create request to its command.

    Args:
        body: Evaluation batch create request.

    Returns:
        Evaluation batch create command.
    """
    return EvaluationBatchCreate(
        input_session_ids=body.input_session_ids,
        evaluators=[evaluator_config_input(config) for config in body.evaluators],
    )


def evaluation_to_response(item: EvaluationWithEvaluator) -> EvaluationResponse:
    """Convert an evaluation paired with its evaluator info to its response DTO.

    Args:
        item: Stored evaluation paired with its evaluator name and version.

    Returns:
        Evaluation response.
    """
    evaluation = item.evaluation
    assert evaluation.created is not None
    assert evaluation.updated is not None
    return EvaluationResponse(
        id=evaluation.id,
        owner_id=evaluation.owner_id,
        evaluator_version_id=evaluation.evaluator_version_id,
        evaluator_name=item.evaluator_name,
        evaluator_version=item.evaluator_version,
        session_id=evaluation.session_id,
        task_id=evaluation.task_id,
        name=evaluation.name,
        data_type=evaluation.data_type,
        score=evaluation.score,
        value=evaluation.value,
        explanation=evaluation.explanation,
        passed=evaluation.passed,
        created=evaluation.created,
        updated=evaluation.updated,
    )


def merged_evaluation_to_response(evaluation: Evaluation) -> EvaluationResponse:
    """Convert a manually merged evaluation to its response DTO.

    A manual evaluation carries no evaluator, so this skips the join
    ``evaluation_to_response`` performs for stored rows.

    Args:
        evaluation: Stored manual evaluation.

    Returns:
        Evaluation response.
    """
    return evaluation_to_response(EvaluationWithEvaluator(evaluation, None, None))


def evaluation_result_to_merge(result: EvaluationResult) -> EvaluationMerge:
    """Convert an evaluation result to its merge command, deriving data_type.

    Args:
        result: Evaluation result from the request.

    Returns:
        Evaluation merge command.
    """
    return EvaluationMerge(
        name=result.name,
        data_type=result.data_type,
        score=result.score,
        value=result.value,
        explanation=result.explanation,
        passed=result.passed,
    )


def session_evaluations_request_to_merges(
    body: SessionEvaluationsRequest,
) -> list[EvaluationMerge]:
    """Convert a session evaluations request to its merge commands.

    Args:
        body: Session evaluations request.

    Returns:
        Evaluation merge commands in request order.
    """
    return [evaluation_result_to_merge(result) for result in body.evaluations]
