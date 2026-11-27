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
"""Actionable CLI parsing and validation for JSON filter expressions."""

import re
from collections.abc import Sequence
from typing import Any, Literal, TypeVar

from pydantic import TypeAdapter, ValidationError
from pydantic_core import from_json

from kitaru.api_models.v1.agent import AgentListParams
from kitaru.api_models.v1.analyzer import AnalyzerListParams
from kitaru.api_models.v1.annotation import AnnotationListParams
from kitaru.api_models.v1.base import ListParams
from kitaru.api_models.v1.cohort import CohortListParams
from kitaru.api_models.v1.evaluation import EvaluationListParams
from kitaru.api_models.v1.evaluator import EvaluatorListParams
from kitaru.api_models.v1.experiment import ExperimentListParams
from kitaru.api_models.v1.experiment_run import ExperimentRunListParams
from kitaru.api_models.v1.filter import Filter, FilterOp
from kitaru.api_models.v1.importer import ImporterListParams
from kitaru.api_models.v1.imports import ImportListParams
from kitaru.api_models.v1.insight import InsightListParams
from kitaru.api_models.v1.investigation import InvestigationListParams
from kitaru.api_models.v1.replay import ReplayListParams
from kitaru.api_models.v1.session import SessionListParams
from kitaru.cli.output import CLIError

ListParamsT = TypeVar("ListParamsT", bound=ListParams)

_FIELD_NAME_PATTERN = r"^[a-z][a-z0-9_]*$"
_FIELD_NAME_RE = re.compile(_FIELD_NAME_PATTERN)
_COMBINATOR_KEYS = ("and", "or", "not")
_CONDITION_KEYS = frozenset({"field", "op", "value"})
_ALLOWED_OPS = tuple(op.value for op in FilterOp)
_ALLOWED_OPS_SET = frozenset(_ALLOWED_OPS)
_ALLOWED_OPS_TEXT = ", ".join(_ALLOWED_OPS)
_FILTER_ADAPTER: TypeAdapter[Filter] = TypeAdapter(Filter)


def parse_filter_option(raw: str, *, option: str = "--filter") -> Filter:
    """Parse and validate a CLI JSON filter string with shape-aware diagnostics.

    Args:
        raw: JSON string passed to a CLI filter option.
        option: Option name included in error messages.

    Raises:
        CLIError: ``raw`` is invalid JSON or violates the filter grammar.

    Returns:
        Validated API filter expression.
    """
    try:
        parsed = from_json(raw)
    except ValueError as error:
        raise CLIError(
            "invalid_arguments", f"{option} is not valid JSON: {error}"
        ) from None
    _validate_filter_node(parsed, path="", option=option)
    try:
        return _FILTER_ADAPTER.validate_python(parsed)
    except ValidationError:
        raise CLIError(
            "invalid_arguments", f"{option} must be a valid JSON filter expression."
        ) from None


def _validate_filter_node(node: Any, *, path: str, option: str) -> None:
    """Validate one parsed filter node recursively against its intended shape."""
    prefix = f"{option} ({path})" if path else option
    if not isinstance(node, dict):
        raise CLIError(
            "invalid_arguments",
            f"{prefix}: expected a JSON object, got {_get_json_type_name(node)}.",
        )
    if not node:
        raise CLIError(
            "invalid_arguments",
            f"{prefix}: filter object cannot be empty. Provide a condition with "
            "'field' and 'op', or a combinator ('and', 'or', 'not').",
        )

    combinators = [key for key in _COMBINATOR_KEYS if key in node]
    if len(combinators) > 1:
        names = ", ".join(repr(key) for key in combinators)
        raise CLIError(
            "invalid_arguments",
            f"{prefix}: filter object cannot combine {names}; use one of 'and', "
            "'or', 'not', or a condition.",
        )
    if len(combinators) == 1:
        _validate_combinator(node, combinators[0], path=path, option=option)
        return

    _validate_condition(node, prefix=prefix)


def _validate_combinator(
    node: dict[str, Any], combinator: str, *, path: str, option: str
) -> None:
    """Validate an ``and``, ``or``, or ``not`` filter node."""
    prefix = f"{option} ({path})" if path else option
    extra = sorted(key for key in node if key != combinator)
    if extra:
        names = ", ".join(repr(key) for key in extra)
        raise CLIError(
            "invalid_arguments",
            f"{prefix}: '{combinator}' filter does not allow extra key(s): {names}.",
        )
    if combinator == "not":
        child_path = f"{path}.not" if path else "not"
        _validate_filter_node(node["not"], path=child_path, option=option)
        return

    operands = node[combinator]
    if not isinstance(operands, list):
        raise CLIError(
            "invalid_arguments",
            f"{prefix}: '{combinator}' must be a non-empty JSON array of filters.",
        )
    if not operands:
        raise CLIError(
            "invalid_arguments",
            f"{prefix}: '{combinator}' must contain at least one filter.",
        )
    for index, operand in enumerate(operands):
        child_path = (
            f"{path}.{combinator}[{index}]" if path else f"{combinator}[{index}]"
        )
        _validate_filter_node(operand, path=child_path, option=option)


def _validate_condition(node: dict[str, Any], *, prefix: str) -> None:
    """Validate a leaf ``field``/``op``/``value`` condition."""
    if not any(key in node for key in _CONDITION_KEYS):
        unknown = ", ".join(repr(key) for key in sorted(node))
        raise CLIError(
            "invalid_arguments",
            f"{prefix}: unrecognized filter key(s): {unknown}. Expected a condition "
            "('field', 'op', optional 'value') or a combinator ('and', 'or', 'not').",
        )
    extra = sorted(key for key in node if key not in _CONDITION_KEYS)
    if extra:
        names = ", ".join(repr(key) for key in extra)
        raise CLIError(
            "invalid_arguments",
            f"{prefix}: unknown key(s) in filter condition: {names}. "
            "Allowed keys: 'field', 'op', 'value'.",
        )
    if "field" not in node:
        raise CLIError(
            "invalid_arguments",
            f"{prefix}: filter condition is missing required key 'field'.",
        )
    field = node["field"]
    if not isinstance(field, str):
        raise CLIError(
            "invalid_arguments",
            f"{prefix}: 'field' must be a string, got {_get_json_type_name(field)}.",
        )
    if not _FIELD_NAME_RE.fullmatch(field):
        raise CLIError(
            "invalid_arguments",
            f"{prefix}: invalid field name {field!r}. Field names must match "
            f"{_FIELD_NAME_PATTERN}.",
        )
    if "op" not in node:
        raise CLIError(
            "invalid_arguments",
            f"{prefix}: filter condition is missing required key 'op'.",
        )
    op = node["op"]
    if not isinstance(op, str):
        raise CLIError(
            "invalid_arguments",
            f"{prefix}: 'op' must be a string, got {_get_json_type_name(op)}. "
            f"Use one of: {_ALLOWED_OPS_TEXT}.",
        )
    if op not in _ALLOWED_OPS_SET:
        raise CLIError(
            "invalid_arguments",
            f"{prefix}: unknown operator {op!r}. Use one of: {_ALLOWED_OPS_TEXT}.",
        )


def _get_json_type_name(value: Any) -> str:
    """Return the JSON type name for a decoded Python value."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def list_params(
    kind: Literal[
        "agent",
        "analyzer",
        "annotation",
        "cohort",
        "evaluation",
        "evaluator",
        "experiment",
        "experiment_run",
        "import",
        "importer",
        "insight",
        "investigation",
        "replay",
        "session",
    ],
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> ListParams:
    """Build a kind-specific list request."""
    request_type = {
        "agent": AgentListParams,
        "analyzer": AnalyzerListParams,
        "annotation": AnnotationListParams,
        "cohort": CohortListParams,
        "evaluation": EvaluationListParams,
        "evaluator": EvaluatorListParams,
        "experiment": ExperimentListParams,
        "experiment_run": ExperimentRunListParams,
        "import": ImportListParams,
        "importer": ImporterListParams,
        "insight": InsightListParams,
        "investigation": InvestigationListParams,
        "replay": ReplayListParams,
        "session": SessionListParams,
    }[kind]
    return build_list_params(
        request_type,
        size=size,
        cursor=cursor,
        sort=sort,
        filter=filter,
    )


_DEFAULT_SORT_FIELDS: tuple[str, ...] = ("created",)
_SORT_FIELDS_BY_REQUEST_TYPE: dict[type[ListParams], tuple[str, ...]] = {
    SessionListParams: ("created", "started_at", "number"),
}


def version_list_params(*, size: int, cursor: str | None, sort: str) -> ListParams:
    """Build shared version-list parameters."""
    _validate_sort(sort)
    try:
        return ListParams(size=size, cursor=cursor, sort=sort)
    except ValidationError as error:
        raise list_validation_error(error) from None


def build_list_params(
    request_type: type[ListParamsT],
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> ListParamsT:
    """Build filterable list params against the shared server sort contract."""
    allowed = _SORT_FIELDS_BY_REQUEST_TYPE.get(request_type, _DEFAULT_SORT_FIELDS)
    _validate_sort(sort, allowed)
    parsed_filter = parse_filter_option(filter) if filter is not None else None
    try:
        return request_type.model_validate(
            {
                "size": size,
                "cursor": cursor,
                "sort": sort,
                "filter": parsed_filter,
            }
        )
    except ValidationError as error:
        raise list_validation_error(error, allowed) from None


def _get_sort_error_message(allowed_fields: Sequence[str]) -> str:
    """Build a concise CLI error naming the allowed sort fields."""
    if tuple(allowed_fields) == _DEFAULT_SORT_FIELDS:
        return "--sort must be created:asc or created:desc."
    *leading, last = allowed_fields
    joined = f"{', '.join(leading)}, or {last}" if leading else last
    return f"--sort must use {joined} with :asc or :desc."


def _validate_sort(
    sort: str, allowed_fields: Sequence[str] = _DEFAULT_SORT_FIELDS
) -> None:
    """Validate a CLI sort value against the allowed field names."""
    field, _, direction = sort.partition(":")
    if direction not in {"asc", "desc"} or field not in allowed_fields:
        raise CLIError(
            "invalid_arguments",
            _get_sort_error_message(allowed_fields),
            hint="Use --filter to narrow results without changing their order.",
        )


def list_validation_error(
    error: ValidationError,
    allowed_fields: Sequence[str] = _DEFAULT_SORT_FIELDS,
) -> CLIError:
    """Convert list-model validation into a concise option-named error."""
    issue = error.errors(include_url=False)[0]
    field = str(issue["loc"][0]) if issue["loc"] else "parameters"
    if field == "filter":
        message = "--filter must be a valid JSON filter expression."
    elif field == "size":
        message = "--size must be between 1 and 1000."
    elif field == "sort":
        message = _get_sort_error_message(allowed_fields)
    else:
        message = f"Invalid --{field.replace('_', '-')}: {issue['msg']}."
    return CLIError(
        "invalid_arguments",
        message,
        hint="--limit is an alias for --size; supply only one."
        if field == "size"
        else None,
    )
