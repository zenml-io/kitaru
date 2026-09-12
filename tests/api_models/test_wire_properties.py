#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Properties of selected API DTO wire representations."""

import json
import uuid
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, NamedTuple

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import BaseModel, ValidationError
from pydantic_core import PydanticSerializationError

from kitaru.api_models.v1.evaluation import (
    EvaluationDataType,
    EvaluationResponse,
    EvaluationResult,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.replay import (
    BaselineEvaluationMode,
    ReplayCreateRequest,
    ToolLookupRequest,
)
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionListParams,
    SessionOrigin,
    SessionResponse,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeCreateRequest,
    SessionNodeResponse,
)


class WireCase(NamedTuple):
    """One selected DTO and the strategy for valid constructor inputs."""

    model_type: type[BaseModel]
    inputs: st.SearchStrategy[tuple[dict[str, Any], dict[str, Any]]]
    exclude_unset: bool


_SAFE_CHARACTERS = st.characters(
    exclude_categories=("Cs",),
    exclude_characters="\x00\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e\x0f"
    "\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f",
)
_SAFE_TEXT = st.text(alphabet=_SAFE_CHARACTERS, max_size=24)
_NONEMPTY_TEXT = st.text(alphabet=_SAFE_CHARACTERS, min_size=1, max_size=24)
_JSON_SCALARS = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**31), max_value=2**31 - 1),
    st.floats(
        min_value=-1e12,
        max_value=1e12,
        allow_nan=False,
        allow_infinity=False,
        allow_subnormal=False,
    ),
    _SAFE_TEXT,
    st.sampled_from(["\t", "\n", "\r"]),
)
JSON_VALUES = st.recursive(
    _JSON_SCALARS,
    lambda children: st.one_of(
        st.lists(children, max_size=4),
        st.dictionaries(_SAFE_TEXT, children, max_size=4),
    ),
    max_leaves=12,
)
_AWARE_DATETIMES = st.datetimes(
    min_value=datetime(2000, 1, 1),
    max_value=datetime(2040, 1, 1),
    timezones=st.sampled_from(
        [UTC, timezone(timedelta(hours=-7)), timezone(timedelta(hours=5, minutes=30))]
    ),
)
_DECIMALS = st.decimals(
    min_value=Decimal("-1000000"),
    max_value=Decimal("1000000"),
    places=6,
    allow_nan=False,
    allow_infinity=False,
)
_OPTIONAL_UUIDS = st.one_of(st.none(), st.uuids())


def _wire_value(value: Any) -> Any:
    """Project a generated Python value to its documented JSON value."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        encoded = value.isoformat()
        return (
            encoded.removesuffix("+00:00") + "Z"
            if encoded.endswith("+00:00")
            else encoded
        )
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, list):
        return [_wire_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _wire_value(item) for key, item in value.items()}
    return value


def _filter_keys(value: dict[str, Any]) -> set[str]:
    """Collect keys from filter nodes without inspecting condition values."""
    keys = set(value)
    if "and" in value:
        return keys.union(*(_filter_keys(child) for child in value["and"]))
    if "or" in value:
        return keys.union(*(_filter_keys(child) for child in value["or"]))
    if "not" in value:
        return keys | _filter_keys(value["not"])
    return keys


@st.composite
def _replay_inputs(
    draw: st.DrawFn,
) -> tuple[dict[str, Any], dict[str, Any]]:
    controls = draw(st.sampled_from(["legacy", "mode", "neither"]))
    policy = draw(
        st.one_of(
            st.just({"default": {"type": "passthrough"}, "tools": {}}),
            st.builds(
                lambda scope, on_miss: {
                    "default": {
                        "type": "history",
                        "scope": scope,
                        "on_miss": on_miss,
                    },
                    "tools": {},
                },
                st.sampled_from(["baseline", "cohort_version", "agent"]),
                st.sampled_from(["fail", "passthrough", "error_result"]),
            ),
        )
    )
    override = draw(
        st.one_of(
            st.none(),
            st.builds(
                lambda value: {"model_params": {"temperature": value}}, JSON_VALUES
            ),
        )
    )
    kwargs: dict[str, Any] = {
        "baseline_session_id": draw(st.uuids()),
        "evaluators": [{"evaluator": draw(_NONEMPTY_TEXT)}],
        "override": override,
        "tool_policy": policy,
    }
    if controls == "legacy":
        kwargs["evaluate_baselines"] = draw(st.booleans())
    elif controls == "mode":
        kwargs["baseline_evaluation_mode"] = draw(
            st.sampled_from(list(BaselineEvaluationMode))
        )
    return kwargs, _wire_value(kwargs)


@st.composite
def _tool_lookup_inputs(
    draw: st.DrawFn,
) -> tuple[dict[str, Any], dict[str, Any]]:
    kwargs: dict[str, Any] = {
        "tool_name": draw(_SAFE_TEXT),
        "cache_key": draw(st.binary(min_size=32, max_size=32)).hex(),
    }
    if draw(st.booleans()):
        kwargs["occurrence"] = draw(st.one_of(st.none(), st.integers(0, 1000)))
    return kwargs, _wire_value(kwargs)


@st.composite
def _evaluation_result_inputs(
    draw: st.DrawFn,
) -> tuple[dict[str, Any], dict[str, Any]]:
    kind = draw(st.sampled_from(list(EvaluationDataType)))
    kwargs: dict[str, Any] = {
        "name": draw(
            st.from_regex(
                r"[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,18}[A-Za-z0-9])?",
                fullmatch=True,
            )
        )
    }
    if kind is EvaluationDataType.BOOL:
        kwargs["score"] = draw(st.booleans())
    elif kind is EvaluationDataType.STR:
        kwargs["value"] = draw(_SAFE_TEXT)
    elif kind is EvaluationDataType.CATEGORICAL:
        kwargs["score"] = draw(
            st.floats(-1000, 1000, allow_nan=False, allow_infinity=False)
        )
        kwargs["value"] = draw(_SAFE_TEXT)
    else:
        kwargs["score"] = draw(
            st.floats(-1000, 1000, allow_nan=False, allow_infinity=False)
        )
        kwargs.update(
            draw(
                st.fixed_dictionaries(
                    {},
                    optional={
                        "min_score": st.floats(
                            -1000, 1000, allow_nan=False, allow_infinity=False
                        ),
                        "max_score": st.floats(
                            -1000, 1000, allow_nan=False, allow_infinity=False
                        ),
                        "target_score": st.floats(
                            -1000, 1000, allow_nan=False, allow_infinity=False
                        ),
                    },
                )
            )
        )
    return kwargs, _wire_value(kwargs)


@st.composite
def _evaluation_response_inputs(
    draw: st.DrawFn,
) -> tuple[dict[str, Any], dict[str, Any]]:
    result_kwargs, _ = draw(_evaluation_result_inputs())
    result = EvaluationResult(**result_kwargs)
    created = draw(_AWARE_DATETIMES)
    kwargs: dict[str, Any] = {
        "id": draw(st.uuids()),
        "owner_id": draw(st.uuids()),
        "created": created,
        "updated": draw(_AWARE_DATETIMES),
        "evaluator_version_id": draw(_OPTIONAL_UUIDS),
        "evaluator_name": draw(st.one_of(st.none(), _SAFE_TEXT)),
        "evaluator_version": draw(st.one_of(st.none(), st.integers(0, 1000))),
        "evaluator_params": draw(
            st.one_of(st.none(), st.dictionaries(_SAFE_TEXT, JSON_VALUES, max_size=3))
        ),
        "session_id": draw(st.uuids()),
        "task_id": draw(_OPTIONAL_UUIDS),
        "name": result.name,
        "data_type": result.data_type,
        "score": result.score,
        "value": result.value,
        "explanation": draw(st.one_of(st.none(), _SAFE_TEXT)),
        "passed": draw(st.one_of(st.none(), st.booleans())),
        "min_score": result.min_score,
        "max_score": result.max_score,
        "target_score": result.target_score,
    }
    return kwargs, _wire_value(kwargs)


@st.composite
def _session_create_inputs(
    draw: st.DrawFn,
) -> tuple[dict[str, Any], dict[str, Any]]:
    kwargs: dict[str, Any] = {
        "origin": draw(st.sampled_from(list(SessionOrigin))),
        "inputs": draw(JSON_VALUES),
        "outputs": draw(JSON_VALUES),
        "metadata": draw(st.dictionaries(_SAFE_TEXT, JSON_VALUES, max_size=3)),
    }
    kwargs.update(
        draw(
            st.fixed_dictionaries(
                {},
                optional={
                    "agent_id": _OPTIONAL_UUIDS,
                    "status": st.one_of(
                        st.none(), st.sampled_from(list(SessionStatus))
                    ),
                    "started_at": st.one_of(st.none(), _AWARE_DATETIMES),
                    "ended_at": st.one_of(st.none(), _AWARE_DATETIMES),
                    "name": st.one_of(st.none(), _SAFE_TEXT),
                },
            )
        )
    )
    return kwargs, _wire_value(kwargs)


@st.composite
def _session_update_inputs(
    draw: st.DrawFn,
) -> tuple[dict[str, Any], dict[str, Any]]:
    kwargs = draw(
        st.fixed_dictionaries(
            {},
            optional={
                "status": st.one_of(st.none(), st.sampled_from(list(SessionStatus))),
                "outputs": JSON_VALUES,
                "error": st.one_of(st.none(), _SAFE_TEXT),
                "ended_at": st.one_of(st.none(), _AWARE_DATETIMES),
                "name": st.one_of(st.none(), _SAFE_TEXT),
                "metadata": st.one_of(
                    st.none(), st.dictionaries(_SAFE_TEXT, JSON_VALUES, max_size=3)
                ),
            },
        )
    )
    return kwargs, _wire_value(kwargs)


@st.composite
def _session_response_inputs(
    draw: st.DrawFn,
) -> tuple[dict[str, Any], dict[str, Any]]:
    kwargs: dict[str, Any] = {
        "id": draw(st.uuids()),
        "owner_id": draw(st.uuids()),
        "created": draw(_AWARE_DATETIMES),
        "updated": draw(_AWARE_DATETIMES),
        "agent_id": draw(st.uuids()),
        "number": draw(st.integers(0, 100000)),
        "agent_version_id": draw(_OPTIONAL_UUIDS),
        "task_id": draw(_OPTIONAL_UUIDS),
        "import_id": draw(_OPTIONAL_UUIDS),
        "origin": draw(st.sampled_from(list(SessionOrigin))),
        "status": draw(st.sampled_from(list(SessionStatus))),
        "name": draw(st.one_of(st.none(), _SAFE_TEXT)),
        "error": draw(st.one_of(st.none(), _SAFE_TEXT)),
        "started_at": draw(st.one_of(st.none(), _AWARE_DATETIMES)),
        "ended_at": draw(st.one_of(st.none(), _AWARE_DATETIMES)),
        "external_id": draw(st.one_of(st.none(), _SAFE_TEXT)),
        "metadata": draw(st.dictionaries(_SAFE_TEXT, JSON_VALUES, max_size=3)),
        "imported_from": draw(st.one_of(st.none(), _SAFE_TEXT)),
        "framework": draw(st.one_of(st.none(), _SAFE_TEXT)),
        "adapter_version": draw(st.one_of(st.none(), _SAFE_TEXT)),
        "cost": draw(st.one_of(st.none(), _DECIMALS)),
        "tokens": None,
        "llm_call_count": draw(st.integers(0, 100000)),
        "tool_call_count": draw(st.integers(0, 100000)),
    }
    return kwargs, _wire_value(kwargs)


@st.composite
def _node_create_inputs(
    draw: st.DrawFn,
) -> tuple[dict[str, Any], dict[str, Any]]:
    index = draw(st.integers(1, 1000))
    kwargs: dict[str, Any] = {
        "index": index,
        "parent_index": draw(st.one_of(st.none(), st.integers(0, index - 1))),
        "secondary_parent_indexes": draw(
            st.lists(st.integers(0, index - 1), max_size=3, unique=True)
        ),
        "node_type": draw(st.sampled_from(list(NodeType))),
        "name": draw(_SAFE_TEXT),
        "status": draw(st.sampled_from(list(NodeStatus))),
        "started_at": draw(st.one_of(st.none(), _AWARE_DATETIMES)),
        "ended_at": draw(st.one_of(st.none(), _AWARE_DATETIMES)),
        "inputs": draw(JSON_VALUES),
        "outputs": draw(JSON_VALUES),
        "attributes": draw(JSON_VALUES),
        "cost": draw(st.one_of(st.none(), _DECIMALS)),
        "model_params": draw(
            st.one_of(st.none(), st.dictionaries(_SAFE_TEXT, JSON_VALUES, max_size=3))
        ),
        "metadata": draw(st.dictionaries(_SAFE_TEXT, JSON_VALUES, max_size=3)),
    }
    return kwargs, _wire_value(kwargs)


@st.composite
def _node_response_inputs(
    draw: st.DrawFn,
) -> tuple[dict[str, Any], dict[str, Any]]:
    request_kwargs, _ = draw(_node_create_inputs())
    kwargs = {
        "id": draw(st.uuids()),
        "session_id": draw(st.uuids()),
        **request_kwargs,
        "parent_id": draw(_OPTIONAL_UUIDS),
        "secondary_parent_ids": draw(st.lists(st.uuids(), max_size=3, unique=True)),
        "external_id": None,
        "trace_id": None,
        "error": None,
        "input_text_selector": None,
        "output_text_selector": None,
        "system_prompt_selector": None,
        "reasoning": None,
        "requested_model": None,
        "model": None,
        "model_provider": None,
        "tokens": None,
        "tool_name": None,
        "subagent_id": None,
        "cache_key": None,
    }
    return kwargs, _wire_value(kwargs)


_FILTER_LEAVES = st.builds(
    lambda field, op, value: {"field": field, "op": op.value, "value": value},
    st.from_regex(r"[a-z][a-z0-9_]{0,16}", fullmatch=True),
    st.sampled_from(list(FilterOp)),
    JSON_VALUES,
)
FILTER_VALUES = st.recursive(
    _FILTER_LEAVES,
    lambda children: st.one_of(
        st.builds(
            lambda values: {"and": values}, st.lists(children, min_size=1, max_size=3)
        ),
        st.builds(
            lambda values: {"or": values}, st.lists(children, min_size=1, max_size=3)
        ),
        st.builds(lambda value: {"not": value}, children),
    ),
    max_leaves=8,
)


@st.composite
def _list_params_inputs(
    draw: st.DrawFn,
) -> tuple[dict[str, Any], dict[str, Any]]:
    filter_value = draw(FILTER_VALUES)
    kwargs: dict[str, Any] = {
        "filter": filter_value,
        "include_payloads": draw(st.booleans()),
        "size": draw(st.integers(1, 1000)),
    }
    expected = {**kwargs, "filter": json.dumps(filter_value)}
    return kwargs, expected


# This is intentionally a bounded registry, not a claim about every API model.
SELECTED_DTO_REGISTRY = (
    WireCase(ReplayCreateRequest, _replay_inputs(), True),
    WireCase(ToolLookupRequest, _tool_lookup_inputs(), True),
    WireCase(EvaluationResult, _evaluation_result_inputs(), True),
    WireCase(EvaluationResponse, _evaluation_response_inputs(), False),
    WireCase(SessionCreateRequest, _session_create_inputs(), True),
    WireCase(SessionUpdateRequest, _session_update_inputs(), True),
    WireCase(SessionResponse, _session_response_inputs(), False),
    WireCase(SessionNodeCreateRequest, _node_create_inputs(), True),
    WireCase(SessionNodeResponse, _node_response_inputs(), False),
    WireCase(SessionListParams, _list_params_inputs(), True),
)


@pytest.mark.parametrize(
    "wire_case", SELECTED_DTO_REGISTRY, ids=lambda case: case.model_type.__name__
)
@given(data=st.data())
def test_selected_dto_wire_projection(wire_case: WireCase, data: st.DataObject) -> None:
    """Selected valid DTOs match independently constructed wire values."""
    kwargs, expected = data.draw(wire_case.inputs)
    model = wire_case.model_type(**kwargs)

    wire = model.model_dump(mode="json", exclude_unset=wire_case.exclude_unset)

    assert wire == expected
    assert wire_case.model_type.model_validate(wire) == model


@given(_replay_inputs())
def test_replay_wire_keeps_controls_mutually_exclusive(
    generated: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    """Unset-field omission never places both baseline controls on the wire."""
    kwargs, _ = generated
    wire = ReplayCreateRequest(**kwargs).model_dump(mode="json", exclude_unset=True)
    assert not {"evaluate_baselines", "baseline_evaluation_mode"} <= wire.keys()
    assert wire["tool_policy"]["default"]["type"] in {"passthrough", "history"}


@given(field=st.sampled_from(["outputs", "error", "ended_at", "name", "metadata"]))
def test_session_update_omission_differs_from_explicit_null(field: str) -> None:
    """An omitted partial-update field and explicit null remain distinct."""
    omitted = SessionUpdateRequest().model_dump(mode="json", exclude_unset=True)
    explicit = SessionUpdateRequest(**{field: None}).model_dump(
        mode="json", exclude_unset=True
    )
    assert field not in omitted
    assert explicit[field] is None


@given(FILTER_VALUES)
def test_filter_list_param_uses_aliases_inside_json(
    filter_value: dict[str, Any],
) -> None:
    """Recursive filter operators keep their public aliases in the query value."""
    wire = SessionListParams(filter=filter_value).model_dump(
        mode="json", exclude_unset=True
    )
    decoded = json.loads(wire["filter"])
    assert decoded == filter_value
    assert not {"and_", "or_", "not_"} & _filter_keys(decoded)


@given(_evaluation_result_inputs())
def test_evaluation_result_type_survives_wire_round_trip(
    generated: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    """Bool, float, string, and categorical result meaning survives JSON."""
    kwargs, _ = generated
    result = EvaluationResult(**kwargs)
    restored = EvaluationResult.model_validate_json(
        result.model_dump_json(exclude_unset=True)
    )
    assert restored.data_type is result.data_type


def test_evaluation_name_rejects_control_character() -> None:
    """The restricted evaluation name rejects a C0 control character."""
    with pytest.raises(ValidationError, match="Name must contain only"):
        EvaluationResult(name="valid\x00name", score=1.0)


def test_json_serialization_rejects_lone_surrogate() -> None:
    """A lone UTF-16 surrogate cannot cross the DTO JSON boundary."""
    request = SessionCreateRequest(
        origin=SessionOrigin.IMPORTED,
        inputs={"value": "\ud800"},
        outputs={},
    )

    with pytest.raises(PydanticSerializationError):
        request.model_dump_json()
