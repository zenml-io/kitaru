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
"""Property tests for the importer `parse()` contract."""

import json
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.task.importer import ImportedSession

from .fuzz_strategies import (
    IMPORTERS,
    encode_records,
    garbage_bytes,
    importer_params,
    invalid_params,
    mutated_seed_lines,
    records_for,
)

IMPORTER_NAMES = sorted(IMPORTERS)


def _assert_contract(name: str, content: bytes, params: dict[str, Any]) -> None:
    """Assert the documented `parse()` contract for one input."""
    module = IMPORTERS[name]
    try:
        items = list(module.parse(content, params))
    except module.InvalidImport:
        return
    for item in items:
        assert isinstance(item, (ImportedSession, ImportFailure)), type(item)
        if isinstance(item, ImportedSession):
            json.loads(item.model_dump_json())


@pytest.mark.parametrize("name", IMPORTER_NAMES)
@given(content=garbage_bytes(), params=importer_params())
def test_parse_contract_on_garbage(
    name: str, content: bytes, params: dict[str, Any]
) -> None:
    _assert_contract(name, content, params)


@pytest.mark.parametrize("name", IMPORTER_NAMES)
@given(data=st.data())
def test_parse_contract_on_records(name: str, data: st.DataObject) -> None:
    records = data.draw(records_for(name))
    params = data.draw(importer_params())
    _assert_contract(name, encode_records(name, records), params)


@pytest.mark.parametrize("name", IMPORTER_NAMES)
@given(data=st.data())
def test_parse_contract_on_invalid_params(name: str, data: st.DataObject) -> None:
    """Cover the parameter-validation branches `importer_params()` avoids."""
    records = data.draw(records_for(name))
    params = data.draw(invalid_params())
    _assert_contract(name, encode_records(name, records), params)


@given(content=mutated_seed_lines(), params=importer_params())
def test_langfuse_contract_on_mutated_seed(
    content: bytes, params: dict[str, Any]
) -> None:
    _assert_contract("langfuse", content, params)


def _session_ids(name: str, content: bytes, params: dict[str, Any]) -> list[str] | None:
    module = IMPORTERS[name]
    try:
        items = list(module.parse(content, params))
    except module.InvalidImport:
        return None
    return sorted(
        item.external_id for item in items if isinstance(item, ImportedSession)
    )


# NEW-FINDING-4: braintrust takes a session's project identity from whichever
# row of a trace it reads first, so rows that disagree about "project_id" give
# that trace a different session external_id depending on record order.
_ORDER_UNSTABLE_IDENTITY: dict[str, dict[str, Any]] = {
    "braintrust": {"project_id": "proj-a"}
}
# NEW-FINDING-5: when two langsmith runs share an id, the run that is read
# first decides the parent links, and one order imports a session while the
# other rejects the whole file.
_ORDER_UNSTABLE_ID_KEYS = {"langsmith": "id"}


def _order_comparable(name: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop the record shapes whose grouping is known to depend on order.

    Both exclusions are known bugs pinned by the regression tests below; see
    design/fuzzing/new-findings.md. Without them the property keeps re-finding
    those two bugs instead of searching for other order dependence.
    """
    pinned = _ORDER_UNSTABLE_IDENTITY.get(name, {})
    id_key = _ORDER_UNSTABLE_ID_KEYS.get(name)
    seen: set[str] = set()
    comparable = []
    for record in records:
        if id_key is not None:
            record_id = str(record.get(id_key))
            if record_id in seen:
                continue
            seen.add(record_id)
        comparable.append({**record, **pinned})
    return comparable


@pytest.mark.parametrize("name", [n for n in IMPORTER_NAMES if n != "jsonl"])
@given(data=st.data())
def test_grouping_is_order_independent(name: str, data: st.DataObject) -> None:
    """Which records form a session must not depend on record order."""
    records = _order_comparable(name, data.draw(records_for(name)))
    params = data.draw(importer_params())
    # A full st.permutations() draw costs entropy proportional to the record
    # list and trips Hypothesis's data_too_large health check under the
    # derandomized "ci" profile. Reversing and rotating changes which record
    # the importer reads first, which is what order dependence turns on, for
    # the price of one small integer.
    rotation = data.draw(st.integers(0, max(0, len(records) - 1)))
    reordered = list(reversed(records))
    reordered = reordered[rotation:] + reordered[:rotation]
    assert _session_ids(name, encode_records(name, records), params) == _session_ids(
        name, encode_records(name, reordered), params
    )


def _assert_order_independent(name: str, rows: list[dict[str, Any]]) -> None:
    assert _session_ids(name, encode_records(name, rows), {}) == _session_ids(
        name, encode_records(name, rows[::-1]), {}
    )


@pytest.mark.xfail(strict=True, reason="NEW-FINDING-4")
def test_conflicting_project_identity_is_order_independent() -> None:
    _assert_order_independent(
        "braintrust",
        [
            {"span_id": "s0", "root_span_id": "t1", "project_id": "proj-a"},
            {"span_id": "s1", "root_span_id": "t1", "project_id": "proj-b"},
        ],
    )


@pytest.mark.xfail(strict=True, reason="NEW-FINDING-5")
def test_duplicate_run_id_grouping_is_order_independent() -> None:
    _assert_order_independent(
        "langsmith",
        [
            {"id": "r0", "trace_id": "t1", "parent_run_id": "r0"},
            {
                "id": "r0",
                "trace_id": "t1",
                "parent_run_id": None,
                "session_id": "proj-a",
            },
        ],
    )
