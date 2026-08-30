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
