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
"""Conventions every ORM table must follow."""

from sqlalchemy import (
    CheckConstraint,
    ForeignKeyConstraint,
    Index,
    PrimaryKeyConstraint,
    Table,
    UniqueConstraint,
)

import kitaru.server.adapters.db.orm  # noqa: F401  Registers every ORM table.
from kitaru.server.adapters.db.orm.base import Base
from kitaru.server.adapters.db.orm.orm_utils import (
    check_constraint_name,
    foreign_key_name,
    index_name,
    unique_constraint_name,
)

# Guards against a registry change that would leave the metadata empty, so the
# check below would pass without inspecting anything.
MIN_NAMED_OBJECT_COUNT = 150

NamedObject = Index | UniqueConstraint | ForeignKeyConstraint | CheckConstraint


def _get_expected_name(table: Table, item: NamedObject) -> str:
    columns = [column.name for column in item.columns]
    if isinstance(item, ForeignKeyConstraint):
        return foreign_key_name(table.name, columns)
    if isinstance(item, CheckConstraint):
        return check_constraint_name(table.name, columns)
    # A unique Index enforces conditional uniqueness, so it shares the uq_ name.
    if isinstance(item, UniqueConstraint) or item.unique:
        return unique_constraint_name(table.name, columns)
    return index_name(table.name, columns)


def test_constraint_names_come_from_orm_utils() -> None:
    """Name every index and constraint with the orm_utils helpers."""
    items = [
        (table, item)
        for table in Base.metadata.tables.values()
        for item in [*table.indexes, *table.constraints]
        if not isinstance(item, PrimaryKeyConstraint)
    ]
    assert len(items) >= MIN_NAMED_OBJECT_COUNT
    mismatched = set()
    for table, item in items:
        expected = _get_expected_name(table, item)
        name = str(item.name)
        # Expression indexes and check constraints expose no plain columns, so
        # only the table prefix can be derived for them.
        matches = name == expected if item.columns else name.startswith(expected)
        if not matches:
            mismatched.add(f"{table.name}: {name} (expected {expected})")
    assert not mismatched, (
        "Build the name with index_name, unique_constraint_name, foreign_key_name, "
        "or check_constraint_name from orm/orm_utils.py."
    )
