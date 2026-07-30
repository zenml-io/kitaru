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
"""Shared query filter predicates."""

from sqlalchemy import ColumnElement
from sqlalchemy.orm import InstrumentedAttribute

LIKE_ESCAPE = "\\"

TextColumn = InstrumentedAttribute[str] | InstrumentedAttribute[str | None]


def contains_text(column: TextColumn, term: str) -> ColumnElement[bool]:
    """Build a case-insensitive substring match on a text column.

    Wildcards in the term are escaped, so a search for a literal `%` or `_`
    matches that character instead of every row. A null column never matches,
    which is the wanted behavior for an optional display name.

    Args:
        column: Text column to match against.
        term: Search term, matched anywhere in the column.

    Returns:
        Substring match predicate.
    """
    escaped = (
        term.replace(LIKE_ESCAPE, LIKE_ESCAPE * 2)
        .replace("%", f"{LIKE_ESCAPE}%")
        .replace("_", f"{LIKE_ESCAPE}_")
    )
    return column.ilike(f"%{escaped}%", escape=LIKE_ESCAPE)
