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
"""Cursor pagination codec."""

import base64
import json

from pydantic import ValidationError as PydanticValidationError

from kitaru.server.base import FrozenModel
from kitaru.server.domain.base import ValidationError


class DecodedCursor(FrozenModel):
    """Decoded pagination cursor."""

    sort: str
    id: str
    filter_hash: str


def encode_cursor(sort: str, last_id: str, filter_hash: str) -> str:
    """Encode a cursor payload into an opaque cursor string.

    Args:
        sort: Sort field and direction that produced this cursor.
        last_id: Last row's id.
        filter_hash: Hash of the filter's non-pagination fields.

    Returns:
        Base64url-encoded cursor string.
    """
    payload = json.dumps(
        {
            "sort": sort,
            "id": last_id,
            "filter_hash": filter_hash,
        }
    )
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("utf-8")


def decode_cursor(cursor: str, sort: str, filter_hash: str) -> DecodedCursor:
    """Decode and validate an opaque cursor string.

    Args:
        cursor: Cursor string from the request.
        sort: Sort field and direction of the current request.
        filter_hash: Hash of the current filter's non-pagination fields.

    Raises:
        ValidationError: The cursor is malformed, or its sort or filter hash
            does not match the current request.

    Returns:
        Decoded cursor.
    """
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor))
    except ValueError as exc:
        raise ValidationError("Invalid cursor") from exc
    if not isinstance(payload, dict):
        raise ValidationError("Invalid cursor")
    try:
        decoded = DecodedCursor(
            sort=payload.get("sort"),
            id=payload.get("id"),
            filter_hash=payload.get("filter_hash"),
        )
    except PydanticValidationError as exc:
        raise ValidationError("Invalid cursor") from exc
    if decoded.sort != sort or decoded.filter_hash != filter_hash:
        raise ValidationError("Invalid cursor")
    return decoded
