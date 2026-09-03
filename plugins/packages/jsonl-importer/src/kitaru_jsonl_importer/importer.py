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
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Kitaru session JSONL importer plugin."""

import json
from collections.abc import Iterator
from typing import Any

from pydantic_core import PydanticSerializationError

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.task.importer import ImportedSession, SessionImportError, flatten_nodes

MAX_UPLOAD_BYTES = 64 * 1024 * 1024


class InvalidImport(ValueError):
    """Raised when a Kitaru JSONL payload cannot be parsed."""


def parse(
    content: bytes, params: dict[str, Any]
) -> Iterator[ImportedSession | ImportFailure]:
    """Parse one Kitaru session per JSONL record.

    Args:
        content: UTF-8 JSONL containing Kitaru session objects.
        params: Import parameters, unused by this importer.

    Raises:
        InvalidImport: The upload is too large, empty, or not UTF-8.

    Yields:
        Valid sessions and isolated line failures.
    """
    _ = params
    if len(content) > MAX_UPLOAD_BYTES:
        raise InvalidImport("Kitaru JSONL import exceeds the 64 MiB upload limit")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidImport("Kitaru JSONL import must be UTF-8") from exc
    if not text.strip():
        raise InvalidImport("Kitaru JSONL import contains no records")

    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        external_id = None
        try:
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError("record must be a JSON object")
            raw_external_id = value.get("external_id")
            external_id = str(raw_external_id) if raw_external_id is not None else None
            session = ImportedSession.model_validate(value)
            if any(node.index is None or node.children for node in session.nodes):
                raise ValueError("nodes must use the flat indexed representation")
            for node in session.nodes:
                if node.cost is not None and (
                    not node.cost.is_finite() or node.cost < 0
                ):
                    raise ValueError("node cost must be finite and nonnegative")
                if node.tokens is not None and any(
                    count is not None and count < 0
                    for count in node.tokens.model_dump().values()
                ):
                    raise ValueError("node token counts must be nonnegative")
            flatten_nodes(session.nodes)
            session.model_dump_json()
            yield session
        except (
            SessionImportError,
            ValueError,
            RecursionError,
            PydanticSerializationError,
        ) as exc:
            yield ImportFailure(
                line=line_number,
                external_id=(
                    external_id.encode("utf-8", errors="backslashreplace").decode(
                        "utf-8"
                    )
                    if external_id is not None
                    else None
                ),
                error=f"Invalid Kitaru session: {exc}".encode(
                    "utf-8", errors="backslashreplace"
                ).decode("utf-8"),
            )
