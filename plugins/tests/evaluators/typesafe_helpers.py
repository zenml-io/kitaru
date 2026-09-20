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
"""Shared helper for building a judge `SessionView` from an imported session."""

from typing import Any

from kitaru.api_models.v1.session import SessionDetailResponse
from kitaru.api_models.v1.session_node import SessionNodeResponse
from kitaru.task.evaluator import SessionView
from kitaru.task.importer import ImportedSession


def build_view_from_imported(imported: ImportedSession) -> SessionView:
    """Flatten an imported session's node tree into the evaluator's `SessionView`."""
    flat: list[Any] = []
    stack = list(reversed(imported.nodes))
    while stack:
        node = stack.pop()
        flat.append(node)
        stack.extend(reversed(node.children))
    nodes = [
        SessionNodeResponse.model_construct(**n.model_dump(exclude={"children"}))
        for n in flat
    ]
    return SessionView(
        session=SessionDetailResponse.model_construct(
            inputs=imported.inputs, outputs=imported.outputs, input_text_selector=None
        ),
        nodes=nodes,
    )
