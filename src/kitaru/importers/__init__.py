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
"""Provider trace normalization helpers."""

from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.importers.models import (
    ImportContext,
    NormalizationError,
    NormalizedImport,
    NormalizedNode,
    NormalizedSession,
    NormalizedTurn,
    ReplayReadiness,
    TokenUsage,
    parsed_items,
)


class InvalidImport(ValueError):
    """Raised when an importer payload cannot be normalized."""


__all__ = [
    "ImportContext",
    "InvalidImport",
    "NodeStatus",
    "NodeType",
    "NormalizationError",
    "NormalizedImport",
    "NormalizedNode",
    "NormalizedSession",
    "NormalizedTurn",
    "ReplayReadiness",
    "SessionStatus",
    "TokenUsage",
    "parsed_items",
]
