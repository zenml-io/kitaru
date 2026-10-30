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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Cost, time, token, and tool-path findings computed in the profiler's scan."""

from collections.abc import Sequence
from typing import Protocol

from kitaru.api_models.v1.session_node import (
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru_post_import_insights.context_size import ContextSize
from kitaru_post_import_insights.cost_concentration import CostConcentration
from kitaru_post_import_insights.findings import CandidateFinding, ContributionSelector
from kitaru_post_import_insights.profiling_state import CountsStore, ObservationStore
from kitaru_post_import_insights.prompt_cache import PromptCacheUse
from kitaru_post_import_insights.resource_inputs import FindingLimits, SessionView
from kitaru_post_import_insights.time_share import ComponentTimeShare
from kitaru_post_import_insights.tool_paths import ToolPaths


class _ResourceFamily(Protocol):
    """One finding family that reads every session once."""

    def consume(self, view: SessionView) -> None:
        """Add one session to the family's aggregates."""

    def build(self, select: ContributionSelector) -> list[CandidateFinding]:
        """Return the family's candidates that meet their thresholds."""


class ResourceProfile:
    """Run every resource finding family over the same streamed sessions.

    The families keep exact aggregates in their own temporary stores, so they
    never draw on a session's shared payload or text inspection budget.
    """

    def __init__(self, limits: FindingLimits) -> None:
        """Initialize every family with shared temporary stores."""
        self._counts = CountsStore()
        self._observations = ObservationStore()
        self._families: tuple[_ResourceFamily, ...] = (
            CostConcentration(self._observations, limits),
            ComponentTimeShare(self._counts, self._observations, limits),
            ContextSize(self._observations, limits),
            PromptCacheUse(self._observations, limits),
            ToolPaths(self._counts, self._observations, limits),
        )

    def consume(
        self, session: SessionWithNodesResponse, nodes: Sequence[SessionNodeResponse]
    ) -> None:
        """Read one session's nodes, already in position order, for every family."""
        view = SessionView.from_session(session, nodes)
        for family in self._families:
            family.consume(view)

    def build_candidates(self, select: ContributionSelector) -> list[CandidateFinding]:
        """Return every family's candidates."""
        return [
            candidate for family in self._families for candidate in family.build(select)
        ]

    def close(self) -> None:
        """Release the temporary stores."""
        self._counts.close()
        self._observations.close()
