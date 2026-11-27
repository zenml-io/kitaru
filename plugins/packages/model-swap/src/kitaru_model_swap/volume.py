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
"""Traffic volume and the per-call costs projected onto it."""

from dataclasses import dataclass
from decimal import Decimal

from kitaru_model_swap.calls import Collection
from kitaru_model_swap.params import ModelSwapParams
from kitaru_model_swap.stats import format_usd

_DAYS_PER_MONTH = 30


# Spans shorter than this are too short to extrapolate traffic from.
_MIN_OBSERVED_DAYS = 1


@dataclass(frozen=True)
class Volume:
    """Observed traffic used to project costs."""

    sessions: int
    calls_per_session: float
    sessions_per_month: float | None
    source: str | None


def get_volume(collection: Collection, params: ModelSwapParams) -> Volume:
    """Derive calls per session and monthly sessions from the import."""
    calls_per_session = collection.llm_calls_seen / max(collection.sessions, 1)
    if params.sessions_per_month is not None:
        return Volume(
            collection.sessions, calls_per_session, params.sessions_per_month, "params"
        )
    if collection.first_started and collection.last_started:
        days = (
            collection.last_started - collection.first_started
        ).total_seconds() / 86400
        if days >= _MIN_OBSERVED_DAYS:
            return Volume(
                collection.sessions,
                calls_per_session,
                collection.sessions / days * _DAYS_PER_MONTH,
                "observed",
            )
    return Volume(collection.sessions, calls_per_session, None, None)


def project_cost(per_call: Decimal | None, volume: Volume) -> dict[str, str | None]:
    """Project a per-call cost to sessions and months."""
    if per_call is None:
        return {"per_call_usd": None, "per_session_usd": None, "per_month_usd": None}
    per_session = per_call * Decimal(str(volume.calls_per_session))
    per_month = (
        None
        if volume.sessions_per_month is None
        else per_session * Decimal(str(volume.sessions_per_month))
    )
    return {
        "per_call_usd": format_usd(per_call),
        "per_session_usd": format_usd(per_session),
        "per_month_usd": format_usd(per_month),
    }
