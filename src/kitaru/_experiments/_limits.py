"""Regression-run limits and conservative between-trial accounting."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from kitaru._llm_usage import LLM_USAGE_COST_POLICY, round_cost_usd
from kitaru.scoring._verdicts import (
    OperationalLimitFacts,
    OperationalLimitOutcome,
    OperationalLimitReason,
    OperationalLimitThresholds,
)


class RegressionLimits(BaseModel):
    """Bound one synchronous suite rerun used as a regression gate.

    ``max_trials`` is checked before Kitaru reserves an experiment. Cost, token,
    and duration limits are checked after each terminal trial, so one trial can
    cross a monetary or token ceiling before Kitaru stops further submissions.
    """

    schema_version: Literal[1] = 1
    max_trials: int = Field(ge=1)
    max_cost_usd: float | None = Field(default=None, gt=0)
    max_incurred_tokens: int | None = Field(default=None, ge=1)
    max_duration_seconds: float | None = Field(default=None, gt=0)

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("max_cost_usd", "max_duration_seconds")
    @classmethod
    def _validate_finite_limits(cls, value: float | None) -> float | None:
        if value is not None and not math.isfinite(value):
            raise ValueError("Regression limits must be finite.")
        return value

    @property
    def has_operational_limits(self) -> bool:
        """Return whether usage or elapsed time must be checked while running."""
        return any(
            value is not None
            for value in (
                self.max_cost_usd,
                self.max_incurred_tokens,
                self.max_duration_seconds,
            )
        )


class RegressionLimitTracker:
    """Accumulate trusted terminal-trial facts without submitting work itself."""

    def __init__(
        self,
        limits: RegressionLimits,
        *,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not limits.has_operational_limits:
            raise ValueError("A tracker requires a cost, token, or duration limit.")
        self.limits = limits
        self._clock = clock
        self._wall_clock = wall_clock or (lambda: datetime.now(UTC))
        self._started_at = clock()
        self._submitted_trials = 0
        self._incurred_cost_usd = 0.0
        self._incurred_tokens = 0
        self._cost_complete = True
        self._tokens_complete = True
        self._stop_reason: OperationalLimitReason | None = None

    @property
    def stop_reason(self) -> OperationalLimitReason | None:
        """Return the first immutable reason further submissions must stop."""
        return self._stop_reason

    def observe_trial(
        self,
        summaries: Sequence[Mapping[str, Any] | None],
    ) -> OperationalLimitReason | None:
        """Record one completed submission attempt and decide whether to stop."""
        self._submitted_trials += 1
        if not summaries:
            summaries = [None]

        for summary in summaries:
            if summary is None or summary.get("cost_policy") != LLM_USAGE_COST_POLICY:
                if self.limits.max_cost_usd is not None:
                    self._cost_complete = False
                if self.limits.max_incurred_tokens is not None:
                    self._tokens_complete = False
                continue

            cost = summary.get("display_cost_usd")
            if isinstance(cost, bool) or not isinstance(cost, (int, float)) or cost < 0:
                self._cost_complete = False
            else:
                self._incurred_cost_usd = round_cost_usd(
                    self._incurred_cost_usd + float(cost)
                )
            unpriced = summary.get("records_without_cost_count")
            if (
                isinstance(unpriced, bool)
                or not isinstance(unpriced, int)
                or unpriced < 0
                or unpriced > 0
            ):
                self._cost_complete = False

            tokens = summary.get("incurred_total_tokens")
            if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens < 0:
                self._tokens_complete = False
            else:
                self._incurred_tokens += tokens

        if self._stop_reason is None:
            if self.limits.max_cost_usd is not None and not self._cost_complete:
                self._stop_reason = OperationalLimitReason.COST_UNVERIFIED
            elif (
                self.limits.max_incurred_tokens is not None
                and not self._tokens_complete
            ):
                self._stop_reason = OperationalLimitReason.TOKENS_UNVERIFIED
            elif (
                self.limits.max_cost_usd is not None
                and self._incurred_cost_usd >= self.limits.max_cost_usd
            ):
                self._stop_reason = OperationalLimitReason.COST_LIMIT_REACHED
            elif (
                self.limits.max_incurred_tokens is not None
                and self._incurred_tokens >= self.limits.max_incurred_tokens
            ):
                self._stop_reason = OperationalLimitReason.TOKEN_LIMIT_REACHED
            elif (
                self.limits.max_duration_seconds is not None
                and self._elapsed_seconds() >= self.limits.max_duration_seconds
            ):
                self._stop_reason = OperationalLimitReason.DURATION_LIMIT_REACHED
        return self._stop_reason

    def outcome(
        self,
        *,
        remaining_trials: int,
        started_at: str | None = None,
    ) -> OperationalLimitOutcome:
        """Freeze measured totals and the final stop decision for the verdict."""
        duration_seconds = self._elapsed_seconds(started_at=started_at)
        if (
            self._stop_reason is None
            and self.limits.max_duration_seconds is not None
            and duration_seconds >= self.limits.max_duration_seconds
        ):
            self._stop_reason = OperationalLimitReason.DURATION_LIMIT_REACHED
        relevant_cost_complete = (
            self._cost_complete if self.limits.max_cost_usd is not None else True
        )
        relevant_tokens_complete = (
            self._tokens_complete
            if self.limits.max_incurred_tokens is not None
            else True
        )
        return OperationalLimitOutcome.create(
            verified=relevant_cost_complete and relevant_tokens_complete,
            stopped=self._stop_reason is not None,
            reason_code=self._stop_reason,
            facts=OperationalLimitFacts(
                limits=OperationalLimitThresholds(
                    max_trials=self.limits.max_trials,
                    max_cost_usd=self.limits.max_cost_usd,
                    max_incurred_tokens=self.limits.max_incurred_tokens,
                    max_duration_seconds=self.limits.max_duration_seconds,
                ),
                submitted_trials=self._submitted_trials,
                remaining_trials=remaining_trials,
                incurred_cost_usd=self._incurred_cost_usd,
                incurred_tokens=self._incurred_tokens,
                duration_seconds=duration_seconds,
                cost_complete=relevant_cost_complete,
                tokens_complete=relevant_tokens_complete,
                checked_between_terminal_trials=True,
                one_trial_may_overshoot=(
                    self.limits.max_cost_usd is not None
                    or self.limits.max_incurred_tokens is not None
                ),
            ),
        )

    def _elapsed_seconds(self, *, started_at: str | None = None) -> float:
        elapsed = max(0.0, self._clock() - self._started_at)
        if started_at is not None:
            persisted_start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            if persisted_start.tzinfo is None:
                persisted_start = persisted_start.replace(tzinfo=UTC)
            elapsed = max(
                elapsed,
                (self._wall_clock() - persisted_start).total_seconds(),
            )
        return round(max(0.0, elapsed), 6)
