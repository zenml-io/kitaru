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
"""Find low prompt-cache reads and system prompts that change between calls."""

from typing import Any

from kitaru.api_models.v1.insight import CategoricalInsightData, CategoryValue
from kitaru.api_models.v1.session_node import SessionNodeResponse
from kitaru.json_pointer import resolve_json_pointer
from kitaru_post_import_insights.findings import (
    CandidateCoverage,
    CandidateFinding,
    ContributionSelector,
    DeterministicFact,
    calculate_percent,
    format_briefing,
    pluralize,
)
from kitaru_post_import_insights.labels import (
    MAX_SELECTOR_LENGTH,
    MAX_SELECTOR_SEGMENTS,
)
from kitaru_post_import_insights.models import EvidenceLocator
from kitaru_post_import_insights.profiling_state import (
    HighestValueNodes,
    Observation,
    ObservationStore,
    SessionReferences,
)
from kitaru_post_import_insights.resource_inputs import (
    FindingLimits,
    SessionView,
    format_count,
    select_evidence,
)

# Fewer calls give a share that one call can swing.
MIN_CACHE_REPORTING_CALLS = 10
# Below half, even the generous reading leaves most input uncached.
LOW_CACHE_SHARE_PERCENT = 50
# A handful of sessions, and a tenth of comparable ones, make churn a pattern.
MIN_CHANGED_SESSIONS = 3
MIN_CHANGED_SHARE_PERCENT = 10
_MAX_PROMPT_PARTS = 1_000
_PREFIX_NAMESPACE = "system-prompt-shared-prefix"
CHANGE_SIGNAL = "system-prompt-change"
UNCACHED_SIGNAL = "uncached-model-call"


def _get_part_text(value: Any) -> str | None:
    """Return the text of one system prompt part, or None for other content."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        text = value.get("text", value.get("content"))
        return text if isinstance(text, str) else None
    return None


def read_system_prompt(node: SessionNodeResponse, max_characters: int) -> str | None:
    """Resolve a node's system prompt selector to bounded text, when possible."""
    pointer = node.system_prompt_selector
    if (
        pointer is None
        or len(pointer) > MAX_SELECTOR_LENGTH
        or pointer.count("/") > MAX_SELECTOR_SEGMENTS
    ):
        return None
    found, value = resolve_json_pointer(node.inputs, pointer)
    if not found:
        return None
    if isinstance(value, list):
        if len(value) > _MAX_PROMPT_PARTS:
            return None
        parts = [_get_part_text(item) for item in value]
        if any(part is None for part in parts):
            return None
        text: str | None = "\n".join(part for part in parts if part is not None)
    else:
        text = _get_part_text(value)
    return text if text is not None and len(text) <= max_characters else None


def get_shared_prefix_length(first: str, second: str) -> int:
    """Return the length of the longest common prefix of two strings."""
    low, high = 0, min(len(first), len(second))
    # Slice comparisons run in C, so a binary search stays fast on long prompts.
    while low < high:
        middle = (low + high + 1) // 2
        if first[:middle] == second[:middle]:
            low = middle
        else:
            high = middle - 1
    return low


class PromptCacheUse:
    """Accumulate cache-read shares and system prompt changes."""

    def __init__(self, store: ObservationStore, limits: FindingLimits) -> None:
        """Initialize empty totals backed by a shared observation store."""
        self._store = store
        self._limits = limits
        self._reporting_calls = 0
        self._calls_with_reads = 0
        self._cached_tokens = 0
        # Two readings of total input bound the true cache-read share: one adds
        # cache reads to every call's input, the other adds them only where the
        # call's source is known to count them separately.
        self._input_with_reads_added = 0
        self._input_as_read = 0
        self._reporting_sessions = SessionReferences(limits.contributors)
        self._uncached_sessions = SessionReferences(limits.contributors)
        self._largest_uncached = HighestValueNodes(limits.evidence)
        self._comparable_sessions = 0
        self._changed_sessions = SessionReferences(limits.contributors)
        self._changes = 0
        self._largest_changes = HighestValueNodes(limits.evidence)

    def consume(self, view: SessionView) -> None:
        """Add one session's cache counts and system prompt comparisons."""
        session_id = view.session.id
        for call in view.model_calls:
            input_tokens = call.input_tokens
            cached = call.cached_input_tokens
            if input_tokens is None or cached is None:
                continue
            self._reporting_calls += 1
            self._reporting_sessions.add(session_id)
            self._cached_tokens += cached
            self._input_with_reads_added += input_tokens + cached
            self._input_as_read += call.prompt_tokens or 0
            if cached:
                self._calls_with_reads += 1
            else:
                self._uncached_sessions.add(session_id)
                self._largest_uncached.add(session_id, call.node.id, input_tokens)
        # Only the previous prompt per model is held, and only for this session.
        previous: dict[str, str] = {}
        comparable = changed = False
        for call in view.model_calls:
            text = read_system_prompt(call.node, self._limits.max_prompt_characters)
            if text is None:
                continue
            prior = previous.get(call.model_key)
            previous[call.model_key] = text
            if prior is None:
                continue
            comparable = True
            if prior == text:
                continue
            changed = True
            self._changes += 1
            prefix = get_shared_prefix_length(prior, text)
            self._largest_changes.add(session_id, call.node.id, len(prior) - prefix)
            self._store.add(
                _PREFIX_NAMESPACE,
                Observation(
                    value=prefix, session=str(session_id), node=str(call.node.id)
                ),
            )
        self._comparable_sessions += comparable
        if changed:
            self._changed_sessions.add(session_id)

    def build(self, select: ContributionSelector) -> list[CandidateFinding]:
        """Return the cache candidate when cache reads are low or prompts churn."""
        if self._reporting_calls < MIN_CACHE_REPORTING_CALLS or not self._input_as_read:
            return []
        changed = len(self._changed_sessions)
        comparable = self._comparable_sessions
        churn = (
            changed >= MIN_CHANGED_SESSIONS
            and changed * 100 >= MIN_CHANGED_SHARE_PERCENT * comparable
        )
        low_reads = (
            self._cached_tokens * 100 < LOW_CACHE_SHARE_PERCENT * self._input_as_read
        )
        if not (churn or low_reads):
            return []
        low_share = calculate_percent(self._cached_tokens, self._input_with_reads_added)
        high_share = calculate_percent(self._cached_tokens, self._input_as_read)
        share_is_range = low_share != high_share
        share = f"{low_share}% to {high_share}%" if share_is_range else f"{low_share}%"
        uncached = self._reporting_calls - self._calls_with_reads
        facts = [
            DeterministicFact(
                name="cache_reporting_calls", value=self._reporting_calls
            ),
            DeterministicFact(
                name="calls_with_cache_reads", value=self._calls_with_reads
            ),
        ]
        if not share_is_range:
            facts.append(
                DeterministicFact(name="cache_read_share_percent", value=low_share)
            )
        else:
            facts += [
                DeterministicFact(name="cache_read_share_low_percent", value=low_share),
                DeterministicFact(
                    name="cache_read_share_high_percent", value=high_share
                ),
            ]
        caveat = (
            "Sources differ on whether input tokens include cache reads. A call that "
            "reports more cache reads than input tokens must count them separately, "
            "so every call to that model in the session is read that way."
        )
        if share_is_range:
            caveat += (
                " Elsewhere the convention is unknown, so the share is a range: the "
                "low end counts cache reads separately and the high end counts them "
                "inside input tokens."
            )
        caveat += (
            " A recorded zero can also mean the source did not report cache reads."
        )
        if comparable:
            caveat += (
                " System prompts are compared between consecutive calls to the same "
                "model in a session, and prompt text is not stored. Sessions that "
                "alternate between agents with different system prompts also count "
                "as changes."
            )
        if churn:
            median_prefix = self._store.get_median(_PREFIX_NAMESPACE)
            facts += [
                DeterministicFact(
                    name="sessions_with_comparable_system_prompts", value=comparable
                ),
                DeterministicFact(
                    name="sessions_with_system_prompt_changes", value=changed
                ),
                DeterministicFact(name="system_prompt_changes", value=self._changes),
                DeterministicFact(
                    name="median_shared_prefix_characters", value=median_prefix
                ),
            ]
            evidence_signal, ranked = CHANGE_SIGNAL, self._largest_changes
            sessions, occurrences, analyzed = (
                self._changed_sessions,
                self._changes,
                comparable,
            )
            title = (
                f"In {changed} of {comparable} sessions the system prompt changed "
                f"between model calls; cache reads covered {share} of input tokens"
            )
            description = (
                "The system prompt changes between calls to the same model, which "
                "stops a prompt cache from reusing it. Check which part of the "
                "prompt changes."
            )
            briefing = _build_churn_briefing(
                changed=changed,
                comparable=comparable,
                changes=self._changes,
                median_prefix=median_prefix,
                share=share,
                high_share=high_share,
                calls=self._reporting_calls,
            )
        else:
            evidence_signal, ranked = UNCACHED_SIGNAL, self._largest_uncached
            # Every call can read a little from cache while most input still
            # misses it; the cohort is then every session reporting cache counts.
            sessions, occurrences = (
                (self._uncached_sessions, uncached)
                if uncached
                else (self._reporting_sessions, self._reporting_calls)
            )
            analyzed = len(self._reporting_sessions)
            title = (
                f"Cache reads covered {share} of model input tokens across "
                f"{format_count(self._reporting_calls)} calls"
            )
            description = (
                "Most model input was not served from a prompt cache. Check whether a "
                "stable prefix, such as the system prompt and tool definitions, comes "
                "first in each call."
            )
            briefing = _build_low_reads_briefing(
                share=share,
                high_share=high_share,
                calls=self._reporting_calls,
                uncached=uncached,
                affected=len(sessions),
            )
        evidence = [
            EvidenceLocator(
                session_id=session_id, node_id=node_id, signal=evidence_signal
            )
            for session_id, node_id in ranked.get_nodes()
        ]
        contributions, evidence = select_evidence(select, sessions, evidence)
        return [
            CandidateFinding(
                id="prompt-cache",
                family="prompt_cache",
                rank=95,
                eyebrow="PROMPT CACHE USE",
                title=title,
                fallback_description=description,
                caveat=caveat,
                data=CategoricalInsightData(
                    unit="calls",
                    values=[
                        CategoryValue(
                            label="Read from cache", value=self._calls_with_reads
                        ),
                        CategoryValue(label="No cache read", value=uncached),
                    ],
                ),
                facts=facts,
                coverage=CandidateCoverage(
                    sessions_analyzed=analyzed,
                    affected_sessions=len(sessions),
                    occurrences=occurrences,
                    evidence_available=occurrences,
                    evidence_retained=len(evidence),
                    contributing_sessions_available=len(sessions),
                    contributing_sessions_retained=len(contributions),
                ),
                contributing_session_ids=contributions,
                evidence=evidence,
                investigation_prompt=briefing,
            )
        ]


def _build_churn_briefing(
    *,
    changed: int,
    comparable: int,
    changes: int,
    median_prefix: int,
    share: str,
    high_share: int | float,
    calls: int,
) -> str:
    """Build the briefing for system prompts that change between calls."""
    return format_briefing(
        odd=(
            f"In {changed} of the {comparable} sessions where consecutive calls to the "
            "same model both have a resolvable system prompt, the system prompt "
            f"changed between calls ({pluralize(changes, 'change')} in all). The "
            f"median change keeps the first {format_count(median_prefix)} characters "
            f"unchanged. Cache reads covered {share} of the input tokens of the "
            f"{format_count(calls)} calls that report cache counts."
        ),
        look_first=(
            "Open the nodes in evidence_locators; each is a model call whose system "
            "prompt differs from the previous call to the same model, ordered by how "
            "much of the previous prompt follows the first changed character. "
            "Compare the two prompts and find the first difference, such as a "
            "timestamp, a user name, retrieved context, or a reordered tool list."
        ),
        cohort=(
            f"the {changed} sessions with a system prompt change, versus sessions "
            "whose system prompt stays the same."
        ),
        hypothesis=(
            "Check whether the changing part could move to the end of the prompt or "
            "into a later message. If it can, keeping the system prompt identical "
            "across calls is the single change to replay, and cache reads should "
            "rise."
        ),
        held=(
            "replayed sessions keep one system prompt per model, and the cache-read "
            f"share rises above {high_share}% while the failed-session rate does not "
            "rise."
        ),
    )


def _build_low_reads_briefing(
    *,
    share: str,
    high_share: int | float,
    calls: int,
    uncached: int,
    affected: int,
) -> str:
    """Build the briefing for model input that rarely comes from a prompt cache."""
    return format_briefing(
        odd=(
            f"Cache reads covered {share} of the input tokens of the "
            f"{format_count(calls)} model calls that report cache counts, and "
            f"{format_count(uncached)} of those calls read nothing from cache."
        ),
        look_first=(
            (
                "Open the nodes in evidence_locators, the largest calls that read "
                "nothing from cache. "
                if uncached
                else "Open a few sessions from contributing_session_ids. "
            )
            + "Check whether consecutive calls start with the same text, such as the "
            "system prompt, tool definitions, and earlier turns, and whether caching "
            "is enabled for this model and provider."
        ),
        cohort=(
            f"the {affected} sessions with at least one model call that read nothing "
            "from cache."
            if uncached
            else f"the {affected} sessions whose model calls report cache counts."
        ),
        hypothesis=(
            "Check whether each call begins with a long prefix that stays the same. "
            "If it does and cache reads stay low, enabling prompt caching or keeping "
            "that prefix identical is the single change to replay."
        ),
        held=(
            f"the replayed cohort's cache-read share rises above {high_share}% and "
            "its input cost falls, while the failed-session rate does not rise."
        ),
    )
