#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Pure transition failure analysis over recorded session nodes."""

import dataclasses
import hashlib
import uuid
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from fnmatch import fnmatchcase

from kitaru.api_models.v1.annotation import AnnotationResponse
from kitaru.api_models.v1.evaluation import EvaluationResponse
from kitaru.api_models.v1.session import SessionResponse, SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType, SessionNodeResponse
from kitaru.mcp.models.failure_matrix import (
    OTHER,
    START,
    CellPattern,
    CellSession,
    EvaluationCount,
    FailureCellData,
    FailureSource,
    GroupRecords,
    GroupSummary,
    MatrixCell,
    StateBy,
)
from kitaru.mcp.redaction import redact

FIRST_FAILURE_KEY = "first_failure"
MAX_STATES = 16
MAX_PATH = 8
MAX_NOTE = 300
MAX_LABEL = 80
MAX_UNLOCATED_EVALUATIONS = 10

Labeler = Callable[[SessionNodeResponse], str | None]
Transition = tuple[str, str]


@dataclass(frozen=True, slots=True)
class FailurePoint:
    """The node where a session first went wrong and how it was found."""

    node: SessionNodeResponse
    source: FailureSource
    note: str | None


@dataclass(frozen=True, slots=True)
class SessionOutcome:
    """One session reduced to its state path and optional failure transition."""

    session: SessionResponse
    failed: bool
    point: FailurePoint | None
    path: tuple[str, ...]
    transitions: tuple[Transition, ...]
    failed_evaluations: tuple[str, ...]
    failure_is_state: bool = True

    @property
    def failure_transition(self) -> Transition | None:
        """Last step that went right and first step that went wrong, if located."""
        if self.point is None:
            return None
        return (self.path[-2] if len(self.path) > 1 else START, self.path[-1])


def build_labeler(state_by: StateBy, state_map: Mapping[str, str] | None) -> Labeler:
    """Return a function mapping a node to its state name, or `None` if not a state."""
    patterns = tuple((state_map or {}).items())

    def label(node: SessionNodeResponse) -> str | None:
        raw = _raw_state(node, state_by)
        return None if raw is None else _display_state(raw, patterns)

    return label


def _display_state(name: str, patterns: Sequence[tuple[str, str]]) -> str:
    # Redact and bound here rather than on output: the view sends labels back to
    # the drill-down tool, which must compare them with the same string it shows.
    return _bounded(redact(map_state(name, patterns)))


def _bounded(label: str) -> str:
    if len(label) <= MAX_LABEL:
        return label
    # A digest keeps two long names that share a prefix apart.
    digest = hashlib.sha256(label.encode()).hexdigest()[:8]
    return f"{label[: MAX_LABEL - 10]}…{digest}"


def map_state(name: str, patterns: Sequence[tuple[str, str]]) -> str:
    """Apply the first matching glob pattern, keeping the name when none matches."""
    return next((target for glob, target in patterns if fnmatchcase(name, glob)), name)


def _raw_state(node: SessionNodeResponse, state_by: StateBy) -> str | None:
    if node.node_type == NodeType.TOOL_CALL and state_by != "span":
        return _node_name(node, node.tool_name)
    if node.node_type == NodeType.SUBAGENT_CALL and state_by != "span":
        return _node_name(node, node.subagent_id)
    # A root span wraps the whole run, so it never marks a step inside it.
    if node.node_type == NodeType.SPAN and state_by != "tool":
        return _node_name(node) if node.parent_external_id is not None else None
    if node.node_type == NodeType.LLM_CALL and state_by == "node":
        return "llm"
    return None


def _node_name(node: SessionNodeResponse, preferred: str | None = None) -> str:
    # Recorded names can be blank, which cannot be drilled into, or equal to the
    # matrix's own labels, whose counts they would silently merge with.
    for name in (preferred, node.name):
        if name and name.strip():
            return f"{name} (recorded)" if name in (START, OTHER) else name
    return f"unnamed {node.node_type}"


def first_failure_marks(
    annotations: Iterable[AnnotationResponse],
) -> dict[uuid.UUID, str | None]:
    """Map node ids a reviewer marked as the first failure to the reviewer's note."""
    marks: dict[uuid.UUID, str | None] = {}
    for annotation in annotations:
        node_id = annotation.selector.node_id if annotation.selector else None
        if node_id is None:
            continue
        value = annotation.value
        if _affirms_first_failure(annotation.question_key, value):
            marks[node_id] = _note_of(value)
    return marks


def _affirms_first_failure(question_key: str | None, value: object) -> bool:
    if isinstance(value, dict) and value.get(FIRST_FAILURE_KEY) is True:
        return True
    # An answer to a `first_failure` question counts only when it says yes, either
    # as `true` or as a non-blank note describing what went wrong.
    if question_key != FIRST_FAILURE_KEY:
        return False
    return value is True or (isinstance(value, str) and bool(value.strip()))


def _note_of(value: object) -> str | None:
    if isinstance(value, str):
        return _clip(value)
    if isinstance(value, dict):
        note = value.get("note")
        return _clip(note) if isinstance(note, str) else None
    return None


def _clip(text: str | None) -> str | None:
    if not text:
        return None
    text = " ".join(text.split())
    return text if len(text) <= MAX_NOTE else text[: MAX_NOTE - 1] + "…"


def locate_failure(
    nodes: Sequence[SessionNodeResponse], marks: Mapping[uuid.UUID, str | None]
) -> FailurePoint | None:
    """Find the node where a session first went wrong.

    A reviewer's first-failure mark wins over recorded errors. Otherwise the
    failure is the earliest *deepest* failed node: adapters copy a failed status
    up to every enclosing span, so the earliest failed node is usually the root
    span that merely contains the real error.

    Args:
        nodes: The session's nodes in position order.
        marks: Node ids a reviewer marked as the first failure, with notes.

    Returns:
        The failure point, or `None` when nothing in the session locates one.
    """
    for node in nodes:
        if node.id in marks:
            return FailurePoint(node, "annotation", marks[node.id])
    failed = [node for node in nodes if node.status == NodeStatus.FAILED]
    # A completed node can sit between two failed ones, so look at every
    # ancestor of each failure, not only its direct parent.
    above_failures = set().union(*(_ancestors(nodes, node) for node in failed))
    for node in failed:
        if node.external_id not in above_failures:
            return FailurePoint(node, "error", _clip(node.error))
    return None


def analyze_session(
    session: SessionResponse,
    nodes: Sequence[SessionNodeResponse],
    marks: Mapping[uuid.UUID, str | None],
    failed_evaluations: Sequence[str],
    labeler: Labeler,
    state_map: Sequence[tuple[str, str]],
) -> SessionOutcome:
    """Reduce one session to its state path and failure transition."""
    session_marks = {node.id: marks[node.id] for node in nodes if node.id in marks}
    failed = (
        session.status == SessionStatus.FAILED
        or bool(failed_evaluations)
        or bool(session_marks)
    )
    point = locate_failure(nodes, session_marks) if failed else None
    if point is None:
        path = tuple(state for node in nodes if (state := labeler(node)) is not None)
        return SessionOutcome(
            session, failed, None, path, _pairs(path), tuple(failed_evaluations)
        )
    ancestors = _ancestors(nodes, point.node)
    index = next(i for i, node in enumerate(nodes) if node.id == point.node.id)
    before = tuple(
        state
        for node in nodes[:index]
        if node.status == NodeStatus.COMPLETED
        and node.external_id not in ancestors
        and (state := labeler(node)) is not None
    )
    own_state = labeler(point.node)
    fallback = _raw_state(point.node, "node") or _node_name(point.node)
    path = (*before, own_state or _display_state(fallback, state_map))
    # A failing step that is not itself a state never counts as a tried transition,
    # so its cell reports failures without a failure rate.
    transitions = _pairs(path) if own_state is not None else _pairs(before)
    return SessionOutcome(
        session,
        True,
        point,
        path,
        transitions,
        tuple(failed_evaluations),
        failure_is_state=own_state is not None,
    )


def _pairs(path: Sequence[str]) -> tuple[Transition, ...]:
    return tuple(zip((START, *path), path, strict=False))


def _ancestors(
    nodes: Sequence[SessionNodeResponse], node: SessionNodeResponse
) -> set[str]:
    parent_of = {item.external_id: item.parent_external_id for item in nodes}
    ancestors: set[str] = set()
    parent = node.parent_external_id
    while parent is not None and parent not in ancestors:
        ancestors.add(parent)
        parent = parent_of.get(parent)
    return ancestors


def analyze_group(
    records: GroupRecords, labeler: Labeler, state_map: Mapping[str, str] | None
) -> list[SessionOutcome]:
    """Analyze every session of a group."""
    marks = first_failure_marks(records.annotations)
    failed_evaluations = _failed_evaluations(records.evaluations)
    patterns = tuple((state_map or {}).items())
    return [
        analyze_session(
            session,
            records.nodes.get(session.id, ()),
            marks,
            failed_evaluations.get(session.id, ()),
            labeler,
            patterns,
        )
        for session in records.sessions
    ]


def _failed_evaluations(
    evaluations: Iterable[EvaluationResponse],
) -> dict[uuid.UUID, tuple[str, ...]]:
    failed: defaultdict[uuid.UUID, list[str]] = defaultdict(list)
    for evaluation in evaluations:
        if evaluation.passed is False:
            failed[evaluation.session_id].append(evaluation.name)
    return {session_id: tuple(names) for session_id, names in failed.items()}


def fold_rare_states(
    groups: Sequence[list[SessionOutcome]],
    sources: Sequence[FailureSource],
    limit: int = MAX_STATES,
) -> list[list[SessionOutcome]]:
    """Merge all but the `limit` most frequent states into one `(other)` state."""
    frequency: Counter[str] = Counter()
    for outcomes in groups:
        for outcome in outcomes:
            frequency.update(outcome.path)
            if is_counted(outcome, sources):
                # Weight failing states so a rare but broken step keeps its own row.
                frequency[outcome.path[-1]] += 1000
    if len(frequency) <= limit:
        return [list(outcomes) for outcomes in groups]
    keep = {state for state, _ in frequency.most_common(limit - 1)} | {START}

    def fold(state: str) -> str:
        return state if state in keep else OTHER

    return [
        [
            dataclasses.replace(
                outcome,
                path=tuple(fold(state) for state in outcome.path),
                transitions=tuple((fold(a), fold(b)) for a, b in outcome.transitions),
            )
            for outcome in outcomes
        ]
        for outcomes in groups
    ]


def order_states(groups: Sequence[Sequence[SessionOutcome]]) -> list[str]:
    """Order states by their average relative position, so pipelines read diagonally."""
    positions: defaultdict[str, list[float]] = defaultdict(list)
    for outcomes in groups:
        for outcome in outcomes:
            for index, state in enumerate(outcome.path):
                positions[state].append(index / max(len(outcome.path) - 1, 1))
    return sorted(
        positions,
        key=lambda state: (sum(positions[state]) / len(positions[state]), state),
    )


def is_counted(outcome: SessionOutcome, sources: Sequence[FailureSource]) -> bool:
    """Whether the outcome is a located failure of one of the selected kinds."""
    return outcome.point is not None and outcome.point.source in sources


def build_cells(
    base: Sequence[SessionOutcome],
    compare: Sequence[SessionOutcome] | None,
    sources: Sequence[FailureSource],
) -> tuple[list[str], list[str], list[MatrixCell]]:
    """Count failures and attempts per transition for one or two groups."""
    failures = _failure_counts(base, sources)
    attempts = _attempt_counts(base)
    compare_failures = _failure_counts(compare, sources) if compare is not None else {}
    compare_attempts = _attempt_counts(compare) if compare is not None else Counter()
    keys = set(failures) | set(attempts) | set(compare_failures) | set(compare_attempts)
    order = order_states([base, compare or ()])
    rank = {state: index for index, state in enumerate(order)}
    rows = [START, *sorted({a for a, _ in keys if a != START}, key=rank.__getitem__)]
    cols = sorted({b for _, b in keys}, key=rank.__getitem__)
    cells = []
    unrated = _unrated_transitions(base, sources)
    compare_unrated = _unrated_transitions(compare or (), sources)
    rank[START] = -1
    for key in sorted(keys, key=lambda k: (rank[k[0]], rank[k[1]])):
        by_source = failures.get(key, Counter())
        compared = compare_failures.get(key, Counter())
        cells.append(
            MatrixCell(
                from_state=key[0],
                to_state=key[1],
                count=by_source.total(),
                attempts=None if key in unrated else attempts.get(key),
                error_count=by_source["error"],
                annotation_count=by_source["annotation"],
                compare_count=compared.total() if compare is not None else None,
                compare_attempts=compare_attempts.get(key)
                if compare is not None and key not in compare_unrated
                else None,
            )
        )
    return rows, cols, cells


def _failure_counts(
    outcomes: Sequence[SessionOutcome], sources: Sequence[FailureSource]
) -> dict[Transition, Counter[str]]:
    counts: defaultdict[Transition, Counter[str]] = defaultdict(Counter)
    for outcome in outcomes:
        pair = outcome.failure_transition
        if pair is not None and outcome.point and is_counted(outcome, sources):
            counts[pair][outcome.point.source] += 1
    return counts


def _unrated_transitions(
    outcomes: Sequence[SessionOutcome], sources: Sequence[FailureSource]
) -> set[Transition]:
    # A failing step that is not a state can share its name with a real state
    # elsewhere; its cell must not borrow that state's attempts as a rate.
    return {
        pair
        for outcome in outcomes
        if not outcome.failure_is_state
        and is_counted(outcome, sources)
        and (pair := outcome.failure_transition) is not None
    }


def _attempt_counts(outcomes: Sequence[SessionOutcome]) -> Counter[Transition]:
    return Counter(pair for outcome in outcomes for pair in outcome.transitions)


def summarize_group(
    label: str,
    outcomes: Sequence[SessionOutcome],
    sources: Sequence[FailureSource],
    truncated: bool,
    records_capped: bool,
) -> GroupSummary:
    """Headline numbers and the evaluations behind unlocated failures."""
    unlocated = [o for o in outcomes if o.failed and o.point is None]
    return GroupSummary(
        label=label,
        session_count=len(outcomes),
        failed_count=sum(o.failed for o in outcomes),
        located_count=sum(o.point is not None for o in outcomes),
        shown_count=sum(is_counted(o, sources) for o in outcomes),
        unlocated_count=len(unlocated),
        unlocated_evaluations=[
            EvaluationCount(name=name, count=count)
            for name, count in Counter(
                name for o in unlocated for name in set(o.failed_evaluations)
            ).most_common(MAX_UNLOCATED_EVALUATIONS)
        ],
        truncated=truncated,
        records_capped=records_capped,
    )


def cell_details(
    outcomes: Sequence[SessionOutcome],
    transition: Transition,
    sources: Sequence[FailureSource],
    limit: int,
) -> FailureCellData:
    """Sessions and repeated notes for one transition cell."""
    matching = [
        o
        for o in outcomes
        if o.failure_transition == transition and is_counted(o, sources)
    ]
    notes = Counter(o.point.note for o in matching if o.point and o.point.note)
    patterns = [CellPattern(note=note, count=n) for note, n in notes.most_common(5)]
    sessions = [
        CellSession(
            session_id=o.session.id,
            number=o.session.number,
            name=_clip(o.session.name),
            source=o.point.source,
            failing_node=_bounded(o.point.node.name),
            note=o.point.note,
            path=list(o.path[-MAX_PATH:]),
            path_truncated=len(o.path) > MAX_PATH,
        )
        for o in matching[:limit]
        if o.point is not None
    ]
    return FailureCellData(
        total=len(matching),
        attempts=None
        if transition in _unrated_transitions(outcomes, sources)
        else _attempt_counts(outcomes).get(transition),
        patterns=patterns,
        sessions=sessions,
    )
