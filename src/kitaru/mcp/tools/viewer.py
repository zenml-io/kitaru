#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Trace viewer handlers: open a session, read viewer pages, save reviewer notes."""

import asyncio
import uuid
from typing import Any, cast

from kitaru.api_models.v1.annotation import (
    AnnotationListParams,
    AnnotationResponse,
    AnnotationSelector,
    AnnotationUpdateRequest,
    InvestigationAnswerCreateRequest,
    ManualAnnotationCreateRequest,
)
from kitaru.api_models.v1.filter import AndFilter, FilterCondition, FilterOp, OrFilter
from kitaru.api_models.v1.investigation import (
    InvestigationSessionResponse,
    InvestigationSessionUpdateRequest,
    InvestigationStatus,
    InvestigationUpdateRequest,
)
from kitaru.api_models.v1.session_node import (
    SessionNodeListParams,
    SessionNodeResponse,
)
from kitaru.json_pointer import resolve_json_pointer
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.links import get_dashboard_info, get_session_url
from kitaru.mcp.models.common import ToolSuccessPayload
from kitaru.mcp.models.viewer import (
    InvestigationPosition,
    SessionViewData,
    SessionViewOpened,
    SessionViewRequest,
    ViewerAnswer,
    ViewerCapabilities,
    ViewerEdit,
    ViewerInvestigation,
    ViewerNodesPage,
    ViewerNodesRead,
    ViewerQuestion,
    ViewerReadRequest,
    ViewerSource,
    ViewerSourceRead,
    ViewerVerdict,
    ViewerViewRead,
    ViewerWriteRequest,
)
from kitaru.mcp.settings import CapabilityMode
from kitaru.mcp.tools.paging import read_up_to
from kitaru.mcp.viewers import ensure_viewer_exists, read_viewer_source

FIRST_NODE_PAGE = 100
MAX_ANNOTATIONS = 1000
ANNOTATION_PAGE = 500
NODE_SCAN_PAGE = 500
# Finding a node for an anchor check scans pages without payloads; the cap
# keeps one save from walking an unbounded session.
MAX_ANCHOR_SCAN_NODES = 20_000


async def handle_session_view(
    state: MCPServerState, request: SessionViewRequest
) -> ToolSuccessPayload:
    """Check that a session can open in a viewer and summarize it."""
    ensure_viewer_exists(state.settings, request.viewer)
    investigation, link, session_id = await _resolve_target(state, request)
    session = await state.client.sessions.get(session_id)
    # The page reads the payloads itself through kitaru_viewer_read: some hosts
    # put a tool's structured result in the model's context, and one session's
    # payloads can run to megabytes.
    opened = SessionViewOpened(
        viewer=request.viewer,
        session_id=session.id,
        session_name=session.name,
        llm_call_count=session.llm_call_count,
        tool_call_count=session.tool_call_count,
        investigation_id=investigation.id if investigation else None,
        investigation_name=investigation.name if investigation else None,
        position=investigation.position if investigation else None,
        session_count=len(investigation.sessions) if investigation else None,
        questions=[
            ViewerQuestion(key=item.key, question=item.question)
            for item in (link.questions if link else [])
        ],
        can_write=_can_write(state),
    )
    return ToolSuccessPayload(data=opened, text=describe_view(opened))


async def handle_viewer_read(
    state: MCPServerState, request: ViewerReadRequest
) -> ToolSuccessPayload | ViewerNodesPage:
    """Read a session for a viewer page, a viewer's HTML, or more nodes."""
    if isinstance(request, ViewerViewRead):
        return await _read_view(state, request)
    if isinstance(request, ViewerSourceRead):
        source = ViewerSource(
            viewer=request.viewer,
            html=read_viewer_source(state.settings, request.viewer),
        )
        # Masking credential-shaped text would rewrite the viewer's own code,
        # for example a template literal that builds a `Bearer` header label.
        return ToolSuccessPayload(data=source, redact=False)
    return await _read_node_page(state, request)


async def _read_view(
    state: MCPServerState, request: ViewerViewRead
) -> ToolSuccessPayload:
    ensure_viewer_exists(state.settings, request.viewer)
    investigation, link, session_id = await _resolve_target(state, request)
    (
        session,
        page,
        (annotations, annotations_truncated),
        (_, dashboard, warnings),
    ) = await asyncio.gather(
        state.client.sessions.get(session_id),
        state.client.sessions.list_nodes(
            session_id,
            SessionNodeListParams(size=FIRST_NODE_PAGE, include_payloads=True),
        ),
        _read_annotations(state, session_id, link),
        get_dashboard_info(
            state, warning="The Kitaru UI link for this session could not be resolved."
        ),
    )
    data = SessionViewData(
        viewer=request.viewer,
        session=session.model_dump(mode="json"),
        nodes=[get_node_record(node) for node in page.items],
        nodes_cursor=page.next_cursor,
        annotations=[item.model_dump(mode="json") for item in annotations],
        annotations_truncated=annotations_truncated,
        investigation=investigation,
        capabilities=ViewerCapabilities(
            mode=state.settings.mode.value, can_write=_can_write(state)
        ),
        links={"session": get_session_url(dashboard, session_id)} if dashboard else {},
    )
    return ToolSuccessPayload(data=data, warnings=warnings)


def _can_write(state: MCPServerState) -> bool:
    return state.settings.mode.rank >= CapabilityMode.STANDARD.rank


async def _resolve_target(
    state: MCPServerState, request: SessionViewRequest
) -> tuple[ViewerInvestigation | None, InvestigationSessionResponse | None, uuid.UUID]:
    if request.investigation_id is None:
        # The request model requires a session id whenever no investigation is set.
        return None, None, cast(uuid.UUID, request.session_id)
    investigation, link = await _read_investigation(
        state, request.investigation_id, request.position
    )
    return investigation, link, link.session_id


async def _read_node_page(
    state: MCPServerState, request: ViewerNodesRead
) -> ViewerNodesPage:
    page = await state.client.sessions.list_nodes(
        request.session_id,
        SessionNodeListParams(
            cursor=request.cursor, size=request.size, include_payloads=True
        ),
    )
    return ViewerNodesPage(
        nodes=[get_node_record(node) for node in page.items],
        nodes_cursor=page.next_cursor,
    )


async def handle_viewer_write(
    state: MCPServerState, request: ViewerWriteRequest
) -> ToolSuccessPayload:
    """Save one reviewer note, answer, edit, or verdict."""
    if isinstance(request, ViewerEdit):
        updated = await state.client.annotations.update(
            request.annotation_id, AnnotationUpdateRequest(value=request.text)
        )
        return ToolSuccessPayload(data=updated.model_dump(mode="json"))
    if isinstance(request, ViewerVerdict):
        return ToolSuccessPayload(data=await _set_verdict(state, request))
    selector, warnings = request.selector, []
    create: InvestigationAnswerCreateRequest | ManualAnnotationCreateRequest
    if isinstance(request, ViewerAnswer):
        # The server resolves an answer's session itself; it is needed here only
        # to check a selector that points into the session's text.
        if selector is not None and selector.path is not None:
            link = await _find_investigation_session(
                state, request.investigation_id, request.investigation_session_id
            )
            # An answer usually inherits its question's highlight, which can
            # point at data the session no longer has; the reviewer's answer
            # still counts without a location.
            selector, warnings = await check_anchor(
                state, link.session_id, selector, request.quote, drop_unresolved=True
            )
        create = InvestigationAnswerCreateRequest(
            investigation_session_id=request.investigation_session_id,
            question_key=request.question_key,
            selector=selector,
            value=request.text,
        )
    else:
        selector, warnings = await check_anchor(
            state, request.session_id, selector, request.quote
        )
        create = ManualAnnotationCreateRequest(
            session_id=request.session_id, selector=selector, value=request.text
        )
    created = await state.client.annotations.create(
        create, idempotency_key=request.idempotency_key
    )
    return ToolSuccessPayload(data=created.model_dump(mode="json"), warnings=warnings)


def get_node_record(node: SessionNodeResponse) -> dict[str, Any]:
    """Dump a node under its wire keys, with the stitched reasoning added.

    Annotation paths such as `/reasoning/...` resolve against this record,
    the same one the Kitaru UI resolves them against.
    """
    record = node.model_dump(mode="json")
    record["reasoning"] = node.reasoning
    return record


async def check_anchor(
    state: MCPServerState,
    session_id: uuid.UUID,
    selector: AnnotationSelector | None,
    quote: str | None,
    *,
    drop_unresolved: bool = False,
) -> tuple[AnnotationSelector | None, list[str]]:
    """Check a span against the stored text and correct it when it is off.

    A viewer computes offsets in the text it rendered, which differs from the
    stored text when the viewer reformats it or when MCP output masked a
    credential inside it. The stored string is the one a later reader cuts the
    span from, so it decides: a matching span is kept, a quote found exactly
    once moves the span there, and anything else keeps the note on the whole
    field rather than on characters the reviewer never selected.

    With `drop_unresolved`, a selector whose node or path the session does not
    have is dropped with a warning instead of refused.

    Raises:
        MCPToolError: The selector's node or path does not resolve in the
            session, and `drop_unresolved` is off.
    """
    if selector is None or selector.path is None:
        return selector, []
    record = await _get_anchor_record(state, session_id, selector.node_id)
    found, value = (
        resolve_json_pointer(record, selector.path) if record else (False, None)
    )
    if not found:
        if drop_unresolved:
            return None, [
                "The location does not resolve in this session, so the answer "
                "is saved without one."
            ]
        target = "node" if selector.node_id else "session"
        raise MCPToolError(
            "invalid_arguments",
            f"Node {selector.node_id} was not found in session {session_id}."
            if record is None
            else f"The path {selector.path!r} does not resolve in the {target} "
            "it targets.",
            recovery="Anchor notes on elements whose data-annot-path names a "
            "field of the rendered record.",
        )
    span = selector.span
    if span is None or quote is None:
        return selector, []
    if not isinstance(value, str):
        return selector.model_copy(update={"span": None}), [
            "The selected value is not text, so the note covers the whole field."
        ]
    # Spans count UTF-16 code units, as JavaScript and the Kitaru UI do, so text
    # with an emoji before the selection keeps the same offsets on both sides.
    if _slice_utf16(value, span.start, span.end) == quote:
        return selector, []
    first = value.find(quote) if quote else -1
    if first != -1 and value.find(quote, first + 1) == -1:
        start = _count_utf16(value[:first])
        moved = span.model_copy(
            update={"start": start, "end": start + _count_utf16(quote)}
        )
        return selector.model_copy(update={"span": moved}), [
            "The selection was moved to where the selected text appears in the "
            "stored text."
        ]
    reason = "does not appear" if first == -1 else "appears more than once"
    return selector.model_copy(update={"span": None}), [
        f"The selected text {reason} in the stored text, so the note covers "
        "the whole field."
    ]


def _count_utf16(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _slice_utf16(text: str, start: int, end: int) -> str:
    # A span may split a surrogate pair; the replacement character then fails
    # the comparison instead of raising.
    units = text.encode("utf-16-le")
    return units[2 * start : 2 * end].decode("utf-16-le", errors="replace")


def get_next_investigation_status(
    current: InvestigationStatus, total: int, reviewed: int
) -> InvestigationStatus | None:
    """Return the status an investigation moves to after a verdict, if any.

    A pending investigation moves to in_progress once any session has a
    verdict, and any open investigation moves to completed once every session
    has one.
    """
    if current is InvestigationStatus.COMPLETED:
        return None
    if total > 0 and reviewed >= total:
        return InvestigationStatus.COMPLETED
    if reviewed > 0 and current is InvestigationStatus.PENDING:
        return InvestigationStatus.IN_PROGRESS
    return None


def describe_view(opened: SessionViewOpened) -> str:
    """Summarize an opened session for the model and for hosts without UI."""
    label = opened.session_name or str(opened.session_id)
    lines = [
        f"Opened session {label} ({opened.session_id}) in the `{opened.viewer}` "
        f"viewer: {opened.llm_call_count} LLM calls, "
        f"{opened.tool_call_count} tool calls."
    ]
    if opened.investigation_id is not None:
        lines.append(
            f'Investigation "{opened.investigation_name}": session '
            f"{(opened.position or 0) + 1} of {opened.session_count}."
        )
        lines.extend(
            f"  Question {item.key}: {item.question}" for item in opened.questions
        )
    lines.append(
        "The reviewer writes notes, answers, and verdicts in the viewer; read them "
        "back with kitaru_review_read. Never write answers on the reviewer's behalf."
        if opened.can_write
        else "Viewing only: the Kitaru MCP server runs in read-only mode, so the "
        "viewer cannot save notes. Restart it with --mode standard to annotate."
    )
    lines.append(
        "Hosts without MCP Apps show only this text; read the session with "
        "kitaru_session_read instead."
    )
    return "\n".join(lines)


async def _read_investigation(
    state: MCPServerState, investigation_id: uuid.UUID, position: int
) -> tuple[ViewerInvestigation, InvestigationSessionResponse]:
    investigation, links = await asyncio.gather(
        state.client.investigations.get(investigation_id),
        _list_investigation_sessions(state, investigation_id),
    )
    if not links:
        raise MCPToolError(
            "not_found", f"Investigation {investigation_id} has no sessions."
        )
    if position >= len(links):
        raise MCPToolError(
            "invalid_arguments",
            f"Position {position} is outside the investigation's "
            f"{len(links)} sessions (0 to {len(links) - 1}).",
        )
    link = links[position]
    return (
        ViewerInvestigation(
            id=investigation.id,
            name=investigation.name,
            description=investigation.description,
            status=investigation.status.value,
            position=position,
            investigation_session=link.model_dump(mode="json"),
            sessions=[
                InvestigationPosition(
                    position=index, session_id=item.session_id, verdict=item.verdict
                )
                for index, item in enumerate(links)
            ],
        ),
        link,
    )


async def _list_investigation_sessions(
    state: MCPServerState, investigation_id: uuid.UUID
) -> list[InvestigationSessionResponse]:
    return [
        item
        async for item in state.client.investigations.iter_sessions(investigation_id)
    ]


def _eq(field: str, value: object) -> FilterCondition:
    return FilterCondition(field=field, op=FilterOp.EQ, value=str(value))


async def _read_annotations(
    state: MCPServerState,
    session_id: uuid.UUID,
    link: InvestigationSessionResponse | None,
) -> tuple[list[AnnotationResponse], bool]:
    # Answers to other investigations of this session belong to those reviews;
    # showing them here would mix two reviewers' questions.
    manual = FilterCondition(field="investigation_session_id", op=FilterOp.IS_NULL)
    scope = (
        OrFilter.model_validate(
            {"or": [manual, _eq("investigation_session_id", link.id)]}
        )
        if link is not None
        else manual
    )
    params = AnnotationListParams(
        filter=AndFilter.model_validate(
            {"and": [_eq("session_id", session_id), scope]}
        ),
        size=ANNOTATION_PAGE,
        sort="created:desc",
    )
    # Read newest first so a capped read keeps the latest answers, which the
    # viewer edits rather than duplicates; then restore creation order.
    items, capped = await read_up_to(
        state.client.annotations.iter(params), MAX_ANNOTATIONS
    )
    return items[::-1], capped


async def _find_investigation_session(
    state: MCPServerState,
    investigation_id: uuid.UUID,
    investigation_session_id: uuid.UUID,
) -> InvestigationSessionResponse:
    for item in await _list_investigation_sessions(state, investigation_id):
        if item.id == investigation_session_id:
            return item
    raise MCPToolError(
        "not_found",
        f"Investigation session {investigation_session_id} is not part of "
        f"investigation {investigation_id}.",
    )


async def _get_anchor_record(
    state: MCPServerState, session_id: uuid.UUID, node_id: uuid.UUID | None
) -> dict[str, Any] | None:
    if node_id is None:
        session = await state.client.sessions.get(session_id)
        return session.model_dump(mode="json")
    # Nodes cannot be fetched by id, so find the page holding this one without
    # payloads, then read only that page with them.
    cursor: str | None = None
    scanned = 0
    while scanned < MAX_ANCHOR_SCAN_NODES:
        page = await state.client.sessions.list_nodes(
            session_id, SessionNodeListParams(size=NODE_SCAN_PAGE, cursor=cursor)
        )
        if any(node.id == node_id for node in page.items):
            full = await state.client.sessions.list_nodes(
                session_id,
                SessionNodeListParams(
                    size=NODE_SCAN_PAGE, cursor=cursor, include_payloads=True
                ),
            )
            node = next(node for node in full.items if node.id == node_id)
            return get_node_record(node)
        scanned += len(page.items)
        if page.next_cursor is None:
            break
        cursor = page.next_cursor
    return None


async def _set_verdict(state: MCPServerState, request: ViewerVerdict) -> dict[str, Any]:
    updated = await state.client.investigations.update_session(
        request.investigation_id,
        request.session_id,
        InvestigationSessionUpdateRequest(verdict=request.verdict),
    )
    # Read after the update, so the counts include this verdict.
    investigation = await state.client.investigations.get(request.investigation_id)
    # The server starts an investigation on its first answer only and never
    # completes one, so the reviewer's client moves it, as the Kitaru UI does.
    status = get_next_investigation_status(
        investigation.status,
        investigation.total_sessions,
        investigation.completed_sessions,
    )
    if status is not None:
        investigation = await state.client.investigations.update(
            request.investigation_id, InvestigationUpdateRequest(status=status)
        )
    return {
        "investigation_session": updated.model_dump(mode="json"),
        "investigation_status": investigation.status.value,
    }
