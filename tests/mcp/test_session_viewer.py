#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Session viewer tools: opening sessions, reading viewers, and saving notes."""

import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult, TextContent
from mcp_fakes import EchoAnnotations, build_server_context

from kitaru.api_models.v1.annotation import (
    AnnotationListParams,
    AnnotationResponse,
    AnnotationSelector,
    AnnotationSpan,
    InvestigationAnswerCreateRequest,
    ManualAnnotationCreateRequest,
)
from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.filter import (
    AndFilter,
    Filter,
    FilterCondition,
    FilterOp,
    OrFilter,
)
from kitaru.api_models.v1.investigation import (
    InvestigationResponse,
    InvestigationSessionQuestion,
    InvestigationSessionResponse,
    InvestigationSessionUpdateRequest,
    InvestigationSessionVerdict,
    InvestigationStatus,
    InvestigationUpdateRequest,
)
from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import NodeStatus, NodeType, SessionNodeResponse
from kitaru.client.exceptions import APIError
from kitaru.mcp.apps import SESSION_VIEWER_URI, load_session_viewer_html
from kitaru.mcp.server import create_server
from kitaru.mcp.settings import CapabilityMode, MCPSettings
from kitaru.mcp.tools.viewer import get_next_investigation_status
from kitaru.mcp.viewers import MAX_VIEWER_BYTES, list_viewers, read_viewer_source

NOW = datetime(2026, 11, 11, 9, 0, tzinfo=UTC)
SUMMARY = "bla is a TEM-1 beta-lactamase, which confers kanamycin resistance."
TOKEN_TEXT = "Authorization: Bearer sk-live-0123456789 then the refund was approved."


def _session(session_id: uuid.UUID) -> SessionDetailResponse:
    return SessionDetailResponse(
        id=session_id,
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        number=7,
        origin=SessionOrigin.IMPORTED,
        status=SessionStatus.COMPLETED,
        name="Annotate pKT-101",
        metadata={},
        llm_call_count=1,
        tool_call_count=0,
        created=NOW,
        updated=NOW,
        inputs={"sequence": "ATGAAATAA"},
        outputs={"summary": SUMMARY},
        output_text_selector="/summary",
    )


def _node(session_id: uuid.UUID, node_id: uuid.UUID, text: str) -> SessionNodeResponse:
    return SessionNodeResponse(
        id=node_id,
        session_id=session_id,
        external_id="n1",
        links=[],
        node_type=NodeType.LLM_CALL,
        name="write_annotation",
        status=NodeStatus.COMPLETED,
        output_text_selector="/content",
        reasoning_selectors=["/thinking"],
        inputs={"messages": []},
        outputs={"content": text, "thinking": "Check the strand."},
        metadata={},
    )


class _Sessions:
    def __init__(
        self, session: SessionDetailResponse, nodes: list[SessionNodeResponse]
    ) -> None:
        self.session, self.nodes = session, nodes

    async def get(self, session_id: uuid.UUID) -> SessionDetailResponse:
        return self.session

    async def list_nodes(
        self, session_id: uuid.UUID, params: Any = None
    ) -> Page[SessionNodeResponse]:
        return Page[SessionNodeResponse](items=self.nodes, next_cursor=None)

    async def iter_nodes(
        self, session_id: uuid.UUID, params: Any = None
    ) -> AsyncIterator[SessionNodeResponse]:
        for node in self.nodes:
            yield node


class _Annotations(EchoAnnotations):
    """Echo annotations that also list a stored set through the handler's filter."""

    def __init__(self, stored: list[AnnotationResponse]) -> None:
        self.stored = stored
        self.created: list[Any] = []
        self.keys: list[str | None] = []

    async def iter(
        self, params: AnnotationListParams
    ) -> AsyncIterator[AnnotationResponse]:
        ordered = self.stored[::-1] if params.sort == "created:desc" else self.stored
        for item in ordered:
            if _matches(params.filter, item.model_dump(mode="json")):
                yield item

    async def create(
        self,
        request: ManualAnnotationCreateRequest | InvestigationAnswerCreateRequest,
        idempotency_key: str | None = None,
    ) -> AnnotationResponse:
        self.created.append(request)
        self.keys.append(idempotency_key)
        return await super().create(request, idempotency_key)


def _matches(node: Filter | None, record: dict[str, Any]) -> bool:
    """Evaluate the equality, null, and boolean filters the viewer sends."""
    if node is None:
        return True
    if isinstance(node, AndFilter):
        return all(_matches(child, record) for child in node.and_)
    if isinstance(node, OrFilter):
        return any(_matches(child, record) for child in node.or_)
    assert isinstance(node, FilterCondition)
    if node.op is FilterOp.IS_NULL:
        return record[node.field] is None
    assert node.op is FilterOp.EQ
    return record[node.field] == node.value


class _Investigations:
    def __init__(
        self,
        investigation: InvestigationResponse,
        links: list[InvestigationSessionResponse],
    ) -> None:
        self.investigation, self.links = investigation, links
        self.status_updates: list[InvestigationStatus] = []

    async def get(self, investigation_id: uuid.UUID) -> InvestigationResponse:
        return self.investigation.model_copy(
            update={
                "total_sessions": len(self.links),
                "completed_sessions": sum(1 for link in self.links if link.verdict),
            }
        )

    async def iter_sessions(
        self, investigation_id: uuid.UUID, params: Any = None
    ) -> AsyncIterator[InvestigationSessionResponse]:
        for link in self.links:
            yield link

    async def update_session(
        self,
        investigation_id: uuid.UUID,
        session_id: uuid.UUID,
        request: InvestigationSessionUpdateRequest,
    ) -> InvestigationSessionResponse:
        for index, link in enumerate(self.links):
            if link.session_id == session_id:
                self.links[index] = link.model_copy(update={"verdict": request.verdict})
                return self.links[index]
        raise APIError(404, "missing")

    async def update(
        self, investigation_id: uuid.UUID, request: InvestigationUpdateRequest
    ) -> InvestigationResponse:
        assert request.status is not None
        self.status_updates.append(request.status)
        self.investigation = self.investigation.model_copy(
            update={"status": request.status}
        )
        return self.investigation


class _Info:
    async def get(self) -> object:
        raise APIError(503, "no info")


class _Client:
    def __init__(
        self, sessions: int = 2, annotations: list[AnnotationResponse] | None = None
    ) -> None:
        self.session_id, self.node_id, self.investigation_id = (
            uuid.uuid4(),
            uuid.uuid4(),
            uuid.uuid4(),
        )
        self.sessions = _Sessions(
            _session(self.session_id), [_node(self.session_id, self.node_id, SUMMARY)]
        )
        self.links = [
            InvestigationSessionResponse(
                id=uuid.uuid4(),
                investigation_id=self.investigation_id,
                session_id=self.session_id if position == 0 else uuid.uuid4(),
                position=position,
                questions=[
                    InvestigationSessionQuestion(
                        key="observation", question="What do you notice?"
                    )
                ],
                verdict=None,
                created=NOW,
                updated=NOW,
            )
            for position in range(sessions)
        ]
        self.investigations = _Investigations(
            InvestigationResponse(
                id=self.investigation_id,
                owner_id=uuid.uuid4(),
                agent_id=uuid.uuid4(),
                name="feature-calls",
                description=None,
                status=InvestigationStatus.PENDING,
                metadata={},
                total_sessions=sessions,
                completed_sessions=0,
                created=NOW,
                updated=NOW,
            ),
            self.links,
        )
        self.annotations = _Annotations(annotations or [])
        self.info = _Info()

    async def close(self) -> None:
        return None


def _context(
    client: _Client,
    mode: CapabilityMode = CapabilityMode.STANDARD,
    viewers_dir: Path | None = None,
) -> tuple[Any, Any]:
    return build_server_context(
        client, settings=MCPSettings(mode=mode, viewers_dir=viewers_dir)
    )


async def _call(
    server: Any, context: Any, name: str, request: dict[str, Any]
) -> dict[str, Any]:
    result = await server.call_tool(name, {"request": request}, context)
    assert isinstance(result, CallToolResult)
    return cast(dict[str, Any], result.structured_content)


async def test_an_investigation_opens_with_its_questions_and_readback_advice() -> None:
    client = _Client()
    server, context = _context(client)

    result = await server.call_tool(
        "kitaru_session_view",
        {"request": {"investigation_id": str(client.investigation_id)}},
        context,
    )

    assert isinstance(result, CallToolResult) and not result.is_error
    data = cast(dict[str, Any], result.structured_content)["data"]
    assert data["session_id"] == str(client.session_id)
    assert (data["position"], data["session_count"]) == (0, 2)
    assert data["questions"] == [
        {"key": "observation", "question": "What do you notice?"}
    ]
    assert data["can_write"] is True
    assert isinstance(result.content[0], TextContent)
    text = result.content[0].text
    assert "Question observation: What do you notice?" in text
    assert "Never write answers on the reviewer's behalf." in text


async def test_the_view_result_names_the_session_without_its_payloads() -> None:
    client = _Client()
    server, context = _context(client)

    envelope = await _call(
        server, context, "kitaru_session_view", {"session_id": str(client.session_id)}
    )

    # Some hosts give the model this structured result, so a session's
    # payloads must stay out of it.
    assert envelope["ok"] is True
    assert SUMMARY not in json.dumps(envelope)
    assert "nodes" not in envelope["data"]


async def test_the_viewer_page_reads_the_whole_session() -> None:
    client = _Client()
    server, context = _context(client)

    envelope = await _call(
        server,
        context,
        "kitaru_viewer_read",
        {
            "operation": "view",
            "investigation_id": str(client.investigation_id),
            "viewer": "default",
        },
    )

    data = envelope["data"]
    assert data["session"]["id"] == str(client.session_id)
    assert (
        data["investigation"]["investigation_session"]["questions"][0]["key"]
        == "observation"
    )
    assert [entry["position"] for entry in data["investigation"]["sessions"]] == [0, 1]
    assert data["capabilities"] == {"mode": "standard", "can_write": True}
    # Viewers resolve `/reasoning/...` paths against the stitched reasoning.
    assert data["nodes"][0]["reasoning"] == "Check the strand."


async def test_a_position_past_the_last_session_names_the_valid_range() -> None:
    client = _Client(sessions=2)
    server, context = _context(client)

    envelope = await _call(
        server,
        context,
        "kitaru_session_view",
        {"investigation_id": str(client.investigation_id), "position": 5},
    )

    assert envelope["ok"] is False
    assert "0 to 1" in envelope["error"]["message"]


async def test_read_only_mode_opens_the_viewer_without_write_access() -> None:
    client = _Client()
    server, context = _context(client, mode=CapabilityMode.READ_ONLY)

    envelope = await _call(
        server, context, "kitaru_session_view", {"session_id": str(client.session_id)}
    )

    assert envelope["data"]["can_write"] is False
    assert "kitaru_viewer_write" not in [
        tool.name for tool in await server.list_tools()
    ]


async def test_answers_to_another_investigation_stay_out_of_the_view() -> None:
    client = _Client()
    other = AnnotationResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        session_id=client.session_id,
        investigation_session_id=uuid.uuid4(),
        question_key="x",
        value="other review",
        created=NOW,
        updated=NOW,
    )
    mine = AnnotationResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        session_id=client.session_id,
        investigation_session_id=client.links[0].id,
        question_key="observation",
        value="mine",
        created=NOW,
        updated=NOW,
    )
    manual = AnnotationResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        session_id=client.session_id,
        value="a note",
        created=NOW,
        updated=NOW,
    )
    client.annotations.stored = [other, mine, manual]
    server, context = _context(client)

    envelope = await _call(
        server,
        context,
        "kitaru_viewer_read",
        {"operation": "view", "investigation_id": str(client.investigation_id)},
    )

    assert [item["value"] for item in envelope["data"]["annotations"]] == [
        "mine",
        "a note",
    ]


async def test_viewers_are_read_fresh_from_the_viewers_folder(tmp_path: Path) -> None:
    (tmp_path / "gene-map.html").write_text("<p>first</p>")
    (tmp_path / "Not A Viewer.html").write_text("ignored")
    (tmp_path / "default.html").write_text("cannot replace the built-in")
    client = _Client()
    server, context = _context(client, viewers_dir=tmp_path)

    first = await _call(
        server,
        context,
        "kitaru_viewer_read",
        {"operation": "viewer_source", "viewer": "gene-map"},
    )
    (tmp_path / "gene-map.html").write_text("<p>edited</p>")
    second = await _call(
        server,
        context,
        "kitaru_viewer_read",
        {"operation": "viewer_source", "viewer": "gene-map"},
    )
    builtin = await _call(
        server,
        context,
        "kitaru_viewer_read",
        {"operation": "viewer_source", "viewer": "default"},
    )

    assert (first["data"]["html"], second["data"]["html"]) == (
        "<p>first</p>",
        "<p>edited</p>",
    )
    # The built-in viewer uses the runtime the loader page provides.
    assert "kitaru.onSession(render)" in builtin["data"]["html"]
    assert "window.kitaru = Object.freeze" not in builtin["data"]["html"]
    assert list_viewers(MCPSettings(viewers_dir=tmp_path)) == ["default", "gene-map"]


async def test_a_missing_viewer_lists_the_ones_that_exist(tmp_path: Path) -> None:
    (tmp_path / "line-schedule.html").write_text("<p>plan</p>")
    client = _Client()
    server, context = _context(client, viewers_dir=tmp_path)

    envelope = await _call(
        server,
        context,
        "kitaru_session_view",
        {"session_id": str(client.session_id), "viewer": "gene-map"},
    )

    assert envelope["error"]["code"] == "not_found"
    assert envelope["error"]["details"]["available"] == ["default", "line-schedule"]


@pytest.mark.parametrize("name", ["../secrets", "gene.map", "Gene", ""])
async def test_viewer_names_cannot_leave_the_viewers_folder(
    name: str, tmp_path: Path
) -> None:
    client = _Client()
    server, context = _context(client, viewers_dir=tmp_path)

    with pytest.raises(ToolError):
        await server.call_tool(
            "kitaru_viewer_read",
            {"request": {"operation": "viewer_source", "viewer": name}},
            context,
        )


def test_read_viewer_source_rejects_an_oversized_file(tmp_path: Path) -> None:
    (tmp_path / "huge.html").write_bytes(b"x" * (MAX_VIEWER_BYTES + 1))
    with pytest.raises(Exception, match="limit"):
        read_viewer_source(MCPSettings(viewers_dir=tmp_path), "huge")


async def _note(
    client: _Client,
    span: tuple[int, int],
    quote: str,
    path: str = "/outputs/content",
    *,
    on_node: bool = True,
) -> dict[str, Any]:
    selector: dict[str, Any] = {
        "path": path,
        "span": dict(zip(("start", "end"), span, strict=True)),
    }
    if on_node:
        selector["node_id"] = str(client.node_id)
    server, context = _context(client)
    return await _call(
        server,
        context,
        "kitaru_viewer_write",
        {
            "operation": "note",
            "session_id": str(client.session_id),
            "selector": selector,
            "quote": quote,
            "text": "Wrong antibiotic.",
            "idempotency_key": "key-1",
        },
    )


async def test_a_matching_span_is_saved_as_selected() -> None:
    client = _Client()
    start = SUMMARY.index("kanamycin")
    envelope = await _note(client, (start, start + 9), "kanamycin")

    assert envelope["ok"] and envelope["warnings"] == []
    request = client.annotations.created[0]
    assert request.selector.span == AnnotationSpan(start=start, end=start + 9)
    assert request.value == "Wrong antibiotic."
    assert client.annotations.keys == ["key-1"]


async def test_a_span_counted_in_reformatted_text_moves_to_where_the_quote_is() -> None:
    client = _Client()
    # A viewer that rendered "**bla**" as bold counts four characters fewer.
    start = SUMMARY.index("kanamycin") - 4
    envelope = await _note(client, (start, start + 9), "kanamycin")

    span = client.annotations.created[0].selector.span
    assert SUMMARY[span.start : span.end] == "kanamycin"
    assert "moved" in envelope["warnings"][0]


async def test_a_span_measured_in_redacted_text_lands_on_the_stored_words() -> None:
    client = _Client()
    client.sessions.nodes = [_node(client.session_id, client.node_id, TOKEN_TEXT)]
    # The viewer saw "Bearer ***", so every offset after it is short.
    shown = TOKEN_TEXT.replace("sk-live-0123456789", "***")
    start = shown.index("refund")
    await _note(client, (start, start + 6), "refund")

    span = client.annotations.created[0].selector.span
    assert TOKEN_TEXT[span.start : span.end] == "refund"


async def test_an_ambiguous_quote_keeps_the_note_on_the_whole_field() -> None:
    client = _Client()
    envelope = await _note(client, (0, 1), "a")

    selector = client.annotations.created[0].selector
    assert (selector.path, selector.span) == ("/outputs/content", None)
    assert "more than once" in envelope["warnings"][0]


async def test_a_path_outside_the_record_is_refused() -> None:
    client = _Client()
    envelope = await _note(client, (0, 1), "x", "/outputs/missing")

    assert envelope["ok"] is False and client.annotations.created == []


async def test_a_session_level_span_is_checked_against_the_session_record() -> None:
    client = _Client()
    start = SUMMARY.index("TEM-1")
    envelope = await _note(
        client, (start, start + 5), "TEM-1", "/outputs/summary", on_node=False
    )

    assert envelope["ok"] and client.annotations.created[0].selector.span.start == start


async def test_an_answer_must_belong_to_the_named_investigation() -> None:
    client = _Client()
    server, context = _context(client)

    envelope = await _call(
        server,
        context,
        "kitaru_viewer_write",
        {
            "operation": "answer",
            "investigation_id": str(client.investigation_id),
            "investigation_session_id": str(uuid.uuid4()),
            "question_key": "observation",
            "text": "x",
            "selector": {"node_id": str(client.node_id), "path": "/outputs/content"},
        },
    )
    saved = await _call(
        server,
        context,
        "kitaru_viewer_write",
        {
            "operation": "answer",
            "investigation_id": str(client.investigation_id),
            "investigation_session_id": str(client.links[0].id),
            "question_key": "observation",
            "text": "Wrong strand.",
            "selector": {"node_id": str(client.node_id), "path": "/outputs/content"},
        },
    )

    assert envelope["error"]["code"] == "not_found"
    assert saved["ok"] and saved["data"]["question_key"] == "observation"
    request = client.annotations.created[0]
    assert (
        isinstance(request, InvestigationAnswerCreateRequest)
        and request.value == "Wrong strand."
    )
    assert request.selector == AnnotationSelector(
        node_id=client.node_id, path="/outputs/content"
    )


async def test_an_answer_whose_highlight_no_longer_resolves_is_still_saved() -> None:
    client = _Client()
    server, context = _context(client)

    envelope = await _call(
        server,
        context,
        "kitaru_viewer_write",
        {
            "operation": "answer",
            "investigation_id": str(client.investigation_id),
            "investigation_session_id": str(client.links[0].id),
            "question_key": "observation",
            "text": "Wrong strand.",
            "selector": {"node_id": str(client.node_id), "path": "/outputs/gone"},
        },
    )

    assert envelope["ok"] is True
    assert client.annotations.created[0].selector is None
    assert "saved without one" in envelope["warnings"][0]


async def test_a_span_after_an_emoji_counts_utf16_units_like_the_browser() -> None:
    client = _Client()
    text = "\U0001f9ec " + SUMMARY
    client.sessions.nodes = [_node(client.session_id, client.node_id, text)]
    # JavaScript counts the emoji as two units, Python as one character.
    start = text.index("kanamycin") + 1
    envelope = await _note(client, (start, start + 9), "kanamycin")

    assert envelope["warnings"] == []
    assert client.annotations.created[0].selector.span == AnnotationSpan(
        start=start, end=start + 9
    )


async def test_a_capped_annotation_read_keeps_the_newest_in_creation_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kitaru.mcp.tools.viewer.MAX_ANNOTATIONS", 2)
    client = _Client()
    client.annotations.stored = [
        AnnotationResponse(
            id=uuid.uuid4(),
            owner_id=uuid.uuid4(),
            session_id=client.session_id,
            value=value,
            created=NOW,
            updated=NOW,
        )
        for value in ("oldest", "middle", "newest")
    ]
    server, context = _context(client)

    envelope = await _call(
        server,
        context,
        "kitaru_viewer_read",
        {"operation": "view", "session_id": str(client.session_id)},
    )

    data = envelope["data"]
    assert [item["value"] for item in data["annotations"]] == ["middle", "newest"]
    assert data["annotations_truncated"] is True


async def test_viewer_source_is_not_masked_like_session_data(tmp_path: Path) -> None:
    source = "<script>const label = `Bearer ${token}`;</script>"
    (tmp_path / "auth.html").write_text(source)
    client = _Client()
    server, context = _context(client, viewers_dir=tmp_path)

    envelope = await _call(
        server,
        context,
        "kitaru_viewer_read",
        {"operation": "viewer_source", "viewer": "auth"},
    )

    assert envelope["data"]["html"] == source


def test_a_blank_viewers_folder_variable_means_the_default_folder() -> None:
    settings = MCPSettings.from_environment({"KITARU_MCP_VIEWERS_DIR": " "})

    assert settings.viewers_dir is None


async def test_verdicts_move_the_investigation_like_the_kitaru_ui_does() -> None:
    client = _Client(sessions=2)
    server, context = _context(client)

    first = await _call(
        server,
        context,
        "kitaru_viewer_write",
        {
            "operation": "verdict",
            "investigation_id": str(client.investigation_id),
            "session_id": str(client.links[0].session_id),
            "verdict": "problematic",
        },
    )
    second = await _call(
        server,
        context,
        "kitaru_viewer_write",
        {
            "operation": "verdict",
            "investigation_id": str(client.investigation_id),
            "session_id": str(client.links[1].session_id),
            "verdict": "acceptable",
        },
    )

    assert (
        first["data"]["investigation_status"],
        second["data"]["investigation_status"],
    ) == ("in_progress", "completed")
    assert client.investigations.status_updates == [
        InvestigationStatus.IN_PROGRESS,
        InvestigationStatus.COMPLETED,
    ]


@pytest.mark.parametrize(
    ("current", "total", "reviewed", "expected"),
    [
        (InvestigationStatus.PENDING, 3, 0, None),
        (InvestigationStatus.PENDING, 3, 1, InvestigationStatus.IN_PROGRESS),
        (InvestigationStatus.IN_PROGRESS, 3, 2, None),
        (InvestigationStatus.IN_PROGRESS, 3, 3, InvestigationStatus.COMPLETED),
        (InvestigationStatus.PENDING, 1, 1, InvestigationStatus.COMPLETED),
        (InvestigationStatus.COMPLETED, 3, 1, None),
        (InvestigationStatus.PENDING, 0, 0, None),
    ],
)
def test_get_next_investigation_status(
    current: InvestigationStatus,
    total: int,
    reviewed: int,
    expected: InvestigationStatus | None,
) -> None:
    assert get_next_investigation_status(current, total, reviewed) is expected


async def test_viewer_tools_are_app_only_and_the_view_tool_opens_the_page() -> None:
    tools = {
        tool.name: tool
        for tool in await create_server(
            MCPSettings(mode=CapabilityMode.STANDARD)
        ).list_tools()
    }

    assert tools["kitaru_session_view"].meta == {
        "ui": {"resourceUri": SESSION_VIEWER_URI}
    }
    for name in ("kitaru_viewer_read", "kitaru_viewer_write"):
        assert tools[name].meta == {
            "ui": {"resourceUri": SESSION_VIEWER_URI, "visibility": ["app"]}
        }


def test_the_loader_page_inlines_the_viewer_runtime() -> None:
    html = load_session_viewer_html()

    assert "{{" not in html
    assert "window.kitaru = Object.freeze" in html
    assert "</script>" not in html.split("<script>", 1)[1].rsplit("</script>", 1)[0]


def test_session_view_requires_exactly_one_target() -> None:
    from pydantic import ValidationError

    from kitaru.mcp.models.viewer import SessionViewRequest

    for values in (
        {},
        {"session_id": uuid.uuid4(), "investigation_id": uuid.uuid4()},
        {"session_id": uuid.uuid4(), "position": 1},
    ):
        with pytest.raises(ValidationError):
            SessionViewRequest.model_validate(values)


async def test_an_unchanged_verdict_does_not_rewrite_a_completed_investigation() -> (
    None
):
    client = _Client(sessions=1)
    client.investigations.investigation = (
        client.investigations.investigation.model_copy(
            update={"status": InvestigationStatus.COMPLETED}
        )
    )
    client.links[0] = client.links[0].model_copy(
        update={"verdict": InvestigationSessionVerdict.ACCEPTABLE}
    )
    server, context = _context(client)

    envelope = await _call(
        server,
        context,
        "kitaru_viewer_write",
        {
            "operation": "verdict",
            "investigation_id": str(client.investigation_id),
            "session_id": str(client.session_id),
            "verdict": None,
        },
    )

    assert envelope["data"]["investigation_status"] == "completed"
    assert client.investigations.status_updates == []
