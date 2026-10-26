#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Review-tool contracts for associating insights with imports."""

import uuid
from types import SimpleNamespace

import pytest
from mcp.types import CallToolResult
from pydantic import TypeAdapter, ValidationError
from review_helpers import build_context, build_insight

from kitaru.api_models.v1.insight import (
    InsightInput,
    InsightResponse,
    InsightUpdateRequest,
)
from kitaru.mcp.models.review import (
    ReviewManageRequest,
)
from kitaru.mcp.server import create_server
from kitaru.mcp.settings import CapabilityMode, MCPSettings


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({"import_id": None}, "import_id cannot be null without clear_import"),
        (
            {"import_id": str(uuid.uuid4()), "clear_import": True},
            "import_id and clear_import conflict",
        ),
        ({"clear_import": False}, "change at least one"),
        ({"import_id": "invalid"}, "valid UUID"),
    ],
)
def test_insight_update_rejects_invalid_import_changes(
    values: dict[str, object], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        TypeAdapter(ReviewManageRequest).validate_python(
            {"operation": "update_insight", "insight_id": uuid.uuid4(), **values}
        )


@pytest.mark.parametrize("import_value", ["omitted", "null", "uuid"])
async def test_review_insight_create_protocol_forwards_batch_import(
    import_value: str,
) -> None:
    agent_id = uuid.uuid4()
    import_id = uuid.uuid4()
    create_calls: list[dict[str, object]] = []

    async def create_insights(
        received_agent_id: uuid.UUID,
        insights: list[InsightInput],
        idempotency_key: str | None = None,
        **kwargs: uuid.UUID,
    ) -> list[InsightResponse]:
        assert received_agent_id == agent_id
        assert len(insights) == 2
        assert idempotency_key == "import-insights"
        assert all("import_id" not in item.model_fields_set for item in insights)
        create_calls.append(dict(kwargs))
        return [
            build_insight(agent_id=agent_id).model_copy(
                update={"import_id": kwargs.get("import_id")}
            )
            for _ in insights
        ]

    client = SimpleNamespace(insights=SimpleNamespace(create=create_insights))
    server, context = build_context(client, CapabilityMode.STANDARD)
    request: dict[str, object] = {
        "operation": "create_insights",
        "agent_id": str(agent_id),
        "insights": [
            {
                "name": name,
                "title": name,
                "data": {"type": "text", "content": "Review finding"},
            }
            for name in ["latency", "quality"]
        ],
        "idempotency_key": "import-insights",
    }
    if import_value != "omitted":
        request["import_id"] = str(import_id) if import_value == "uuid" else None
    result = await server.call_tool(
        "kitaru_review_manage", {"request": request}, context
    )
    assert isinstance(result, CallToolResult)
    assert result.is_error is False
    assert create_calls == (
        [{"import_id": import_id}] if import_value == "uuid" else [{}]
    )
    assert result.structured_content is not None
    assert [item["import_id"] for item in result.structured_content["data"]] == [
        str(import_id) if import_value == "uuid" else None
    ] * 2


async def test_review_insight_update_protocol_import_lifecycle() -> None:
    agent_id = uuid.uuid4()
    insight = build_insight(agent_id=agent_id)
    first_import_id = uuid.uuid4()
    second_import_id = uuid.uuid4()
    update_calls: list[dict[str, object]] = []

    async def update_insight(
        received_id: uuid.UUID, request: InsightUpdateRequest
    ) -> InsightResponse:
        nonlocal insight
        assert received_id == insight.id
        values = request.model_dump(exclude_unset=True)
        update_calls.append(values)
        insight = insight.model_copy(update=values)
        return insight

    client = SimpleNamespace(insights=SimpleNamespace(update=update_insight))
    server, context = build_context(client, CapabilityMode.STANDARD)
    for changes, expected_import in [
        ({"import_id": str(first_import_id)}, first_import_id),
        ({"import_id": str(second_import_id)}, second_import_id),
        ({"title": "Renamed"}, second_import_id),
        ({"clear_import": True}, None),
        ({"import_id": None, "clear_import": True}, None),
    ]:
        result = await server.call_tool(
            "kitaru_review_manage",
            {
                "request": {
                    "operation": "update_insight",
                    "insight_id": str(insight.id),
                    **changes,
                }
            },
            context,
        )
        assert isinstance(result, CallToolResult)
        assert result.is_error is False
        assert result.structured_content is not None
        assert result.structured_content["data"]["import_id"] == (
            str(expected_import) if expected_import else None
        )
    assert update_calls == [
        {"import_id": first_import_id},
        {"import_id": second_import_id},
        {"title": "Renamed"},
        {"import_id": None},
        {"import_id": None},
    ]


async def test_review_discovery_exposes_batch_and_update_import_fields() -> None:
    tools = await create_server(MCPSettings(mode=CapabilityMode.STANDARD)).list_tools()
    tool = next(tool for tool in tools if tool.name == "kitaru_review_manage")
    definitions = tool.input_schema["$defs"]
    for name in ["InsightsCreate", "InsightUpdate"]:
        assert definitions[name]["properties"]["import_id"]["anyOf"] == [
            {"format": "uuid", "type": "string"},
            {"type": "null"},
        ]
        assert "import_id" not in definitions[name]["required"]
    assert (
        definitions["InsightUpdate"]["properties"]["clear_import"]["default"] is False
    )
    assert "import_id" not in definitions["InsightInput"]["properties"]
