#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""HTML insight report rendering and its CLI command."""

import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.insight import InsightListParams, InsightResponse
from kitaru.cli import app as app_module
from kitaru.cli.insight_report import REPORT_SECTION_KEY, render_insight_report

IMPORT_ID = uuid.UUID("01990000-0000-7000-8000-000000000010")
NOW = datetime(2026, 10, 20, tzinfo=UTC)


def insight(
    name: str,
    data: dict[str, Any] | None = None,
    *,
    created: datetime = NOW,
    metadata: dict[str, Any] | None = None,
) -> InsightResponse:
    return InsightResponse.model_validate(
        {
            "id": uuid.uuid4(),
            "owner_id": uuid.uuid4(),
            "created": created,
            "updated": created,
            "agent_id": uuid.uuid4(),
            "import_id": IMPORT_ID,
            "name": name,
            "title": name.title(),
            "description": None,
            "data": data or {"type": "text", "content": f"{name} content"},
            "metadata": metadata or {},
        }
    )


def section(html: str, covers: list[str]) -> dict[str, Any]:
    return {REPORT_SECTION_KEY: {"html": html, "covers": covers}}


CHART = insight(
    "tool-retries",
    {"type": "categorical", "unit": "calls", "values": [{"label": "a", "value": 2}]},
)


def test_renders_every_insight_as_a_card() -> None:
    document = render_insight_report(
        [CHART, insight("summary-text")], import_id=IMPORT_ID
    )

    assert "<title>Insight report</title>" in document
    assert "Tool-Retries" in document
    assert "2 calls" in document
    assert "summary-text content" in document
    assert "Other insights" not in document


def test_embeds_sections_and_hides_what_they_cover() -> None:
    summary = insight(
        "swap-summary", metadata=section("<h2>Swap section</h2>", ["swap-cost"])
    )
    covered = insight("swap-cost")

    document = render_insight_report([summary, covered, CHART], import_id=IMPORT_ID)

    assert "<h2>Swap section</h2>" in document
    assert "swap-cost content" not in document
    assert "swap-summary content" not in document
    assert "Other insights" in document
    assert "Tool-Retries" in document


def test_only_the_latest_section_per_name_is_embedded() -> None:
    older = insight(
        "swap-summary",
        created=NOW - timedelta(hours=1),
        metadata=section("<p>old run</p>", []),
    )
    newer = insight("swap-summary", metadata=section("<p>new run</p>", []))

    document = render_insight_report([newer, older], import_id=IMPORT_ID)

    assert "new run" in document
    assert "old run" not in document


def test_page_forbids_scripts_and_escapes_card_content() -> None:
    hostile = insight("x", {"type": "text", "content": "<script>alert(1)</script>"})

    document = render_insight_report([hostile], import_id=IMPORT_ID)

    assert "default-src 'none'" in document
    assert "<script>" not in document
    assert "&lt;script&gt;" in document


def test_malformed_sections_fall_back_to_cards() -> None:
    broken = insight("x", metadata={REPORT_SECTION_KEY: {"html": 3}})

    document = render_insight_report([broken], import_id=IMPORT_ID)

    assert "x content" in document


class StubClient:
    def __init__(self, insights: list[InsightResponse]) -> None:
        self.list_params: list[InsightListParams] = []
        self._insights = insights
        self.insights = SimpleNamespace(iter=self._iter)

    async def _iter(self, params: InsightListParams) -> AsyncIterator[InsightResponse]:
        self.list_params.append(params)
        for item in self._insights:
            yield item


def use_client(monkeypatch: pytest.MonkeyPatch, client: StubClient) -> None:
    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)


def report_argv(out: Path) -> list[str]:
    return [
        "insight",
        "report",
        "--import-id",
        str(IMPORT_ID),
        "--out",
        str(out),
        "-o",
        "json",
    ]


def test_command_writes_the_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    summary = insight("swap-summary", metadata=section("<p>swap</p>", []))
    client = StubClient([summary, CHART])
    use_client(monkeypatch, client)
    out = tmp_path / "report.html"

    assert app_module.main(report_argv(out)) == 0

    response = json.loads(capsys.readouterr().out)
    assert response["command"] == "insight.report"
    assert response["item"]["sections"] == ["swap-summary"]
    assert response["item"]["insights"] == 2
    assert "<p>swap</p>" in out.read_text()
    [params] = client.list_params
    assert params.filter is not None
    assert params.filter.model_dump()["value"] == str(IMPORT_ID)


def test_command_fails_without_insights(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    use_client(monkeypatch, StubClient([]))
    out = tmp_path / "r.html"

    assert app_module.main(report_argv(out)) == 4
    assert not out.exists()
    capsys.readouterr()
