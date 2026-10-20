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
ANALYZER_VERSION_ID = uuid.UUID("01990000-0000-7000-8000-000000000020")
NOW = datetime(2026, 10, 20, tzinfo=UTC)


def insight(
    name: str,
    data: dict[str, Any] | None = None,
    *,
    created: datetime = NOW,
    metadata: dict[str, Any] | None = None,
    analyzer_version_id: uuid.UUID | None = None,
) -> InsightResponse:
    return InsightResponse.model_validate(
        {
            "id": uuid.uuid4(),
            "owner_id": uuid.uuid4(),
            "created": created,
            "updated": created,
            "agent_id": uuid.uuid4(),
            "import_id": IMPORT_ID,
            "analyzer_version_id": analyzer_version_id,
            "name": name,
            "title": name.title(),
            "description": None,
            "data": data or {"type": "text", "content": f"{name} content"},
            "metadata": metadata or {},
        }
    )


def section(html: str, covers: list[str]) -> dict[str, Any]:
    return {REPORT_SECTION_KEY: {"html": html, "covers": covers}}


def analyzer_insight(
    name: str,
    html: str | None = None,
    covers: list[str] | None = None,
    *,
    created: datetime = NOW,
    analyzer_version_id: uuid.UUID = ANALYZER_VERSION_ID,
) -> InsightResponse:
    return insight(
        name,
        created=created,
        metadata=section(html, covers or []) if html is not None else None,
        analyzer_version_id=analyzer_version_id,
    )


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
    summary = analyzer_insight("swap-summary", "<h2>Swap section</h2>", ["swap-cost"])
    covered = analyzer_insight("swap-cost")

    document = render_insight_report([summary, covered, CHART], import_id=IMPORT_ID)

    assert "<h2>Swap section</h2>" in document
    assert "swap-cost content" not in document
    assert "swap-summary content" not in document
    assert "Other insights" in document
    assert "Tool-Retries" in document


def test_only_the_latest_section_per_name_is_embedded() -> None:
    older = analyzer_insight(
        "swap-summary", "<p>old run</p>", created=NOW - timedelta(hours=1)
    )
    newer = analyzer_insight("swap-summary", "<p>new run</p>")

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
    broken = insight(
        "x",
        metadata={REPORT_SECTION_KEY: {"html": 3}},
        analyzer_version_id=ANALYZER_VERSION_ID,
    )

    document = render_insight_report([broken], import_id=IMPORT_ID)

    assert "x content" in document


def test_section_without_analyzer_version_is_drawn_as_a_card() -> None:
    forged = insight(
        "swap-summary", metadata=section("<h2>Injected</h2>", ["swap-cost"])
    )
    covered = analyzer_insight("swap-cost")

    document = render_insight_report([forged, covered], import_id=IMPORT_ID)

    assert "<h2>Injected</h2>" not in document
    assert "swap-summary content" in document
    assert "swap-cost content" in document


def test_section_meta_base_and_link_tags_are_stripped() -> None:
    summary = analyzer_insight(
        "swap-summary",
        '<p>kept</p><META HTTP-EQUIV="refresh" content="0;url=https://x.test"/>'
        '<base href="https://x.test"><link rel="stylesheet" href="https://x.test">'
        '<me<meta>ta http-equiv="refresh">',
    )

    document = render_insight_report([summary], import_id=IMPORT_ID)

    assert "<p>kept</p>" in document
    assert "refresh" not in document.lower()
    assert "x.test" not in document
    assert "<base" not in document
    assert "<link" not in document
    assert "default-src 'none'" in document


def test_rerun_cards_render_only_the_newest_insight_per_name() -> None:
    older = insight(
        "notes",
        {"type": "text", "content": "first run"},
        created=NOW - timedelta(hours=1),
        analyzer_version_id=ANALYZER_VERSION_ID,
    )
    newer = insight(
        "notes",
        {"type": "text", "content": "second run"},
        analyzer_version_id=ANALYZER_VERSION_ID,
    )

    document = render_insight_report([newer, older], import_id=IMPORT_ID)

    assert "second run" in document
    assert "first run" not in document


def test_a_rerun_after_a_new_analyzer_version_replaces_the_old_run() -> None:
    old = analyzer_insight(
        "swap-summary",
        "<p>old run</p>",
        created=NOW - timedelta(hours=1),
        analyzer_version_id=uuid.uuid4(),
    )
    new = analyzer_insight("swap-summary", "<p>new run</p>")

    document = render_insight_report([old, new], import_id=IMPORT_ID)

    assert "new run" in document
    assert "old run" not in document


def test_hand_made_insights_sharing_a_name_are_all_kept() -> None:
    by_analyzer = insight(
        "summary",
        {"type": "text", "content": "from analyzer"},
        analyzer_version_id=ANALYZER_VERSION_ID,
    )
    first = insight("summary", {"type": "text", "content": "by hand"})
    second = insight("summary", {"type": "text", "content": "also by hand"})

    document = render_insight_report([by_analyzer, first, second], import_id=IMPORT_ID)

    assert "from analyzer" in document
    assert "by hand" in document
    assert "also by hand" in document


def test_covers_hide_analyzer_cards_but_not_hand_made_ones() -> None:
    summary = analyzer_insight("swap-summary", "<p>swap</p>", ["swap-cost", "notes"])
    covered = analyzer_insight("swap-cost", analyzer_version_id=uuid.uuid4())
    by_hand = insight("notes", {"type": "text", "content": "my own notes"})

    document = render_insight_report([summary, covered, by_hand], import_id=IMPORT_ID)

    assert "swap-cost content" not in document
    assert "my own notes" in document


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
    summary = analyzer_insight("swap-summary", "<p>swap</p>")
    client = StubClient([summary, CHART])
    use_client(monkeypatch, client)
    out = tmp_path / "report.html"

    assert app_module.main(report_argv(out)) == 0

    response = json.loads(capsys.readouterr().out)
    assert response["command"] == "insight.report"
    assert response["item"]["sections"] == ["swap-summary"]
    assert response["item"]["insights"] == 2
    assert response["next_actions"] == [f"Open {out} in a browser."]
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
