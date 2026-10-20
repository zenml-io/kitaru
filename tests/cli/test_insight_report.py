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
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from kitaru.api_models.v1.insight import InsightListParams, InsightResponse
from kitaru.cli import app as app_module
from kitaru.cli.insight_report import MODEL_SWAP_REPORT_KEY, render_insight_report

IMPORT_ID = uuid.UUID("01990000-0000-7000-8000-000000000010")
SESSION_ID = "01990000-0000-7000-8000-000000000011"
NOW = datetime(2026, 10, 20, tzinfo=UTC)

REPORT: dict[str, Any] = {
    "schema": 1,
    "dry_run": False,
    "coverage": {
        "sessions": 10,
        "llm_calls_seen": 33,
        "replayable": 30,
        "replayable_inferred": 0,
        "sampled": 30,
        "skipped": {"missing_tool_schema": 3},
        "schema_sources": {"recorded": 30},
        "calls_with_dropped_reasoning": 20,
    },
    "volume": {"sessions": 10, "calls_per_session": 3.3, "sessions_per_month": 300},
    "recorded": {
        "models": {"gpt-5-nano": 30},
        "cost": {
            "per_call_usd": "0.0003",
            "per_session_usd": "0.001",
            "per_month_usd": "0.3",
        },
    },
    "candidates": [
        {
            "name": "open-model",
            "model": "qwen3",
            "reference": False,
            "price": {
                "input_per_mtok": "0.1",
                "output_per_mtok": "0.3",
                "source": "params",
            },
            "agreement": {"same_tool_same_args": 20, "different_tool": 10},
            "agreement_rate": 0.6667,
            "tool_choice_rate": 0.8,
            "judged_equivalent": 0,
            "failures": {"tool_call_parse_error": 2},
            "failure_rate": 0.0625,
            "by_tool": {
                "final_result": {
                    "calls": 10,
                    "agreement_rate": 0.1,
                    "fields": {"final_result.action": 0.9},
                }
            },
            "latency_seconds": {"p50": 1.2, "p95": 3.0},
            "tokens_per_call": {
                "input_tokens": 900.0,
                "cached_input_tokens": 0.0,
                "output_tokens": 300.0,
                "reasoning_tokens": 100.0,
            },
            "retries": 1,
            "cost": {
                "per_call_usd": "0.0001",
                "per_session_usd": "0.0003",
                "per_month_usd": "0.1",
            },
            "recommendation": "Review the disagreements.",
        }
    ],
    "disagreements": [
        {
            "candidate": "open-model",
            "session_id": SESSION_ID,
            "node_id": "01990000-0000-7000-8000-000000000012",
            "node_name": "chat gpt-5-nano",
            "group": "final_result",
            "agreement": "different_tool",
            "schema_source": "recorded",
            "recorded": 'final_result({"reply": "<script>alert(1)</script>"})',
            "candidate_answer": 'escalate_to_human({"reason": "x"})',
            "judge_equivalent": None,
        }
    ],
    "disagreements_total": 1,
    "stopped_early": None,
    "judge": None,
    "notes": ["Reasoning is not sent to candidates."],
}


def insight(name: str, data: dict[str, Any], **metadata: Any) -> InsightResponse:
    return InsightResponse.model_validate(
        {
            "id": uuid.uuid4(),
            "owner_id": uuid.uuid4(),
            "created": NOW,
            "updated": NOW,
            "agent_id": uuid.uuid4(),
            "import_id": IMPORT_ID,
            "name": name,
            "title": name.title(),
            "description": None,
            "data": data,
            "metadata": metadata,
        }
    )


SUMMARY = insight(
    "model-swap-summary",
    {"type": "text", "content": "summary"},
    **{MODEL_SWAP_REPORT_KEY: REPORT},
)
OTHER = insight(
    "tool-retries",
    {"type": "categorical", "unit": "calls", "values": [{"label": "a", "value": 2}]},
)


def test_renders_model_swap_section_with_links() -> None:
    document = render_insight_report(
        [SUMMARY, OTHER], import_id=IMPORT_ID, dashboard_url="https://ui.example"
    )

    assert "<title>Model swap report</title>" in document
    assert "open-model" in document
    assert "67%" in document
    assert "tool_call_parse_error" in document
    assert "Skipped: missing_tool_schema" in document
    assert f'href="https://ui.example/sessions/{SESSION_ID}"' in document
    assert "Other insights" in document
    assert "Tool-Retries" in document


def test_recorded_content_is_escaped() -> None:
    document = render_insight_report([SUMMARY], import_id=IMPORT_ID, dashboard_url=None)

    assert "<script>" not in document
    assert "&lt;script&gt;" in document
    assert "href=" not in document


def test_dry_run_report_shows_estimates() -> None:
    dry = REPORT | {
        "dry_run": True,
        "candidates": [
            {
                "name": "open-model",
                "model": "qwen3",
                "reference": False,
                "estimate": {
                    "calls": 30,
                    "spend_usd": "0.004",
                    "cost": {"per_month_usd": "0.1"},
                },
            }
        ],
    }
    document = render_insight_report(
        [
            insight(
                "model-swap-summary",
                {"type": "text", "content": "x"},
                **{MODEL_SWAP_REPORT_KEY: dry},
            )
        ],
        import_id=IMPORT_ID,
        dashboard_url=None,
    )

    assert "Dry run" in document
    assert "Disagreements" not in document


def test_without_model_swap_all_insights_are_rendered() -> None:
    document = render_insight_report([OTHER], import_id=IMPORT_ID, dashboard_url=None)

    assert "<title>Insight report</title>" in document
    assert "Tool-Retries" in document


class StubClient:
    def __init__(self, insights: list[InsightResponse], info_error: bool = False):
        self.base_url = "http://localhost:8000"
        self.list_params: list[InsightListParams] = []
        self._insights = insights
        self.insights = SimpleNamespace(iter=self._iter)
        self.info = SimpleNamespace(get=self._info)
        self._info_error = info_error

    async def _iter(self, params: InsightListParams) -> AsyncIterator[InsightResponse]:
        self.list_params.append(params)
        for item in self._insights:
            yield item

    async def _info(self) -> Any:
        if self._info_error:
            raise httpx.ConnectError("down")
        return SimpleNamespace(dashboard_url=None, ui_version="1.0", server_url=None)


def use_client(monkeypatch: pytest.MonkeyPatch, client: StubClient) -> None:
    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)


def test_command_writes_the_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = StubClient([SUMMARY])
    use_client(monkeypatch, client)
    out = tmp_path / "report.html"

    code = app_module.main(
        [
            "insight",
            "report",
            "--import-id",
            str(IMPORT_ID),
            "--out",
            str(out),
            "-o",
            "json",
        ]
    )

    assert code == 0
    response = json.loads(capsys.readouterr().out)
    assert response["command"] == "insight.report"
    assert response["item"]["model_swap"] is True
    assert "localhost:8000/sessions/" in out.read_text()
    [params] = client.list_params
    assert params.filter is not None
    assert params.filter.model_dump()["value"] == str(IMPORT_ID)


def test_command_survives_a_failing_info_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    use_client(monkeypatch, StubClient([SUMMARY], info_error=True))
    out = tmp_path / "report.html"

    code = app_module.main(
        [
            "insight",
            "report",
            "--import-id",
            str(IMPORT_ID),
            "--out",
            str(out),
            "-o",
            "json",
        ]
    )

    assert code == 0
    assert json.loads(capsys.readouterr().out)["warnings"]
    assert "href=" not in out.read_text()


def test_command_fails_without_insights(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    use_client(monkeypatch, StubClient([]))

    code = app_module.main(
        [
            "insight",
            "report",
            "--import-id",
            str(IMPORT_ID),
            "--out",
            str(tmp_path / "r.html"),
            "-o",
            "json",
        ]
    )

    assert code == 4
    assert not (tmp_path / "r.html").exists()
    capsys.readouterr()
