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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Rework insight rows into structured insight reports.

Revision ID: 028_structured_insight_reports
Revises: 027_session_group
Create Date: 2026-11-25

"""

import json
from collections import defaultdict
from typing import Any

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "028_structured_insight_reports"
down_revision = "027_session_group"
branch_labels = None
depends_on = None


def _convert_legacy_artifact(title: str, data: dict[str, Any]) -> dict[str, Any]:
    """Convert one legacy InsightData payload into a valid InsightArtifact dict."""
    kind = data.get("type")
    if kind == "text":
        content = str(data.get("content") or title or "Insight").strip() or "Insight"
        return {"type": "text", "style": "body", "content": content}
    if kind == "categorical":
        values = list(data.get("values") or [])
        unit = data.get("unit")
        if len(values) == 1:
            only = values[0]
            return {
                "type": "stat_grid",
                "items": [
                    {
                        "label": str(only.get("label") or title)[:60],
                        "value": only.get("value", 0),
                        "unit": unit,
                        "tone": "neutral",
                    }
                ],
            }
        if len(values) > 8:
            top = values[:7]
            other_total = sum(float(v.get("value", 0)) for v in values[7:])
            values = [*top, {"label": "Other", "value": other_total}]
        return {
            "type": "categorical",
            "presentation": "ranked_bars",
            "unit": unit,
            "values": [
                {"label": str(v.get("label", "")), "value": v.get("value", 0)}
                for v in values
            ],
        }
    if kind == "binned":
        bins = list(data.get("bins") or [])
        unit = data.get("unit")
        if len(bins) < 2:
            count = bins[0].get("count", 0) if bins else 0
            return {
                "type": "stat_grid",
                "items": [
                    {
                        "label": title[:60],
                        "value": count,
                        "unit": unit,
                        "tone": "neutral",
                    }
                ],
            }
        return {
            "type": "binned",
            "presentation": "columns",
            "unit": unit,
            "bins": bins[:12],
        }
    return {"type": "text", "style": "body", "content": title or "Insight"}


def _row_to_finding(
    row: dict[str, Any], index: int
) -> tuple[dict[str, Any], dict[str, Any] | None, str | None]:
    """Convert one legacy insight row into a finding, coverage, and provider."""
    metadata = row["metadata"] if isinstance(row["metadata"], dict) else {}
    v1 = metadata.get("kitaru.insights/v1")
    v1 = v1 if isinstance(v1, dict) else {}
    sf = metadata.get("kitaru.session-findings/v1")
    sf = sf if isinstance(sf, dict) else {}

    title = str(row["title"])
    description = row["description"]
    summary = (
        str(description)[:320] if isinstance(description, str) and description else None
    )
    artifact = _convert_legacy_artifact(
        title, row["data"] if isinstance(row["data"], dict) else {}
    )
    artifacts: list[dict[str, Any]] = [artifact]

    contributing = (
        v1.get("contributing_session_ids") or sf.get("contributing_session_ids") or []
    )
    evidence = v1.get("evidence")
    if isinstance(evidence, list) and evidence:
        ev_rows = [
            {
                "session_id": str(item["session_id"]),
                "node_id": (
                    str(item["node_id"]) if item.get("node_id") is not None else None
                ),
                "signal": str(item["signal"])[:120] if item.get("signal") else None,
            }
            for item in evidence[:25]
            if isinstance(item, dict) and item.get("session_id")
        ]
        if ev_rows:
            artifacts.append(
                {
                    "type": "evidence",
                    "presentation": "table",
                    "total_sessions": max(len(contributing), len(ev_rows), 1),
                    "rows": ev_rows,
                }
            )

    action: dict[str, Any] | None = None
    prompt = v1.get("investigation_prompt")
    if isinstance(prompt, str) and prompt.strip():
        cta = v1.get("cta_label")
        label = (
            str(cta)[:60]
            if isinstance(cta, str) and cta.strip()
            else "Investigate this insight"
        )
        action = {"label": label, "prompt": prompt[:16_000]}

    harm = sf.get("harm")
    severity = harm if harm in ("high", "medium", "low") else None
    finding_id = str(row["name"] or f"finding-{index + 1}")[:120]
    finding = {
        "id": finding_id,
        "title": title[:255],
        "summary": summary,
        "body": description if isinstance(description, str) and description else None,
        "tier": "primary",
        "kind": "finding",
        "severity": severity,
        "change_status": None,
        "artifacts": artifacts,
        "contributing_session_ids": [str(sid) for sid in contributing[:1000]],
        "check_first": (
            str(v1["check_first"])[:1000]
            if isinstance(v1.get("check_first"), str) and v1["check_first"]
            else None
        ),
        "action": action,
        "suggested_questions": [],
    }

    coverage: dict[str, Any] | None = None
    raw_cov = v1.get("coverage")
    if isinstance(raw_cov, dict) and isinstance(raw_cov.get("sessions_analyzed"), int):
        avail = raw_cov.get("sessions_available")
        analyzed = raw_cov["sessions_analyzed"]
        coverage = {
            "sessions_analyzed": analyzed,
            "sessions_available": (
                avail if isinstance(avail, int) and avail >= analyzed else None
            ),
            "skipped": 0,
            "failed": 0,
            "caveats": [
                str(c)
                for c in (raw_cov.get("caveats") or [])[:10]
                if isinstance(c, str)
            ],
        }

    provider: str | None = None
    raw_ctx = v1.get("context")
    if isinstance(raw_ctx, dict):
        src = raw_ctx.get("source_import")
        if isinstance(src, dict) and isinstance(src.get("provider"), str):
            provider = src["provider"][:80]

    return finding, coverage, provider


def upgrade() -> None:
    """Upgrade database schema and backfill existing insight rows into reports."""
    with op.batch_alter_table("insight", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("source_provider", sa.String(length=80), nullable=True)
        )
        batch_op.add_column(sa.Column("summary", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("status", sa.String(length=32), nullable=True))
        batch_op.add_column(
            sa.Column(
                "coverage", postgresql.JSONB(astext_type=sa.Text()), nullable=True
            )
        )
        batch_op.add_column(
            sa.Column(
                "highlight", postgresql.JSONB(astext_type=sa.Text()), nullable=True
            )
        )
        batch_op.add_column(
            sa.Column(
                "next_step", postgresql.JSONB(astext_type=sa.Text()), nullable=True
            )
        )
        batch_op.add_column(
            sa.Column(
                "findings", postgresql.JSONB(astext_type=sa.Text()), nullable=True
            )
        )
        batch_op.add_column(
            sa.Column(
                "suggested_questions",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=True,
            )
        )

    connection = op.get_bind()
    rows = (
        connection.execute(
            sa.text(
                "SELECT id, invocation_id, name, title, description, type, data, "
                "metadata, created FROM insight ORDER BY created ASC, id ASC"
            )
        )
        .mappings()
        .all()
    )

    grouped: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    standalone: list[dict[str, Any]] = []
    for raw in rows:
        item = dict(raw)
        if item["invocation_id"] is not None:
            grouped[item["invocation_id"]].append(item)
        else:
            standalone.append(item)

    def _sort_key(item: dict[str, Any]) -> tuple[int, Any]:
        meta = item["metadata"] if isinstance(item["metadata"], dict) else {}
        v1 = meta.get("kitaru.insights/v1")
        pos = v1.get("position") if isinstance(v1, dict) else None
        return (pos if isinstance(pos, int) else 9999, item["created"])

    batches = [sorted(group, key=_sort_key) for group in grouped.values()]
    batches.extend([item] for item in standalone)

    update_stmt = sa.text(
        "UPDATE insight SET source_provider = :source_provider, summary = :summary, "
        "status = :status, coverage = CAST(:coverage AS jsonb), "
        "highlight = CAST(:highlight AS jsonb), next_step = CAST(:next_step AS jsonb), "
        "findings = CAST(:findings AS jsonb), "
        "suggested_questions = CAST(:suggested_questions AS jsonb) "
        "WHERE id = :id"
    )
    delete_stmt = sa.text("DELETE FROM insight WHERE id = :id")

    for batch in batches:
        primary = batch[0]
        findings: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        coverage: dict[str, Any] | None = None
        provider: str | None = None
        for idx, item in enumerate(batch[:30]):
            finding, item_cov, item_provider = _row_to_finding(item, idx)
            base_id = finding["id"]
            candidate_id = base_id
            suffix = 2
            while candidate_id in seen_ids:
                candidate_id = f"{base_id[:110]}-{suffix}"
                suffix += 1
            finding["id"] = candidate_id
            seen_ids.add(candidate_id)
            findings.append(finding)
            if coverage is None and item_cov is not None:
                coverage = item_cov
            if provider is None and item_provider is not None:
                provider = item_provider

        highlight = {"finding_id": findings[0]["id"], "artifact_index": 0}
        connection.execute(
            update_stmt,
            {
                "id": primary["id"],
                "source_provider": provider,
                "summary": findings[0]["summary"],
                "status": "new",
                "coverage": json.dumps(coverage) if coverage is not None else None,
                "highlight": json.dumps(highlight),
                "next_step": None,
                "findings": json.dumps(findings),
                "suggested_questions": json.dumps([]),
            },
        )
        for sibling in batch[1:]:
            connection.execute(delete_stmt, {"id": sibling["id"]})

    with op.batch_alter_table("insight", schema=None) as batch_op:
        batch_op.alter_column("status", nullable=False)
        batch_op.alter_column("findings", nullable=False)
        batch_op.alter_column("suggested_questions", nullable=False)
        batch_op.drop_column("description")
        batch_op.drop_column("type")
        batch_op.drop_column("data")


def downgrade() -> None:
    """Downgrade structured insight reports back to legacy single-card columns."""
    with op.batch_alter_table("insight", schema=None) as batch_op:
        batch_op.add_column(sa.Column("description", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("type", sa.String(length=32), nullable=True))
        batch_op.add_column(
            sa.Column("data", postgresql.JSONB(astext_type=sa.Text()), nullable=True)
        )

    connection = op.get_bind()
    rows = (
        connection.execute(sa.text("SELECT id, title, summary, findings FROM insight"))
        .mappings()
        .all()
    )
    update_stmt = sa.text(
        "UPDATE insight SET description = :description, type = :type, "
        "data = CAST(:data AS jsonb) WHERE id = :id"
    )
    for row in rows:
        findings = row["findings"] if isinstance(row["findings"], list) else []
        first = findings[0] if findings and isinstance(findings[0], dict) else {}
        artifacts = (
            first.get("artifacts") if isinstance(first.get("artifacts"), list) else []
        )
        first_art = (
            artifacts[0] if artifacts and isinstance(artifacts[0], dict) else None
        )
        if first_art and first_art.get("type") in ("text", "categorical", "binned"):
            legacy_type = first_art["type"]
            legacy_data = first_art
        else:
            legacy_type = "text"
            legacy_data = {
                "type": "text",
                "content": row["summary"] or row["title"] or "Insight",
            }
        connection.execute(
            update_stmt,
            {
                "id": row["id"],
                "description": row["summary"],
                "type": legacy_type,
                "data": json.dumps(legacy_data),
            },
        )

    with op.batch_alter_table("insight", schema=None) as batch_op:
        batch_op.alter_column("type", nullable=False)
        batch_op.alter_column("data", nullable=False)
        batch_op.drop_column("suggested_questions")
        batch_op.drop_column("findings")
        batch_op.drop_column("next_step")
        batch_op.drop_column("highlight")
        batch_op.drop_column("coverage")
        batch_op.drop_column("status")
        batch_op.drop_column("summary")
        batch_op.drop_column("source_provider")
