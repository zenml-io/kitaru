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
"""Report of one workspace push and its rendering."""

from dataclasses import dataclass, field
from typing import Any

from kitaru.api_models.v1.insight import InsightResponse
from kitaru.cli.output import CLIError, CommandResult

KINDS = ("agents", "agent_versions", "connections", "sessions", "insights")
OUTCOMES = ("copied", "already_present", "skipped", "failed")
NOT_COPIED = (
    "cohorts",
    "evaluators",
    "evaluations",
    "investigations",
    "annotations",
    "experiments",
    "replays",
    "tags",
    "importers",
    "analyzers",
)

_MAX_DETAILS = 200
_MAX_TEXT_DETAILS = 20


@dataclass(slots=True)
class PushReport:
    """Outcome of one workspace push, by kind of record."""

    source: str
    destination: str
    dry_run: bool
    counts: dict[str, dict[str, int]] = field(
        default_factory=lambda: {kind: dict.fromkeys(OUTCOMES, 0) for kind in KINDS}
    )
    skipped: list[dict[str, str]] = field(default_factory=list)
    failed: list[dict[str, str]] = field(default_factory=list)
    incomplete_connections: list[dict[str, Any]] = field(default_factory=list)
    unmapped_references: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    details_truncated: bool = False

    def count(self, kind: str, outcome: str, amount: int = 1) -> None:
        """Add records of one kind to an outcome."""
        self.counts[kind][outcome] += amount

    def skip(self, kind: str, record: str, reason: str) -> None:
        """Count one skipped record and keep why it was skipped."""
        self.count(kind, "skipped")
        self._add_detail(
            self.skipped, {"kind": kind, "record": record, "reason": reason}
        )

    def fail(self, kind: str, record: str, error: str) -> None:
        """Count one failed record and keep the failure."""
        self.count(kind, "failed")
        self._add_detail(self.failed, {"kind": kind, "record": record, "error": error})

    def add_unmapped(self, insight: InsightResponse, unmapped: dict[str, str]) -> None:
        """Keep the references one copied insight still makes to the source."""
        self._add_detail(
            self.unmapped_references,
            {
                "insight": insight.name,
                "source_insight_id": str(insight.id),
                "references": [
                    {"id": reference, "kind": kind}
                    for reference, kind in sorted(unmapped.items())
                ],
            },
        )

    def add_incomplete_connection(self, name: str, missing_keys: list[str]) -> None:
        """Keep the secret keys a copied connection still needs."""
        self._add_detail(
            self.incomplete_connections,
            {"connection": name, "missing_keys": sorted(missing_keys)},
        )

    @property
    def failures(self) -> int:
        """Return the number of records that failed to copy."""
        return sum(counts["failed"] for counts in self.counts.values())

    def to_item(self) -> dict[str, Any]:
        """Return the JSON document describing the push."""
        return {
            "source": self.source,
            "destination": self.destination,
            "dry_run": self.dry_run,
            "counts": self.counts,
            "skipped": self.skipped,
            "failed": self.failed,
            "incomplete_connections": self.incomplete_connections,
            "unmapped_references": self.unmapped_references,
            "details_truncated": self.details_truncated,
            "not_copied": list(NOT_COPIED),
        }

    def _add_detail(self, details: list[Any], detail: dict[str, Any]) -> None:
        """Keep one detail while the list stays within its bound."""
        if len(details) < _MAX_DETAILS:
            details.append(detail)
        else:
            self.details_truncated = True


def build_push_result(report: PushReport) -> CommandResult:
    """Render a push report, failing when any record failed to copy.

    Args:
        report: Outcome of the push.

    Raises:
        CLIError: At least one record failed to copy.

    Returns:
        Result carrying the report and the follow-up actions.
    """
    if report.failures:
        raise CLIError(
            "partial_failure",
            f"{report.failures} record(s) failed to copy to {report.destination}.",
            retryable=True,
            details=report.to_item(),
            hint="Rerun the same command. Records already copied are not copied again.",
        )
    warnings = list(report.warnings)
    if report.unmapped_references:
        warnings.append(
            f"{len(report.unmapped_references)} copied insight(s) still reference "
            "records that exist only on the source. Each lists them under "
            "`unmapped_references` in its `kitaru.push/v1` metadata."
        )
    next_actions = []
    if report.dry_run:
        next_actions.append("Rerun without --dry-run to copy the records.")
    else:
        next_actions.append(
            f"Run `kitaru --server {report.destination} agent list` to inspect "
            "the destination."
        )
    if report.counts["connections"]["skipped"]:
        next_actions.append(
            "Rerun with --copy-secrets to copy the skipped connections, or "
            "recreate them on the destination with `kitaru connection create`."
        )
    if report.incomplete_connections:
        next_actions.append(
            "Set the missing secret values on the destination with `kitaru "
            f"--server {report.destination} connection update NAME "
            "--set-secret KEY=VALUE` for each key under `incomplete_connections`."
        )
    next_actions.append(
        "Recreate what a push does not copy on the destination: "
        f"{', '.join(NOT_COPIED)}."
    )
    return CommandResult(
        item=report.to_item(),
        warnings=warnings,
        next_actions=next_actions,
        text=_render_text(report),
    )


def _render_text(report: PushReport) -> str:
    """Render the per-kind counts as aligned plain text."""
    verb = "Would copy" if report.dry_run else "Copied"
    lines = [f"{verb} from {report.source} to {report.destination}", ""]
    header = ("", *(outcome.replace("_", " ") for outcome in OUTCOMES))
    rows = [header] + [
        (
            kind.replace("_", " "),
            *(str(report.counts[kind][outcome]) for outcome in OUTCOMES),
        )
        for kind in KINDS
    ]
    widths = [max(len(row[index]) for row in rows) for index in range(len(header))]
    lines.extend(
        "  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True))
        for row in rows
    )
    lines.extend(
        f"Skipped {detail['kind']} {detail['record']}: {detail['reason']}"
        for detail in report.skipped[:_MAX_TEXT_DETAILS]
    )
    lines.extend(
        f"Connection {detail['connection']} needs: {', '.join(detail['missing_keys'])}"
        for detail in report.incomplete_connections[:_MAX_TEXT_DETAILS]
    )
    if len(report.skipped) > _MAX_TEXT_DETAILS or report.details_truncated:
        lines.append(
            "More records were skipped than are listed here. The counts above "
            "are complete."
        )
    return "\n".join(line.rstrip() for line in lines)
