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

import shlex
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
_MAX_CONNECTION_ACTIONS = 5


def describe_not_copied() -> str:
    """Return the kinds of record a push leaves behind as an English list."""
    return f"{', '.join(NOT_COPIED[:-1])}, and {NOT_COPIED[-1]}"


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

    def add_unmapped(
        self, insight: InsightResponse, references: list[dict[str, str]]
    ) -> None:
        """Keep the references one copied insight still makes to the source."""
        self._add_detail(
            self.unmapped_references,
            {
                "insight": insight.name,
                "source_insight_id": str(insight.id),
                "references": references,
            },
        )

    def add_incomplete_connection(
        self, name: str, missing_keys: set[str], stored_keys: set[str]
    ) -> None:
        """Keep the secret keys a destination connection still needs.

        The command names the stored keys too, because a connection update
        replaces every stored secret value with the ones it is given.

        Args:
            name: Connection name.
            missing_keys: Secret keys the source has and the destination
                lacks.
            stored_keys: Secret keys the destination already holds.
        """
        options = " ".join(
            f"--set-secret {shlex.quote(key)}=<value>"
            for key in sorted(missing_keys | stored_keys)
        )
        self._add_detail(
            self.incomplete_connections,
            {
                "connection": name,
                "missing_keys": sorted(missing_keys),
                "command": f"kitaru --server {self.destination} connection "
                f"update {shlex.quote(name)} {options}",
            },
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
        CLIError: At least one record failed to copy. Its details carry the
            report with the warnings and next actions.

    Returns:
        Result carrying the report and the follow-up actions.
    """
    warnings = _build_warnings(report)
    next_actions = _build_next_actions(report)
    if report.failures:
        raise CLIError(
            "partial_failure",
            f"{report.failures} record(s) failed to copy to {report.destination}.",
            retryable=True,
            details={
                **report.to_item(),
                "warnings": warnings,
                "next_actions": next_actions,
            },
            hint="Rerun the same command. Records already copied are not copied again.",
        )
    return CommandResult(
        item=report.to_item(),
        warnings=warnings,
        next_actions=next_actions,
        text=_render_text(report),
    )


def _build_warnings(report: PushReport) -> list[str]:
    """Collect the warnings a push ends with."""
    warnings = list(report.warnings)
    if report.incomplete_connections:
        verb = "would lack" if report.dry_run else "lack"
        warnings.append(
            f"{len(report.incomplete_connections)} connection(s) on "
            f"{report.destination} {verb} secret values, because a Kitaru "
            "server never returns the secret values of a connection. Their "
            "keys are listed under `incomplete_connections`."
        )
    if report.unmapped_references:
        warnings.append(
            f"{len(report.unmapped_references)} copied insight(s) still reference "
            "records that exist only on the source. Each lists them under "
            "`unmapped_references` in its `kitaru.push/v1` metadata."
        )
    return warnings


def _build_next_actions(report: PushReport) -> list[str]:
    """Collect the follow-up actions a push ends with."""
    if report.dry_run:
        next_actions = ["Rerun without --dry-run to copy the records."]
    else:
        next_actions = [
            f"Run `kitaru --server {report.destination} agent list` to inspect "
            "the destination."
        ]
    if report.incomplete_connections and not report.dry_run:
        next_actions.append(
            "Set the secret values of each incomplete connection with one "
            "command naming all of its keys, because an update replaces every "
            "stored secret value:"
        )
        next_actions.extend(
            f"`{detail['command']}`"
            for detail in report.incomplete_connections[:_MAX_CONNECTION_ACTIONS]
        )
        if len(report.incomplete_connections) > _MAX_CONNECTION_ACTIONS:
            next_actions.append(
                "Find the command for each remaining connection under "
                "`incomplete_connections`."
            )
    next_actions.append(
        f"Recreate what a push does not copy on the destination: "
        f"{describe_not_copied()}."
    )
    return next_actions


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
