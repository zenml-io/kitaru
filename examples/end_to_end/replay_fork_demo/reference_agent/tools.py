"""Local tools used by the reference-agent LangGraph graph."""

from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from . import db
from .config import ESCALATION_AUDIT_REASONS
from .knowledge import search_kb
from .mock_api import fetch_json

WRITE_TOOL_NAMES = {
    "create_support_ticket",
    "escalate_to_human",
    "update_customer_setting",
}


class ToolExecution(BaseModel):
    """A trace-friendly record of one local tool call."""

    name: str
    kind: str
    args: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any]
    evidence_ids: list[str] = Field(default_factory=list)
    wrote_state: bool = False
    blocked: bool = False


class SupportTools:
    """Tool registry backed by local SQLite, HTTP, and Markdown files."""

    def __init__(self, *, db_path: Path, api_base_url: str, kb_dir: Path) -> None:
        self.db_path = db_path
        self.api_base_url = api_base_url
        self.kb_dir = kb_dir
        self.write_tool_names = WRITE_TOOL_NAMES

    def run(self, name: str, args: dict[str, Any]) -> ToolExecution:
        """Run one named tool and return a JSON-safe execution record."""
        if name == "lookup_customer":
            result = db.lookup_customer(str(args["email_or_id"]), self.db_path)
            evidence_ids = (
                [f"db:customers:{result['customer_id']}"] if result.get("found") else []
            )
            return ToolExecution(
                name=name,
                kind="db_read",
                args=args,
                result=result,
                evidence_ids=evidence_ids,
            )
        if name == "get_service_status":
            result = fetch_json(
                self.api_base_url,
                "/status",
                {"service": str(args["service"])},
            )
            evidence_ids = [f"api:status:{result.get('service', args['service'])}"]
            if result.get("incident_id"):
                evidence_ids.append(f"incident:{result['incident_id']}")
            return ToolExecution(
                name=name,
                kind="http_read",
                args=args,
                result=result,
                evidence_ids=evidence_ids,
            )
        if name == "get_recent_usage":
            result = fetch_json(
                self.api_base_url,
                "/usage",
                {"customer_id": str(args["customer_id"])},
            )
            return ToolExecution(
                name=name,
                kind="http_read",
                args=args,
                result=result,
                evidence_ids=[f"api:usage:{args['customer_id']}"],
            )
        if name == "get_billing":
            result = fetch_json(
                self.api_base_url,
                "/billing",
                {"customer_id": str(args["customer_id"])},
            )
            return ToolExecution(
                name=name,
                kind="http_read",
                args=args,
                result=result,
                evidence_ids=[f"api:billing:{args['customer_id']}"],
            )
        if name == "search_kb":
            hits = search_kb(str(args["query"]), self.kb_dir)
            return ToolExecution(
                name=name,
                kind="kb_read",
                args=args,
                result={"hits": hits},
                evidence_ids=[str(hit["document_id"]) for hit in hits],
            )
        if name == "create_support_ticket":
            result = db.create_support_ticket(
                str(args["customer_id"]),
                str(args["summary"]),
                str(args["priority"]),
                self.db_path,
            )
            return ToolExecution(
                name=name,
                kind="db_write",
                args=args,
                result=result,
                evidence_ids=[f"db:tickets:{result['ticket_id']}"],
                wrote_state=True,
            )
        if name == "escalate_to_human":
            policy_label = str(args["policy_label"])
            if policy_label not in ESCALATION_AUDIT_REASONS:
                raise ValueError(f"Unknown escalation policy label: {policy_label}")
            reason = ESCALATION_AUDIT_REASONS[policy_label]
            result = db.escalate_to_human(
                str(args["customer_id"]),
                reason,
                self.db_path,
            )
            return ToolExecution(
                name=name,
                kind="db_write",
                args=args,
                result=result,
                evidence_ids=[f"db:audit:escalation:{args['customer_id']}"],
                wrote_state=True,
            )
        if name == "update_customer_setting":
            result = db.update_customer_setting(
                str(args["customer_id"]),
                str(args["setting"]),
                str(args["value"]),
                self.db_path,
            )
            return ToolExecution(
                name=name,
                kind="dangerous_db_write",
                args=args,
                result=result,
                evidence_ids=[f"db:settings:{args['customer_id']}:{args['setting']}"],
                wrote_state=True,
            )
        raise ValueError(f"Unknown tool: {name}")


def blocked_tool_execution(
    name: str, args: dict[str, Any], reason: str
) -> ToolExecution:
    """Return a blocked tool-call record without touching local state."""
    return ToolExecution(
        name=name,
        kind="blocked",
        args=args,
        result={"blocked": True, "reason": reason},
        blocked=True,
    )
