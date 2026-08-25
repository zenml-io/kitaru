"""Typed inputs, outputs, and mock commerce records."""

from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, Field


class ResolutionAction(StrEnum):
    """Terminal outcome selected for a support ticket."""

    REFUND = "refund"
    REPLACEMENT = "replacement"
    ESCALATE = "escalate"
    REJECT = "reject"


class TicketInput(BaseModel):
    """One synthetic customer email resolved in a single agent invocation."""

    ticket_id: str
    customer_name: str
    email: str
    subject: str
    body: str


class Resolution(BaseModel):
    """Structured resolution and customer reply returned by the agent."""

    action: ResolutionAction
    amount: Decimal | None = None
    reason: str
    customer_reply: str


class Order(BaseModel):
    """Synthetic order record returned by the commerce system."""

    order_id: str
    email: str
    product: str
    category: str
    amount_paid: Decimal
    status: str
    days_since_delivery: int | None
    final_sale: bool = False
    tracking_no: str | None = None
    risk_flags: list[str] = Field(default_factory=list)
    already_refunded: bool = False


class ReturnPolicy(BaseModel):
    """Category return policy used to decide refund eligibility."""

    category: str
    window_days: int
    defective_full_refund: bool
    unused_return: bool
    final_sale_defect_exception: bool
    human_approval_threshold: Decimal


class PolicyLookup(BaseModel):
    """Return-policy lookup result with a recoverable miss."""

    found: bool
    policy: ReturnPolicy | None = None
    message: str


class ShippingStatus(BaseModel):
    """Synthetic carrier result."""

    tracking_no: str
    status: str
    detail: str


class OrderLookup(BaseModel):
    """Order lookup result supporting exact and email searches."""

    found: bool
    orders: list[Order] = Field(default_factory=list)
    message: str


class ActionReceipt(BaseModel):
    """Recorded result of a mock refund, replacement, or escalation."""

    accepted: bool
    action: ResolutionAction
    order_id: str | None = None
    amount: Decimal | None = None
    receipt_id: str | None = None
    message: str
