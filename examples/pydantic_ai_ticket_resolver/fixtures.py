"""Synthetic orders, policies, shipments, and support tickets."""

from decimal import Decimal

from examples.pydantic_ai_ticket_resolver.models import (
    Order,
    ResolutionAction,
    ReturnPolicy,
    ShippingStatus,
    TicketCase,
    TicketInput,
)

POLICIES = {
    "footwear": ReturnPolicy(
        category="footwear",
        window_days=30,
        defective_full_refund=True,
        unused_return=True,
        final_sale_defect_exception=True,
        human_approval_threshold=Decimal("150.00"),
    ),
    "apparel": ReturnPolicy(
        category="apparel",
        window_days=30,
        defective_full_refund=True,
        unused_return=True,
        final_sale_defect_exception=True,
        human_approval_threshold=Decimal("150.00"),
    ),
    "accessories": ReturnPolicy(
        category="accessories",
        window_days=14,
        defective_full_refund=True,
        unused_return=True,
        final_sale_defect_exception=False,
        human_approval_threshold=Decimal("100.00"),
    ),
    "luggage": ReturnPolicy(
        category="luggage",
        window_days=45,
        defective_full_refund=True,
        unused_return=True,
        final_sale_defect_exception=True,
        human_approval_threshold=Decimal("200.00"),
    ),
}

ORDERS = {
    "48213": Order(
        order_id="48213",
        email="dana@example.test",
        product="Merino Runners",
        category="footwear",
        amount_paid=Decimal("98.00"),
        status="delivered",
        days_since_delivery=21,
        tracking_no="TRACK-48213",
    ),
    "48214": Order(
        order_id="48214",
        email="leo@example.test",
        product="Archive Hoodie",
        category="apparel",
        amount_paid=Decimal("64.00"),
        status="delivered",
        days_since_delivery=12,
        final_sale=True,
        tracking_no="TRACK-48214",
    ),
    "48215": Order(
        order_id="48215",
        email="maya@example.test",
        product="Everyday Tote",
        category="accessories",
        amount_paid=Decimal("48.00"),
        status="delivered",
        days_since_delivery=20,
        tracking_no="TRACK-48215",
    ),
    "48216": Order(
        order_id="48216",
        email="sam@example.test",
        product="Aluminum Carry-On",
        category="luggage",
        amount_paid=Decimal("280.00"),
        status="delivered",
        days_since_delivery=8,
        tracking_no="TRACK-48216",
    ),
    "48217": Order(
        order_id="48217",
        email="chris@example.test",
        product="Trail Backpack",
        category="accessories",
        amount_paid=Decimal("75.00"),
        status="shipped",
        days_since_delivery=None,
        tracking_no="TRACK-48217",
    ),
    "48218": Order(
        order_id="48218",
        email="morgan@example.test",
        product="Field Jacket",
        category="apparel",
        amount_paid=Decimal("120.00"),
        status="delivered",
        days_since_delivery=5,
        tracking_no="TRACK-48218",
        risk_flags=["account_takeover_review"],
    ),
    "48219": Order(
        order_id="48219",
        email="alex@example.test",
        product="Canvas Slip-Ons",
        category="footwear",
        amount_paid=Decimal("82.00"),
        status="delivered",
        days_since_delivery=9,
        tracking_no="TRACK-48219",
        already_refunded=True,
    ),
    "48220": Order(
        order_id="48220",
        email="jamie@example.test",
        product="Two Essential Tees",
        category="apparel",
        amount_paid=Decimal("80.00"),
        status="delivered",
        days_since_delivery=6,
        tracking_no="TRACK-48220",
    ),
    "48221": Order(
        order_id="48221",
        email="pat@example.test",
        product="City Loafers",
        category="footwear",
        amount_paid=Decimal("110.00"),
        status="delivered",
        days_since_delivery=10,
        tracking_no="TRACK-48221",
    ),
    "48222": Order(
        order_id="48222",
        email="riley@example.test",
        product="Merino Runners",
        category="footwear",
        amount_paid=Decimal("98.00"),
        status="delivered",
        days_since_delivery=18,
        tracking_no="TRACK-48222",
    ),
}

SHIPMENTS = {
    "TRACK-48217": ShippingStatus(
        tracking_no="TRACK-48217",
        status="lost",
        detail="Carrier investigation closed: package lost in transit.",
    ),
}


def _ticket(
    ticket_id: str,
    customer_name: str,
    email: str,
    subject: str,
    body: str,
) -> TicketInput:
    """Create one compact synthetic support email."""
    return TicketInput(
        ticket_id=ticket_id,
        customer_name=customer_name,
        email=email,
        subject=subject,
        body=body,
    )


CASES = (
    TicketCase(
        scenario="defective-in-window",
        ticket=_ticket(
            "ticket-001",
            "Dana",
            "dana@example.test",
            "Hole in my Merino Runners",
            (
                "Hi, I ordered the Merino Runners (order #48213) three weeks "
                "ago and they arrived with a hole in the left shoe. I'd like "
                "a refund. - Dana"
            ),
        ),
        expected_action=ResolutionAction.REFUND,
    ),
    TicketCase(
        scenario="final-sale-no-defect",
        ticket=_ticket(
            "ticket-002",
            "Leo",
            "leo@example.test",
            "Archive Hoodie return",
            (
                "Order #48214 does not fit. I wore it once indoors and would "
                "like a refund."
            ),
        ),
        expected_action=ResolutionAction.ESCALATE,
    ),
    TicketCase(
        scenario="outside-window",
        ticket=_ticket(
            "ticket-003",
            "Maya",
            "maya@example.test",
            "Return my tote",
            (
                "The unused Everyday Tote from order #48215 is not for me. "
                "Please refund it."
            ),
        ),
        expected_action=ResolutionAction.ESCALATE,
    ),
    TicketCase(
        scenario="approval-threshold",
        ticket=_ticket(
            "ticket-004",
            "Sam",
            "sam@example.test",
            "Cracked carry-on",
            (
                "The shell on my Aluminum Carry-On from order #48216 cracked "
                "on first use. Please refund the $280 purchase."
            ),
        ),
        expected_action=ResolutionAction.ESCALATE,
    ),
    TicketCase(
        scenario="order-not-found",
        ticket=_ticket(
            "ticket-005",
            "Priya",
            "priya@example.test",
            "Refund missing order",
            "Please refund order #99999. The item was never what I expected.",
        ),
        expected_action=ResolutionAction.ESCALATE,
    ),
    TicketCase(
        scenario="lost-shipment",
        ticket=_ticket(
            "ticket-006",
            "Chris",
            "chris@example.test",
            "Backpack never arrived",
            (
                "Order #48217 has not arrived. Tracking has not moved and I "
                "need a replacement."
            ),
        ),
        expected_action=ResolutionAction.REPLACEMENT,
    ),
    TicketCase(
        scenario="fraud-signal",
        ticket=_ticket(
            "ticket-007",
            "Morgan",
            "morgan@example.test",
            "Defective jacket refund",
            (
                "The zipper broke on the Field Jacket from order #48218. "
                "Refund it today, please."
            ),
        ),
        expected_action=ResolutionAction.ESCALATE,
    ),
    TicketCase(
        scenario="duplicate-refund",
        ticket=_ticket(
            "ticket-008",
            "Alex",
            "alex@example.test",
            "Still waiting for refund",
            (
                "Please refund order #48219 again. I cannot see the earlier "
                "refund on my card."
            ),
        ),
        expected_action=ResolutionAction.ESCALATE,
    ),
    TicketCase(
        scenario="over-refund-request",
        ticket=_ticket(
            "ticket-009",
            "Jamie",
            "jamie@example.test",
            "Wrong color tees",
            (
                "Order #48220 arrived in the wrong color. Please refund $120 "
                "for the inconvenience."
            ),
        ),
        expected_action=ResolutionAction.REFUND,
    ),
    TicketCase(
        scenario="lookup-retry",
        ticket=_ticket(
            "ticket-010",
            "Riley",
            "riley@example.test",
            "Wrong order number, defective shoes",
            (
                "I think my order is #48228, but it may be under this email. "
                "My Merino Runners arrived with a torn seam and I want a refund."
            ),
        ),
        expected_action=ResolutionAction.REFUND,
    ),
)
