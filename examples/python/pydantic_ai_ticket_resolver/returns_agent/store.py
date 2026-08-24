"""Deterministic local implementations of the commerce tools."""

from decimal import Decimal

from returns_agent.fixtures import ORDERS, POLICIES, SHIPMENTS
from returns_agent.models import (
    ActionReceipt,
    Order,
    OrderLookup,
    PolicyLookup,
    ResolutionAction,
    ShippingStatus,
)


class MockCommerceStore:
    """Hold synthetic commerce data and record local side effects."""

    def __init__(self) -> None:
        """Initialize an isolated copy of the fixture data."""
        self.orders = {
            key: value.model_copy(deep=True) for key, value in ORDERS.items()
        }
        self.actions: list[ActionReceipt] = []

    def lookup_order(
        self, order_id: str | None = None, email: str | None = None
    ) -> OrderLookup:
        """Look up an order by number or customer email."""
        if order_id is not None and order_id in self.orders:
            return OrderLookup(
                found=True,
                orders=[self.orders[order_id]],
                message="One order matched the supplied order number.",
            )
        if email is not None:
            matches = [order for order in self.orders.values() if order.email == email]
            if matches:
                return OrderLookup(
                    found=True,
                    orders=matches,
                    message=f"{len(matches)} order(s) matched the supplied email.",
                )
        return OrderLookup(
            found=False,
            message="No order matched the supplied information.",
        )

    def get_return_policy(self, category: str) -> PolicyLookup:
        """Return a policy by canonical category or common product alias."""
        aliases = {
            "backpack": "accessories",
            "carry-on": "luggage",
            "hoodie": "apparel",
            "jacket": "apparel",
            "loafers": "footwear",
            "shoes": "footwear",
            "tote": "accessories",
        }
        normalized = aliases.get(category.lower(), category.lower())
        policy = POLICIES.get(normalized)
        if policy is None:
            return PolicyLookup(
                found=False,
                message=(
                    f"No policy matched {category!r}. Use the category returned "
                    "by lookup_order."
                ),
            )
        return PolicyLookup(
            found=True,
            policy=policy,
            message=f"Policy matched canonical category {normalized!r}.",
        )

    def check_shipping(self, tracking_no: str) -> ShippingStatus:
        """Return the current synthetic carrier status."""
        return SHIPMENTS.get(
            tracking_no,
            ShippingStatus(
                tracking_no=tracking_no,
                status="delivered",
                detail="Carrier reports the package as delivered.",
            ),
        )

    def issue_refund(self, order_id: str, amount: Decimal) -> ActionReceipt:
        """Record a mock refund without contacting a payment processor."""
        order = self.orders.get(order_id)
        if order is None:
            return self._record(
                ActionReceipt(
                    accepted=False,
                    action=ResolutionAction.REFUND,
                    order_id=order_id,
                    amount=amount,
                    message="Refund rejected because the order does not exist.",
                )
            )
        if order.already_refunded:
            return self._record(
                ActionReceipt(
                    accepted=False,
                    action=ResolutionAction.REFUND,
                    order_id=order_id,
                    amount=amount,
                    message="Refund rejected because the order was already refunded.",
                )
            )
        if amount <= 0 or amount > order.amount_paid:
            return self._record(
                ActionReceipt(
                    accepted=False,
                    action=ResolutionAction.REFUND,
                    order_id=order_id,
                    amount=amount,
                    message=f"Refund rejected. The maximum is {order.amount_paid}.",
                )
            )
        order.already_refunded = True
        return self._record(
            ActionReceipt(
                accepted=True,
                action=ResolutionAction.REFUND,
                order_id=order_id,
                amount=amount,
                receipt_id=f"mock-refund-{order_id}",
                message="Mock refund recorded.",
            )
        )

    def create_replacement(self, order_id: str) -> ActionReceipt:
        """Record a mock replacement without creating a fulfillment order."""
        if order_id not in self.orders:
            return self._record(
                ActionReceipt(
                    accepted=False,
                    action=ResolutionAction.REPLACEMENT,
                    order_id=order_id,
                    message="Replacement rejected because the order does not exist.",
                )
            )
        return self._record(
            ActionReceipt(
                accepted=True,
                action=ResolutionAction.REPLACEMENT,
                order_id=order_id,
                receipt_id=f"mock-replacement-{order_id}",
                message="Mock replacement recorded.",
            )
        )

    def escalate_to_human(self, reason: str) -> ActionReceipt:
        """Record a mock escalation without contacting a support queue."""
        return self._record(
            ActionReceipt(
                accepted=True,
                action=ResolutionAction.ESCALATE,
                receipt_id=f"mock-escalation-{len(self.actions) + 1}",
                message=f"Mock escalation recorded: {reason}",
            )
        )

    def _record(self, receipt: ActionReceipt) -> ActionReceipt:
        """Append and return one mock side-effect receipt."""
        self.actions.append(receipt)
        return receipt


def get_order(order_id: str) -> Order:
    """Return one immutable fixture order for tests and labels."""
    return ORDERS[order_id]
