# tests/test_models.py

import pytest
from datetime import datetime, timezone
from mongodb.models import (
    Order, OrderItem, OrderStatus, Address, Money,
    Payment, PaymentStatus, Notification, NotificationStatus,
)


# ─── Money ────────────────────────────────────────────────────────────────────

def test_money_from_float():
    m = Money.from_float(29.99)
    assert m.amount_cents == 2999
    assert m.currency == "USD"


def test_money_from_float_rounds_correctly():
    m = Money.from_float(0.1 + 0.2)  # classic float precision issue
    assert m.amount_cents == 30


def test_money_to_dict():
    m = Money(amount_cents=5000, currency="EUR")
    assert m.to_dict() == {"amount_cents": 5000, "currency": "EUR"}


def test_money_from_dict():
    m = Money.from_dict({"amount_cents": 1999, "currency": "GBP"})
    assert m.amount_cents == 1999
    assert m.currency == "GBP"


# ─── Order ────────────────────────────────────────────────────────────────────

def _make_order(**kwargs) -> Order:
    defaults = dict(
        user_id="user-1",
        items=[
            OrderItem(
                product_id="prod-1",
                sku="SKU-001",
                name="Test Product",
                quantity=2,
                unit_price=Money.from_float(29.99),
            )
        ],
        shipping_address=Address(
            line1="1 Main St",
            city="Lagos",
            country="NG",
            postal_code="100001",
        ),
    )
    defaults.update(kwargs)
    return Order(**defaults)


def test_order_computes_total():
    order = _make_order()
    assert order.total.amount_cents == 5998  # 2 × 29.99


def test_order_initial_status_is_pending():
    order = _make_order()
    assert order.status == OrderStatus.PENDING


def test_order_status_history_initialized():
    order = _make_order()
    assert len(order.status_history) == 1
    assert order.status_history[0]["status"] == "pending"


def test_order_transition_status():
    order = _make_order()
    order.transition_status(OrderStatus.CONFIRMED)
    assert order.status == OrderStatus.CONFIRMED
    assert order.version == 2
    assert len(order.status_history) == 2


def test_order_to_dict_omits_null_payment_intent():
    order = _make_order()
    d = order.to_dict()
    assert "payment_intent_id" not in d
    assert "external_order_ref" not in d


def test_order_to_dict_includes_payment_intent_when_set():
    order = _make_order()
    order.payment_intent_id = "pi_123"
    d = order.to_dict()
    assert d["payment_intent_id"] == "pi_123"


def test_order_to_dict_created_at_is_string():
    order = _make_order()
    d = order.to_dict()
    assert isinstance(d["created_at"], str)
    assert isinstance(d["updated_at"], str)


def test_order_from_dict_roundtrip():
    order = _make_order()
    order.payment_intent_id = "pi_abc"
    d = order.to_dict()
    restored = Order.from_dict(d)
    assert restored.order_id == order.order_id
    assert restored.user_id == order.user_id
    assert restored.status == order.status
    assert restored.total.amount_cents == order.total.amount_cents
    assert restored.payment_intent_id == "pi_abc"


def test_order_from_dict_without_payment_intent():
    order = _make_order()
    d = order.to_dict()
    restored = Order.from_dict(d)
    assert restored.payment_intent_id is None


# ─── Payment ──────────────────────────────────────────────────────────────────

def _make_payment(**kwargs) -> Payment:
    defaults = dict(
        order_id="order-1",
        user_id="user-1",
        amount=Money(amount_cents=5998, currency="USD"),
        provider="stripe",
        idempotency_key="idem-key-1",
    )
    defaults.update(kwargs)
    return Payment(**defaults)


def test_payment_initial_status_is_pending():
    p = _make_payment()
    assert p.status == PaymentStatus.PENDING


def test_payment_to_dict_has_correct_fields():
    p = _make_payment()
    d = p.to_dict()
    assert d["_id"] == p.payment_id
    assert d["payment_id"] == p.payment_id
    assert d["order_id"] == "order-1"
    assert d["provider"] == "stripe"
    assert d["status"] == "pending"
    assert d["amount"] == {"amount_cents": 5998, "currency": "USD"}


def test_payment_to_dict_no_items_field():
    p = _make_payment()
    d = p.to_dict()
    assert "items" not in d
    assert "shipping_address" not in d
    assert "version" not in d


def test_payment_to_dict_timestamps_are_strings():
    p = _make_payment()
    d = p.to_dict()
    assert isinstance(d["created_at"], str)
    assert isinstance(d["updated_at"], str)


def test_payment_from_dict_roundtrip():
    p = _make_payment()
    d = p.to_dict()
    restored = Payment.from_dict(d)
    assert restored.payment_id == p.payment_id
    assert restored.order_id == p.order_id
    assert restored.amount.amount_cents == p.amount.amount_cents
    assert restored.status == p.status


# ─── Notification ─────────────────────────────────────────────────────────────

def test_notification_to_dict_has_correct_fields():
    n = Notification(
        user_id="user-1",
        template_id="order_confirmed",
        channel="email",
        recipient="test@example.com",
        payload={"order_id": "order-1"},
    )
    d = n.to_dict()
    assert d["_id"] == n.notification_id
    assert d["user_id"] == "user-1"
    assert d["status"] == "pending"
    assert d["send_at"] is None
    assert d["sent_at"] is None
    assert isinstance(d["created_at"], str)