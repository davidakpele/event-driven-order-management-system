import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from mongodb.models import Order, OrderStatus, OrderItem, Address, Money, Payment, PaymentStatus
from services.order_service.service import OrderService


def _make_items_data():
    return [
        {
            "product_id": "prod-1",
            "sku": "SKU-001",
            "name": "Test Product",
            "quantity": 2,
            "unit_price": 29.99,
            "currency": "USD",
        }
    ]


def _make_address_data():
    return {
        "line1": "1 Main St",
        "city": "Lagos",
        "country": "NG",
        "postal_code": "100001",
    }


def _make_service():
    db = MagicMock()
    producer = MagicMock()
    stripe_client = MagicMock()
    idempotency_store = MagicMock()

    # Patch repositories so they don't hit MongoDB
    with patch("services.order_service.service.OrderRepository"), \
         patch("services.order_service.service.PaymentRepository"), \
         patch("services.order_service.service.NotificationRepository"):
        svc = OrderService(
            db=db,
            producer=producer,
            stripe_client=stripe_client,
            idempotency_store=idempotency_store,
        )
    return svc, producer, stripe_client, idempotency_store


# ─── create_order 

def test_create_order_returns_order():
    svc, producer, _, idempotency_store = _make_service()

    idempotency_store.try_start.return_value = MagicMock(result=None)
    svc._order_repo.insert.return_value = "order-1"

    order = svc.create_order(
        user_id="user-1",
        items_data=_make_items_data(),
        shipping_address_data=_make_address_data(),
        idempotency_key="idem-1",
    )

    assert isinstance(order, Order)
    assert order.user_id == "user-1"
    assert order.status == OrderStatus.PENDING
    assert order.total.amount_cents == 5998


def test_create_order_publishes_kafka_event():
    svc, producer, _, idempotency_store = _make_service()
    idempotency_store.try_start.return_value = MagicMock(result=None)

    svc.create_order(
        user_id="user-1",
        items_data=_make_items_data(),
        shipping_address_data=_make_address_data(),
        idempotency_key="idem-2",
    )

    producer.produce.assert_called_once()
    call_kwargs = producer.produce.call_args
    assert call_kwargs.kwargs["topic"] == "orders.created"


def test_create_order_returns_cached_on_duplicate_idempotency_key():
    svc, producer, _, idempotency_store = _make_service()

    cached_order = Order(
        user_id="user-1",
        items=[OrderItem(
            product_id="prod-1", sku="SKU-001", name="Test",
            quantity=1, unit_price=Money(1000)
        )],
        shipping_address=Address(line1="1 St", city="Lagos", country="NG", postal_code="100"),
    )
    idempotency_store.try_start.return_value = MagicMock(result={"order_id": cached_order.order_id})
    svc._order_repo.get_by_id.return_value = cached_order

    result = svc.create_order(
        user_id="user-1",
        items_data=_make_items_data(),
        shipping_address_data=_make_address_data(),
        idempotency_key="idem-duplicate",
    )

    svc._order_repo.insert.assert_not_called()
    producer.produce.assert_not_called()
    assert result.order_id == cached_order.order_id


def test_create_order_marks_idempotency_failed_on_exception():
    svc, _, _, idempotency_store = _make_service()
    idempotency_store.try_start.return_value = MagicMock(result=None)
    svc._order_repo.insert.side_effect = RuntimeError("DB error")

    with pytest.raises(RuntimeError):
        svc.create_order(
            user_id="user-1",
            items_data=_make_items_data(),
            shipping_address_data=_make_address_data(),
            idempotency_key="idem-fail",
        )

    idempotency_store.mark_failed.assert_called_once()


# ─── cancel_order

def test_cancel_order_success():
    svc, producer, _, _ = _make_service()

    order = Order(
        user_id="user-1",
        items=[OrderItem(
            product_id="p1", sku="s1", name="n",
            quantity=1, unit_price=Money(1000)
        )],
        shipping_address=Address(line1="1 St", city="Lagos", country="NG", postal_code="100"),
    )
    svc._order_repo.get_by_id.return_value = order
    svc._order_repo.update_status.return_value = True

    result = svc.cancel_order(order.order_id)

    assert result.status == OrderStatus.CANCELLED
    svc._order_repo.update_status.assert_called_once_with(
        order.order_id, OrderStatus.CANCELLED, expected_version=1
    )
    producer.produce.assert_called_once()


def test_cancel_order_raises_if_not_found():
    svc, _, _, _ = _make_service()
    svc._order_repo.get_by_id.return_value = None

    with pytest.raises(ValueError, match="Order not found"):
        svc.cancel_order("nonexistent-id")


def test_cancel_order_raises_if_already_paid():
    svc, _, _, _ = _make_service()

    order = Order(
        user_id="user-1",
        items=[OrderItem(
            product_id="p1", sku="s1", name="n",
            quantity=1, unit_price=Money(1000)
        )],
        shipping_address=Address(line1="1 St", city="Lagos", country="NG", postal_code="100"),
        status=OrderStatus.PAID,
    )
    svc._order_repo.get_by_id.return_value = order

    with pytest.raises(ValueError, match="Cannot cancel"):
        svc.cancel_order(order.order_id)


def test_cancel_order_raises_on_version_conflict():
    svc, _, _, _ = _make_service()

    order = Order(
        user_id="user-1",
        items=[OrderItem(
            product_id="p1", sku="s1", name="n",
            quantity=1, unit_price=Money(1000)
        )],
        shipping_address=Address(line1="1 St", city="Lagos", country="NG", postal_code="100"),
    )
    svc._order_repo.get_by_id.return_value = order
    svc._order_repo.update_status.return_value = False  # version conflict

    with pytest.raises(RuntimeError, match="Concurrent modification"):
        svc.cancel_order(order.order_id)