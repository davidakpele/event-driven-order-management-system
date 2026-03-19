# tests/test_events.py

import json
import pytest
from datetime import datetime
from kafka.events import (
    BaseEvent, EventTypes, Topics,
    make_order_created_event,
    make_payment_initiated_event,
    make_notification_event,
)


def test_base_event_to_json_is_valid():
    event = BaseEvent(
        event_type=EventTypes.ORDER_CREATED,
        payload={"order_id": "order-1"},
        correlation_id="corr-1",
        source_service="order-service",
    )
    raw = event.to_json()
    data = json.loads(raw)

    assert data["event_type"] == "order.created"
    assert data["payload"] == {"order_id": "order-1"}
    assert data["correlation_id"] == "corr-1"
    assert data["source_service"] == "order-service"
    assert "event_id" in data
    assert "occurred_at" in data


def test_base_event_from_json_roundtrip():
    event = BaseEvent(
        event_type=EventTypes.PAYMENT_SUCCEEDED,
        payload={"payment_id": "pay-1", "order_id": "order-1"},
        correlation_id="corr-2",
    )
    restored = BaseEvent.from_json(event.to_json())

    assert restored.event_type == event.event_type
    assert restored.event_id == event.event_id
    assert restored.payload == event.payload
    assert restored.correlation_id == event.correlation_id


def test_base_event_to_json_handles_datetime_in_payload():
    """Payloads with datetime values (from order.to_dict()) must serialize cleanly."""
    event = BaseEvent(
        event_type=EventTypes.ORDER_CREATED,
        payload={
            "order_id": "order-1",
            "created_at": "2026-03-19T05:45:25.369737+00:00",  # already a string
        },
    )
    raw = event.to_json()
    data = json.loads(raw)
    assert data["payload"]["created_at"] == "2026-03-19T05:45:25.369737+00:00"


def test_make_order_created_event():
    event = make_order_created_event(
        {"order_id": "order-1", "user_id": "user-1"},
        correlation_id="corr-3",
    )
    assert event.event_type == EventTypes.ORDER_CREATED
    assert event.source_service == "order-service"
    assert event.payload["order_id"] == "order-1"
    assert event.correlation_id == "corr-3"


def test_make_payment_initiated_event():
    event = make_payment_initiated_event(
        {"payment_id": "pay-1", "order_id": "order-1"},
    )
    assert event.event_type == EventTypes.PAYMENT_INITIATED
    assert event.source_service == "payment-service"


def test_make_notification_event():
    event = make_notification_event(
        {
            "user_id": "user-1",
            "template_id": "order_confirmed",
            "channel": "email",
            "recipient": "test@example.com",
            "payload": {},
        }
    )
    assert event.event_type == EventTypes.NOTIFICATION_REQUESTED
    assert event.payload["user_id"] == "user-1"


def test_topics_constants():
    assert Topics.ORDERS_CREATED == "orders.created"
    assert Topics.PAYMENTS_INITIATED == "payments.initiated"
    assert Topics.PAYMENTS_COMPLETED == "payments.completed"
    assert Topics.PAYMENTS_FAILED == "payments.failed"
    assert Topics.NOTIFICATIONS_REQUESTED == "notifications.requested"
    assert Topics.DEAD_LETTER == "blast.dlq"