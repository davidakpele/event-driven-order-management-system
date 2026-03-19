from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
import uuid
import json


# ─── Topic Definitions

class Topics:
    ORDERS_CREATED = "orders.created"
    ORDERS_UPDATED = "orders.updated"
    PAYMENTS_INITIATED = "payments.initiated"
    PAYMENTS_COMPLETED = "payments.completed"
    PAYMENTS_FAILED = "payments.failed"
    NOTIFICATIONS_REQUESTED = "notifications.requested"
    NOTIFICATIONS_SENT = "notifications.sent"
    WEBHOOKS_RECEIVED = "webhooks.received"
    DEAD_LETTER = "blast.dlq"


# ─── Event Base 

@dataclass
class BaseEvent:
    event_type: str
    payload: Dict[str, Any]
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str = "1.0"
    occurred_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    correlation_id: Optional[str] = None
    source_service: Optional[str] = None

    def to_json(self) -> str:
        return json.dumps({
            "event_id": self.event_id,
            "event_type": self.event_type,
            "version": self.version,
            "occurred_at": self.occurred_at,
            "correlation_id": self.correlation_id,
            "source_service": self.source_service,
            "payload": self.payload,
        })

    @classmethod
    def from_json(cls, data: str) -> "BaseEvent":
        d = json.loads(data)
        return cls(
            event_id=d["event_id"],
            event_type=d["event_type"],
            payload=d["payload"],
            version=d.get("version", "1.0"),
            occurred_at=d.get("occurred_at"),
            correlation_id=d.get("correlation_id"),
            source_service=d.get("source_service"),
        )


# ─── Domain Events 

class EventTypes:
    ORDER_CREATED = "order.created"
    ORDER_CONFIRMED = "order.confirmed"
    ORDER_CANCELLED = "order.cancelled"
    PAYMENT_INITIATED = "payment.initiated"
    PAYMENT_SUCCEEDED = "payment.succeeded"
    PAYMENT_FAILED = "payment.failed"
    NOTIFICATION_REQUESTED = "notification.requested"
    NOTIFICATION_SENT = "notification.sent"
    WEBHOOK_RECEIVED = "webhook.received"


def make_order_created_event(order_dict: Dict[str, Any], correlation_id: Optional[str] = None) -> BaseEvent:
    return BaseEvent(
        event_type=EventTypes.ORDER_CREATED,
        payload=order_dict,
        correlation_id=correlation_id,
        source_service="order-service",
    )


def make_payment_initiated_event(payment_dict: Dict[str, Any], correlation_id: Optional[str] = None) -> BaseEvent:
    return BaseEvent(
        event_type=EventTypes.PAYMENT_INITIATED,
        payload=payment_dict,
        correlation_id=correlation_id,
        source_service="payment-service",
    )


def make_payment_result_event(
    event_type: str,
    payment_dict: Dict[str, Any],
    correlation_id: Optional[str] = None,
) -> BaseEvent:
    return BaseEvent(
        event_type=event_type,
        payload=payment_dict,
        correlation_id=correlation_id,
        source_service="payment-service",
    )


def make_notification_event(
    notification_dict: Dict[str, Any],
    correlation_id: Optional[str] = None,
) -> BaseEvent:
    return BaseEvent(
        event_type=EventTypes.NOTIFICATION_REQUESTED,
        payload=notification_dict,
        correlation_id=correlation_id,
        source_service="order-service",
    )