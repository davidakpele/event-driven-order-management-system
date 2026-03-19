from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional
import uuid

class OrderStatus(str, Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    PAYMENT_PROCESSING = "payment_processing"
    PAYMENT_FAILED = "payment_failed"
    PAID = "paid"
    FULFILLING = "fulfilling"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"


class PaymentStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    REFUNDED = "refunded"


class NotificationStatus(str, Enum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    BOUNCED = "bounced"


@dataclass
class Money:
    amount_cents: int
    currency: str = "USD"

    @property
    def amount(self) -> Decimal:
        return Decimal(self.amount_cents) / 100

    @classmethod
    def from_float(cls, amount: float, currency: str = "USD") -> "Money":
        return cls(amount_cents=round(amount * 100), currency=currency)

    def to_dict(self) -> Dict[str, Any]:
        return {"amount_cents": self.amount_cents, "currency": self.currency}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Money":
        return cls(amount_cents=data["amount_cents"], currency=data.get("currency", "USD"))


@dataclass
class OrderItem:
    product_id: str
    sku: str
    name: str
    quantity: int
    unit_price: Money

    @property
    def subtotal(self) -> Money:
        return Money(
            amount_cents=self.unit_price.amount_cents * self.quantity,
            currency=self.unit_price.currency,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "product_id": self.product_id,
            "sku": self.sku,
            "name": self.name,
            "quantity": self.quantity,
            "unit_price": self.unit_price.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderItem":
        return cls(
            product_id=data["product_id"],
            sku=data["sku"],
            name=data["name"],
            quantity=data["quantity"],
            unit_price=Money.from_dict(data["unit_price"]),
        )


@dataclass
class Address:
    line1: str
    city: str
    country: str
    postal_code: str
    line2: Optional[str] = None
    state: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Address":
        return cls(**{k: data.get(k) for k in cls.__dataclass_fields__ if k in data or data.get(k)})


@dataclass
class Order:
    user_id: str
    items: List[OrderItem]
    shipping_address: Address
    order_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: OrderStatus = OrderStatus.PENDING
    total: Optional[Money] = None
    payment_intent_id: Optional[str] = None
    external_order_ref: Optional[str] = None
    status_history: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    def __post_init__(self):
        if self.total is None:
            self.total = self._compute_total()
        if not self.status_history:
            self.status_history = [self._status_entry(self.status)]

    def _compute_total(self) -> Money:
        if not self.items:
            return Money(0)
        currency = self.items[0].unit_price.currency
        total_cents = sum(item.subtotal.amount_cents for item in self.items)
        return Money(amount_cents=total_cents, currency=currency)

    def _status_entry(self, status: OrderStatus) -> Dict[str, Any]:
        return {"status": status.value, "at": datetime.now(timezone.utc).isoformat()}

    def transition_status(self, new_status: OrderStatus) -> None:
        self.status = new_status
        self.status_history.append(self._status_entry(new_status))
        self.updated_at = datetime.now(timezone.utc)
        self.version += 1

    def to_dict(self) -> Dict[str, Any]:
        doc: Dict[str, Any] = {
            "_id": self.order_id,
            "order_id": self.order_id,
            "user_id": self.user_id,
            "status": self.status.value,
            "items": [item.to_dict() for item in self.items],
            "total": self.total.to_dict() if self.total else None,
            "shipping_address": self.shipping_address.to_dict(),
            "status_history": self.status_history,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
            "updated_at": self.updated_at.isoformat() if isinstance(self.updated_at, datetime) else self.updated_at,
            "version": self.version,
        }
        if self.payment_intent_id is not None:
            doc["payment_intent_id"] = self.payment_intent_id
        if self.external_order_ref is not None:
            doc["external_order_ref"] = self.external_order_ref
        return doc

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Order":
        obj = cls.__new__(cls)
        obj.order_id = data.get("order_id") or data.get("_id")
        obj.user_id = data["user_id"]
        obj.status = OrderStatus(data["status"])
        obj.items = [OrderItem.from_dict(i) for i in data.get("items", [])]
        obj.total = Money.from_dict(data["total"]) if data.get("total") else None
        obj.shipping_address = Address.from_dict(data["shipping_address"])
        obj.payment_intent_id = data.get("payment_intent_id")
        obj.external_order_ref = data.get("external_order_ref")
        obj.status_history = data.get("status_history", [])
        obj.metadata = data.get("metadata", {})
        obj.created_at = data.get("created_at", datetime.now(timezone.utc))
        obj.updated_at = data.get("updated_at", datetime.now(timezone.utc))
        obj.version = data.get("version", 1)
        return obj


@dataclass
class Payment:
    order_id: str
    user_id: str
    amount: Money
    provider: str
    idempotency_key: str
    payment_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: PaymentStatus = PaymentStatus.PENDING
    provider_payment_id: Optional[str] = None
    provider_charge_id: Optional[str] = None
    failure_reason: Optional[str] = None
    refunded_amount: Optional[Money] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "_id": self.payment_id,
            "payment_id": self.payment_id,
            "order_id": self.order_id,
            "user_id": self.user_id,
            "amount": self.amount.to_dict(),
            "provider": self.provider,
            "idempotency_key": self.idempotency_key,
            "status": self.status.value,
            "provider_payment_id": self.provider_payment_id,
            "provider_charge_id": self.provider_charge_id,
            "failure_reason": self.failure_reason,
            "refunded_amount": self.refunded_amount.to_dict() if self.refunded_amount else None,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
            "updated_at": self.updated_at.isoformat() if isinstance(self.updated_at, datetime) else self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Payment":
        obj = cls.__new__(cls)
        obj.payment_id = data.get("payment_id") or data.get("_id")
        obj.order_id = data["order_id"]
        obj.user_id = data["user_id"]
        obj.amount = Money.from_dict(data["amount"])
        obj.provider = data["provider"]
        obj.idempotency_key = data["idempotency_key"]
        obj.status = PaymentStatus(data["status"])
        obj.provider_payment_id = data.get("provider_payment_id")
        obj.provider_charge_id = data.get("provider_charge_id")
        obj.failure_reason = data.get("failure_reason")
        obj.refunded_amount = Money.from_dict(data["refunded_amount"]) if data.get("refunded_amount") else None
        obj.metadata = data.get("metadata", {})
        obj.created_at = data.get("created_at", datetime.now(timezone.utc))
        obj.updated_at = data.get("updated_at", datetime.now(timezone.utc))
        return obj


@dataclass
class Notification:
    user_id: str
    template_id: str
    channel: str
    recipient: str
    payload: Dict[str, Any]
    notification_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: NotificationStatus = NotificationStatus.PENDING
    reference_id: Optional[str] = None
    reference_type: Optional[str] = None
    provider_message_id: Optional[str] = None
    failure_reason: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    send_at: Optional[datetime] = None
    sent_at: Optional[datetime] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "_id": self.notification_id,
            "notification_id": self.notification_id,
            "user_id": self.user_id,
            "template_id": self.template_id,
            "channel": self.channel,
            "recipient": self.recipient,
            "payload": self.payload,
            "status": self.status.value,
            "reference_id": self.reference_id,
            "reference_type": self.reference_type,
            "provider_message_id": self.provider_message_id,
            "failure_reason": self.failure_reason,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "send_at": self.send_at.isoformat() if self.send_at else None,
            "sent_at": self.sent_at.isoformat() if self.sent_at else None,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
        }