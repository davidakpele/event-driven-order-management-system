# blast_assessment/kafka/consumers/payment_consumer.py

from confluent_kafka import Message
from pymongo.database import Database

from kafka.consumers.base_consumer import BaseConsumer
from kafka.events import BaseEvent, EventTypes, Topics, make_notification_event
from kafka.producers.blast_producer import BlastProducer
from mongodb.models import OrderStatus, PaymentStatus
from mongodb.repositories import OrderRepository, PaymentRepository, NotificationRepository
from mongodb.models import Notification
from shared.logging import get_logger

logger = get_logger(__name__)


class PaymentResultConsumer(BaseConsumer):
    """
    Consumes payment.succeeded and payment.failed events.
    Updates order status and enqueues notification events.

    Consumer group: blast.payment-result-consumer
    Topics: payments.completed, payments.failed

    Ordering: payment events are keyed by order_id, ensuring that
    all payment events for a given order land on the same partition
    and are processed in order within that partition.
    """

    GROUP_ID = "payment-result-consumer"

    def __init__(self, db: Database, producer: BlastProducer):
        super().__init__(
            group_id=self.GROUP_ID,
            topics=[Topics.PAYMENTS_COMPLETED, Topics.PAYMENTS_FAILED],
            producer=producer,
        )
        self._order_repo = OrderRepository(db)
        self._payment_repo = PaymentRepository(db)
        self._notification_repo = NotificationRepository(db)

    def handle(self, event: BaseEvent, msg: Message) -> None:
        event_type = event.event_type

        if event_type == EventTypes.PAYMENT_SUCCEEDED:
            self._handle_payment_succeeded(event)
        elif event_type == EventTypes.PAYMENT_FAILED:
            self._handle_payment_failed(event)
        else:
            logger.warning("Unhandled event type", extra={"event_type": event_type})

    def _handle_payment_succeeded(self, event: BaseEvent) -> None:
        payload = event.payload
        order_id = payload["order_id"]
        payment_id = payload["payment_id"]

        logger.info("Handling payment succeeded", extra={"order_id": order_id})

        order = self._order_repo.get_by_id(order_id)
        if not order:
            raise ValueError(f"Order not found: {order_id}")

        # Update payment record
        self._payment_repo.update_status(
            payment_id=payment_id,
            new_status=PaymentStatus.SUCCEEDED,
            provider_payment_id=payload.get("provider_payment_id"),
            provider_charge_id=payload.get("provider_charge_id"),
        )

        # Transition order status with optimistic locking
        success = self._order_repo.update_status(
            order_id=order_id,
            new_status=OrderStatus.PAID,
            expected_version=order.version,
        )
        if not success:
            logger.warning(
                "Order status update conflict on payment success, will be retried",
                extra={"order_id": order_id},
            )
            raise RuntimeError(f"Version conflict updating order {order_id}")

        # Emit notification event
        notification_event = make_notification_event(
            notification_dict={
                "user_id": order.user_id,
                "template_id": "order_payment_confirmed",
                "channel": "email",
                "recipient": payload.get("customer_email", ""),
                "reference_id": order_id,
                "reference_type": "order",
                "payload": {
                    "order_id": order_id,
                    "total": order.total.to_dict() if order.total else {},
                },
            },
            correlation_id=event.correlation_id,
        )
        self._producer.produce(
            topic=Topics.NOTIFICATIONS_REQUESTED,
            event=notification_event,
            key=order.user_id,
        )
        logger.info("Payment succeeded, order marked PAID", extra={"order_id": order_id})

    def _handle_payment_failed(self, event: BaseEvent) -> None:
        payload = event.payload
        order_id = payload["order_id"]
        payment_id = payload["payment_id"]
        failure_reason = payload.get("failure_reason", "unknown")

        logger.warning(
            "Handling payment failed",
            extra={"order_id": order_id, "reason": failure_reason},
        )

        order = self._order_repo.get_by_id(order_id)
        if not order:
            raise ValueError(f"Order not found: {order_id}")

        self._payment_repo.update_status(
            payment_id=payment_id,
            new_status=PaymentStatus.FAILED,
            failure_reason=failure_reason,
        )

        success = self._order_repo.update_status(
            order_id=order_id,
            new_status=OrderStatus.PAYMENT_FAILED,
            expected_version=order.version,
        )
        if not success:
            raise RuntimeError(f"Version conflict updating order {order_id}")

        # Notify user of payment failure
        notification_event = make_notification_event(
            notification_dict={
                "user_id": order.user_id,
                "template_id": "order_payment_failed",
                "channel": "email",
                "recipient": payload.get("customer_email", ""),
                "reference_id": order_id,
                "reference_type": "order",
                "payload": {
                    "order_id": order_id,
                    "failure_reason": failure_reason,
                },
            },
            correlation_id=event.correlation_id,
        )
        self._producer.produce(
            topic=Topics.NOTIFICATIONS_REQUESTED,
            event=notification_event,
            key=order.user_id,
        )
        logger.warning("Payment failed, order marked PAYMENT_FAILED", extra={"order_id": order_id})