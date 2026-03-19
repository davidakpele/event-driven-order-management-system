# blast_assessment/services/order_service/service.py

import uuid
from typing import Any, Dict, List, Optional

from pymongo.database import Database

from integrations.stripe import StripeClient
from kafka.events import (
    Topics,
    make_order_created_event,
    make_payment_initiated_event,
    make_payment_result_event,
)
from kafka.producers.blast_producer import BlastProducer
from mongodb.models import (
    Address,
    Money,
    Notification,
    Order,
    OrderItem,
    OrderStatus,
    Payment,
    PaymentStatus,
)
from mongodb.repositories import OrderRepository, PaymentRepository, NotificationRepository
from shared.idempotency import IdempotencyStore, generate_idempotency_key
from shared.logging import get_logger

logger = get_logger(__name__)


class OrderService:
    """
    Core order domain service.

    Responsibilities:
    - Create and validate orders
    - Initiate payment via Stripe
    - Publish domain events to Kafka
    - Maintain idempotency for all mutating operations
    - Use optimistic locking on status transitions
    """

    def __init__(
        self,
        db: Database,
        producer: BlastProducer,
        stripe_client: StripeClient,
        idempotency_store: IdempotencyStore,
    ):
        self._order_repo = OrderRepository(db)
        self._payment_repo = PaymentRepository(db)
        self._notification_repo = NotificationRepository(db)
        self._producer = producer
        self._stripe = stripe_client
        self._idempotency = idempotency_store

    def create_order(
        self,
        user_id: str,
        items_data: List[Dict[str, Any]],
        shipping_address_data: Dict[str, Any],
        correlation_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> Order:
        """
        Create a new order.
        Idempotent: same idempotency_key returns cached order.
        """
        idem_key = idempotency_key or generate_idempotency_key("create_order", user_id, str(sorted(
            (i["product_id"], i["quantity"]) for i in items_data
        )))

        # Check idempotency
        existing = self._idempotency.try_start(idem_key)
        if existing and existing.result:
            logger.info("Returning cached order from idempotency store", extra={"idem_key": idem_key})
            return self._order_repo.get_by_id(existing.result["order_id"])

        try:
            items = [
                OrderItem(
                    product_id=item["product_id"],
                    sku=item["sku"],
                    name=item["name"],
                    quantity=item["quantity"],
                    unit_price=Money.from_float(item["unit_price"], item.get("currency", "USD")),
                )
                for item in items_data
            ]

            address = Address(
                line1=shipping_address_data["line1"],
                city=shipping_address_data["city"],
                country=shipping_address_data["country"],
                postal_code=shipping_address_data["postal_code"],
                line2=shipping_address_data.get("line2"),
                state=shipping_address_data.get("state"),
            )

            order = Order(
                user_id=user_id,
                items=items,
                shipping_address=address,
            )

            self._order_repo.insert(order)
            logger.info("Order created", extra={"order_id": order.order_id, "user_id": user_id})

            # Publish domain event
            event = make_order_created_event(order.to_dict(), correlation_id=correlation_id)
            self._producer.produce(
                topic=Topics.ORDERS_CREATED,
                event=event,
                key=order.order_id,  # same order always same partition
            )

            self._idempotency.mark_completed(idem_key, {"order_id": order.order_id})
            return order

        except Exception as e:
            self._idempotency.mark_failed(idem_key, str(e))
            raise

    async def initiate_payment(
        self,
        order_id: str,
        payment_method_id: str,
        customer_email: str,
        stripe_customer_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> Payment:
        """
        Initiate a Stripe payment for an order.
        Idempotent: retrying with same order_id returns the same PaymentIntent.
        """
        order = self._order_repo.get_by_id(order_id)
        if not order:
            raise ValueError(f"Order not found: {order_id}")

        if order.status not in (OrderStatus.PENDING, OrderStatus.CONFIRMED):
            raise ValueError(f"Cannot pay for order in status {order.status}")

        # Derive idempotency key from order_id to make this retry-safe
        idem_key = generate_idempotency_key("payment", order_id)

        # Check for existing payment
        existing_payment = self._payment_repo.get_by_idempotency_key(idem_key)
        if existing_payment and existing_payment.status != PaymentStatus.FAILED:
            logger.info(
                "Returning existing payment",
                extra={"payment_id": existing_payment.payment_id, "order_id": order_id},
            )
            # Ensure order status is updated even on retry
            self._order_repo.update_status(
                order_id,
                OrderStatus.PAYMENT_PROCESSING,
                expected_version=order.version,
            )
            return existing_payment

        # Create payment record
        payment = Payment(
            order_id=order_id,
            user_id=order.user_id,
            amount=order.total,
            provider="stripe",
            idempotency_key=idem_key,
        )
        self._payment_repo.insert(payment)

        # Create Stripe PaymentIntent (idempotent via Stripe's own idempotency key)
        stripe_idem_key = generate_idempotency_key("stripe_intent", order_id)
        intent = await self._stripe.create_payment_intent(
            amount_cents=order.total.amount_cents,
            currency=order.total.currency,
            customer_id=stripe_customer_id,
            metadata={
                "order_id": order_id,
                "payment_id": payment.payment_id,
                "user_id": order.user_id,
            },
            idempotency_key=stripe_idem_key,
        )

        # Store PaymentIntent ID on both order and payment
        provider_payment_id = intent["id"]
        self._order_repo.set_payment_intent(order_id, provider_payment_id)
        self._payment_repo.update_status(
            payment.payment_id,
            PaymentStatus.PROCESSING,
            provider_payment_id=provider_payment_id,
        )

        # Confirm the PaymentIntent
        confirm_idem_key = generate_idempotency_key("stripe_confirm", order_id)
        await self._stripe.confirm_payment_intent(
            payment_intent_id=provider_payment_id,
            payment_method_id=payment_method_id,
            idempotency_key=confirm_idem_key,
        )

        # Transition order to payment_processing
        self._order_repo.update_status(
            order_id,
            OrderStatus.PAYMENT_PROCESSING,
            expected_version=order.version,
        )

        # Publish payment.initiated event
        event = make_payment_initiated_event(payment.to_dict(), correlation_id=correlation_id)
        self._producer.produce(
            topic=Topics.PAYMENTS_INITIATED,
            event=event,
            key=order_id,
        )

        logger.info(
            "Payment initiated",
            extra={
                "order_id": order_id,
                "payment_id": payment.payment_id,
                "provider_payment_id": provider_payment_id,
            },
        )
        return payment

    def cancel_order(
        self,
        order_id: str,
        reason: str = "customer_request",
        correlation_id: Optional[str] = None,
    ) -> Order:
        order = self._order_repo.get_by_id(order_id)
        if not order:
            raise ValueError(f"Order not found: {order_id}")

        cancellable_statuses = {
            OrderStatus.PENDING,
            OrderStatus.CONFIRMED,
            OrderStatus.PAYMENT_FAILED,
        }
        if order.status not in cancellable_statuses:
            raise ValueError(f"Cannot cancel order in status: {order.status}")

        success = self._order_repo.update_status(
            order_id, OrderStatus.CANCELLED, expected_version=order.version
        )
        if not success:
            raise RuntimeError("Concurrent modification detected, please retry")

        from kafka.events import BaseEvent, EventTypes
        event = BaseEvent(
            event_type=EventTypes.ORDER_CANCELLED,
            payload={"order_id": order_id, "reason": reason},
            correlation_id=correlation_id,
            source_service="order-service",
        )
        self._producer.produce(topic=Topics.ORDERS_UPDATED, event=event, key=order_id)

        logger.info("Order cancelled", extra={"order_id": order_id, "reason": reason})
        order.transition_status(OrderStatus.CANCELLED)
        return order

    def get_order(self, order_id: str) -> Optional[Order]:
        return self._order_repo.get_by_id(order_id)

    def list_user_orders(
        self,
        user_id: str,
        status: Optional[OrderStatus] = None,
        limit: int = 20,
        after_id: Optional[str] = None,
    ) -> List[Order]:
        return self._order_repo.list_by_user(user_id, status, limit, after_id)