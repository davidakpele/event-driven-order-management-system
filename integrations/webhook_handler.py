# blast_assessment/integrations/webhook_handler.py

import json
import uuid
from typing import Any, Dict, Optional

from kafka.events import BaseEvent, EventTypes, Topics
from kafka.producers.blast_producer import BlastProducer
from integrations.stripe import StripeClient
from shared.logging import get_logger, set_correlation_id

logger = get_logger(__name__)

# Stripe event types we care about
HANDLED_STRIPE_EVENTS = {
    "payment_intent.succeeded",
    "payment_intent.payment_failed",
    "charge.refunded",
    "customer.subscription.deleted",
    "invoice.payment_failed",
}


class StripeWebhookHandler:
    """
    Processes inbound Stripe webhooks.

    Flow:
    1. Verify HMAC signature (reject invalid/replayed requests)
    2. Extract event type and route to handler
    3. Publish domain event to Kafka for downstream consumers
    4. Return 200 quickly — heavy processing is async via Kafka

    Idempotency: Stripe retries webhooks on non-2xx. We use the Stripe event_id
    as an idempotency key to deduplicate at the Kafka level (the producer
    has idempotence enabled, and consumers check event_id before processing).
    """

    def __init__(self, stripe_client: StripeClient, producer: BlastProducer):
        self._stripe = stripe_client
        self._producer = producer

    def process(self, raw_body: bytes, stripe_signature: str) -> Dict[str, Any]:
        """
        Entry point for webhook handler.
        Returns response dict with status.
        """
        try:
            stripe_event = self._stripe.verify_webhook(raw_body, stripe_signature)
        except ValueError as e:
            logger.warning("Webhook signature verification failed", extra={"error": str(e)})
            return {"status": "rejected", "reason": str(e)}

        event_id = stripe_event.get("id", str(uuid.uuid4()))
        event_type = stripe_event.get("type", "")
        stripe_object = stripe_event.get("data", {}).get("object", {})

        # Use Stripe event ID as correlation ID for full traceability
        set_correlation_id(event_id)
        logger.info("Stripe webhook received", extra={"event_type": event_type, "event_id": event_id})

        if event_type not in HANDLED_STRIPE_EVENTS:
            logger.info("Unhandled Stripe event type, ignoring", extra={"event_type": event_type})
            return {"status": "ignored", "event_type": event_type}

        handler = self._get_handler(event_type)
        if handler:
            handler(stripe_event, stripe_object)

        return {"status": "ok", "event_id": event_id}

    def _get_handler(self, event_type: str):
        return {
            "payment_intent.succeeded": self._on_payment_intent_succeeded,
            "payment_intent.payment_failed": self._on_payment_intent_failed,
            "charge.refunded": self._on_charge_refunded,
        }.get(event_type)

    def _on_payment_intent_succeeded(
        self, stripe_event: Dict[str, Any], payment_intent: Dict[str, Any]
    ) -> None:
        metadata = payment_intent.get("metadata", {})
        order_id = metadata.get("order_id")
        payment_id = metadata.get("payment_id")

        if not order_id or not payment_id:
            logger.error(
                "payment_intent.succeeded missing metadata",
                extra={"payment_intent_id": payment_intent.get("id")},
            )
            return

        charges = payment_intent.get("charges", {}).get("data", [])
        charge_id = charges[0].get("id") if charges else None

        domain_event = BaseEvent(
            event_type=EventTypes.PAYMENT_SUCCEEDED,
            payload={
                "order_id": order_id,
                "payment_id": payment_id,
                "provider_payment_id": payment_intent.get("id"),
                "provider_charge_id": charge_id,
                "amount_cents": payment_intent.get("amount"),
                "currency": payment_intent.get("currency"),
                "customer_email": payment_intent.get("receipt_email"),
            },
            correlation_id=stripe_event.get("id"),
            source_service="webhook-handler",
        )

        self._producer.produce(
            topic=Topics.PAYMENTS_COMPLETED,
            event=domain_event,
            key=order_id,  # route by order_id for ordering
        )
        logger.info("Payment succeeded event published", extra={"order_id": order_id})

    def _on_payment_intent_failed(
        self, stripe_event: Dict[str, Any], payment_intent: Dict[str, Any]
    ) -> None:
        metadata = payment_intent.get("metadata", {})
        order_id = metadata.get("order_id")
        payment_id = metadata.get("payment_id")

        last_error = payment_intent.get("last_payment_error", {})
        failure_reason = last_error.get("message", "Payment declined")

        domain_event = BaseEvent(
            event_type=EventTypes.PAYMENT_FAILED,
            payload={
                "order_id": order_id,
                "payment_id": payment_id,
                "provider_payment_id": payment_intent.get("id"),
                "failure_reason": failure_reason,
                "failure_code": last_error.get("code"),
                "customer_email": payment_intent.get("receipt_email"),
            },
            correlation_id=stripe_event.get("id"),
            source_service="webhook-handler",
        )

        self._producer.produce(
            topic=Topics.PAYMENTS_FAILED,
            event=domain_event,
            key=order_id,
        )
        logger.warning("Payment failed event published", extra={"order_id": order_id})

    def _on_charge_refunded(
        self, stripe_event: Dict[str, Any], charge: Dict[str, Any]
    ) -> None:
        # Emit refund event for downstream handling (out of scope for this assessment)
        logger.info(
            "Charge refunded",
            extra={"charge_id": charge.get("id"), "amount_refunded": charge.get("amount_refunded")},
        )