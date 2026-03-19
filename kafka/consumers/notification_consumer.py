
from confluent_kafka import Message
from pymongo.database import Database

from integrations.sendgrid import SendGridClient
from kafka.consumers.base_consumer import BaseConsumer
from kafka.events import BaseEvent, EventTypes, Topics
from kafka.producers.blast_producer import BlastProducer
from mongodb.models import Notification, NotificationStatus
from mongodb.repositories import NotificationRepository
from shared.logging import get_logger

logger = get_logger(__name__)


class NotificationConsumer(BaseConsumer):
    """
    Consumes notification.requested events and delivers via SendGrid.

    Consumer group: blast.notification-consumer
    Topics: notifications.requested

    Keyed by user_id to maintain per-user notification ordering.
    Failed notifications are persisted for retry by the NotificationRetryWorker.
    """

    GROUP_ID = "notification-consumer"

    def __init__(self, db: Database, producer: BlastProducer, sendgrid: SendGridClient):
        super().__init__(
            group_id=self.GROUP_ID,
            topics=[Topics.NOTIFICATIONS_REQUESTED],
            producer=producer,
        )
        self._notification_repo = NotificationRepository(db)
        self._sendgrid = sendgrid

    def handle(self, event: BaseEvent, msg: Message) -> None:
        if event.event_type != EventTypes.NOTIFICATION_REQUESTED:
            logger.warning("Unexpected event type", extra={"event_type": event.event_type})
            return

        payload = event.payload
        notification = Notification(
            user_id=payload["user_id"],
            template_id=payload["template_id"],
            channel=payload.get("channel", "email"),
            recipient=payload["recipient"],
            payload=payload.get("payload", {}),
            reference_id=payload.get("reference_id"),
            reference_type=payload.get("reference_type"),
        )

        notification_id = self._notification_repo.insert(notification)
        logger.info(
            "Notification record created",
            extra={"notification_id": notification_id, "template": notification.template_id},
        )

        try:
            provider_message_id = self._sendgrid.send_template(
                to_email=notification.recipient,
                template_id=notification.template_id,
                template_data=notification.payload,
            )
            self._notification_repo.mark_sent(notification_id, provider_message_id)
            logger.info(
                "Notification sent",
                extra={"notification_id": notification_id, "provider_message_id": provider_message_id},
            )
        except Exception as e:
            logger.error(
                "Notification send failed",
                extra={"notification_id": notification_id, "error": str(e)},
            )
            self._notification_repo.mark_failed(notification_id, str(e))
            raise