"""
Notification Service
Consumes notification.requested events from Kafka and delivers emails via SendGrid.
Runs as a standalone process.
"""
import signal
import sys

from pymongo import MongoClient

from integrations.sendgrid import SendGridClient
from kafka.consumers.notification_consumer import NotificationConsumer
from kafka.producers.blast_producer import BlastProducer
from shared.config import config
from shared.logging import get_logger, set_service_name

logger = get_logger(__name__)


def main() -> None:
    set_service_name("notification-service")
    logger.info("Starting notification service")

    mongo_client = MongoClient(
        config.mongo.uri,
        maxPoolSize=config.mongo.max_pool_size,
        minPoolSize=config.mongo.min_pool_size,
        retryWrites=config.mongo.retry_writes,
    )
    db = mongo_client[config.mongo.database]
    producer = BlastProducer()
    sendgrid = SendGridClient()

    consumer = NotificationConsumer(db=db, producer=producer, sendgrid=sendgrid)

    try:
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        consumer.stop()
        producer.flush()
        mongo_client.close()
        logger.info("Notification service stopped")


if __name__ == "__main__":
    main()