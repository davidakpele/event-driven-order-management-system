# blast_assessment/services/payment_service/main.py

"""
Payment Result Service
Consumes payment.succeeded / payment.failed events from Kafka and updates order state.
Runs as a standalone process.
"""
from pymongo import MongoClient

from kafka.consumers.payment_consumer import PaymentResultConsumer
from kafka.producers.blast_producer import BlastProducer
from shared.config import config
from shared.logging import get_logger, set_service_name

logger = get_logger(__name__)


def main() -> None:
    set_service_name("payment-service")
    logger.info("Starting payment result service")

    mongo_client = MongoClient(
        config.mongo.uri,
        maxPoolSize=config.mongo.max_pool_size,
        minPoolSize=config.mongo.min_pool_size,
        retryWrites=config.mongo.retry_writes,
    )
    db = mongo_client[config.mongo.database]
    producer = BlastProducer()

    consumer = PaymentResultConsumer(db=db, producer=producer)

    try:
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        consumer.stop()
        producer.flush()
        mongo_client.close()
        logger.info("Payment service stopped")


if __name__ == "__main__":
    main()