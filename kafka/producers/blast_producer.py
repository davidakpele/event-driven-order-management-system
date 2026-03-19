# blast_assessment/kafka/producers/blast_producer.py

import json
import time
from typing import Any, Callable, Dict, List, Optional

from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from kafka.events import BaseEvent, Topics
from shared.config import config
from shared.logging import get_logger, correlation_id_var

logger = get_logger(__name__)


def _build_producer_conf() -> Dict[str, Any]:
    kafka = config.kafka
    return {
        "bootstrap.servers": ",".join(kafka.bootstrap_servers),
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 1,
        "retries": kafka.retries,
        "retry.backoff.ms": kafka.retry_backoff_ms,
        # Give broker time to elect coordinator before failing idempotence init
        "transaction.timeout.ms": 60000,
        "linger.ms": 5,
        "batch.size": 65536,
        "compression.type": "snappy",
        "error_cb": _on_error,
    }


def _on_error(err: KafkaError) -> None:
    # COORDINATOR_LOAD_IN_PROGRESS is transient during broker startup — not fatal
    if "Coordinator load in progress" in str(err):
        logger.debug("Broker coordinator loading, will retry automatically")
        return
    logger.error("Kafka producer error", extra={"error": str(err), "fatal": err.fatal()})


class BlastProducer:

    def __init__(self):
        self._producer = Producer(_build_producer_conf())
        logger.info("Kafka producer initialized")

    def produce(
        self,
        topic: str,
        event: BaseEvent,
        key: Optional[str] = None,
        partition: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        correlation_id = correlation_id_var.get() or event.correlation_id
        kafka_headers = {"correlation_id": (correlation_id or "").encode()}
        if headers:
            kafka_headers.update({k: v.encode() for k, v in headers.items()})

        encoded_key = key.encode("utf-8") if key else None
        value = event.to_json().encode("utf-8")

        kwargs: Dict[str, Any] = {
            "topic": topic,
            "value": value,
            "key": encoded_key,
            "headers": list(kafka_headers.items()),
            "on_delivery": self._delivery_report,
        }
        if partition is not None:
            kwargs["partition"] = partition

        try:
            self._producer.produce(**kwargs)
            self._producer.poll(0)
        except BufferError:
            logger.warning("Producer queue full, flushing before retry")
            self._producer.flush(timeout=10)
            self._producer.produce(**kwargs)
        except KafkaException as e:
            logger.error(
                "Failed to produce message",
                extra={"topic": topic, "event_type": event.event_type, "error": str(e)},
            )
            raise

        logger.info(
            "Event produced",
            extra={
                "topic": topic,
                "event_type": event.event_type,
                "event_id": event.event_id,
                "key": key,
                "correlation_id": correlation_id,
            },
        )

    def produce_to_dlq(self, original_topic: str, raw_value: bytes, error: str, key: Optional[str] = None) -> None:
        dlq_payload = json.dumps({
            "original_topic": original_topic,
            "error": error,
            "raw_value": raw_value.decode("utf-8", errors="replace"),
        }).encode("utf-8")
        self._producer.produce(
            topic=Topics.DEAD_LETTER,
            value=dlq_payload,
            key=key,
            on_delivery=self._delivery_report,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning("Flush timed out with messages in queue", extra={"remaining": remaining})
        return remaining

    def _delivery_report(self, err: Optional[KafkaError], msg) -> None:
        if err:
            logger.error(
                "Message delivery failed",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "error": str(err),
                },
            )
        else:
            logger.debug(
                "Message delivered",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )

    def __del__(self):
        if hasattr(self, "_producer"):
            self._producer.flush(timeout=5)


def ensure_topics(topics: List[str], num_partitions: int = 3, replication_factor: int = 1) -> None:
    """
    Create topics if they don't exist. Retries until broker is ready.
    """
    for attempt in range(10):
        try:
            admin = AdminClient({"bootstrap.servers": ",".join(config.kafka.bootstrap_servers)})
            new_topics = [
                NewTopic(t, num_partitions=num_partitions, replication_factor=replication_factor)
                for t in topics
            ]
            futures = admin.create_topics(new_topics)
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info("Topic created", extra={"topic": topic})
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.debug("Topic already exists", extra={"topic": topic})
                    else:
                        logger.error("Failed to create topic", extra={"topic": topic, "error": str(e)})
            return
        except Exception as e:
            logger.warning(
                f"Broker not ready, retrying topic creation ({attempt + 1}/10)",
                extra={"error": str(e)},
            )
            time.sleep(3)

    logger.error("Could not create topics after 10 attempts — broker may be unavailable")