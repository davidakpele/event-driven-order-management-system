# blast_assessment/kafka/consumers/base_consumer.py

import signal
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition

from kafka.events import BaseEvent, Topics
from kafka.producers.blast_producer import BlastProducer
from shared.config import config
from shared.logging import get_logger, set_correlation_id, LogContext

logger = get_logger(__name__)


def _build_consumer_conf(group_id: str) -> Dict[str, Any]:
    kafka = config.kafka
    return {
        "bootstrap.servers": ",".join(kafka.bootstrap_servers),
        "group.id": f"{kafka.group_id_prefix}.{group_id}",
        "enable.auto.commit": False,
        "auto.offset.reset": kafka.auto_offset_reset,
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms": kafka.session_timeout_ms,
        "heartbeat.interval.ms": kafka.heartbeat_interval_ms,
        "fetch.min.bytes": 1,
        "fetch.wait.max.ms": 500,
        "on_commit": _on_commit,
    }


def _on_commit(err, partitions: List[TopicPartition]) -> None:
    if err:
        # _NO_OFFSET is not a real error — it means no messages were consumed yet
        if hasattr(err, 'code') and err.code() == KafkaError._NO_OFFSET:
            return
        logger.error("Offset commit failed", extra={"error": str(err)})
    else:
        for tp in partitions:
            logger.debug(
                "Offset committed",
                extra={"topic": tp.topic, "partition": tp.partition, "offset": tp.offset},
            )


class BaseConsumer(ABC):

    def __init__(self, group_id: str, topics: List[str], producer: Optional[BlastProducer] = None):
        self._consumer = Consumer(_build_consumer_conf(group_id))
        self._topics = topics
        self._producer = producer or BlastProducer()
        self._running = False
        self._shutdown_event = threading.Event()
        self._group_id = group_id

    def start(self) -> None:
        self._consumer.subscribe(
            self._topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        self._running = True
        logger.info(
            "Consumer started",
            extra={"group_id": self._group_id, "topics": self._topics},
        )

        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        self._poll_loop()

    def _poll_loop(self) -> None:
        while self._running and not self._shutdown_event.is_set():
            msg: Optional[Message] = self._consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                self._handle_kafka_error(msg)
                continue

            self._process_message(msg)

        logger.info("Consumer poll loop exited", extra={"group_id": self._group_id})
        self._consumer.close()

    def _process_message(self, msg: Message) -> None:
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        raw_key = msg.key()
        key = raw_key.decode("utf-8") if raw_key else None

        headers = dict(msg.headers() or [])
        correlation_id = (headers.get("correlation_id") or b"").decode("utf-8") or None

        with LogContext(correlation_id=correlation_id, service=self._group_id):
            logger.info(
                "Processing message",
                extra={"topic": topic, "partition": partition, "offset": offset, "key": key},
            )
            try:
                event = BaseEvent.from_json(msg.value().decode("utf-8"))
                self.handle(event, msg)
                self._consumer.commit(message=msg, asynchronous=True)
                logger.info(
                    "Message processed and committed",
                    extra={"topic": topic, "partition": partition, "offset": offset},
                )
            except Exception as e:
                logger.error(
                    "Failed to process message, routing to DLQ",
                    extra={
                        "topic": topic,
                        "partition": partition,
                        "offset": offset,
                        "error": str(e),
                    },
                )
                self._producer.produce_to_dlq(
                    original_topic=topic,
                    raw_value=msg.value(),
                    error=str(e),
                    key=key,
                )
                self._consumer.commit(message=msg, asynchronous=True)

    @abstractmethod
    def handle(self, event: BaseEvent, msg: Message) -> None:
        ...

    def _handle_kafka_error(self, msg: Message) -> None:
        err = msg.error()
        if err.code() == KafkaError._PARTITION_EOF:
            logger.debug(
                "Reached partition EOF",
                extra={"topic": msg.topic(), "partition": msg.partition()},
            )
        elif err.code() == KafkaError.OFFSET_OUT_OF_RANGE:
            logger.warning(
                "Offset out of range, resetting to latest",
                extra={"topic": msg.topic(), "partition": msg.partition()},
            )
        elif err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            # Topics not yet created — broker still starting up, safe to ignore
            logger.warning(
                "Topic not yet available, waiting for broker",
                extra={"topic": msg.topic()},
            )
        else:
            logger.error("Consumer error", extra={"error": str(err), "code": err.code()})

    def _on_assign(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info(
            "Partitions assigned",
            extra={"partitions": [(tp.topic, tp.partition) for tp in partitions]},
        )

    def _on_revoke(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        logger.info(
            "Partitions revoked",
            extra={"partitions": [(tp.topic, tp.partition) for tp in partitions]},
        )
        # Only commit if we actually have offsets stored — avoids _NO_OFFSET crash
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as e:
            if "_NO_OFFSET" not in str(e):
                logger.warning("Commit on revoke failed", extra={"error": str(e)})

    def _handle_shutdown(self, signum, frame) -> None:
        logger.info("Shutdown signal received", extra={"signal": signum})
        self._running = False
        self._shutdown_event.set()

    def stop(self) -> None:
        self._running = False
        self._shutdown_event.set()