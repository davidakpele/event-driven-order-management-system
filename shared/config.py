# blast_assessment/shared/config.py

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class KafkaConfig:
    bootstrap_servers: List[str] = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
    )
    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    group_id_prefix: str = os.getenv("KAFKA_GROUP_ID_PREFIX", "blast")
    max_poll_records: int = int(os.getenv("KAFKA_MAX_POLL_RECORDS", "500"))
    session_timeout_ms: int = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000"))
    heartbeat_interval_ms: int = int(os.getenv("KAFKA_HEARTBEAT_INTERVAL_MS", "10000"))
    auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    enable_auto_commit: bool = os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false").lower() == "true"
    acks: str = os.getenv("KAFKA_ACKS", "all")
    retries: int = int(os.getenv("KAFKA_RETRIES", "5"))
    retry_backoff_ms: int = int(os.getenv("KAFKA_RETRY_BACKOFF_MS", "300"))
    max_in_flight_requests_per_connection: int = 1  # ensures ordering when retries > 0
    enable_idempotence: bool = True


@dataclass
class MongoConfig:
    uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    database: str = os.getenv("MONGO_DB", "blast_db")
    max_pool_size: int = int(os.getenv("MONGO_MAX_POOL_SIZE", "50"))
    min_pool_size: int = int(os.getenv("MONGO_MIN_POOL_SIZE", "5"))
    connect_timeout_ms: int = int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "5000"))
    server_selection_timeout_ms: int = int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS", "5000"))
    retry_writes: bool = True


@dataclass
class StripeConfig:
    api_key: str = os.getenv("STRIPE_API_KEY", "")
    webhook_secret: str = os.getenv("STRIPE_WEBHOOK_SECRET", "")
    base_url: str = "https://api.stripe.com/v1"
    max_retries: int = int(os.getenv("STRIPE_MAX_RETRIES", "3"))
    timeout_seconds: int = int(os.getenv("STRIPE_TIMEOUT_SECONDS", "10"))


@dataclass
class SendGridConfig:
    api_key: str = os.getenv("SENDGRID_API_KEY", "")
    from_email: str = os.getenv("SENDGRID_FROM_EMAIL", "noreply@blast.com")
    base_url: str = "https://api.sendgrid.com/v3"
    max_retries: int = int(os.getenv("SENDGRID_MAX_RETRIES", "3"))


@dataclass
class AppConfig:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    mongo: MongoConfig = field(default_factory=MongoConfig)
    stripe: StripeConfig = field(default_factory=StripeConfig)
    sendgrid: SendGridConfig = field(default_factory=SendGridConfig)
    environment: str = os.getenv("APP_ENV", "development")
    service_name: str = os.getenv("SERVICE_NAME", "blast-service")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


config = AppConfig()