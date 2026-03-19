import hashlib
import json
import uuid
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Dict, Optional

from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection

from shared.logging import get_logger

logger = get_logger(__name__)


class IdempotencyStatus(str, Enum):
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class IdempotencyRecord:
    def __init__(
        self,
        key: str,
        status: IdempotencyStatus,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        created_at: Optional[datetime] = None,
        expires_at: Optional[datetime] = None,
    ):
        self.key = key
        self.status = status
        self.result = result
        self.error = error
        self.created_at = created_at or datetime.now(timezone.utc)
        self.expires_at = expires_at or self.created_at + timedelta(days=7)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IdempotencyRecord":
        return cls(
            key=data["key"],
            status=IdempotencyStatus(data["status"]),
            result=data.get("result"),
            error=data.get("error"),
            created_at=data.get("created_at"),
            expires_at=data.get("expires_at"),
        )


class IdempotencyStore:
    """
    MongoDB-backed idempotency store.
    Guarantees at-most-once execution for a given key within the TTL window.
    Uses MongoDB findOneAndUpdate with upsert for atomic check-and-set.
    """

    COLLECTION = "idempotency_records"

    def __init__(self, collection: Collection):
        self._col = collection
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self._col.create_index("key", unique=True)
        self._col.create_index("expires_at", expireAfterSeconds=0)  # TTL index

    def try_start(self, key: str) -> Optional[IdempotencyRecord]:
        """
        Attempt to claim an idempotency key for processing.
        Returns:
            None if the key was newly claimed (caller should proceed).
            IdempotencyRecord if the key already exists (caller should return cached result).
        """
        try:
            result = self._col.find_one_and_update(
                {"key": key},
                {
                    "$setOnInsert": IdempotencyRecord(
                        key=key, status=IdempotencyStatus.PROCESSING
                    ).to_dict()
                },
                upsert=True,
                return_document=False, 
            )
            if result is None:
                logger.info("Idempotency key claimed", extra={"key": key})
                return None
            record = IdempotencyRecord.from_dict(result)
            logger.info(
                "Idempotency key already exists",
                extra={"key": key, "status": record.status},
            )
            return record
        except Exception as e:
            logger.error("Failed to claim idempotency key", extra={"key": key, "error": str(e)})
            raise

    def mark_completed(self, key: str, result: Dict[str, Any]) -> None:
        self._col.update_one(
            {"key": key},
            {"$set": {"status": IdempotencyStatus.COMPLETED.value, "result": result}},
        )
        logger.info("Idempotency key marked completed", extra={"key": key})

    def mark_failed(self, key: str, error: str) -> None:
        self._col.update_one(
            {"key": key},
            {"$set": {"status": IdempotencyStatus.FAILED.value, "error": error}},
        )
        logger.warning("Idempotency key marked failed", extra={"key": key, "error": error})

    def get(self, key: str) -> Optional[IdempotencyRecord]:
        doc = self._col.find_one({"key": key})
        if doc:
            return IdempotencyRecord.from_dict(doc)
        return None


def generate_idempotency_key(*components: Any) -> str:
    """
    Deterministically derive an idempotency key from semantic components.
    Example: generate_idempotency_key("order", order_id, "payment")
    """
    payload = json.dumps([str(c) for c in components], sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()