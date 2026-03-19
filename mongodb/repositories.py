# blast_assessment/mongodb/repositories.py

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo import MongoClient, ASCENDING, DESCENDING, IndexModel
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError, OperationFailure

from mongodb.models import Order, OrderStatus, Payment, PaymentStatus, Notification
from shared.logging import get_logger

logger = get_logger(__name__)


def _safe_create_indexes(col: Collection, indexes: List[IndexModel]) -> None:
    """
    Create indexes safely. If an index exists with the same name but different
    options (e.g. missing sparse), drop it first then recreate it.
    """
    for index in indexes:
        try:
            col.create_indexes([index])
        except OperationFailure as e:
            if "already exists with different options" in str(e) or "IndexOptionsConflict" in str(e):
                index_name = index.document.get("name")
                logger.warning(
                    "Index options conflict, dropping and recreating",
                    extra={"index": index_name, "collection": col.name},
                )
                try:
                    col.drop_index(index_name)
                except Exception:
                    pass
                col.create_indexes([index])
            else:
                raise


class OrderRepository:

    COLLECTION = "orders"

    def __init__(self, db: Database):
        self._col: Collection = db[self.COLLECTION]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        # Unconditionally drop sparse/unique indexes before recreating — MongoDB
        # silently skips creation if the index exists with different options
        # (e.g. missing sparse), which causes DuplicateKeyError on null values.
        for name in ("payment_intent", "external_ref"):
            try:
                self._col.drop_index(name)
            except Exception:
                pass  # doesn't exist yet, that's fine

        indexes = [
            IndexModel(
                [("user_id", ASCENDING), ("created_at", DESCENDING)],
                name="user_orders",
            ),
            IndexModel(
                [("status", ASCENDING), ("created_at", ASCENDING)],
                name="status_created",
            ),
            IndexModel(
                [("payment_intent_id", ASCENDING)],
                name="payment_intent",
                sparse=True,   # MUST be sparse — null values are not indexed
                unique=True,
            ),
            IndexModel(
                [("external_order_ref", ASCENDING)],
                name="external_ref",
                sparse=True,   # MUST be sparse — null values are not indexed
                unique=True,
            ),
        ]
        _safe_create_indexes(self._col, indexes)
        logger.info("Order collection indexes ensured")

    def insert(self, order: Order) -> str:
        doc = order.to_dict()
        self._col.insert_one(doc)
        logger.info("Order inserted", extra={"order_id": order.order_id})
        return order.order_id

    def get_by_id(self, order_id: str) -> Optional[Order]:
        doc = self._col.find_one({"_id": order_id})
        return Order.from_dict(doc) if doc else None

    def get_by_payment_intent(self, payment_intent_id: str) -> Optional[Order]:
        doc = self._col.find_one({"payment_intent_id": payment_intent_id})
        return Order.from_dict(doc) if doc else None

    def list_by_user(
        self,
        user_id: str,
        status: Optional[OrderStatus] = None,
        limit: int = 20,
        after_id: Optional[str] = None,
    ) -> List[Order]:
        query: Dict[str, Any] = {"user_id": user_id}
        if status:
            query["status"] = status.value
        if after_id:
            query["_id"] = {"$gt": after_id}
        cursor = self._col.find(query).sort("created_at", DESCENDING).limit(limit)
        return [Order.from_dict(doc) for doc in cursor]

    def update_status(self, order_id: str, new_status: OrderStatus, expected_version: int) -> bool:
        status_entry = {
            "status": new_status.value,
            "at": datetime.now(timezone.utc).isoformat(),
        }
        result = self._col.update_one(
            {"_id": order_id, "version": expected_version},
            {
                "$set": {
                    "status": new_status.value,
                    "updated_at": datetime.now(timezone.utc),
                },
                "$inc": {"version": 1},
                "$push": {"status_history": status_entry},
            },
        )
        if result.matched_count == 0:
            logger.warning(
                "Order update skipped: version conflict or not found",
                extra={"order_id": order_id, "expected_version": expected_version},
            )
            return False
        return True

    def set_payment_intent(self, order_id: str, payment_intent_id: str) -> None:
        self._col.update_one(
            {"_id": order_id},
            {
                "$set": {
                    "payment_intent_id": payment_intent_id,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )

    def find_by_status(
        self,
        status: OrderStatus,
        limit: int = 100,
        older_than: Optional[datetime] = None,
    ) -> List[Order]:
        query: Dict[str, Any] = {"status": status.value}
        if older_than:
            query["created_at"] = {"$lt": older_than}
        cursor = self._col.find(query).sort("created_at", ASCENDING).limit(limit)
        return [Order.from_dict(doc) for doc in cursor]

    def count_by_status(self) -> Dict[str, int]:
        pipeline = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
        return {doc["_id"]: doc["count"] for doc in self._col.aggregate(pipeline)}


class PaymentRepository:

    COLLECTION = "payments"

    def __init__(self, db: Database):
        self._col: Collection = db[self.COLLECTION]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        indexes = [
            IndexModel([("order_id", ASCENDING)], name="order_id"),
            IndexModel(
                [("idempotency_key", ASCENDING)],
                name="idempotency_key",
                unique=True,
            ),
            IndexModel(
                [("provider_payment_id", ASCENDING)],
                name="provider_payment_id",
                sparse=True,
            ),
            IndexModel(
                [("status", ASCENDING), ("created_at", DESCENDING)],
                name="status_created",
            ),
        ]
        _safe_create_indexes(self._col, indexes)

    def insert(self, payment: Payment) -> Optional[str]:
        try:
            self._col.insert_one(payment.to_dict())
            logger.info("Payment inserted", extra={"payment_id": payment.payment_id})
            return payment.payment_id
        except DuplicateKeyError:
            logger.warning(
                "Duplicate payment insert (idempotency key exists)",
                extra={"idempotency_key": payment.idempotency_key},
            )
            return None

    def get_by_id(self, payment_id: str) -> Optional[Payment]:
        doc = self._col.find_one({"_id": payment_id})
        return Payment.from_dict(doc) if doc else None

    def get_by_idempotency_key(self, key: str) -> Optional[Payment]:
        doc = self._col.find_one({"idempotency_key": key})
        return Payment.from_dict(doc) if doc else None

    def get_by_order_id(self, order_id: str) -> Optional[Payment]:
        doc = self._col.find_one({"order_id": order_id})
        return Payment.from_dict(doc) if doc else None

    def get_by_provider_id(self, provider_payment_id: str) -> Optional[Payment]:
        doc = self._col.find_one({"provider_payment_id": provider_payment_id})
        return Payment.from_dict(doc) if doc else None

    def update_status(
        self,
        payment_id: str,
        new_status: PaymentStatus,
        provider_payment_id: Optional[str] = None,
        provider_charge_id: Optional[str] = None,
        failure_reason: Optional[str] = None,
    ) -> None:
        update: Dict[str, Any] = {
            "status": new_status.value,
            "updated_at": datetime.now(timezone.utc),
        }
        if provider_payment_id:
            update["provider_payment_id"] = provider_payment_id
        if provider_charge_id:
            update["provider_charge_id"] = provider_charge_id
        if failure_reason:
            update["failure_reason"] = failure_reason
        self._col.update_one({"_id": payment_id}, {"$set": update})


class NotificationRepository:

    COLLECTION = "notifications"

    def __init__(self, db: Database):
        self._col: Collection = db[self.COLLECTION]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        indexes = [
            IndexModel(
                [("user_id", ASCENDING), ("created_at", DESCENDING)],
                name="user_notifications",
            ),
            IndexModel(
                [("reference_id", ASCENDING), ("reference_type", ASCENDING)],
                name="reference",
            ),
            IndexModel(
                [("status", ASCENDING), ("retry_count", ASCENDING)],
                name="status_retry",
            ),
        ]
        _safe_create_indexes(self._col, indexes)

    def insert(self, notification: Notification) -> str:
        self._col.insert_one(notification.to_dict())
        return notification.notification_id

    def mark_sent(self, notification_id: str, provider_message_id: Optional[str] = None) -> None:
        self._col.update_one(
            {"_id": notification_id},
            {
                "$set": {
                    "status": "sent",
                    "provider_message_id": provider_message_id,
                    "sent_at": datetime.now(timezone.utc),
                }
            },
        )

    def mark_failed(self, notification_id: str, reason: str) -> None:
        self._col.update_one(
            {"_id": notification_id},
            {
                "$set": {"status": "failed", "failure_reason": reason},
                "$inc": {"retry_count": 1},
            },
        )

    def find_pending_retries(self, limit: int = 50) -> List[Notification]:
        cursor = self._col.find(
            {"status": "failed", "retry_count": {"$lt": 3}}
        ).limit(limit)
        return [Notification(**{k: v for k, v in doc.items() if k != "_id"}) for doc in cursor]