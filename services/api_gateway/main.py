import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional
from pymongo.errors import DuplicateKeyError
import uvicorn
from fastapi import FastAPI, HTTPException, Header, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pymongo import MongoClient

from integrations.stripe import StripeClient
from integrations.sendgrid import SendGridClient
from integrations.webhook_handler import StripeWebhookHandler
from kafka.producers.blast_producer import BlastProducer, ensure_topics
from kafka.events import Topics
from mongodb.models import OrderStatus
from mongodb.repositories import OrderRepository
from services.order_service.service import OrderService
from shared.config import config
from shared.idempotency import IdempotencyStore
from shared.logging import get_logger, set_correlation_id, set_service_name

logger = get_logger(__name__)

# ─── Startup / Shutdown 

_resources: Dict[str, Any] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    set_service_name("api-gateway")
    logger.info("Starting API gateway")

    mongo_client = MongoClient(
        config.mongo.uri,
        maxPoolSize=config.mongo.max_pool_size,
        minPoolSize=config.mongo.min_pool_size,
        connectTimeoutMS=config.mongo.connect_timeout_ms,
        serverSelectionTimeoutMS=config.mongo.server_selection_timeout_ms,
        retryWrites=config.mongo.retry_writes,
    )
    db = mongo_client[config.mongo.database]

    producer = BlastProducer()
    stripe_client = StripeClient()
    sendgrid_client = SendGridClient()
    idempotency_store = IdempotencyStore(db["idempotency_records"])

    ensure_topics(
        [
            Topics.ORDERS_CREATED,
            Topics.ORDERS_UPDATED,
            Topics.PAYMENTS_INITIATED,
            Topics.PAYMENTS_COMPLETED,
            Topics.PAYMENTS_FAILED,
            Topics.NOTIFICATIONS_REQUESTED,
            Topics.NOTIFICATIONS_SENT,
            Topics.WEBHOOKS_RECEIVED,
            Topics.DEAD_LETTER,
        ]
    )

    _resources["db"] = db
    _resources["producer"] = producer
    _resources["stripe"] = stripe_client
    _resources["sendgrid"] = sendgrid_client
    _resources["idempotency"] = idempotency_store
    _resources["mongo_client"] = mongo_client

    logger.info("API gateway started")
    yield

    logger.info("Shutting down API gateway")
    producer.flush()
    await stripe_client.close()
    await sendgrid_client.close()
    mongo_client.close()


app = FastAPI(title="Blast API", version="1.0.0", lifespan=lifespan)


# ─── Dependency Helpers

def get_order_service() -> OrderService:
    return OrderService(
        db=_resources["db"],
        producer=_resources["producer"],
        stripe_client=_resources["stripe"],
        idempotency_store=_resources["idempotency"],
    )


def get_webhook_handler() -> StripeWebhookHandler:
    return StripeWebhookHandler(
        stripe_client=_resources["stripe"],
        producer=_resources["producer"],
    )


# ─── Request/Response Models 

class OrderItemRequest(BaseModel):
    product_id: str
    sku: str
    name: str
    quantity: int = Field(..., gt=0)
    unit_price: float = Field(..., gt=0)
    currency: str = "USD"


class AddressRequest(BaseModel):
    line1: str
    line2: Optional[str] = None
    city: str
    state: Optional[str] = None
    postal_code: str
    country: str


class CreateOrderRequest(BaseModel):
    user_id: str
    items: List[OrderItemRequest] = Field(..., min_length=1)
    shipping_address: AddressRequest


class InitiatePaymentRequest(BaseModel):
    payment_method_id: str
    customer_email: str
    stripe_customer_id: Optional[str] = None


class OrderResponse(BaseModel):
    order_id: str
    user_id: str
    status: str
    total_cents: int
    total_currency: str
    item_count: int
    created_at: str


def order_to_response(order) -> OrderResponse:
    return OrderResponse(
        order_id=order.order_id,
        user_id=order.user_id,
        status=order.status.value,
        total_cents=order.total.amount_cents if order.total else 0,
        total_currency=order.total.currency if order.total else "USD",
        item_count=len(order.items),
        created_at=order.created_at if isinstance(order.created_at, str) else order.created_at.isoformat(),
    )


# ─── Middleware

@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
    set_correlation_id(correlation_id)
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    return response


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    return JSONResponse(status_code=400, content={"error": str(exc)})


@app.exception_handler(RuntimeError)
async def runtime_error_handler(request: Request, exc: RuntimeError):
    return JSONResponse(status_code=409, content={"error": str(exc)})

@app.exception_handler(DuplicateKeyError)
async def duplicate_key_handler(request: Request, exc: DuplicateKeyError):
    return JSONResponse(status_code=409, content={"error": "Resource already exists."})

# ─── Orders

@app.post("/orders", status_code=status.HTTP_201_CREATED, response_model=OrderResponse)
def create_order(
    req: CreateOrderRequest,
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
    correlation_id: Optional[str] = Header(default=None, alias="X-Correlation-ID"),
):
    svc = get_order_service()
    order = svc.create_order(
        user_id=req.user_id,
        items_data=[item.model_dump() for item in req.items],
        shipping_address_data=req.shipping_address.model_dump(),
        correlation_id=correlation_id,
        idempotency_key=idempotency_key,
    )
    return order_to_response(order)


@app.get("/orders/{order_id}", response_model=OrderResponse)
def get_order(order_id: str):
    svc = get_order_service()
    order = svc.get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order_to_response(order)


@app.get("/users/{user_id}/orders", response_model=List[OrderResponse])
def list_user_orders(
    user_id: str,
    status_filter: Optional[str] = None,
    limit: int = 20,
    after_id: Optional[str] = None,
):
    svc = get_order_service()
    status_enum = OrderStatus(status_filter) if status_filter else None
    orders = svc.list_user_orders(user_id, status=status_enum, limit=limit, after_id=after_id)
    return [order_to_response(o) for o in orders]


@app.post("/orders/{order_id}/cancel", response_model=OrderResponse)
def cancel_order(
    order_id: str,
    reason: str = "customer_request",
    correlation_id: Optional[str] = Header(default=None, alias="X-Correlation-ID"),
):
    svc = get_order_service()
    order = svc.cancel_order(order_id, reason=reason, correlation_id=correlation_id)
    return order_to_response(order)


# ─── Payments ─────────────────────────────────────────────────────────────────

@app.post("/orders/{order_id}/pay", status_code=status.HTTP_202_ACCEPTED)
async def initiate_payment(
    order_id: str,
    req: InitiatePaymentRequest,
    correlation_id: Optional[str] = Header(default=None, alias="X-Correlation-ID"),
):
    svc = get_order_service()
    payment = await svc.initiate_payment(
        order_id=order_id,
        payment_method_id=req.payment_method_id,
        customer_email=req.customer_email,
        stripe_customer_id=req.stripe_customer_id,
        correlation_id=correlation_id,
    )
    return {
        "payment_id": payment.payment_id,
        "status": payment.status.value,
        "message": "Payment processing initiated. You will receive a confirmation shortly.",
    }


# ─── Webhooks ─────────────────────────────────────────────────────────────────

@app.post("/webhooks/stripe", status_code=200)
async def stripe_webhook(
    request: Request,
    stripe_signature: str = Header(alias="Stripe-Signature"),
):
    """
    Stripe sends webhooks on payment events.
    We verify the signature, publish to Kafka, and return 200 fast.
    Stripe will retry on non-2xx with exponential backoff for up to 3 days.
    """
    raw_body = await request.body()
    handler = get_webhook_handler()
    result = handler.process(raw_body, stripe_signature)

    if result.get("status") == "rejected":
        raise HTTPException(status_code=400, detail=result.get("reason", "Invalid webhook"))

    return result


if __name__ == "__main__":
    uvicorn.run(
        "services.api_gateway.main:app",
        host="0.0.0.0",
        port=8292,
        reload=config.environment == "development",
        log_config=None, 
    )