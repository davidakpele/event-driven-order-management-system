# Blast Assessment — Senior Backend Engineer

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.95+-green.svg)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://img.shields.io/pypi/v/wifi-densepose.svg)](https://pypi.org/project/wifi-densepose/)
[![PyPI downloads](https://img.shields.io/pypi/dm/wifi-densepose.svg)](https://pypi.org/project/wifi-densepose/)
[![Test Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen.svg)](https://github.com/ruvnet/wifi-densepose)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](https://hub.docker.com/r/ruvnet/wifi-densepose)

### A production-grade event-driven order management system built with Python, FastAPI, Kafka, MongoDB, and Stripe.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   API Gateway   │────▶│     Kafka        │────▶│ Payment Service │
│   (FastAPI)     │     │  (Event Bus)     │     │  (Consumer)     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                        │                        │
        │                        │               ┌─────────────────┐
        ▼                        └──────────────▶│  Notification   │
┌─────────────────┐                              │  Service        │
│    MongoDB      │                              └─────────────────┘
│  (Datastore)    │
└─────────────────┘
```

## Services

- **api-gateway** — REST API, order management, Stripe payment initiation
- **payment-service** — Kafka consumer for payment result events
- **notification-service** — Kafka consumer for notification events via SendGrid

## Prerequisites

- Docker and Docker Compose
- A Stripe test account (free at stripe.com)

## Environment Setup

Create a `.env` file in the project root:

```dotenv
STRIPE_API_KEY=sk_test_your_stripe_secret_key
STRIPE_WEBHOOK_SECRET=whsec_your_webhook_secret
SENDGRID_API_KEY=SG.your_sendgrid_key
SENDGRID_FROM_EMAIL=noreply@yourdomain.com
```

## Running the Project

**Start all services:**
```bash
docker-compose up -d
```

**Start with rebuild (after code changes):**
```bash
docker-compose up -d --build
```

**Stop all services:**
```bash
docker-compose down
```

**View logs:**
```bash
# All services
docker-compose logs -f

# Specific service
docker logs blast_assessment-api-gateway-1 --tail 50
docker logs blast_assessment-payment-service-1 --tail 50
docker logs blast_assessment-notification-service-1 --tail 50
```

## Service URLs

| Service        | URL                        |
|----------------|----------------------------|
| API Gateway    | http://localhost:8000      |
| API Docs       | http://localhost:8000/docs |
| Kafka UI       | http://localhost:8080      |
| Mongo Express  | http://localhost:8081      |

## Health Check

```bash
GET http://localhost:8000/health
```

Expected response:
```json
{
  "status": "ok",
  "service": "blast-api"
}
```

---

## API Testing Guide

### 1. Create an Order

```
POST http://localhost:8000/orders
```

**Headers:**
```
Content-Type: application/json
Idempotency-Key: test-order-001
```

**Body:**
```json
{
  "user_id": "1",
  "items": [
    {
      "product_id": "prod-3",
      "sku": "SKU-003",
      "name": "Test Product 3",
      "quantity": 12,
      "unit_price": 102.24
    }
  ],
  "shipping_address": {
    "line1": "1 Main Street",
    "city": "Lagos",
    "country": "NG",
    "postal_code": "1001"
  }
}
```

**Expected Response (201):**
```json
{
  "order_id": "c3b554c9-8921-4717-82ff-00daf9259022",
  "user_id": "1",
  "status": "pending",
  "total_cents": 122688,
  "total_currency": "USD",
  "item_count": 1,
  "created_at": "2026-03-19T09:35:25.192835+00:00"
}
```

> **Note:** The `Idempotency-Key` header ensures the same request returns the same order if retried. Use a unique key per distinct order.

---

### 2. Get a Single Order

```
GET http://localhost:8000/orders/{order_id}
```

**Example:**
```
GET http://localhost:8000/orders/c3b554c9-8921-4717-82ff-00daf9259022
```

**Expected Response (200):**
```json
{
  "order_id": "c3b554c9-8921-4717-82ff-00daf9259022",
  "user_id": "1",
  "status": "pending",
  "total_cents": 122688,
  "total_currency": "USD",
  "item_count": 1,
  "created_at": "2026-03-19T09:35:25.192835+00:00"
}
```

---

### 3. List All Orders for a User

```
GET http://localhost:8000/users/{user_id}/orders
```

**Example:**
```
GET http://localhost:8000/users/1/orders
```

**Optional query parameters:**
- `status_filter` — filter by status (e.g. `pending`, `payment_processing`, `cancelled`)
- `limit` — number of results (default: 20)
- `after_id` — cursor-based pagination

**Example with filter:**
```
GET http://localhost:8000/users/1/orders?status_filter=pending
```

**Expected Response (200):**
```json
[
  {
    "order_id": "c3b554c9-8921-4717-82ff-00daf9259022",
    "user_id": "1",
    "status": "pending",
    "total_cents": 122688,
    "total_currency": "USD",
    "item_count": 1,
    "created_at": "2026-03-19T09:35:25.192835+00:00"
  }
]
```

---

### 4. Initiate Payment

```
POST http://localhost:8000/orders/{order_id}/pay
```

**Example:**
```
POST http://localhost:8000/orders/c3b554c9-8921-4717-82ff-00daf9259022/pay
```

**Body:**
```json
{
  "payment_method_id": "pm_card_visa",
  "customer_email": "test@example.com"
}
```

> **Stripe Test Cards:**
> - `pm_card_visa` — succeeds
> - `pm_card_visa_debit` — succeeds
> - `pm_card_chargeDeclined` — declined

**Expected Response (202):**
```json
{
  "payment_id": "0ee28ee4-4887-4578-81c3-9b38c1522cf0",
  "status": "processing",
  "message": "Payment processing initiated. You will receive a confirmation shortly."
}
```

After a successful payment, fetch the order again — status will be `payment_processing`.

---

### 5. Cancel an Order

```
POST http://localhost:8000/orders/{order_id}/cancel
```

**Example:**
```
POST http://localhost:8000/orders/cd1e7e44-195e-4924-ba63-9166c2ff289a/cancel
```

> Only orders in `pending`, `confirmed`, or `payment_failed` status can be cancelled.

**Expected Response (200):**
```json
{
  "order_id": "cd1e7e44-195e-4924-ba63-9166c2ff289a",
  "user_id": "1",
  "status": "cancelled",
  "total_cents": 261200,
  "total_currency": "USD",
  "item_count": 1,
  "created_at": "2026-03-19T09:34:43.180439+00:00"
}
```

---

## Kafka Event Flow

Every action publishes a domain event to Kafka:

| Action               | Topic                   | Event Type               |
|----------------------|-------------------------|--------------------------|
| Order created        | `orders.created`        | `order.created`          |
| Payment initiated    | `payments.initiated`    | `payment.initiated`      |
| Payment succeeded    | `payments.completed`    | `payment.succeeded`      |
| Payment failed       | `payments.failed`       | `payment.failed`         |
| Order cancelled      | `orders.updated`        | `order.cancelled`        |
| Notification queued  | `notifications.requested` | `notification.requested` |

View live events at **http://localhost:8080** (Kafka UI).

---

## Key Design Decisions

- **Idempotency** — All mutating operations accept an idempotency key, preventing duplicate processing on retries
- **Optimistic locking** — Orders use a `version` field to detect concurrent modifications
- **Dead Letter Queue** — Failed Kafka messages are routed to `blast.dlq` for inspection and replay
- **Sparse indexes** — MongoDB sparse unique indexes on `payment_intent_id` and `external_order_ref` allow multiple null values
- **Correlation IDs** — Every request is tagged with a correlation ID propagated through all services and Kafka events for full traceability
- **Retry with backoff** — Stripe and SendGrid clients implement exponential backoff with jitter


##  UNIT TESTING 

```pytest
pytest tests/ -v
```
## Webhook Note

The Stripe webhook endpoint (`POST /webhooks/stripe`) is not fully implemented with HMAC signature verification. End-to-end webhook testing requires a publicly accessible URL. In production this would be configured via ngrok or a deployed environment with the webhook secret set in `.env`.