# blast_assessment/integrations/stripe.py

import hashlib
import hmac
import json
import time
from typing import Any, Dict, Optional

import httpx

from shared.config import config
from shared.logging import get_logger
from shared.retry import async_retry, RateLimiter

logger = get_logger(__name__)

# Stripe non-retryable HTTP status codes
NON_RETRYABLE_STATUSES = {400, 401, 403, 404}

# Stripe rate limit: 100 req/s in live mode, 25 req/s in test — be conservative
_rate_limiter = RateLimiter(rate=20.0, burst=5)


class StripeError(Exception):
    def __init__(self, message: str, status_code: int = 0, error_type: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.error_type = error_type


class StripeNonRetryableError(StripeError):
    pass


class StripeClient:
    """
    Stripe API client.

    Features:
    - Idempotency keys on all mutating calls (POST)
    - Exponential backoff with jitter on 429 / 5xx
    - Rate limiting via token bucket
    - Webhook signature verification
    """

    def __init__(self):
        self._cfg = config.stripe
        self._http = httpx.AsyncClient(
            base_url=self._cfg.base_url,
            headers={
                "Authorization": f"Bearer {self._cfg.api_key}",
                "Stripe-Version": "2024-06-20",
            },
            timeout=httpx.Timeout(self._cfg.timeout_seconds),
        )

    async def create_payment_intent(
        self,
        amount_cents: int,
        currency: str,
        customer_id: Optional[str],
        metadata: Dict[str, str],
        idempotency_key: str,
    ) -> Dict[str, Any]:
        """
        Create a Stripe PaymentIntent.
        Idempotency key ensures the same intent is returned on retry.
        """
        payload = {
            "amount": amount_cents,
            "currency": currency.lower(),
            "automatic_payment_methods[enabled]": "true",
            **{f"metadata[{k}]": v for k, v in metadata.items()},
        }
        if customer_id:
            payload["customer"] = customer_id

        return await self._post(
            "/payment_intents",
            data=payload,
            idempotency_key=idempotency_key,
        )

    async def confirm_payment_intent(
        self,
        payment_intent_id: str,
        payment_method_id: str,
        idempotency_key: str,
    ) -> Dict[str, Any]:
        return await self._post(
            f"/payment_intents/{payment_intent_id}/confirm",
            data={
                "payment_method": payment_method_id,
                "automatic_payment_methods[allow_redirects]": "never",
            },
            idempotency_key=idempotency_key,
        )

    async def create_refund(
        self,
        charge_id: str,
        amount_cents: Optional[int],
        reason: str,
        idempotency_key: str,
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {"charge": charge_id, "reason": reason}
        if amount_cents:
            data["amount"] = amount_cents
        return await self._post("/refunds", data=data, idempotency_key=idempotency_key)

    async def retrieve_payment_intent(self, payment_intent_id: str) -> Dict[str, Any]:
        return await self._get(f"/payment_intents/{payment_intent_id}")

    @async_retry(
        max_attempts=4,
        base_delay=1.0,
        max_delay=30.0,
        jitter=True,
        retryable_exceptions=(StripeError,),
        non_retryable_exceptions=(StripeNonRetryableError,),
    )
    async def _post(
        self,
        path: str,
        data: Dict[str, Any],
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        await _rate_limiter.async_acquire()
        headers = {}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key

        resp = await self._http.post(path, data=data, headers=headers)
        return self._handle_response(resp)

    @async_retry(
        max_attempts=3,
        base_delay=0.5,
        max_delay=10.0,
        jitter=True,
        retryable_exceptions=(StripeError,),
        non_retryable_exceptions=(StripeNonRetryableError,),
    )
    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        await _rate_limiter.async_acquire()
        resp = await self._http.get(path, params=params)
        return self._handle_response(resp)

    def _handle_response(self, resp: httpx.Response) -> Dict[str, Any]:
        if resp.status_code == 200:
            return resp.json()

        body = {}
        try:
            body = resp.json()
        except Exception:
            pass

        error_type = body.get("error", {}).get("type", "unknown")
        message = body.get("error", {}).get("message", resp.text)

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "2"))
            logger.warning("Stripe rate limited", extra={"retry_after": retry_after})
            time.sleep(retry_after)
            raise StripeError(f"Rate limited: {message}", resp.status_code, error_type)

        if resp.status_code in NON_RETRYABLE_STATUSES:
            raise StripeNonRetryableError(message, resp.status_code, error_type)

        # 5xx and other errors are retryable
        raise StripeError(message, resp.status_code, error_type)

    def verify_webhook(self, payload: bytes, stripe_signature: str) -> Dict[str, Any]:
        """
        Verify Stripe webhook signature using HMAC-SHA256.
        Raises ValueError if signature is invalid or timestamp is too old.
        """
        MAX_TOLERANCE_SECONDS = 300  # 5 minutes

        parts = {k: v for k, v in (p.split("=", 1) for p in stripe_signature.split(","))}
        timestamp = parts.get("t")
        signature = parts.get("v1")

        if not timestamp or not signature:
            raise ValueError("Invalid Stripe-Signature header")

        # Replay attack protection: reject if timestamp is too old
        age = int(time.time()) - int(timestamp)
        if age > MAX_TOLERANCE_SECONDS:
            raise ValueError(f"Webhook timestamp too old: {age}s")

        signed_payload = f"{timestamp}.{payload.decode('utf-8')}"
        expected = hmac.new(
            self._cfg.webhook_secret.encode("utf-8"),
            signed_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        if not hmac.compare_digest(expected, signature):
            raise ValueError("Stripe webhook signature verification failed")

        return json.loads(payload)

    async def close(self) -> None:
        await self._http.aclose()