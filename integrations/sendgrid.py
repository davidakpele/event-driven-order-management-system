import time
from typing import Any, Dict, List, Optional

import httpx

from shared.config import config
from shared.logging import get_logger
from shared.retry import RateLimiter, async_retry

logger = get_logger(__name__)

_rate_limiter = RateLimiter(rate=10.0, burst=5)

NON_RETRYABLE_STATUSES = {400, 401, 403}


class SendGridError(Exception):
    def __init__(self, message: str, status_code: int = 0):
        super().__init__(message)
        self.status_code = status_code


class SendGridNonRetryableError(SendGridError):
    pass


class SendGridClient:
    """
    SendGrid Email API client.

    Supports:
    - Dynamic transactional templates
    - Rate limiting (token bucket)
    - Retry with exponential backoff on 429 / 5xx
    - Batch sending (up to 1000 recipients per call)
    """

    def __init__(self):
        self._cfg = config.sendgrid
        self._http = httpx.AsyncClient(
            base_url=self._cfg.base_url,
            headers={
                "Authorization": f"Bearer {self._cfg.api_key}",
                "Content-Type": "application/json",
            },
            timeout=httpx.Timeout(10.0),
        )

    @async_retry(
        max_attempts=3,
        base_delay=1.0,
        max_delay=30.0,
        jitter=True,
        retryable_exceptions=(SendGridError,),
        non_retryable_exceptions=(SendGridNonRetryableError,),
    )
    async def send_template(
        self,
        to_email: str,
        template_id: str,
        template_data: Dict[str, Any],
        reply_to: Optional[str] = None,
    ) -> str:
        """
        Send a transactional email using a SendGrid dynamic template.
        Returns the X-Message-Id header from the response.
        """
        await _rate_limiter.async_acquire()

        payload = {
            "from": {"email": self._cfg.from_email},
            "personalizations": [
                {
                    "to": [{"email": to_email}],
                    "dynamic_template_data": template_data,
                }
            ],
            "template_id": template_id,
        }
        if reply_to:
            payload["reply_to"] = {"email": reply_to}

        resp = await self._http.post("/mail/send", json=payload)

        if resp.status_code == 202:
            message_id = resp.headers.get("X-Message-Id", "")
            logger.info(
                "Email sent via SendGrid",
                extra={"to": to_email, "template": template_id, "message_id": message_id},
            )
            return message_id

        self._handle_error(resp, to_email, template_id)

    @async_retry(
        max_attempts=3,
        base_delay=1.0,
        max_delay=30.0,
        retryable_exceptions=(SendGridError,),
        non_retryable_exceptions=(SendGridNonRetryableError,),
    )
    async def send_batch(
        self,
        recipients: List[Dict[str, Any]],  # [{"email": ..., "data": {...}}]
        template_id: str,
    ) -> str:
        """
        Send the same template to up to 1000 recipients in a single API call.
        Each recipient can have their own dynamic_template_data.
        """
        if len(recipients) > 1000:
            raise ValueError("SendGrid batch limit is 1000 recipients per call")

        await _rate_limiter.async_acquire()

        personalizations = [
            {"to": [{"email": r["email"]}], "dynamic_template_data": r.get("data", {})}
            for r in recipients
        ]
        payload = {
            "from": {"email": self._cfg.from_email},
            "personalizations": personalizations,
            "template_id": template_id,
        }

        resp = await self._http.post("/mail/send", json=payload)
        if resp.status_code == 202:
            return resp.headers.get("X-Message-Id", "")
        self._handle_error(resp, f"batch({len(recipients)})", template_id)

    def _handle_error(self, resp: httpx.Response, recipient: str, template_id: str) -> None:
        body = {}
        try:
            body = resp.json()
        except Exception:
            pass

        errors = body.get("errors", [])
        message = "; ".join(e.get("message", "") for e in errors) or resp.text

        logger.error(
            "SendGrid error",
            extra={
                "status": resp.status_code,
                "template": template_id,
                "recipient": recipient,
                "errors": errors,
            },
        )

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "5"))
            time.sleep(retry_after)
            raise SendGridError(f"Rate limited: {message}", resp.status_code)

        if resp.status_code in NON_RETRYABLE_STATUSES:
            raise SendGridNonRetryableError(message, resp.status_code)

        raise SendGridError(message, resp.status_code)

    async def close(self) -> None:
        await self._http.aclose()