"""Checks that a webhook delivery really came from Carbium. Deliveries are signed the Standard Webhooks way:
HMAC-SHA256 with the secret's base64 after whsec_, over "webhook-id.webhook-timestamp.body"."""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import math
import time
from typing import Any, Callable, Mapping

from .errors import CarbiumError


def _fail(reason: str, message: str) -> CarbiumError:
    return CarbiumError("webhook", message, reason=reason)


def _header(headers: Mapping[str, Any], name: str) -> str | None:
    value = headers.get(name)
    if value is None:
        value = next((v for k, v in headers.items() if str(k).lower() == name), None)
    return value[0] if isinstance(value, (list, tuple)) and value else value


def verify_webhook_signature(raw_body: bytes | str, headers: Mapping[str, Any], secret: str, *, tolerance_seconds: float = 300, now: Callable[[], float] = time.time) -> bytes:
    """Returns the body's bytes when one signature entry matches, and raises a CarbiumError of kind "webhook" otherwise,
    with reason missing_header, bad_timestamp, stale, bad_secret or no_match. Pass the body exactly as received:
    a body that was parsed and encoded again will not verify. The header holds two entries while a secret rotates."""
    message_id, stamp, signature = (_header(headers, name) for name in ("webhook-id", "webhook-timestamp", "webhook-signature"))
    if not message_id or not stamp or not signature:
        raise _fail("missing_header", "webhook-id, webhook-timestamp and webhook-signature are all required")
    if not stamp.isascii() or not stamp.isdigit() or len(stamp) > 15:  # Unix seconds: a longer number is not a time
        raise _fail("bad_timestamp", "webhook-timestamp must be Unix seconds")
    clock = now() if callable(now) else None
    skew = abs(clock - int(stamp)) if isinstance(clock, (int, float)) else math.nan
    if not skew <= tolerance_seconds:  # written so a missing or NaN clock, or a NaN tolerance, fails closed
        raise _fail("stale", f"webhook-timestamp is more than {tolerance_seconds:g} s from the clock")
    if not secret.startswith("whsec_") or len(secret) <= 6:
        raise _fail("bad_secret", "the secret must be the whsec_ value shown once when the webhook was created or rotated")
    try:
        key = base64.b64decode(secret[6:], validate=True)
    except (binascii.Error, ValueError):
        raise _fail("bad_secret", "the secret after whsec_ is not base64") from None
    if not key:
        raise _fail("bad_secret", "the secret is empty")
    body = raw_body.encode() if isinstance(raw_body, str) else bytes(raw_body)
    expected = hmac.new(key, f"{message_id}.{stamp}.".encode() + body, hashlib.sha256).digest()
    for entry in signature.split(" "):
        if not entry.startswith("v1,"):
            continue
        try:
            candidate = base64.b64decode(entry[3:], validate=True)
        except (binascii.Error, ValueError):
            continue
        if hmac.compare_digest(candidate, expected):
            return body
    raise _fail("no_match", "no webhook-signature entry matches this body and secret")


def verify_webhook(raw_body: bytes | str, headers: Mapping[str, Any], secret: str, *, tolerance_seconds: float = 300, now: Callable[[], float] = time.time) -> list[dict[str, Any]]:
    """Verifies a delivery and returns its events: a Carbium body is always a JSON list. Adds reason bad_json."""
    body = verify_webhook_signature(raw_body, headers, secret, tolerance_seconds=tolerance_seconds, now=now)
    try:
        events = json.loads(body)
    except ValueError:
        events = None
    if not isinstance(events, list):
        raise _fail("bad_json", "the body verified but is not a JSON list")
    return events
