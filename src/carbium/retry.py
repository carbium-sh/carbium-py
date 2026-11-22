"""When a failed call is sent again, and how long to wait first. The table follows the service's own retry rules:
only what waiting can fix is retried, and a transaction that was sent is never sent again by this package."""
from __future__ import annotations

import re
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, Mapping


@dataclass(frozen=True)
class Plan:
    base_ms: float
    cap_ms: float
    max_attempts: int
    fixed_ms: float | None = None  # a wait the answer itself sets, used instead of the backoff


STANDARD = Plan(250, 4000, 4)
_INTERNAL = re.compile(r"^(Internal error|das upstream refused the request|upstream internal error)$")


def decide_retry(
    *,
    method: str | None,
    status: int,
    code: int | str | None = None,
    message: str = "",
    reason: str | None = None,
    retryable: bool | None = None,
    timed_out: bool = False,
    data: Any = None,
    min_context_slot: float | None = None,
) -> Plan | None:
    """The plan for a failed attempt, or None when it must not be sent again. Rules are tried in order."""
    if method == "sendTransaction":
        return None
    if status == 0:
        return Plan(STANDARD.base_ms, STANDARD.cap_ms, 2) if timed_out else STANDARD
    if retryable is False:
        return None
    if status == 429:
        return Plan(100, 5000, 6) if code == -32010 else STANDARD
    if status == 425:
        return Plan(0, 0, 2)
    if status == 502:
        return None if "response too large" in message else STANDARD
    if status == 503:
        if message.startswith("upstream RPC unavailable"):
            return Plan(2000, 8000, 4)
        if "compat verbs not initialised" in message:
            return Plan(STANDARD.base_ms, STANDARD.cap_ms, 3)
        return STANDARD
    if status == 504:
        return None if message.startswith("upstream_slow") else Plan(STANDARD.base_ms, STANDARD.cap_ms, 2)
    if status != 200 or reason:
        return None
    if code == -32602 and message.startswith("the walk read no history"):
        return STANDARD
    if code == -32016:
        slot = data.get("contextSlot") if isinstance(data, dict) else None
        number = lambda value: isinstance(value, (int, float)) and not isinstance(value, bool)
        behind = min_context_slot - slot if number(min_context_slot) and number(slot) else 0  # how far behind, only from numbers
        return Plan(0, 0, 4, fixed_ms=min(30000, max(250, 220 * behind)))
    if code == -32004:
        return Plan(1000, 2000, 4)
    if code == -32014:
        return Plan(1000, 1000, 4)
    if code in (-32005, -32021, -32024, -32026):
        return STANDARD
    if code == -32011 and message == "das upstream timed out":
        return STANDARD
    if code == -32603 and _INTERNAL.match(message):
        return Plan(STANDARD.base_ms, STANDARD.cap_ms, 2)
    if code == -32010 and "peer refused the copy" in message:
        return Plan(20000, 20000, 2)
    return None


def server_hint_ms(headers: Mapping[str, str], data: Any = None, now: float | None = None) -> int | None:
    """The wait the service asked for: retry-after-ms, then data.retryAfterMs, then Retry-After (seconds or a date)."""
    exact = headers.get("retry-after-ms")
    if exact and exact.isdigit():
        return int(exact)
    if isinstance(data, dict) and isinstance(data.get("retryAfterMs"), (int, float)) and data["retryAfterMs"] >= 0:
        return int(data["retryAfterMs"])
    after = headers.get("retry-after")
    if not after:
        return None
    if after.strip().isdigit():
        return int(after.strip()) * 1000
    try:
        import time

        return max(0, int((parsedate_to_datetime(after).timestamp() - (now if now is not None else time.time())) * 1000))
    except (TypeError, ValueError):
        return None


def wait_ms(plan: Plan, attempt: int, hint_ms: int | None, jitter_ms: float) -> float:
    """The wait before the next try: the larger of the service's hint and the backoff, plus the jitter."""
    if plan.fixed_ms is not None:
        return max(hint_ms or 0, plan.fixed_ms) + jitter_ms
    backoff = min(plan.cap_ms, plan.base_ms * 2 ** (attempt - 1))
    return max(hint_ms or 0, backoff) + jitter_ms
