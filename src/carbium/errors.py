"""One error type for every failure. Whatever it shows has API keys and webhook secrets cut out."""
from __future__ import annotations

import re
from typing import Any

_PATTERNS = (
    (re.compile(r"\b(c_serv_|c_acct_|whsec_)[A-Za-z0-9+/=_-]+"), r"\1…"),
    (re.compile(r"((?:api[-_]?key|apiKey|token)=)[^&\s\"'<>]+", re.IGNORECASE), r"\1…"),
    (re.compile(r"(api-key\.)[A-Za-z0-9_-]+"), r"\1…"),
)


def redact(value: Any, _depth: int = 0) -> Any:
    """A copy of `value` with every key or secret in its strings replaced by its prefix and an ellipsis."""
    if isinstance(value, str):
        for pattern, keep in _PATTERNS:
            value = pattern.sub(keep, value)
        return value
    if isinstance(value, (dict, list, tuple)):
        if _depth >= 6:
            return "…"
        if isinstance(value, dict):
            return {redact(k, _depth + 1): redact(v, _depth + 1) for k, v in value.items()}
        return [redact(v, _depth + 1) for v in value]
    return value


class CarbiumError(Exception):
    """Raised for every failure.

    kind: auth, credits, rate_limit, request, upstream, route, node, transport, socket, stream, account, dca,
    tokens or webhook. http_status is 0 when no answer came. code is the service's own code (a number for
    JSON-RPC, a string for the account API). retry_after_ms is the wait the service asked for, when it gave one.
    """

    def __init__(
        self,
        kind: str,
        message: str,
        *,
        http_status: int = 0,
        code: int | str | None = None,
        reason: str | None = None,
        retryable: bool = False,
        retry_after_ms: int | None = None,
        request_id: str | None = None,
        method: str | None = None,
        data: Any = None,
    ) -> None:
        text = redact(message)
        if request_id:
            text += f" (x-request-id: {request_id})"
        super().__init__(text)
        self.kind = kind
        self.message = text
        self.http_status = http_status
        self.code = code
        self.reason = reason
        self.retryable = retryable
        self.retry_after_ms = retry_after_ms
        self.request_id = request_id
        self.method = method
        self.data = redact(data)

    def __repr__(self) -> str:
        return f"CarbiumError(kind={self.kind!r}, code={self.code!r}, reason={self.reason!r}, message={self.message!r})"

    def __reduce__(self) -> tuple:
        return _restore, (type(self), dict(vars(self)))  # every field, already redacted, so pickle and copy round-trip


def _restore(cls: type, fields: dict[str, Any]) -> CarbiumError:
    error = cls.__new__(cls)
    Exception.__init__(error, fields["message"])
    error.__dict__.update(fields)
    return error


class Secret:
    """A key, session token or DCA key, held in this one place. It prints as a placeholder, has no attribute dict for
    a walk over an object's attributes to find, and is never pickled or copied; each request takes reveal()."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        return self._value

    def __bool__(self) -> bool:
        return bool(self._value)

    def __repr__(self) -> str:
        return "Secret(…)"

    __str__ = __repr__

    def __reduce_ex__(self, protocol: object) -> tuple:
        raise TypeError("a key is not pickled or copied")


class Cause(Exception):
    """What a failure came from, as text only: the original type name and its redacted message. It holds no
    request, no headers and no frames, so nothing reached through an error's __cause__ carries a key."""

    def __init__(self, original: BaseException) -> None:
        self.type_name = type(original).__name__
        super().__init__(f"{self.type_name}: {redact(str(original))}")
