"""The account API on carbium.sh: CarbiumAccount (blocking) and AsyncCarbiumAccount. Every operation of the API's
description is a typed call, generated in _account_gen.py; this file holds the credential and error rules. Nothing is
retried here: a create is not safe to repeat, so a 429 or 503 comes back with retry_after_ms for the caller."""
from __future__ import annotations

import re
from typing import Any

import httpx

from . import _http
from ._account_gen import OPERATIONS, AccountMethods, AsyncAccountMethods
from .errors import CarbiumError, Secret, redact
from .retry import server_hint_ms

DEFAULT_BASE_URL = "https://carbium.sh"
_ACCOUNT_KEY = re.compile(r"^c_acct_[A-Za-z0-9]+$")
_HELD = {"key": "an account key", "session": "a session token", "none": "no credential"}
_TEST_TIMEOUT = 25.0  # the test delivery is tried for up to 15 s before the answer comes


class _AccountCore:
    def __init__(self, api_key: str | None, session_token: str | None, base_url: str, timeout: float) -> None:
        if api_key and session_token:
            raise CarbiumError("auth", "pass an account key or a session token, not both")
        if api_key and not _ACCOUNT_KEY.fullmatch(api_key):
            raise CarbiumError("auth", "api_key must be an account API key (c_acct_…) from the dashboard; a service key (c_serv_) is for services.carbium.sh")
        if session_token and (redact(session_token) != session_token or not _http.HEADER_SAFE.fullmatch(session_token)):
            raise CarbiumError("auth", "session_token must be the token a sign-in answered: printable, with no spaces, not an API key or a secret")
        if redact(base_url) != base_url:
            raise CarbiumError("request", "base_url must not carry a key or a token: pass it as api_key or session_token")
        self.base_url = _http.checked_url(base_url, "base_url").rstrip("/")
        self.timeout = timeout
        self._held = "key" if api_key else "session" if session_token else "none"
        self._secret = Secret(api_key or session_token or "")

    def _exchange(self, name: str, path: str, query: dict[str, Any] | None, body: Any) -> _http.Exchange:
        """The request for one operation, with the credential it takes. A public operation is sent with none."""
        op = OPERATIONS[name]
        headers = {}
        if self._held in op.takes and self._held != "none":
            headers["authorization"] = f"Bearer {self._secret.reveal()}"
        elif "none" not in op.takes:
            wanted = " or ".join(_HELD[t] for t in op.takes)
            raise CarbiumError("auth", f"{name} takes {wanted}; this client holds {_HELD[self._held]}", method=name)
        timeout = max(self.timeout, _TEST_TIMEOUT) if name == "webhooks_test" else self.timeout
        return _http.Exchange("account", name, op.verb, self.base_url + path, headers, query, body, timeout)

    def _result(self, x: _http.Exchange, answer: _http.Answer) -> Any:
        if answer.status < 400:
            if answer.json is _http.NOT_JSON:
                raise CarbiumError("account", f"the account API did not answer in JSON: {answer.text.strip()[:200]}", http_status=answer.status, method=x.method)
            return answer.json
        answer = _http.scrubbed(answer, self._secret.reveal())
        body = answer.json if isinstance(answer.json, dict) else {}
        code = body.get("error") if isinstance(body.get("error"), str) else None
        message = body.get("message") if isinstance(body.get("message"), str) else answer.text.strip()[:200] or f"HTTP {answer.status}"
        raise CarbiumError(
            "account",
            message,
            http_status=answer.status,
            code=code,
            retryable=answer.status == 503,
            retry_after_ms=server_hint_ms(answer.headers),
            request_id=answer.headers.get("x-request-id"),
            method=x.method,
            data=body or None,
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(base_url={self.base_url!r}, credential={self._held!r})"


class CarbiumAccount(_AccountCore, AccountMethods):
    """A blocking client for the account API. Hold an account key (c_acct_…) or a session token from a sign-in; the
    public calls (terms, plans, the sign-in steps) need neither. A call the held credential cannot make is refused
    before it is sent."""

    def __init__(self, api_key: str | None = None, *, session_token: str | None = None, base_url: str = DEFAULT_BASE_URL,
                 timeout: float = 30.0, http_client: httpx.Client | None = None) -> None:
        super().__init__(api_key, session_token, base_url, timeout)
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client()

    def _request(self, name: str, path: str, *, query: dict[str, Any] | None = None, body: Any = None) -> Any:
        x = self._exchange(name, path, query, body)
        return self._result(x, _http.send(self._client, x))

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "CarbiumAccount":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class AsyncCarbiumAccount(_AccountCore, AsyncAccountMethods):
    """The same client for asyncio."""

    def __init__(self, api_key: str | None = None, *, session_token: str | None = None, base_url: str = DEFAULT_BASE_URL,
                 timeout: float = 30.0, http_client: httpx.AsyncClient | None = None) -> None:
        super().__init__(api_key, session_token, base_url, timeout)
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient()

    async def _request(self, name: str, path: str, *, query: dict[str, Any] | None = None, body: Any = None) -> Any:
        x = self._exchange(name, path, query, body)
        return self._result(x, await _http.asend(self._client, x))

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "AsyncCarbiumAccount":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
