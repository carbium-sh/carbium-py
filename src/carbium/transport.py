"""Sends one JSON-RPC call or one batch over HTTPS. The request rules the service enforces are checked here first,
so a call it would refuse costs no round trip, and a failed call is retried only as the retry table allows."""
from __future__ import annotations

import asyncio
import itertools
import json
import random
import re
import time
from typing import Any, Awaitable, Callable, Sequence

import httpx

from ._gen import METHODS
from ._version import __version__
from ._http import checked_url
from .errors import CarbiumError, Cause, Secret
from .retry import decide_retry, server_hint_ms, wait_ms

DEFAULT_ENDPOINT = "https://services.carbium.sh"
MAX_BODY_BYTES = 51200
MAX_BATCH = 100
MAX_PERMIT_PER_BATCH = 2
_KEY = re.compile(r"^c_serv_[A-Za-z0-9]+$")
_KEY_IN_URL = re.compile(r"api[-_]?key=|/c_serv_", re.IGNORECASE)
_REQUEST_CODES = {-32600, -32601, -32602, -32700, -32012, -32015}
_REQUEST_REASONS = {"PROGRAM_NOT_PROVISIONED", "CLASSIC_SPL_SCAN", "METHOD_NOT_SERVED", "UNKNOWN_METHOD"}
_VERSIONED = ("getTransaction", "getBlock")  # mainnet carries version-1 transactions; without the option they are refused

Call = tuple[str, Any]


def _kind(status: int, code: Any, reason: str | None) -> str:
    if status in (401, 403):
        return "auth"
    if status == 402:
        return "credits"
    if status == 429:
        return "rate_limit"
    if status in (400, 413, 425, 501):
        return "request"
    if status != 200:
        return "upstream"
    if code in _REQUEST_CODES or reason in _REQUEST_REASONS:
        return "request"
    if isinstance(code, int) and -32099 <= code <= -32051:
        return "route"
    return "node"


def _with_version(params: Any) -> Any:
    """getTransaction and getBlock ask for version-1 transactions unless the caller chose otherwise."""
    if not isinstance(params, list) or not params:
        return params
    if len(params) == 1:
        return [params[0], {"maxSupportedTransactionVersion": 1}]
    if isinstance(params[1], dict) and "maxSupportedTransactionVersion" not in params[1]:
        return [params[0], {**params[1], "maxSupportedTransactionVersion": 1}, *params[2:]]
    return params


def _min_context_slot(params: Any) -> float | None:
    if isinstance(params, list):
        for item in reversed(params):
            if isinstance(item, dict):
                return item.get("minContextSlot")
    return None


class _Core:
    """Everything but the I/O: builds bodies, reads answers and decides what one attempt leads to."""

    def __init__(
        self,
        api_key: str | None,
        endpoint: str,
        *,
        timeout: float,
        max_attempts: int | None,
        retry: bool,
        max_hint_ms: int,
        jitter: Callable[[], float] | None,
    ) -> None:
        key = api_key or ""
        if key and not _KEY.fullmatch(key):
            raise CarbiumError("auth", "api_key must be a service key: c_serv_ followed by letters and digits")
        if _KEY_IN_URL.search(endpoint):
            raise CarbiumError("request", "the endpoint must not carry a key: pass api_key, which is sent in a header")
        self.endpoint = checked_url(endpoint, "the endpoint")
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.retry = retry
        self.max_hint_ms = max_hint_ms
        self._jitter = jitter or (lambda: random.uniform(0, 250))
        self._ids = itertools.count(1)  # one id per call, never the same twice, even across threads
        self._headers = {"content-type": "application/json", "solana-client": f"python/carbium@{__version__}"}
        self._key = Secret(key)

    def _request_headers(self) -> dict[str, str]:
        """One request's headers, the key put in from its wrapper each time: no header dict on the client holds it."""
        return {**self._headers, "x-api-key": self._key.reveal()} if self._key else self._headers

    def _encode(self, payload: Any) -> bytes:
        body = json.dumps(payload, separators=(",", ":")).encode()
        if len(body) > MAX_BODY_BYTES:
            raise CarbiumError("request", f"the request body is {len(body)} bytes; the service takes at most {MAX_BODY_BYTES}")
        return body

    def _one(self, method: str, params: Any) -> dict[str, Any]:
        call: dict[str, Any] = {"jsonrpc": "2.0", "id": next(self._ids), "method": method}
        if method in _VERSIONED:
            params = _with_version(params)
        if params is not None:
            call["params"] = params
        return call

    def _batch_body(self, calls: Sequence[Call]) -> tuple[bytes, list[int]]:
        """One batch body, after the checks the service would make: size, served names, and which methods may share a batch."""
        if not 1 <= len(calls) <= MAX_BATCH:
            raise CarbiumError("request", f"a batch holds 1 to {MAX_BATCH} calls, not {len(calls)}")
        names = [method for method, _ in calls]
        for name in names:
            if name not in METHODS:
                raise CarbiumError("request", f"{name} is not a method the services serve", code=-32601, method=name)
            if METHODS[name].batch == "never":
                raise CarbiumError("request", f"{name} cannot be sent in a batch", method=name)
        das = [name for name in names if METHODS[name].batch == "das"]
        if das and len(das) != len(names):
            raise CarbiumError("request", "DAS methods share a batch only with other DAS methods")
        if sum(METHODS[name].permit for name in names) > MAX_PERMIT_PER_BATCH:
            raise CarbiumError("request", f"a batch holds at most {MAX_PERMIT_PER_BATCH} of the heavier reads")
        payload = [self._one(method, params) for method, params in calls]
        return self._encode(payload), [call["id"] for call in payload]

    def _error(self, status: int, error: dict[str, Any] | None, *, method: str | None, fallback: str = "", **more: Any) -> CarbiumError:
        data = error.get("data") if error else None
        reason = data.get("reason") if isinstance(data, dict) else None
        code = error.get("code") if error else None
        kind = more.pop("kind", None) or _kind(status, code, reason)
        message = (error or {}).get("message") or fallback or f"HTTP {status}"
        return CarbiumError(kind, str(message), http_status=status, code=code, reason=reason, method=method, data=data, **more)

    def _settle(
        self,
        *,
        method: str | None,
        attempt: int,
        status: int = 0,
        headers: Any = None,
        text: str = "",
        exc: BaseException | None = None,
        timed_out: bool = False,
        min_context_slot: float | None = None,
        batch: bool = False,
        rest: bool = False,
    ) -> tuple[str, Any]:
        """What one attempt leads to: ("done", result) or ("wait", milliseconds). Raises once the call has failed for good."""
        headers = {str(k).lower(): v for k, v in dict(headers or {}).items()}
        request_id = headers.get("x-request-id")
        try:
            body = json.loads(text) if status else None
        except ValueError:
            body = None
        if status == 200 and batch and isinstance(body, list):
            return "done", (body, headers)
        if status == 200 and not batch and isinstance(body, dict) and "result" in body and not body.get("error"):
            return "done", body["result"]
        if status == 200 and rest and isinstance(body, dict) and not isinstance(body.get("error"), dict):
            return "done", body  # a REST route answers the object itself, not a JSON-RPC envelope

        first = body[0] if isinstance(body, list) and body else body
        error = first.get("error") if isinstance(first, dict) and isinstance(first.get("error"), dict) else None
        data = error.get("data") if error else None
        flagged = data.get("retryable") if isinstance(data, dict) and isinstance(data.get("retryable"), bool) else None
        hint = server_hint_ms(headers, data)

        if status == 200 and error is None:  # an answer, but not a JSON-RPC one: nothing a retry would fix
            shown = text.strip()[:200] or "an empty body"
            raise self._error(200, None, method=method, kind="upstream", request_id=request_id, fallback=f"the service did not answer in JSON-RPC: {shown}")

        plan = None if status == 200 and batch else decide_retry(
            method=method,
            status=status,
            code=error.get("code") if error else None,
            message=str((error or {}).get("message") or text),
            reason=data.get("reason") if isinstance(data, dict) else None,
            retryable=flagged,
            timed_out=timed_out,
            data=data,
            min_context_slot=min_context_slot,
        )
        limit = plan.max_attempts if plan else 1
        if self.max_attempts is not None:
            limit = min(limit, self.max_attempts)
        if not self.retry:
            limit = 1
        too_long = status != 0 and hint is not None and hint > self.max_hint_ms  # 0: nothing answered, so no hint
        if plan is None or attempt >= limit or too_long:
            failure = self._error(
                status,
                error,
                method=method,
                kind="transport" if status == 0 else "route" if rest and status == 422 else None,
                fallback=("the call timed out" if timed_out else str(exc)) if exc else text.strip()[:200],
                retryable=flagged if flagged is not None else plan is not None,
                retry_after_ms=hint,
                request_id=request_id,
            )
            raise failure from (Cause(exc) if exc is not None else None)
        return "wait", wait_ms(plan, attempt, hint, self._jitter())

    def _element_error(self, error: dict[str, Any], method: str | None, headers: dict[str, Any]) -> CarbiumError:
        """A batch element's error, with retryable, the wait and the request id worked out as for a single call."""
        data = error.get("data")
        reason = data.get("reason") if isinstance(data, dict) else None
        flagged = data.get("retryable") if isinstance(data, dict) and isinstance(data.get("retryable"), bool) else None
        plan = decide_retry(method=method, status=200, code=error.get("code"), message=str(error.get("message") or ""), reason=reason, retryable=flagged, data=data)
        return self._error(200, error, method=method, retryable=flagged if flagged is not None else plan is not None,
                           retry_after_ms=server_hint_ms(headers, data), request_id=headers.get("x-request-id"))

    def _batch_results(self, calls: Sequence[Call], ids: list[int], answers: list[Any], headers: dict[str, Any]) -> list[dict[str, Any]]:
        """One entry per call, in the order sent: {"ok": True, "result": ...} or {"ok": False, "error": CarbiumError}."""
        errors = [a["error"] for a in answers if isinstance(a, dict) and isinstance(a.get("error"), dict)]
        if answers and len(errors) == len(answers):
            reason = (errors[0].get("data") or {}).get("reason") if isinstance(errors[0].get("data"), dict) else None
            same = lambda e: e.get("code") == errors[0].get("code") and isinstance(e.get("data"), dict) and e["data"].get("reason") == reason
            if reason and all(same(e) for e in errors):  # the service refused the batch as a whole
                raise self._element_error(errors[0], None, headers)
        by_id = {a.get("id"): a for a in answers if isinstance(a, dict)}
        out: list[dict[str, Any]] = []
        for (method, _), call_id in zip(calls, ids):
            answer = by_id.get(call_id)
            if answer is None:
                out.append({"ok": False, "error": self._error(200, None, method=method, kind="upstream", fallback="the batch answer has no element for this call",
                                                              request_id=headers.get("x-request-id"))})
            elif isinstance(answer.get("error"), dict):
                out.append({"ok": False, "error": self._element_error(answer["error"], method, headers)})
            else:
                out.append({"ok": True, "result": answer.get("result")})
        return out


class Transport(_Core):
    def __init__(self, api_key: str | None = None, endpoint: str = DEFAULT_ENDPOINT, *, timeout: float = 100.0, max_attempts: int | None = None,
                 retry: bool = True, max_hint_ms: int = 30000, jitter: Callable[[], float] | None = None, client: httpx.Client | None = None) -> None:
        super().__init__(api_key, endpoint, timeout=timeout, max_attempts=max_attempts, retry=retry, max_hint_ms=max_hint_ms, jitter=jitter)
        self._owns_client = client is None
        self._client = client or httpx.Client()

    def _exchange(self, **request: Any) -> dict[str, Any]:
        try:
            answer = self._client.request(headers=self._request_headers(), timeout=self.timeout, **request)
            return {"status": answer.status_code, "headers": answer.headers, "text": answer.text}
        except httpx.TimeoutException as exc:
            return {"exc": exc, "timed_out": True}
        except httpx.HTTPError as exc:
            return {"exc": exc}

    def _send(self, request: Callable[[], dict[str, Any]], **about: Any) -> Any:
        attempt = 0
        while True:
            attempt += 1
            what, value = self._settle(attempt=attempt, **about, **request())
            if what == "done":
                return value
            time.sleep(value / 1000)

    def call(self, method: str, params: Any = None) -> Any:
        body = self._encode(self._one(method, params))
        return self._send(lambda: self._exchange(method="POST", url=self.endpoint, content=body), method=method, min_context_slot=_min_context_slot(params))

    def batch(self, calls: Sequence[Call]) -> list[dict[str, Any]]:
        body, ids = self._batch_body(calls)
        sends = "sendTransaction" if any(method == "sendTransaction" for method, _ in calls) else None
        answers, headers = self._send(lambda: self._exchange(method="POST", url=self.endpoint, content=body), method=sends, batch=True)
        return self._batch_results(calls, ids, answers, headers)

    def rest(self, path: str, params: dict[str, str], method: str) -> Any:
        """One GET on a REST route of the services endpoint, under the same retry rules. The key stays in its header."""
        url = self.endpoint.rstrip("/") + path
        return self._send(lambda: self._exchange(method="GET", url=url, params=params), method=method, rest=True)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


class AsyncTransport(_Core):
    def __init__(self, api_key: str | None = None, endpoint: str = DEFAULT_ENDPOINT, *, timeout: float = 100.0, max_attempts: int | None = None,
                 retry: bool = True, max_hint_ms: int = 30000, jitter: Callable[[], float] | None = None, client: httpx.AsyncClient | None = None) -> None:
        super().__init__(api_key, endpoint, timeout=timeout, max_attempts=max_attempts, retry=retry, max_hint_ms=max_hint_ms, jitter=jitter)
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient()

    async def _exchange(self, **request: Any) -> dict[str, Any]:
        try:
            answer = await self._client.request(headers=self._request_headers(), timeout=self.timeout, **request)
            return {"status": answer.status_code, "headers": answer.headers, "text": answer.text}
        except httpx.TimeoutException as exc:
            return {"exc": exc, "timed_out": True}
        except httpx.HTTPError as exc:
            return {"exc": exc}

    async def _send(self, request: Callable[[], Awaitable[dict[str, Any]]], **about: Any) -> Any:
        attempt = 0
        while True:
            attempt += 1
            what, value = self._settle(attempt=attempt, **about, **await request())
            if what == "done":
                return value
            await asyncio.sleep(value / 1000)

    async def call(self, method: str, params: Any = None) -> Any:
        body = self._encode(self._one(method, params))
        return await self._send(lambda: self._exchange(method="POST", url=self.endpoint, content=body), method=method, min_context_slot=_min_context_slot(params))

    async def batch(self, calls: Sequence[Call]) -> list[dict[str, Any]]:
        body, ids = self._batch_body(calls)
        sends = "sendTransaction" if any(method == "sendTransaction" for method, _ in calls) else None
        answers, headers = await self._send(lambda: self._exchange(method="POST", url=self.endpoint, content=body), method=sends, batch=True)
        return self._batch_results(calls, ids, answers, headers)

    async def rest(self, path: str, params: dict[str, str], method: str) -> Any:
        url = self.endpoint.rstrip("/") + path
        return await self._send(lambda: self._exchange(method="GET", url=url, params=params), method=method, rest=True)

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()
