"""One JSON exchange over HTTP for the account, DCA and token clients. Nothing here retries: each client reads the
answer and decides, and these three APIs leave the retry to the caller."""
from __future__ import annotations

import json
import re
from typing import Any, NamedTuple

import httpx

from .errors import CarbiumError, Cause

NOT_JSON: Any = object()  # Answer.json when the body is not JSON
_SAFE_INT = 2**53 - 1
HEADER_SAFE = re.compile(r"[\x21-\x7e]+")  # a credential that can go in a header: the HTTP library would quote any other in its error


def _exact(value: Any) -> Any:
    """Integers past 2^53 as decimal strings, as the TypeScript SDK sends a bigint: a JSON number that large is
    rounded by most readers."""
    if isinstance(value, int) and not isinstance(value, bool) and abs(value) > _SAFE_INT:
        return str(value)
    if isinstance(value, dict):
        return {key: _exact(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_exact(item) for item in value]
    return value


def _parse(text: str) -> Any:
    if not text.strip():
        return None
    try:
        return json.loads(text)
    except ValueError:
        return NOT_JSON


class Answer(NamedTuple):
    status: int
    headers: dict[str, str]  # names in lower case
    text: str
    json: Any  # None for an empty body, NOT_JSON when the body is not JSON


class Exchange(NamedTuple):
    """One request, built once and sent by the blocking or the async client."""

    kind: str  # the error kind of a failure: account, dca or tokens
    method: str  # the call's name, for errors
    verb: str
    url: str
    headers: dict[str, str]
    params: dict[str, Any] | None  # query values; None ones are left out
    body: Any  # sent as JSON; None sends no body
    timeout: float


def checked_url(url: str, what: str) -> str:
    """An http or https address with a host name. Anything else is refused now, without quoting it, instead of
    failing later inside the HTTP library."""
    try:
        parsed = httpx.URL(url)
    except (httpx.InvalidURL, TypeError):
        parsed = None
    if parsed is None or parsed.scheme not in ("http", "https") or not parsed.host:
        raise CarbiumError("request", f"{what} must be an http or https address with a host name")
    return url


def scrubbed(answer: Answer, secret: str) -> Answer:
    """The answer with a credential the service echoed cut out, before any of it reaches an error."""
    if not secret or secret not in answer.text:
        return answer
    text = answer.text.replace(secret, "…")
    return answer._replace(text=text, json=_parse(text))


def _arguments(x: Exchange) -> dict[str, Any]:
    headers = {"accept": "application/json", **x.headers}
    content = None
    if x.body is not None:
        headers["content-type"] = "application/json"
        content = json.dumps(_exact(x.body), separators=(",", ":")).encode()
    params = {key: value for key, value in (x.params or {}).items() if value is not None}
    return {"method": x.verb, "url": x.url, "headers": headers, "params": params or None, "content": content, "timeout": x.timeout}


def answer_of(response: httpx.Response) -> Answer:
    return Answer(response.status_code, {k.lower(): v for k, v in response.headers.items()}, response.text, _parse(response.text))


def failed(x: Exchange, exc: httpx.HTTPError) -> CarbiumError:
    """The error for a call that got no answer. Its cause is a stand-in: the HTTP library's exception holds the request."""
    message = "the call timed out" if isinstance(exc, httpx.TimeoutException) else str(exc) or type(exc).__name__
    error = CarbiumError(x.kind, message, method=x.method, retryable=x.verb == "GET")
    error.__cause__ = Cause(exc)
    return error


def send(client: httpx.Client, x: Exchange) -> Answer:
    try:
        response = client.request(**_arguments(x))
    except httpx.HTTPError as exc:
        failure = failed(x, exc)
    else:
        return answer_of(response)
    raise failure  # outside the except block, so the original is not kept as the context either


async def asend(client: httpx.AsyncClient, x: Exchange) -> Answer:
    try:
        response = await client.request(**_arguments(x))
    except httpx.HTTPError as exc:
        failure = failed(x, exc)
    else:
        return answer_of(response)
    raise failure
