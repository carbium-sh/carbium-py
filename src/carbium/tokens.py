"""The token metadata API at tokens.carbium.io: CarbiumTokens (blocking) and AsyncCarbiumTokens, on its four public
routes. Keyless; 300 requests a minute per client. Nothing is retried: a 429 or 503 comes back with retry_after_ms."""
from __future__ import annotations

from typing import Any, Sequence, TypedDict
from urllib.parse import quote as _encode

import httpx

from . import _http
from .errors import CarbiumError
from .retry import server_hint_ms

DEFAULT_BASE_URL = "https://tokens.carbium.io"
BATCH_MAX_MINTS = 500


class Token(TypedDict):
    """A token as the registry serves it. Times are Unix milliseconds; supply is a decimal string. A mint seen but not
    resolved yet has symbol and name "" and decimals 0 until it is enriched."""

    mint: str
    symbol: str
    name: str
    decimals: int
    logo_url: str | None
    logo_status: str  # ok, broken, broken_perm, missing or unchecked; a new value comes as it is
    is_token22: bool
    daily_volume_usd: float | None
    first_seen_at: int
    last_seen_at: int
    supply: str | None
    mint_authority: str | None
    freeze_authority: str | None
    metaplex_uri: str | None
    update_authority: str | None
    seller_fee_basis_points: int | None
    token_standard: int | None  # Metaplex's TokenStandard number (0 NonFungible, 1 FungibleAsset, 2 Fungible, ...)
    description: str | None
    external_url: str | None
    extensions: dict[str, Any] | None
    tags: list[Any] | None  # as the off-chain metadata holds them: strings, or objects


class TokenSearch(TypedDict):
    items: list[Token]
    page: int
    limit: int
    total: int | None


class TokenBatch(TypedDict):
    items: list[Token]
    unknown: list[str]  # mints the registry does not hold yet; the call itself queued them


class _TokensCore:
    def __init__(self, base_url: str, timeout: float) -> None:
        self.base_url = _http.checked_url(base_url, "base_url").rstrip("/")
        self.timeout = timeout

    def _exchange(self, method: str, verb: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None) -> _http.Exchange:
        return _http.Exchange("tokens", method, verb, self.base_url + path, {}, params, body, self.timeout)

    def _error(self, method: str, answer: _http.Answer) -> CarbiumError:
        body = answer.json if isinstance(answer.json, dict) else {}
        code = body.get("error") if isinstance(body.get("error"), str) else None
        hint = server_hint_ms(answer.headers)
        if hint is None and isinstance(body.get("retryAfterSec"), (int, float)):
            hint = int(body["retryAfterSec"] * 1000)
        message = body.get("message") if isinstance(body.get("message"), str) else code or answer.text.strip()[:200] or f"HTTP {answer.status}"
        return CarbiumError("tokens", message, http_status=answer.status, code=code, retryable=answer.status in (429, 503), retry_after_ms=hint,
                            method=method, data=body or None)

    def _json(self, x: _http.Exchange, answer: _http.Answer) -> Any:
        if answer.status >= 400:
            raise self._error(x.method, answer)
        if answer.json is _http.NOT_JSON:
            raise CarbiumError("tokens", f"the token API did not answer in JSON: {answer.text.strip()[:200]}", http_status=answer.status, method=x.method)
        return answer.json

    def _lookup_result(self, x: _http.Exchange, answer: _http.Answer) -> Token | None:
        return None if answer.status == 404 else self._json(x, answer)  # 404: not indexed yet

    def _lookup(self, mint: str) -> _http.Exchange:
        return self._exchange("lookup", "GET", f"/tokens/{_encode(mint, safe='')}")

    def _search(self, q: str, limit: int | None, page: int | None, include_token22: bool | None) -> _http.Exchange:
        return self._exchange("search", "GET", "/tokens", params={"q": q, "limit": limit, "page": page, "include_token22": include_token22})

    def _batch(self, mints: Sequence[str]) -> _http.Exchange:
        if not 1 <= len(mints) <= BATCH_MAX_MINTS:
            raise CarbiumError("tokens", f"batch takes 1 to {BATCH_MAX_MINTS} mints, not {len(mints)}", method="batch")
        return self._exchange("batch", "POST", "/tokens/batch", body={"mints": list(mints)})

    def image_url(self, mint: str) -> str:
        """The address of GET /img/:mint, for an image tag. It always answers, with a fallback when no logo is known."""
        return f"{self.base_url}/img/{_encode(mint, safe='')}"

    def __repr__(self) -> str:
        return f"{type(self).__name__}(base_url={self.base_url!r})"


class CarbiumTokens(_TokensCore):
    """A blocking client for the token API. Use it in a with block to close its connections."""

    def __init__(self, *, base_url: str = DEFAULT_BASE_URL, timeout: float = 30.0, http_client: httpx.Client | None = None) -> None:
        super().__init__(base_url, timeout)
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client()

    def _send(self, x: _http.Exchange) -> _http.Answer:
        return _http.send(self._client, x)

    def lookup(self, mint: str) -> Token | None:
        """GET /tokens/:mint. None when the registry does not hold the mint yet."""
        x = self._lookup(mint)
        return self._lookup_result(x, self._send(x))

    def search(self, q: str, *, limit: int | None = None, page: int | None = None, include_token22: bool | None = None) -> TokenSearch:
        """GET /tokens?q=: a ranked search, or the top listing by volume when q is empty. limit 1 to 200, page from 1."""
        x = self._search(q, limit, page, include_token22)
        return self._json(x, self._send(x))

    def batch(self, mints: Sequence[str]) -> TokenBatch:
        """POST /tokens/batch, 1 to 500 mints. Unknown mints come back in unknown and are queued by the service itself."""
        x = self._batch(mints)
        return self._json(x, self._send(x))

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "CarbiumTokens":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class AsyncCarbiumTokens(_TokensCore):
    """The same client for asyncio."""

    def __init__(self, *, base_url: str = DEFAULT_BASE_URL, timeout: float = 30.0, http_client: httpx.AsyncClient | None = None) -> None:
        super().__init__(base_url, timeout)
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient()

    async def _send(self, x: _http.Exchange) -> _http.Answer:
        return await _http.asend(self._client, x)

    async def lookup(self, mint: str) -> Token | None:
        x = self._lookup(mint)
        return self._lookup_result(x, await self._send(x))

    async def search(self, q: str, *, limit: int | None = None, page: int | None = None, include_token22: bool | None = None) -> TokenSearch:
        x = self._search(q, limit, page, include_token22)
        return self._json(x, await self._send(x))

    async def batch(self, mints: Sequence[str]) -> TokenBatch:
        x = self._batch(mints)
        return self._json(x, await self._send(x))

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "AsyncCarbiumTokens":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
