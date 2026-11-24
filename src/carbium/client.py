"""The clients: Carbium (blocking) and AsyncCarbium. Each served method is a typed call; rpc() takes one by name."""
from __future__ import annotations

from typing import Any, Callable, Sequence, TypedDict

import httpx

from ._gen import METHODS, AsyncRpcMethods, RpcMethods
from .errors import CarbiumError
from .transport import DEFAULT_ENDPOINT, AsyncTransport, Call, Transport


class Cq1SwapInfo(TypedDict):
    ammKey: str
    label: str
    inputMint: str
    outputMint: str
    inAmount: str
    outAmount: str
    feeAmount: str
    feeMint: str


class Cq1RouteStep(TypedDict):
    swapInfo: Cq1SwapInfo
    percent: str
    bps: str


class Cq1Quote(TypedDict):
    """The REST quote. Every value is a string; fields the route gains later pass through."""

    srcAmountIn: str
    destAmountOut: str
    destAmountOutMin: str
    swapMode: str
    slippage: str
    priceImpactPct: str
    routePlan: list[Cq1RouteStep]
    feeMint: str
    feeBps: str
    signatureFeeLamports: str
    prioritizationFeeLamports: str
    rentFeeLamports: str
    inputMint: str
    outputMint: str
    swapType: str
    router: str
    requestId: str
    priceImpact: str


def _cq1(src_mint: str, dst_mint: str, amount_in: int | str, slippage_bps: int) -> dict[str, str]:
    """The four query values of GET /cq1/quote, the amount as decimal digits."""
    whole = isinstance(amount_in, int) and not isinstance(amount_in, bool) and amount_in >= 0
    if not (whole or isinstance(amount_in, str) and amount_in.isascii() and amount_in.isdigit()):
        raise CarbiumError("request", "amount_in must be a whole number: an int or decimal digits", method="cq1_quote")
    if isinstance(slippage_bps, bool) or not isinstance(slippage_bps, int):
        raise CarbiumError("request", "slippage_bps must be an int", method="cq1_quote")
    return {"src_mint": src_mint, "dst_mint": dst_mint, "amount_in": str(amount_in), "slippage_bps": str(slippage_bps)}


def _served(method: str) -> None:
    if method not in METHODS:
        raise CarbiumError("request", f"{method} is not a method the services serve", code=-32601, method=method)


class Carbium(RpcMethods):
    """A blocking client for services.carbium.sh. The key is sent in the x-api-key header and never in a URL."""

    def __init__(self, api_key: str | None = None, *, endpoint: str = DEFAULT_ENDPOINT, timeout: float = 100.0, max_attempts: int | None = None,
                 retry: bool = True, max_hint_ms: int = 30000, http_client: httpx.Client | None = None, _jitter: Callable[[], float] | None = None) -> None:
        self._transport = Transport(api_key, endpoint, timeout=timeout, max_attempts=max_attempts, retry=retry, max_hint_ms=max_hint_ms, jitter=_jitter, client=http_client)

    def _call(self, method: str, params: Any) -> Any:
        return self._transport.call(method, params)

    def rpc(self, method: str, params: Any = None) -> Any:
        """Calls a served method by its wire name, with params as the service takes them."""
        _served(method)
        return self._transport.call(method, params)

    def batch(self, calls: Sequence[Call]) -> list[dict[str, Any]]:
        """Sends up to 100 (method, params) calls in one request. Each entry comes back as {"ok", "result"} or {"ok", "error"}."""
        return self._transport.batch(calls)

    def cq1_quote(self, src_mint: str, dst_mint: str, amount_in: int | str, slippage_bps: int) -> Cq1Quote:
        """GET /cq1/quote, the old quote path, kept for callers already on it; new code uses quote(). Admitted as the quote method: same scope, plan and rate."""
        return self._transport.rest("/cq1/quote", _cq1(src_mint, dst_mint, amount_in, slippage_bps), "cq1_quote")

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> "Carbium":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"Carbium(endpoint={self._transport.endpoint!r})"


class AsyncCarbium(AsyncRpcMethods):
    """The same client for asyncio."""

    def __init__(self, api_key: str | None = None, *, endpoint: str = DEFAULT_ENDPOINT, timeout: float = 100.0, max_attempts: int | None = None,
                 retry: bool = True, max_hint_ms: int = 30000, http_client: httpx.AsyncClient | None = None, _jitter: Callable[[], float] | None = None) -> None:
        self._transport = AsyncTransport(api_key, endpoint, timeout=timeout, max_attempts=max_attempts, retry=retry, max_hint_ms=max_hint_ms, jitter=_jitter, client=http_client)

    async def _call(self, method: str, params: Any) -> Any:
        return await self._transport.call(method, params)

    async def rpc(self, method: str, params: Any = None) -> Any:
        _served(method)
        return await self._transport.call(method, params)

    async def batch(self, calls: Sequence[Call]) -> list[dict[str, Any]]:
        return await self._transport.batch(calls)

    async def cq1_quote(self, src_mint: str, dst_mint: str, amount_in: int | str, slippage_bps: int) -> Cq1Quote:
        """GET /cq1/quote, the old quote path, kept for callers already on it; new code uses quote()."""
        return await self._transport.rest("/cq1/quote", _cq1(src_mint, dst_mint, amount_in, slippage_bps), "cq1_quote")

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> "AsyncCarbium":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    def __repr__(self) -> str:
        return f"AsyncCarbium(endpoint={self._transport.endpoint!r})"
