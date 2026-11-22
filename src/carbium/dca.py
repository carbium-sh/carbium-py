"""The DCA API on the dex: CarbiumDca (blocking) and AsyncCarbiumDca. Reads are keyless. open, update, withdraw and
close return the unsigned transaction the service built for the owner's own wallet; send relays bytes the owner already
signed. Nothing is retried, send least of all: a failure comes back with retryable and retry_after_ms for the caller.
Server-side only: a browser page is refused with forbidden_origin."""
from __future__ import annotations

from typing import Any, Literal, NotRequired, TypeAlias, TypedDict
from urllib.parse import quote as _encode

import httpx

from . import _http
from .errors import CarbiumError, Secret
from .retry import server_hint_ms

DEFAULT_BASE_URL = "https://dex.carbium.io/api/dca"
_RETRYABLE = {429, 502, 503, 504}
_RETRYABLE_CODES = {"rate_limited", "rate_limited_open", "rpc_unavailable", "quote_unavailable", "door_busy"}  # a refusal can arrive with HTTP 200 and ok: false

U64: TypeAlias = str | int  # a u64: a decimal string or an int; an int past 2^53 is sent as a decimal string
DcaFrequency: TypeAlias = Literal["hourly", "daily", "weekly", "biweekly", "monthly"]


class DcaOpenRequest(TypedDict):
    """Exactly one of amountPerCycle and totalAmount, and exactly one of intervalSecs and frequency."""

    owner: str  # the wallet that signs and pays
    inputMint: str
    outputMint: str
    cycles: int  # 1 or more buys
    amountPerCycle: NotRequired[U64]
    totalAmount: NotRequired[U64]
    intervalSecs: NotRequired[int]
    frequency: NotRequired[DcaFrequency]
    maxSlippageBps: NotRequired[int]  # default 50, cap 300
    startAt: NotRequired[int]  # 0 or a Unix time up to 30 days ahead
    index: NotRequired[int]  # 0 to 255, for several positions on one pair
    minOutE6: NotRequired[U64]  # a floor on the output per 1e6 of input; exclusive with maxPrice
    maxPrice: NotRequired[str]  # a decimal string: the most whole input tokens per whole output token ("150.5"); exclusive with minOutE6


class DcaUpdateRequest(TypedDict):
    owner: str
    position: str
    paused: NotRequired[bool]
    minOutE6: NotRequired[U64]
    maxPrice: NotRequired[str | None]  # a decimal string as at open; None clears the maximum price
    maxSlippageBps: NotRequired[int]


class DcaWithdrawRequest(TypedDict):
    owner: str
    position: str
    amount: NotRequired[U64]  # 0 or left out withdraws the whole escrow


class DcaCloseRequest(TypedDict):
    owner: str
    position: str


class DcaSendRequest(TypedDict):
    txn_base64: str  # the transaction this API built, signed by the owner's own wallet
    built_txn_base64: NotRequired[str]  # the build answer's txn_base64, when the wallet changed the message


class DcaQuoteRequest(TypedDict):
    inputMint: str
    outputMint: str
    amount: U64
    slippageBps: NotRequired[int]
    owner: NotRequired[str]


class DcaBuildSummary(TypedDict):
    kind: str
    owner: str
    feePayer: str
    instructions: list[str]


class DcaBuild(TypedDict):
    """A built, unsigned v0 transaction for the owner to sign and send."""

    ok: Literal[True]
    txn_base64: str
    blockhash: str
    last_valid_block_height: str  # a u64, so a decimal string
    position: str
    cost: dict[str, Any]
    summary: DcaBuildSummary


class DcaOpenBuild(DcaBuild):
    authority: str
    escrowAccount: str
    index: int
    plan: dict[str, Any]


class DcaSent(TypedDict):
    ok: Literal[True]
    kind: str
    signature: str
    sent: bool
    position: str


class DcaLimits(TypedDict):
    minAmountPerCycle: str | None  # None when the amount has no USDC value
    minValueUsdc: str
    minIntervalSecs: int
    maxCycles: int
    maxActive: int


class DcaCost(TypedDict):
    requiredLamports: str
    heldLamports: str


class DcaQuote(TypedDict):
    ok: Literal[True]
    amount: str  # the amount asked, echoed
    out: str
    minOut: str
    rateE6: str
    feeBps: int
    inDecimals: int
    outDecimals: int
    valueUsdc: str  # the amount's value in USDC base units (6 decimals)
    limits: DcaLimits
    cost: NotRequired[DcaCost]


class DcaBuy(TypedDict):
    signature: str
    cycle: int
    inAmount: str
    outAmount: str
    ts: str  # Unix seconds, as a decimal string
    slot: int | None
    openedSlot: int
    verdict: Literal["filled", "missed"]
    detail: str | None


class DcaLastTry(TypedDict):
    at: int
    cycle: int
    result: str
    code: str
    detail: str | None
    signature: str | None


class DcaPosition(TypedDict):
    address: str
    owner: str
    inMint: str
    outMint: str
    crank: str
    inProgram: str
    outProgram: str
    index: int
    maxSlippageBps: int
    cyclesTotal: int
    cyclesDone: int
    intervalSecs: int
    amountPerCycle: str
    minOutE6: str
    cumIn: str
    cumOut: str
    nextTs: str
    active: bool
    escrow: str | None
    middleDebt: Any
    status: str  # active, waiting, paused, not_served, empty, finished or closed
    statusCode: str
    statusDetail: str | None
    lastTry: DcaLastTry | None
    openedSlot: int | None  # None until the keeper first sees the opening
    openedSignature: str | None
    served: bool | None
    notServed: str | None


class DcaPositionDetail(DcaPosition):
    ownerIn: str | None  # the owner's input and output token balances; None when the account does not exist
    ownerOut: str | None
    tracked: bool
    fills: list[DcaBuy]


class DcaClosedRecord(TypedDict):
    address: str
    closed: Literal[True]
    closedSlot: int
    openedSlot: int
    openedSignature: str | None  # None until the keeper finds the opening, and for good when it finds none
    status: Literal["closed"]
    statusCode: Literal["closed"]
    statusDetail: None
    lastTry: Any
    fills: list[DcaBuy]


class DcaPositions(TypedDict):
    ok: Literal[True]
    owner: str
    positions: list[DcaPosition]
    next: str | None


class DcaHistoryOrder(TypedDict):
    """An ended DCA as /history lists it. Times are Unix seconds as decimal strings."""

    address: str
    inMint: str
    outMint: str
    cyclesDone: int
    cyclesTotal: int
    amountPerCycle: str
    intervalSecs: int
    cumIn: str
    cumOut: str
    minOutE6: str
    openedSlot: int
    openedSignature: str | None
    openedTs: str | None
    closedSlot: int
    closedSignature: str | None
    closedTs: str | None
    lastTry: Any
    ended: Literal["finished", "emptied", "closed_early"]


class DcaHistory(TypedDict):
    ok: Literal[True]
    owner: str
    orders: list[DcaHistoryOrder]
    next: str | None


class DcaHealth(TypedDict):
    ok: bool
    keeper: str


class _DcaCore:
    def __init__(self, api_key: str | None, base_url: str, timeout: float) -> None:
        if api_key and not _http.HEADER_SAFE.fullmatch(api_key):
            raise CarbiumError("auth", "api_key must be the DCA API key from the dex account menu: printable, with no spaces")
        self.base_url = _http.checked_url(base_url, "base_url").rstrip("/")
        self.timeout = timeout
        self._key = Secret(api_key or "")

    def _exchange(self, method: str, verb: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None) -> _http.Exchange:
        headers = {"x-api-key": self._key.reveal()} if self._key else {}
        return _http.Exchange("dca", method, verb, self.base_url + path, headers, params, body, self.timeout)

    def _result(self, x: _http.Exchange, answer: _http.Answer) -> Any:
        """The answer, or an error: any status from 400, and an answer with ok: false at any status."""
        body = answer.json
        if answer.status < 400 and isinstance(body, (dict, list)) and not (isinstance(body, dict) and body.get("ok") is False):
            return body
        answer = _http.scrubbed(answer, self._key.reveal())
        body = answer.json if isinstance(answer.json, dict) else {}
        code = body.get("err") if isinstance(body.get("err"), str) else None
        detail = body.get("detail")
        message = detail if isinstance(detail, str) else code or answer.text.strip()[:200] or f"HTTP {answer.status}"
        raise CarbiumError(
            "dca",
            message,
            http_status=answer.status,
            code=code,
            retryable=answer.status in _RETRYABLE or code in _RETRYABLE_CODES,
            retry_after_ms=server_hint_ms(answer.headers),
            method=x.method,
            data=body or None,
        )

    def _quote(self, request: DcaQuoteRequest) -> _http.Exchange:
        return self._exchange("quote", "GET", "/quote", params=dict(request))

    def _positions(self, owner: str, after: str | None) -> _http.Exchange:
        return self._exchange("positions", "GET", "/positions", params={"owner": owner, "after": after})

    def _history(self, owner: str, after: str | None) -> _http.Exchange:
        return self._exchange("history", "GET", "/history", params={"owner": owner, "after": after})

    def _position(self, address: str, opened_slot: int | None) -> _http.Exchange:
        return self._exchange("position", "GET", f"/position/{_encode(address, safe='')}", params={"openedSlot": opened_slot})

    def _build(self, method: str, request: Any) -> _http.Exchange:
        return self._exchange(method, "POST", f"/{method}", body=request)

    def _send(self, request: DcaSendRequest) -> _http.Exchange:
        documented = DcaSendRequest.__required_keys__ | DcaSendRequest.__optional_keys__  # any other key a caller passes is not sent
        return self._exchange("send", "POST", "/send", body={k: v for k, v in request.items() if k in documented})

    def __repr__(self) -> str:
        return f"{type(self).__name__}(base_url={self.base_url!r}, keyed={bool(self._key)})"


class CarbiumDca(_DcaCore):
    """A blocking client for the DCA API. An optional api_key (the DCA API key from the dex account menu, not a
    Carbium key) raises the rate limits; it is sent as x-api-key. Use it in a with block to close its connections."""

    def __init__(self, api_key: str | None = None, *, base_url: str = DEFAULT_BASE_URL, timeout: float = 30.0, http_client: httpx.Client | None = None) -> None:
        super().__init__(api_key, base_url, timeout)
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client()

    def _run(self, x: _http.Exchange) -> Any:
        return self._result(x, _http.send(self._client, x))

    def quote(self, request: DcaQuoteRequest) -> DcaQuote:
        """GET /quote: a price for one buy, the fee rate and the owner's limits."""
        return self._run(self._quote(request))

    def positions(self, owner: str, *, after: str | None = None) -> DcaPositions:
        """GET /positions: the owner's open positions, at most 100; pass next back as after for the rest."""
        return self._run(self._positions(owner, after))

    def history(self, owner: str, *, after: str | None = None) -> DcaHistory:
        """GET /history: the owner's ended DCAs, newest close first."""
        return self._run(self._history(owner, after))

    def position(self, address: str, *, opened_slot: int | None = None) -> DcaPositionDetail | DcaClosedRecord:
        """GET /position/{address}: one position with its fills, or its closed record."""
        return self._run(self._position(address, opened_slot))

    def health(self) -> DcaHealth:
        """GET /health: the keeper's state."""
        return self._run(self._exchange("health", "GET", "/health"))

    def open(self, request: DcaOpenRequest) -> DcaOpenBuild:
        """POST /open: the unsigned open transaction."""
        return self._run(self._build("open", request))

    def update(self, request: DcaUpdateRequest) -> DcaBuild:
        """POST /update: the unsigned pause, resume or new-floor transaction."""
        return self._run(self._build("update", request))

    def withdraw(self, request: DcaWithdrawRequest) -> DcaBuild:
        """POST /withdraw: the unsigned withdraw transaction."""
        return self._run(self._build("withdraw", request))

    def close(self, request: DcaCloseRequest) -> DcaBuild:
        """POST /close: the unsigned close transaction."""
        return self._run(self._build("close", request))

    def send(self, request: DcaSendRequest) -> DcaSent:
        """POST /send: relays a transaction this API built once the owner's wallet signed it. Sent once, never again."""
        return self._run(self._send(request))

    def __enter__(self) -> "CarbiumDca":
        return self

    def __exit__(self, *exc: object) -> None:
        if self._owns_client:
            self._client.close()


class AsyncCarbiumDca(_DcaCore):
    """The same client for asyncio."""

    def __init__(self, api_key: str | None = None, *, base_url: str = DEFAULT_BASE_URL, timeout: float = 30.0, http_client: httpx.AsyncClient | None = None) -> None:
        super().__init__(api_key, base_url, timeout)
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient()

    async def _run(self, x: _http.Exchange) -> Any:
        return self._result(x, await _http.asend(self._client, x))

    async def quote(self, request: DcaQuoteRequest) -> DcaQuote:
        return await self._run(self._quote(request))

    async def positions(self, owner: str, *, after: str | None = None) -> DcaPositions:
        return await self._run(self._positions(owner, after))

    async def history(self, owner: str, *, after: str | None = None) -> DcaHistory:
        return await self._run(self._history(owner, after))

    async def position(self, address: str, *, opened_slot: int | None = None) -> DcaPositionDetail | DcaClosedRecord:
        return await self._run(self._position(address, opened_slot))

    async def health(self) -> DcaHealth:
        return await self._run(self._exchange("health", "GET", "/health"))

    async def open(self, request: DcaOpenRequest) -> DcaOpenBuild:
        return await self._run(self._build("open", request))

    async def update(self, request: DcaUpdateRequest) -> DcaBuild:
        return await self._run(self._build("update", request))

    async def withdraw(self, request: DcaWithdrawRequest) -> DcaBuild:
        return await self._run(self._build("withdraw", request))

    async def close(self, request: DcaCloseRequest) -> DcaBuild:
        return await self._run(self._build("close", request))

    async def send(self, request: DcaSendRequest) -> DcaSent:
        return await self._run(self._send(request))

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "AsyncCarbiumDca":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
