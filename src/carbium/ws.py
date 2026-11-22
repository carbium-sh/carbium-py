"""JSON-RPC subscriptions over wss://services.carbium.sh, for asyncio, with the published reconnect rules: every live
subscription is bound again after a reconnect, the new server ids replace the old ones, and on_reseed tells you to
re-read what you may have missed. The server replays nothing after a disconnect."""
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from typing import Any, Callable, Coroutine

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus, InvalidURI

from ._gen import SUBSCRIPTIONS
from .errors import CarbiumError, Secret, redact
from .retry import server_hint_ms
from .transport import _KEY_IN_URL, _kind

DEFAULT_URL = "wss://services.carbium.sh"
_KEY = re.compile(r"^c_serv_[A-Za-z0-9]+$")
_RETRY_REASONS = {"RATE_LIMITED", "LOAD_SHED", "FREE_SUBNET_LIMITED", "FREE_TIER_BUSY", "CREDITS_PENDING"}
_BIND_BATCH = 32
_MAX_MESSAGE = 100 * 2**20  # a full block notification can run to many megabytes

Callback = Callable[[Any], None]


class _Redacting(logging.LoggerAdapter):
    """websockets writes every handshake header at DEBUG, the key's too: each line is cut before it is written."""

    def log(self, level: int, msg: object, *args: Any, **kwargs: Any) -> None:
        if self.isEnabledFor(level):
            self.logger.log(level, redact(str(msg) % args if args else str(msg)), **kwargs)


_LOG = _Redacting(logging.getLogger("carbium.ws"), {})


class _Handle:
    def __init__(self, method: str, params: list[Any], callback: Callback, ready: asyncio.Future[int]) -> None:
        self.method, self.params, self.callback, self.ready = method, params, callback, ready
        self.server_id: int | None = None
        self.acked = False
        self.binding = False  # a bind of this handle is in flight


class Subscription:
    """One subscription. Await ready for the server's id; it raises when the server refuses the subscription."""

    def __init__(self, socket: "CarbiumSocket", local: int, ready: asyncio.Future[int]) -> None:
        self._socket, self._local, self.ready = socket, local, ready

    async def unsubscribe(self) -> None:
        await self._socket._unsubscribe(self._local)


def _unsubscribe_name(method: str) -> str:
    return method[: -len("Subscribe")] + "Unsubscribe"


def _rpc_error(method: str, error: dict[str, Any]) -> CarbiumError:
    data = error.get("data")
    reason = data.get("reason") if isinstance(data, dict) else None
    code = error.get("code")
    return CarbiumError(_kind(200, code, reason), str(error.get("message") or "refused"), http_status=200, code=code, reason=reason,
                        retry_after_ms=server_hint_ms({}, data), method=method, data=data)


def _quiet(ready: asyncio.Future[int]) -> None:
    if not ready.cancelled():
        ready.exception()  # a refusal nobody awaited is not reported as lost


class CarbiumSocket:
    """Subscriptions on one WebSocket, opened on the first subscribe. The service key goes in the x-api-key header,
    never in the address. Callbacks: on_reseed(info) after every reconnect once all is bound again (info["gap"] is
    true after close 1012 stream_gap_reseed_required); on_fatal(error) once, when the socket gives up; on_error(error)
    for a subscription refused when bound again; on_state(state, info). Notification callbacks run in the loop."""

    def __init__(self, api_key: str | None = None, *, url: str = DEFAULT_URL, on_reseed: Callback | None = None,
                 on_fatal: Callable[[CarbiumError], None] | None = None, on_error: Callable[[CarbiumError], None] | None = None,
                 on_state: Callable[[str, Any], None] | None = None, _random: Callable[[], float] | None = None) -> None:
        key = api_key or ""
        if key and not _KEY.fullmatch(key):
            raise CarbiumError("auth", "api_key must be the full service key (c_serv_…) with the websocket scope")
        self.url = re.sub(r"^http", "ws", url).rstrip("/")
        if _KEY_IN_URL.search(self.url):
            raise CarbiumError("request", "put the key in api_key, not in the url")
        self._key = Secret(key)
        self._on_reseed, self._on_fatal, self._on_error, self._on_state = on_reseed, on_fatal, on_error, on_state
        self._random = _random or random.random
        self.state = "idle"  # idle, connecting, open, backoff or closed
        self._handles: dict[int, _Handle] = {}
        self._by_server: dict[int, _Handle] = {}
        self._pending: dict[int, tuple[str, asyncio.Future[Any], _Handle | None, Any]] = {}  # method, answer, the handle a bind names, its connection
        self._next_id = self._next_local = 1
        self._attempt = self._overload_closes = 0
        self._credits_exhausted_once = self._ever_opened = self._closed_by_user = False
        self._fatal: CarbiumError | None = None
        self._ws: ClientConnection | None = None
        self._task: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._last_close: tuple[int, str] = (0, "")
        self._gap = False  # a stream gap was announced and on_reseed has not told the caller yet

    @property
    def size(self) -> int:
        """How many subscriptions this socket holds."""
        return len(self._handles)

    def subscribe(self, method: str, params: list[Any], on_notification: Callback) -> Subscription:
        """Any served subscription by name; the typed calls below use this."""
        if method not in SUBSCRIPTIONS:
            raise CarbiumError("request", f"{method} is not a served subscription", method=method)
        if self._fatal:
            raise self._fatal
        if self._closed_by_user:
            raise CarbiumError("socket", "the socket was closed", method=method)
        ready: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        ready.add_done_callback(_quiet)
        local, self._next_local = self._next_local, self._next_local + 1
        handle = self._handles[local] = _Handle(method, list(params), on_notification, ready)
        if self.state == "open":
            self._spawn(self._bind(handle))
        elif self._task is None:  # one connection task at a time; a running one binds this on its next open
            self._task = asyncio.get_running_loop().create_task(self._run())
        return Subscription(self, local, ready)

    def account_subscribe(self, pubkey: str, on_notification: Callback, config: dict[str, Any] | None = None) -> Subscription:
        return self.subscribe("accountSubscribe", [pubkey, config] if config else [pubkey], on_notification)

    def program_subscribe(self, program_id: str, on_notification: Callback, config: dict[str, Any] | None = None) -> Subscription:
        """config may hold up to four filters, each a dataSize or a memcmp."""
        return self.subscribe("programSubscribe", [program_id, config] if config else [program_id], on_notification)

    def logs_subscribe(self, filter: str | dict[str, Any], on_notification: Callback, config: dict[str, Any] | None = None) -> Subscription:
        """filter: "all", "allWithVotes" or {"mentions": [address]}."""
        return self.subscribe("logsSubscribe", [filter, config] if config else [filter], on_notification)

    def signature_subscribe(self, signature: str, on_notification: Callback, config: dict[str, Any] | None = None) -> Subscription:
        """Fires once, then the server ends it. After a reconnect, check getSignatureStatuses."""
        return self.subscribe("signatureSubscribe", [signature, config] if config else [signature], on_notification)

    def slot_subscribe(self, on_notification: Callback) -> Subscription:
        return self.subscribe("slotSubscribe", [], on_notification)

    def root_subscribe(self, on_notification: Callback) -> Subscription:
        return self.subscribe("rootSubscribe", [], on_notification)

    def slots_updates_subscribe(self, on_notification: Callback) -> Subscription:
        return self.subscribe("slotsUpdatesSubscribe", [], on_notification)

    def block_subscribe(self, filter: str | dict[str, Any], on_notification: Callback, config: dict[str, Any] | None = None) -> Subscription:
        """maxSupportedTransactionVersion defaults to 1: without it a block holding a v1 transaction arrives as None."""
        return self.subscribe("blockSubscribe", [filter, {"maxSupportedTransactionVersion": 1, **(config or {})}], on_notification)

    async def close(self) -> None:
        """Closes the socket; no reconnect follows. Subscriptions not yet acknowledged raise."""
        self._closed_by_user = True
        closed = CarbiumError("socket", "the socket was closed")
        for handle in self._handles.values():
            if not handle.ready.done():
                handle.ready.set_exception(closed)
        self._handles.clear()
        self._by_server.clear()
        ws, self._ws = self._ws, None
        self._set_state("closed")
        if ws is not None:
            await ws.close(1000, "client close")
        running = [t for t in (self._task, *self._tasks) if t is not None and not t.done() and t is not asyncio.current_task()]
        for task in running:
            task.cancel()
        await asyncio.gather(*running, return_exceptions=True)

    async def __aenter__(self) -> "CarbiumSocket":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def __repr__(self) -> str:
        return f"CarbiumSocket(url={self.url!r}, state={self.state!r}, size={self.size})"

    def _set_state(self, state: str, info: Any = None) -> None:
        self.state = state
        if self._on_state:
            self._on_state(state, info)

    def _spawn(self, work: Coroutine[Any, Any, Any]) -> None:
        task = asyncio.get_running_loop().create_task(work)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run(self) -> None:
        """One connection after another, until the subscriptions are gone, the caller closes, or a rule says stop."""
        try:
            await self._connections()
        finally:
            if self._task is asyncio.current_task():
                self._task = None

    async def _connections(self) -> None:
        while True:
            self._set_state("connecting")
            hint = None
            try:
                headers = {"x-api-key": self._key.reveal()} if self._key else None  # built for this handshake only
                ws = await connect(self.url, additional_headers=headers, logger=_LOG, max_size=_MAX_MESSAGE)
            except InvalidStatus as e:
                status = e.response.status_code
                if status in (401, 403):
                    why = "unknown, suspended or revoked key, or no websocket scope" if status == 401 else "address not allowed or no websocket scope"
                    return self._die(CarbiumError("auth", f"websocket handshake refused: {why}", http_status=status))
                if status == 429:
                    hint = server_hint_ms({k.lower(): v for k, v in e.response.headers.raw_items()})
                info = (1006, f"handshake refused with HTTP {status}")
            except InvalidURI as e:
                return self._die(CarbiumError("socket", f"the url cannot be opened: {e}"))
            except (OSError, InvalidHandshake, TimeoutError):
                info = (1006, "handshake failed")
            else:
                info = await self._serve(ws)
            delay = self._after_close(info, hint)
            if delay is None:
                return
            self._set_state("backoff", {"code": info[0], "reason": info[1]})
            await asyncio.sleep(delay)

    async def _serve(self, ws: ClientConnection) -> tuple[int, str]:
        self._ws = ws
        self._attempt = 0
        reconnect, self._ever_opened = self._ever_opened, True
        handles = list(self._handles.values())  # read before the open is announced: a subscribe from then on binds itself
        self._set_state("open")
        self._spawn(self._after_open(ws, reconnect, handles))
        try:
            async for raw in ws:
                self._on_message(raw)
        except ConnectionClosed:
            pass
        await ws.wait_closed()
        return (ws.close_code if ws.close_code is not None else 1006), ws.close_reason or ""

    async def _after_open(self, ws: ClientConnection, reconnect: bool, handles: list[_Handle]) -> None:
        for start in range(0, len(handles), _BIND_BATCH):
            if self.state != "open":
                return
            await asyncio.gather(*(self._bind(h) for h in handles[start : start + _BIND_BATCH]))
        if reconnect and ws is self._ws and self.state == "open" and self._on_reseed:
            code, reason = self._last_close
            gap, self._gap = self._gap, False
            self._on_reseed({"code": code, "reason": reason, "gap": gap})

    def _on_message(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except ValueError:
            return
        if not isinstance(msg, dict):
            return
        if isinstance(msg.get("id"), int) and msg["id"] in self._pending:
            method, future, handle, sent_on = self._pending.pop(msg["id"])
            if not future.done():
                if isinstance(msg.get("error"), dict):
                    future.set_exception(_rpc_error(method, msg["error"]))
                else:
                    result = msg.get("result")
                    if handle is not None and isinstance(result, int) and not isinstance(result, bool) and sent_on is self._ws and self._held(handle):
                        self._acknowledged(handle, result)  # now: a notification read right after this frame must find its handle
                    future.set_result(result)
            return
        name, params = msg.get("method"), msg.get("params")
        if isinstance(name, str) and name.endswith("Notification") and isinstance(params, dict) and isinstance(params.get("subscription"), int):
            handle = self._by_server.get(params["subscription"])
            if handle is None:
                return
            self._overload_closes = 0  # a connection that delivers is not overloaded
            if name == "signatureNotification":
                del self._by_server[params["subscription"]]
                self._forget(handle)
            asyncio.get_running_loop().call_soon(handle.callback, params.get("result"))

    async def _request(self, method: str, params: list[Any], handle: _Handle | None = None) -> Any:
        ws = self._ws
        if ws is None or self.state != "open":
            raise CarbiumError("socket", "the socket is not open", method=method)
        call_id, self._next_id = self._next_id, self._next_id + 1
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[call_id] = (method, future, handle, ws)
        frame: dict[str, Any] = {"jsonrpc": "2.0", "id": call_id, "method": method}
        if params:
            frame["params"] = params
        try:
            await ws.send(json.dumps(frame))
        except ConnectionClosed:
            self._pending.pop(call_id, None)
            closed = CarbiumError("socket", "the socket closed while sending", method=method)
        else:
            return await future
        raise closed

    def _acknowledged(self, handle: _Handle, server_id: int) -> None:
        """A bind's acknowledgement, recorded as its frame is read: the server id, and ready once."""
        handle.server_id = server_id
        self._by_server[server_id] = handle
        if not handle.acked:
            handle.acked = True
            if not handle.ready.done():
                handle.ready.set_result(server_id)

    def _held(self, handle: _Handle) -> bool:
        return any(h is handle for h in self._handles.values())

    def _forget(self, handle: _Handle) -> None:
        for local in [k for k, h in self._handles.items() if h is handle]:
            del self._handles[local]

    async def _bind(self, handle: _Handle) -> None:
        if handle.binding:
            return  # one bind at a time: a second would subscribe twice
        handle.binding = True
        try:
            await self._bind_now(handle)
        finally:
            handle.binding = False

    async def _bind_now(self, handle: _Handle) -> None:
        ws = self._ws
        attempt = 0
        while True:
            attempt += 1
            if ws is not self._ws or handle.server_id is not None or not self._held(handle):
                return  # the connection changed, another bind won, or the caller left
            try:
                result = await self._request(handle.method, handle.params, handle)
            except CarbiumError as e:
                if e.kind == "socket":
                    return  # the close rules bind it again on the next connection
                if e.code == -32005 and (e.reason is None or e.reason in _RETRY_REASONS) and attempt < 4:
                    backoff = min(4.0, 0.25 * 2 ** (attempt - 1))
                    await asyncio.sleep(max((e.retry_after_ms if e.retry_after_ms is not None else 1000) / 1000, backoff) + int(self._random() * 251) / 1000)
                    # The connection may have dropped and come back during the wait. Its re-bind found this one still in
                    # flight and left it to us, so bind on whichever connection is current now.
                    ws = self._ws
                    continue
                if e.code == -32002:
                    return self._die(e)
                self._forget(handle)
                if handle.acked:
                    if self._on_error:
                        self._on_error(e)
                elif not handle.ready.done():
                    handle.ready.set_exception(e)
                return
            # The acknowledgement itself recorded the handle (_on_message). Left to do here: a caller who unsubscribed
            # before it came, so nothing was recorded; the server's slot is freed. An answer on an older connection is ignored.
            if isinstance(result, int) and ws is self._ws and handle.server_id is None and not self._held(handle):
                self._spawn(self._request_quietly(_unsubscribe_name(handle.method), [result]))
            return

    async def _request_quietly(self, method: str, params: list[Any]) -> None:
        try:
            await self._request(method, params)
        except CarbiumError:
            pass  # false, -32602 and a closed socket all mean the subscription is gone

    async def _unsubscribe(self, local: int) -> None:
        handle = self._handles.pop(local, None)
        if handle is None:
            return
        if handle.server_id is not None:
            self._by_server.pop(handle.server_id, None)
            if self.state == "open":
                await self._request_quietly(_unsubscribe_name(handle.method), [handle.server_id])
        if not handle.ready.done():
            handle.ready.set_exception(CarbiumError("socket", "unsubscribed before the acknowledgement", method=handle.method))
        if not self._handles and self.state == "open" and self._ws is not None:
            await self._ws.close(1000, "no subscriptions")

    def _die(self, error: CarbiumError) -> None:
        self._fatal = error
        for handle in self._handles.values():
            if not handle.ready.done():
                handle.ready.set_exception(error)
        self._handles.clear()
        self._by_server.clear()
        ws, self._ws = self._ws, None
        if ws is not None:
            self._spawn(ws.close(1000, "fatal"))
        self._set_state("closed")
        if self._on_fatal:
            self._on_fatal(error)

    def _after_close(self, info: tuple[int, str], hint: int | None) -> float | None:
        """What a close leads to: the wait in seconds before the next connection, or None to stop."""
        code, reason = info
        self._last_close = info
        if code == 1012 and reason == "stream_gap_reseed_required":
            self._gap = True
        self._ws = None
        dropped = CarbiumError("socket", f"socket closed {code} {reason}".strip(), code=code, reason=reason or None)
        for _, future, _, _ in self._pending.values():
            if not future.done():
                future.set_exception(dropped)
        self._pending.clear()
        self._by_server.clear()
        for handle in self._handles.values():
            handle.server_id = None
        if self._closed_by_user or self._fatal:
            if self._fatal and self.state != "closed":
                self._die(self._fatal)
            return None
        if not self._handles:
            self._task = None  # a subscribe from here on starts a new connection
            self._set_state("idle", {"code": code, "reason": reason})
            return None
        r = self._random()
        if code == 1008 and reason.startswith("key_"):
            return self._die(CarbiumError("auth", f"the server closed the socket: {reason}; a human must fix the key", code=1008, reason=reason))
        if code == 1008 and reason == "credits_exhausted":
            if self._credits_exhausted_once:
                return self._die(CarbiumError("credits", "credits exhausted: buy a plan or upgrade", code=1008, reason=reason))
            self._credits_exhausted_once = True
            return 5.0
        if code == 1008:
            return self._die(CarbiumError("socket", f"the server closed the socket: {reason}", code=1008, reason=reason or None))
        if code == 1013 and reason == "credits_pending":
            return 1.0 + r * 0.5
        if code == 1013:  # slow_consumer or delivery_overload_retry: at least 1 s, further back after each close in a row
            cap = min(30.0, 2.0 ** min(self._overload_closes, 5))  # bounded: the count may grow without end
            self._overload_closes += 1
            return max(1.0, cap / 2 + r * cap / 2)
        if code in (1012, 1001):
            return 0.25 + r * 0.75
        cap = min(30.0, 0.5 * 2 ** min(self._attempt, 6))  # a network failure only backs off: a refusal always has its status
        self._attempt += 1
        delay = cap / 2 + r * cap / 2
        return max(delay, hint / 1000) if hint is not None else delay
