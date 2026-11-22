"""Carbon Stream (Yellowstone gRPC) on services.carbium.sh:9443, for asyncio, with the published rules: the fields the
service refuses are caught before anything is sent, the reconnect table, an in-stream ping every 10 s, a re-seed on
OUT_OF_RANGE, and a resume from the last slot when the retained floor allows it. It needs the stream extra,
pip install 'carbium[stream]'; importing this module does not load gRPC."""
from __future__ import annotations

import asyncio
import inspect
import random
import re
from typing import Any, Callable, NamedTuple

from .errors import CarbiumError, Cause, Secret
from .transport import _KEY_IN_URL

DEFAULT_ENDPOINT = "https://services.carbium.sh:9443"
MAX_MESSAGE = 64 * 2**20  # block and transaction frames can be several MiB
PING_EVERY = 10.0
_KEY = re.compile(r"c_serv_[A-Za-z0-9]+")
_STOP = {"UNAUTHENTICATED", "PERMISSION_DENIED", "INVALID_ARGUMENT", "UNIMPLEMENTED"}
_SLOTTED = ("slot", "transaction", "transaction_status", "account", "block_meta", "block", "entry")
_EXTRA = "Carbon Stream needs the stream extra: pip install 'carbium[stream]'"


def _modules() -> tuple[Any, Any, Any]:
    """gRPC and the generated messages, loaded on first use."""
    try:
        import grpc

        from ._geyser import geyser_pb2, geyser_pb2_grpc
    except Exception as e:
        if isinstance(e, ModuleNotFoundError) and (e.name or "").split(".")[0] in ("grpc", "google"):
            failure = CarbiumError("stream", f"{_EXTRA} ({type(e).__name__})")  # the extra is not installed
        else:  # installed, but refused: a protobuf or grpcio version the generated code does not take, say
            failure = CarbiumError("stream", f"Carbon Stream could not load: {type(e).__name__}: {e}")
    else:
        return grpc, geyser_pb2, geyser_pb2_grpc
    raise failure


class StreamErrorInfo(NamedTuple):
    status: str  # the gRPC status name
    message: str
    action: str  # stop, reseed or retry
    retry_after_ms: int  # from the published table: the wait before the next try


def classify_stream_error(error: BaseException | None) -> StreamErrorInfo:
    """The status of a stream's end and what the published table says to do about it."""
    code = getattr(error, "code", None)
    status = getattr(code(), "name", "UNKNOWN") if callable(code) else "UNKNOWN"
    details = getattr(error, "details", None)
    message = (details() if callable(details) else None) or (str(error) if error is not None else "") or "stream ended"
    action, wait = "retry", 2000
    if status in _STOP or status == "RESOURCE_EXHAUSTED" and re.search(r"credits.exhausted", message, re.I):
        action = "stop"
    elif status == "OUT_OF_RANGE":
        action = "reseed"
    elif re.search(r"still loading", message, re.I):
        wait = 1000
    return StreamErrorInfo(status, message, action, wait)


def _failure(info: StreamErrorInfo) -> CarbiumError:
    return CarbiumError("stream", info.message, reason=info.status, retryable=info.action != "stop", retry_after_ms=info.retry_after_ms)


def _refused(field: str) -> CarbiumError:
    return CarbiumError("request", f"{field} is refused by Carbon Stream: remove the field")


def build_request(filters: dict[str, Any]) -> Any:
    """One SubscribeRequest holding the whole filter set, by its proto field names (accounts, slots, transactions,
    transactions_status, blocks, blocks_meta, entry, commitment, accounts_data_slice, from_slot). What the service
    refuses is refused here first."""
    _, pb, _ = _modules()
    from google.protobuf import json_format

    try:
        request = json_format.ParseDict(filters, pb.SubscribeRequest())
    except json_format.ParseError as e:
        problem = str(e)
    else:
        problem = None
    if problem is not None:
        raise CarbiumError("request", f"the filters do not fit a SubscribeRequest: {problem}")
    for name, f in request.accounts.items():
        if f.HasField("cuckoo_accounts_filter"):
            raise _refused(f"accounts.{name}.cuckoo_accounts_filter")
    for kind in ("transactions", "transactions_status"):
        for name, f in getattr(request, kind).items():
            for field in ("token_accounts", "cuckoo_account_include"):
                if f.HasField(field):
                    raise _refused(f"{kind}.{name}.{field}")
            if f.ByteSize() == 0:
                raise CarbiumError("request", f"{kind}.{name} is a bare transaction filter: name account_include, account_required, account_exclude, signature, vote or failed")
    for name, f in request.blocks.items():
        if f.HasField("cuckoo_account_include"):
            raise _refused(f"blocks.{name}.cuckoo_account_include")
    for name, f in request.entry.items():
        if f.include_update_parent:
            raise _refused(f"entry.{name}.include_update_parent")
    if len(request.block_footer):
        raise _refused("block_footer")
    if request.HasField("from_slot") and request.HasField("commitment") and request.commitment != pb.PROCESSED:
        raise CarbiumError("request", "from_slot replay is available only at processed commitment")
    return request


def _without_from_slot(request: Any) -> Any:
    copy = type(request)()
    copy.CopyFrom(request)
    copy.ClearField("from_slot")
    return copy


class _HandlerFailed(Exception):
    """A caller's handler raised: the stream stops with that exception, it is not a stream end to retry."""

    def __init__(self, original: Exception) -> None:
        self.original = original


def _handle(callback: Callable[[Any], Any], value: Any) -> None:
    try:
        callback(value)
    except Exception as e:
        raise _HandlerFailed(e) from None


async def _call(callback: Callable[[Any], Any] | None, value: Any) -> None:
    if callback is not None:
        result = callback(value)
        if inspect.isawaitable(result):
            await result


class ReconnectingStream:
    """A stream that follows the reconnect table. close() ends it at once; await done for its end, which raises the
    CarbiumError that stopped it, or the exception a handler raised. The open call lives only inside its task."""

    def __init__(self) -> None:
        self._stopped = asyncio.Event()
        self._worker: asyncio.Task[None] | None = None
        self.done: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.done.add_done_callback(lambda done: done.cancelled() or done.exception())  # an end nobody awaits is not reported as lost

    def close(self) -> None:
        self._stopped.set()
        if self._worker is not None:
            self._worker.cancel()  # wakes whatever the task awaits: a first write, a read or a wait
        if not self.done.done():
            self.done.set_result(None)


class CarbiumStream:
    """Carbon Stream with a service key that has the grpc scope. Make it with await CarbiumStream.connect(key). The
    key goes as x-token metadata on each call, built from its wrapper, and never in an address."""

    def __init__(self, channel: Any, key: Secret) -> None:
        _, self._pb, pb_grpc = _modules()
        self._channel = channel
        self._stub = pb_grpc.GeyserStub(channel)
        self._key = key

    @classmethod
    async def connect(cls, api_key: str, *, endpoint: str = DEFAULT_ENDPOINT, max_message: int = MAX_MESSAGE) -> "CarbiumStream":
        if not _KEY.fullmatch(api_key or ""):
            raise CarbiumError("auth", "api_key must be the full service key (c_serv_…) with the grpc scope")
        if _KEY_IN_URL.search(endpoint):
            raise CarbiumError("request", "put the key in api_key, not in the endpoint")
        grpc, _, _ = _modules()
        scheme, _, target = endpoint.partition("://")
        if scheme not in ("https", "http") or not target:
            raise CarbiumError("request", "the endpoint must be https://host:port, or http://host:port for a local server")
        options = [("grpc.max_receive_message_length", max_message)]
        channel = grpc.aio.secure_channel(target, grpc.ssl_channel_credentials(), options) if scheme == "https" else grpc.aio.insecure_channel(target, options)
        return cls(channel, Secret(api_key))

    def _metadata(self) -> tuple[tuple[str, str], ...]:
        return (("x-token", self._key.reveal()),)

    async def _unary(self, name: str, request: Any) -> Any:
        try:
            return await getattr(self._stub, name)(request, metadata=self._metadata())
        except Exception as e:
            if not callable(getattr(e, "code", None)):
                raise
            failure = _failure(classify_stream_error(e))
            failure.__cause__ = Cause(e)
        raise failure

    # The seven unary calls, 1 credit each.
    async def get_slot(self, commitment: int | None = None) -> Any:
        return await self._unary("GetSlot", self._pb.GetSlotRequest(**_set(commitment=commitment)))

    async def get_block_height(self, commitment: int | None = None) -> Any:
        return await self._unary("GetBlockHeight", self._pb.GetBlockHeightRequest(**_set(commitment=commitment)))

    async def get_latest_blockhash(self, commitment: int | None = None) -> Any:
        return await self._unary("GetLatestBlockhash", self._pb.GetLatestBlockhashRequest(**_set(commitment=commitment)))

    async def is_blockhash_valid(self, blockhash: str, commitment: int | None = None) -> Any:
        return await self._unary("IsBlockhashValid", self._pb.IsBlockhashValidRequest(blockhash=blockhash, **_set(commitment=commitment)))

    async def get_version(self) -> Any:
        return await self._unary("GetVersion", self._pb.GetVersionRequest())

    async def ping(self, count: int) -> Any:
        """The unary Ping costs 1 credit; the in-stream ping subscribe_with_reconnect sends costs nothing."""
        return await self._unary("Ping", self._pb.PingRequest(count=count))

    async def subscribe_replay_info(self) -> Any:
        """The first slot a from_slot replay can start from."""
        return await self._unary("SubscribeReplayInfo", self._pb.SubscribeReplayInfoRequest())

    async def subscribe(self, filters: dict[str, Any]) -> Any:
        """One plain stream with the whole filter set in its first message; no reconnect, no ping. For short reads."""
        request = build_request(filters)  # a refused shape opens nothing
        call = self._stub.Subscribe(metadata=self._metadata())
        await call.write(request)
        return call

    def subscribe_with_reconnect(self, filters: dict[str, Any], on_update: Callable[[Any], Any], *, on_reseed: Callable[[dict[str, Any]], Any] | None = None,
                                 on_status: Callable[[dict[str, Any]], Any] | None = None, _random: Callable[[], float] | None = None) -> ReconnectingStream:
        """A stream that follows the published reconnect table. The filter set is fixed for its life. on_update gets
        every update but pings and pongs and is called, never awaited: a reader must not block the stream, so hand slow
        work to your own task or queue. on_reseed({"last_slot"}) runs after a gap, or when a resume cannot replay:
        reload state over RPC there; it may be a coroutine function. An exception from a handler stops the stream."""
        if inspect.iscoroutinefunction(on_update):
            raise CarbiumError("request", "on_update must be a plain function, not a coroutine function: a reader never awaits, so hand slow work to your own task or queue")
        base = build_request(filters)
        live = ReconnectingStream()
        status = on_status or (lambda event: None)
        live._worker = asyncio.get_running_loop().create_task(self._settle(live, self._follow(base, live, on_update, on_reseed, status, _random or random.random), status))
        return live

    @staticmethod
    async def _settle(live: ReconnectingStream, work: Any, on_status: Callable[[dict[str, Any]], Any]) -> None:
        """Runs the stream and settles done with its end."""
        try:
            await work
        except asyncio.CancelledError:
            if live._stopped.is_set():
                on_status({"type": "stopped", "error": None})
            elif not live.done.done():
                live.done.cancel()
        except Exception as e:
            if not live.done.done():
                live.done.set_exception(e)
        else:
            if not live.done.done():
                live.done.set_result(None)

    async def _ping(self, call: Any, pong: dict[str, bool]) -> None:
        """A ping every PING_EVERY seconds on the stream, never with more than one pong outstanding."""
        sent = 0
        try:
            while True:
                await asyncio.sleep(PING_EVERY)
                if pong["pending"]:
                    continue  # the server's control queue is small: never pile pings up
                pong["pending"] = True
                sent += 1
                await call.write(self._pb.SubscribeRequest(ping=self._pb.SubscribeRequestPing(id=sent)))
        except Exception:
            return  # the stream's own end carries the error

    async def _follow(self, base: Any, live: ReconnectingStream, on_update: Callable[[Any], Any], on_reseed: Callable[[dict[str, Any]], Any] | None,
                      on_status: Callable[[dict[str, Any]], Any], rnd: Callable[[], float]) -> None:
        can_resume = not base.HasField("commitment") or base.commitment == self._pb.PROCESSED
        last_slot = base.from_slot if base.HasField("from_slot") else None
        request, backoff, attempt = base, 2.0, 0
        while not live._stopped.is_set():
            error, failed, got_data, pinger = None, None, False, None
            call = self._stub.Subscribe(metadata=self._metadata())
            try:
                await call.write(request)
                _handle(on_status, {"type": "connected", "attempt": attempt, "from_slot": request.from_slot if request.HasField("from_slot") else None})
                pong = {"pending": False}
                pinger = asyncio.get_running_loop().create_task(self._ping(call, pong))
                async for update in call:
                    kind = update.WhichOneof("update_oneof")
                    if kind == "pong":
                        pong["pending"] = False
                        continue
                    if kind == "ping":
                        continue
                    got_data = True
                    if kind in _SLOTTED:
                        last_slot = getattr(update, kind).slot
                    _handle(on_update, update)
            except _HandlerFailed as e:
                failed = e.original
            except Exception as e:
                error = e
            finally:
                if pinger is not None:
                    pinger.cancel()
                call.cancel()  # always: a call left open keeps streaming, and billing, on the server
            if failed is not None:
                raise failed
            if live._stopped.is_set():
                break
            info = classify_stream_error(error)
            failure = _failure(info)
            if error is not None:
                failure.__cause__ = Cause(error)
            if info.action == "stop":
                on_status({"type": "stopped", "error": failure})
                raise failure
            if got_data:
                backoff, attempt = 2.0, 0
            attempt += 1
            if info.action == "reseed":  # a gap: reload over RPC, then subscribe again live, at once
                on_status({"type": "reseed", "error": failure, "last_slot": last_slot})
                await _call(on_reseed, {"last_slot": last_slot})
                request = _without_from_slot(base)
                continue
            delay = max(info.retry_after_ms / 1000, backoff) + rnd()
            backoff = _next_backoff(backoff)
            on_status({"type": "retry", "error": failure, "delay": delay, "attempt": attempt})
            await asyncio.sleep(delay)  # close() cancels the task, so the wait ends at once
            request = _without_from_slot(base)
            if can_resume and last_slot is not None:
                try:
                    floor = (await self._stub.SubscribeReplayInfo(self._pb.SubscribeReplayInfoRequest(), metadata=self._metadata())).first_available
                except Exception:
                    floor = None  # the floor read failed: resume live
                if floor is not None and last_slot >= floor:
                    request.from_slot = last_slot
                elif floor is not None:
                    on_status({"type": "reseed", "error": None, "last_slot": last_slot})
                    await _call(on_reseed, {"last_slot": last_slot})

    async def close(self) -> None:
        await self._channel.close()

    async def __aenter__(self) -> "CarbiumStream":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def __repr__(self) -> str:
        return "CarbiumStream()"


def _next_backoff(backoff: float) -> float:
    """Doubles from 2 s and stays at 30 s, however many tries in a row."""
    return min(30.0, backoff * 2)


def _set(**fields: Any) -> dict[str, Any]:
    return {key: value for key, value in fields.items() if value is not None}
