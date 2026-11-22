"""A planted HTTP server: each test scripts the answers it gives, then reads what was sent and when."""
from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest


class Planted:
    def __init__(self) -> None:
        self.script: list[dict] = []  # each: status, headers, body (a value sent as JSON, or raw text), delay
        self.requests: list[dict] = []
        handler = self._handler()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def answer(self, status: int = 200, body=None, headers: dict | None = None, raw: str | None = None, delay: float = 0, times: int = 1,
               content_type: str = "application/json", stall: float = 0) -> "Planted":
        """stall: send the body with no length, then hold the connection open that long (a stream gone quiet)."""
        self.script += [{"status": status, "body": body, "headers": headers or {}, "raw": raw, "delay": delay, "type": content_type, "stall": stall}] * times
        return self

    @property
    def routes(self) -> list[str]:
        """Each request as "VERB path", the path with its query."""
        return [f"{r['method']} {r['path']}" for r in self.requests]

    @property
    def gaps_ms(self) -> list[float]:
        at = [r["at"] for r in self.requests]
        return [(b - a) * 1000 for a, b in zip(at, at[1:])]

    def _handler(self):
        planted = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                text = self.rfile.read(int(self.headers.get("content-length", 0))).decode()
                n = len(planted.requests) + 1
                try:
                    sent = json.loads(text) if text else None
                except ValueError:
                    sent = None
                planted.requests.append({"at": time.monotonic(), "method": self.command, "path": self.path, "headers": {k.lower(): v for k, v in self.headers.items()}, "text": text, "json": sent})
                step = planted.script[min(n, len(planted.script)) - 1] if planted.script else {"status": 200, "body": {}, "headers": {}, "raw": None, "delay": 0, "type": "application/json", "stall": 0}
                if step["delay"]:
                    time.sleep(step["delay"])
                payload = step["raw"] if step["raw"] is not None else json.dumps(step["body"])
                try:
                    self.send_response(step["status"])
                    self.send_header("content-type", step["type"])
                    self.send_header("x-request-id", f"rid-{n}")
                    for key, value in step["headers"].items():
                        self.send_header(key, value)
                    if not step["stall"]:
                        self.send_header("content-length", str(len(payload.encode())))
                    self.end_headers()
                    self.wfile.write(payload.encode())
                    if step["stall"]:
                        self.wfile.flush()
                        time.sleep(step["stall"])
                except (BrokenPipeError, ConnectionResetError):
                    pass  # the client gave up waiting, as the timeout test intends

            do_GET = do_PATCH = do_DELETE = do_POST

            def log_message(self, *args) -> None:
                pass

        return Handler


@pytest.fixture
def planted():
    server = Planted()
    yield server
    server.server.shutdown()
    server.server.server_close()


def ok(result=1, id=1) -> dict:
    return {"jsonrpc": "2.0", "id": id, "result": result}


def err(code: int, message: str, reason: str | None = None, id=1, **data) -> dict:
    if reason:
        data["reason"] = reason
    return {"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message, **({"data": data} if data else {})}}


class FakeSocketServer:
    """A planted WebSocket server: acknowledges each subscribe with a rising id (from 100), records frames and
    handshakes, and can refuse handshakes, close every socket with a code, or send a notification."""

    def __init__(self, refuse: tuple[int, dict] | None = None, refuse_first: int | None = None, ack_delay: float = 0, refuse_numbers: set[int] | None = None,
                 with_ack: tuple[str, object] | None = None, with_ack_on: set[int] | None = None, refuse_binds: int = 0) -> None:
        """with_ack: a (method, result) notification written in the same write as each subscribe's acknowledgement, on
        the connections numbered in with_ack_on (all when None)."""
        self.refuse, self.refuse_first, self.ack_delay, self.refuse_numbers = refuse, refuse_first, ack_delay, refuse_numbers
        self.with_ack, self.with_ack_on = with_ack, with_ack_on
        self.refuse_binds, self.refused = refuse_binds, 0  # the first refuse_binds subscribes get -32005 with an 800 ms hint
        self.subscribes: list[tuple[int, str]] = []  # each subscribe frame with the number of the connection it came on
        self.received: list[dict] = []
        self.handshakes: list = []
        self.connections = 0
        self.sockets: set = set()
        self.next_id = 100

    async def __aenter__(self) -> "FakeSocketServer":
        from websockets.asyncio.server import serve

        self.server = await serve(self._handler, "127.0.0.1", 0, process_request=self._process)
        self.url = f"ws://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"
        return self

    async def __aexit__(self, *exc) -> None:
        self.server.close()
        await self.server.wait_closed()

    def _process(self, connection, request):
        self.handshakes.append(request)
        number = len(self.handshakes)
        if self.refuse and (self.refuse_first is None or number <= self.refuse_first) and (self.refuse_numbers is None or number in self.refuse_numbers):
            response = connection.respond(self.refuse[0], "refused\n")
            for key, value in self.refuse[1].items():
                response.headers[key] = value
            return response
        return None

    async def _handler(self, ws) -> None:
        import asyncio

        from websockets.exceptions import ConnectionClosed

        self.connections += 1
        number = self.connections
        self.sockets.add(ws)

        async def ack_and_notify(ack: dict, delay: float) -> None:
            await asyncio.sleep(delay)
            method, result = self.with_ack
            for message in (ack, {"jsonrpc": "2.0", "method": method, "params": {"subscription": ack["result"], "result": result}}):
                ws.protocol.send_text(json.dumps(message).encode())
            ws.send_data()  # both frames go to the socket in one go, before the loop turns

        async def reply(message: dict, delay: float) -> None:
            await asyncio.sleep(delay)
            try:
                await ws.send(json.dumps(message))
            except ConnectionClosed:
                pass

        try:
            async for raw in ws:
                msg = json.loads(raw)
                self.received.append({"method": msg["method"], **({"params": msg["params"]} if "params" in msg else {})})
                if msg["method"].endswith("Subscribe") and self.refused < self.refuse_binds:
                    self.subscribes.append((number, msg["method"]))
                    self.refused += 1
                    await reply({"jsonrpc": "2.0", "id": msg["id"], "error": {"code": -32005, "message": "rate limit exceeded", "data": {"reason": "RATE_LIMITED", "retryAfterMs": 800}}}, 0)
                elif msg["method"].endswith("Subscribe"):
                    self.subscribes.append((number, msg["method"]))
                    self.next_id += 1
                    ack = {"jsonrpc": "2.0", "id": msg["id"], "result": self.next_id - 1}
                    together = self.with_ack and (self.with_ack_on is None or number in self.with_ack_on)
                    asyncio.get_running_loop().create_task(ack_and_notify(ack, self.ack_delay) if together else reply(ack, self.ack_delay))
                else:
                    await reply({"jsonrpc": "2.0", "id": msg["id"], "result": True}, 0)
        except ConnectionClosed:
            pass
        finally:
            self.sockets.discard(ws)

    async def close_all(self, code: int, reason: str) -> None:
        for ws in list(self.sockets):
            await ws.close(code, reason)

    async def notify(self, server_id: int, method: str, result) -> None:
        for ws in list(self.sockets):
            await ws.send(json.dumps({"jsonrpc": "2.0", "method": method, "params": {"subscription": server_id, "result": result}}))


async def until(condition, seconds: float = 8.0) -> None:
    import asyncio

    deadline = time.monotonic() + seconds
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("timed out waiting")
        await asyncio.sleep(0.01)
