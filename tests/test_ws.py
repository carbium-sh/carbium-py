"""The WebSocket client against a planted server: the key's carrier, routing by server id, and the close-code rules.
The cases port the TypeScript SDK's ws tests."""
from __future__ import annotations

import asyncio
import json
import logging
import time

import pytest

from carbium import CarbiumError, CarbiumSocket

from conftest import FakeSocketServer, until

KEY = "c_serv_" + "Ws1234Key"  # a made-up key, joined here so no file holds one whole
PUBKEY = "11111111111111111111111111111111"


def run(main):
    return asyncio.run(asyncio.wait_for(main(), 30))


def test_the_key_goes_in_a_header_and_notifications_are_routed_by_server_id():
    async def main():
        async with FakeSocketServer() as f:
            seen = []
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0)
            sub = socket.account_subscribe(PUBKEY, seen.append, {"commitment": "confirmed"})
            assert await sub.ready == 100
            handshake = f.handshakes[0]
            assert (handshake.path, handshake.headers["x-api-key"], handshake.headers.get("sec-websocket-protocol")) == ("/", KEY, None)
            assert f.received[0] == {"method": "accountSubscribe", "params": [PUBKEY, {"commitment": "confirmed"}]}
            await f.notify(100, "accountNotification", {"context": {"slot": 1}, "value": {"lamports": 5}})
            await f.notify(999, "accountNotification", {"stray": True})
            await until(lambda: len(seen) == 1)
            assert seen == [{"context": {"slot": 1}, "value": {"lamports": 5}}]
            await sub.unsubscribe()
            assert f.received[-1] == {"method": "accountUnsubscribe", "params": [100]}
            await until(lambda: socket.state == "idle")
            await socket.close()
            assert KEY not in repr(socket)

    run(main)


def test_close_1013_credits_pending_binds_again_after_a_second_and_tells_the_caller_to_reseed():
    async def main():
        async with FakeSocketServer() as f:
            reseeds = []
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0, on_reseed=reseeds.append)
            await socket.logs_subscribe({"mentions": [PUBKEY]}, lambda n: None).ready
            start = time.monotonic()
            await f.close_all(1013, "credits_pending")
            await until(lambda: f.connections == 2 and len(f.received) == 2)
            assert time.monotonic() - start >= 1.0
            assert f.received[1] == f.received[0]
            await until(lambda: len(reseeds) == 1)
            assert reseeds == [{"code": 1013, "reason": "credits_pending", "gap": False}]
            await socket.close()

    run(main)


def test_close_1012_stream_gap_reconnects_at_once_and_reseeds_with_gap():
    async def main():
        async with FakeSocketServer() as f:
            reseeds = []
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0, on_reseed=reseeds.append)
            await socket.slot_subscribe(lambda n: None).ready
            start = time.monotonic()
            await f.close_all(1012, "stream_gap_reseed_required")
            await until(lambda: len(reseeds) == 1)
            assert time.monotonic() - start < 1.2 and reseeds[0]["gap"] is True and f.connections == 2
            await socket.close()

    run(main)


def test_close_1008_key_revoked_is_fatal_with_no_reconnect():
    async def main():
        async with FakeSocketServer() as f:
            fatal = []
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0, on_fatal=fatal.append)
            await socket.root_subscribe(lambda n: None).ready
            await f.close_all(1008, "key_revoked")
            await until(lambda: len(fatal) == 1)
            await asyncio.sleep(0.3)
            assert (f.connections, fatal[0].kind, fatal[0].reason, socket.state) == (1, "auth", "key_revoked", "closed")
            with pytest.raises(CarbiumError) as caught:
                socket.slot_subscribe(lambda n: None)
            assert caught.value is fatal[0]

    run(main)


def test_close_1008_credits_exhausted_reconnects_once_after_5_s_and_the_second_is_fatal():
    async def main():
        async with FakeSocketServer() as f:
            fatal = []
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0, on_fatal=fatal.append)
            await socket.slots_updates_subscribe(lambda n: None).ready
            start = time.monotonic()
            await f.close_all(1008, "credits_exhausted")
            await until(lambda: f.connections == 2, 9)
            assert time.monotonic() - start >= 5.0
            await until(lambda: len(f.received) == 2)
            await f.close_all(1008, "credits_exhausted")
            await until(lambda: len(fatal) == 1)
            assert (fatal[0].kind, f.connections) == ("credits", 2)

    run(main)


def test_a_refused_handshake_is_fatal_at_once_with_its_status():
    async def main():
        async with FakeSocketServer(refuse=(401, {})) as f:
            fatal = []
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0, on_fatal=fatal.append)
            with pytest.raises(CarbiumError) as caught:
                await socket.slot_subscribe(lambda n: None).ready
            assert (caught.value.kind, caught.value.http_status, len(f.handshakes), len(fatal)) == ("auth", 401, 1, 1)

    run(main)


def test_a_429_handshake_waits_for_retry_after_and_is_never_taken_for_a_key_refusal():
    async def main():
        async with FakeSocketServer(refuse=(429, {"Retry-After": "1"}), refuse_first=3) as f:
            fatal = []
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0, on_fatal=fatal.append)
            start = time.monotonic()
            await socket.slot_subscribe(lambda n: None).ready
            assert (len(f.handshakes), fatal) == (4, []) and time.monotonic() - start >= 3.0
            await socket.close()

    run(main)


def test_network_failures_keep_backing_off_and_are_never_blamed_on_the_key():
    async def main():
        fatal, attempts = [], []
        socket = CarbiumSocket(KEY, url="ws://127.0.0.1:9", _random=lambda: 0, on_fatal=fatal.append,
                               on_state=lambda state, info: attempts.append(state) if state == "connecting" else None)
        sub = socket.slot_subscribe(lambda n: None)
        await until(lambda: len(attempts) >= 6, 12)
        assert (fatal, sub.ready.done()) == ([], False)
        await socket.close()

    run(main)


def test_a_subscribe_made_as_the_socket_opens_is_bound_once():
    async def main():
        async with FakeSocketServer() as f:
            added = []

            def on_state(state, info):
                if state == "open" and not added:
                    added.append(socket.root_subscribe(lambda n: None))

            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0, on_state=on_state)
            await socket.slot_subscribe(lambda n: None).ready
            await added[0].ready
            await asyncio.sleep(0.1)
            assert sorted(r["method"] for r in f.received) == ["rootSubscribe", "slotSubscribe"]  # each bound once
            await socket.close()

    run(main)


def test_the_backoff_stays_bounded_after_a_great_many_closes():
    from carbium.ws import _Handle

    async def main():
        socket = CarbiumSocket(KEY, _random=lambda: 1)
        socket._handles[1] = _Handle("slotSubscribe", [], lambda n: None, asyncio.get_running_loop().create_future())
        socket._attempt = socket._overload_closes = 5000
        assert socket._after_close((1006, ""), None) <= 30
        assert socket._after_close((1013, "slow_consumer"), None) <= 30

    run(main)


def test_the_gap_flag_survives_a_failed_handshake_before_the_next_open():
    async def main():
        async with FakeSocketServer(refuse=(503, {}), refuse_numbers={2}) as f:
            reseeds = []
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0, on_reseed=reseeds.append)
            await socket.slot_subscribe(lambda n: None).ready
            await f.close_all(1012, "stream_gap_reseed_required")
            await until(lambda: len(reseeds) == 1)
            assert (len(f.handshakes), reseeds[0]["gap"]) == (3, True)
            await socket.close()

    run(main)


def test_signature_fires_once_and_is_dropped_and_block_asks_for_version_one():
    async def main():
        async with FakeSocketServer() as f:
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0)
            seen = []
            sub = socket.signature_subscribe("5" * 64, seen.append)
            server_id = await sub.ready
            assert socket.size == 1
            await f.notify(server_id, "signatureNotification", {"context": {"slot": 2}, "value": {"err": None}})
            await until(lambda: len(seen) == 1)
            assert socket.size == 0
            await sub.unsubscribe()
            assert [r for r in f.received if r["method"] == "signatureUnsubscribe"] == []
            await socket.block_subscribe("all", lambda n: None, {"commitment": "confirmed"}).ready
            assert f.received[-1]["params"] == ["all", {"maxSupportedTransactionVersion": 1, "commitment": "confirmed"}]
            await socket.close()

    run(main)


def test_unsubscribing_before_the_ack_frees_the_servers_slot_once_the_ack_arrives():
    async def main():
        async with FakeSocketServer(ack_delay=0.15) as f:
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0)
            keep = socket.slot_subscribe(lambda n: None)
            dropped = socket.account_subscribe(PUBKEY, lambda n: None)
            await until(lambda: any(r["method"] == "accountSubscribe" for r in f.received))  # sent; its acknowledgement is 0.15 s away
            await dropped.unsubscribe()
            with pytest.raises(CarbiumError):
                await dropped.ready
            await keep.ready
            await until(lambda: any(r["method"] == "accountUnsubscribe" for r in f.received))
            assert [r["params"] for r in f.received if r["method"] == "accountUnsubscribe"] == [[101]] and socket.size == 1
            assert f.connections == 1  # two subscribes in a row share one connection
            await socket.close()

    run(main)


def test_1013_slow_consumer_waits_at_least_a_second_and_backs_off_on_each_close_in_a_row():
    async def main():
        async with FakeSocketServer() as f:
            opens = []
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0, on_state=lambda state, info: opens.append(time.monotonic()) if state == "open" else None)
            await socket.slot_subscribe(lambda n: None).ready
            for n in (2, 3, 4):
                await until(lambda: f.connections == n - 1 and len(f.received) == n - 1)
                await f.close_all(1013, "slow_consumer")
                await until(lambda: len(opens) == n, 12)
            gaps = [b - a for a, b in zip(opens, opens[1:])]
            assert all(g >= 1.0 for g in gaps) and gaps[2] >= 2.0, gaps
            await socket.close()

    run(main)


def test_a_key_in_the_url_an_account_key_or_an_unserved_name_is_refused_first():
    for make in (lambda: CarbiumSocket("c_acct_" + "Abc123"), lambda: CarbiumSocket("not a key")):
        assert pytest.raises(CarbiumError, make).value.kind == "auth"
    assert pytest.raises(CarbiumError, lambda: CarbiumSocket(url="wss://services.carbium.sh/?api-key=" + KEY)).value.kind == "request"

    async def main():
        with pytest.raises(CarbiumError) as caught:
            CarbiumSocket(KEY).subscribe("voteSubscribe", [], lambda n: None)
        assert (caught.value.kind, caught.value.method) == ("request", "voteSubscribe")

    run(main)


def test_the_key_never_reaches_a_log_line_even_at_debug(caplog):
    async def main():
        async with FakeSocketServer() as f:
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0)
            await socket.slot_subscribe(lambda n: None).ready
            await socket.close()

    with caplog.at_level(logging.DEBUG):
        run(main)
    lines = [record.getMessage() for record in caplog.records if record.name != "websockets.server"]  # the planted server logs what it received
    assert any("x-api-key: c_serv_…" in line for line in lines), "the handshake header line is logged, cut"
    assert not any(KEY in line for line in lines)


def test_a_notification_handled_right_after_its_acknowledgement_reaches_its_handle():
    from carbium.ws import _Handle

    class Sent:  # the connection the bind writes to
        def __init__(self):
            self.frames = []

        async def send(self, text):
            self.frames.append(json.loads(text))

    async def main():
        socket = CarbiumSocket(KEY, _random=lambda: 0)
        socket._ws, socket.state = Sent(), "open"
        seen = []
        handle = socket._handles[1] = _Handle("signatureSubscribe", ["5" * 64], seen.append, asyncio.get_running_loop().create_future())
        bind = asyncio.ensure_future(socket._bind(handle))
        await until(lambda: socket._ws.frames)
        socket._on_message(json.dumps({"jsonrpc": "2.0", "id": socket._ws.frames[0]["id"], "result": 100}))
        socket._on_message(json.dumps({"jsonrpc": "2.0", "method": "signatureNotification", "params": {"subscription": 100, "result": {"value": {"err": None}}}}))
        await bind  # the two frames above were handled back to back, with no loop turn between them
        await asyncio.sleep(0.01)
        return seen, handle.ready.result(), socket.size, [frame["method"] for frame in socket._ws.frames]

    seen, ready, size, sent = run(main)
    assert (seen, ready, size) == ([{"value": {"err": None}}], 100, 0)
    assert sent == ["signatureSubscribe"]  # the server ends a signature subscription itself: nothing to unsubscribe


def test_a_notification_written_with_its_acknowledgement_is_delivered():
    async def main():
        async with FakeSocketServer(with_ack=("signatureNotification", {"context": {"slot": 9}, "value": {"err": None}})) as f:
            seen = []
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0)
            assert await socket.signature_subscribe("5" * 64, seen.append).ready == 100
            await until(lambda: seen, 3)
            await asyncio.sleep(0.1)
            assert (seen, socket.size) == ([{"context": {"slot": 9}, "value": {"err": None}}], 0)
            assert [r["method"] for r in f.received] == ["signatureSubscribe"]
            await socket.close()

    run(main)


def test_a_notification_written_with_a_rebinds_acknowledgement_is_delivered():
    async def main():
        async with FakeSocketServer(with_ack=("accountNotification", {"context": {"slot": 9}, "value": {"lamports": 7}}), with_ack_on={2}) as f:
            seen = []
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0)
            await socket.account_subscribe(PUBKEY, seen.append).ready
            assert seen == []
            await f.close_all(1012, "stream_gap_reseed_required")
            await until(lambda: seen, 4)
            assert (seen, f.connections, f.received[-1]["method"]) == ([{"context": {"slot": 9}, "value": {"lamports": 7}}], 2, "accountSubscribe")
            await socket.close()

    run(main)


def test_unsubscribed_before_an_acknowledgement_that_carries_a_notification_frees_the_slot_and_delivers_nothing():
    async def main():
        async with FakeSocketServer(ack_delay=0.15, with_ack=("accountNotification", {"value": {"lamports": 7}})) as f:
            seen = []
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0)
            keep = socket.slot_subscribe(lambda n: None)
            dropped = socket.account_subscribe(PUBKEY, seen.append)
            await until(lambda: any(r["method"] == "accountSubscribe" for r in f.received))  # sent; its acknowledgement is 0.15 s away
            await dropped.unsubscribe()
            await keep.ready
            await until(lambda: any(r["method"] == "accountUnsubscribe" for r in f.received))
            await asyncio.sleep(0.1)
            assert seen == [] and [r["params"] for r in f.received if r["method"] == "accountUnsubscribe"] == [[101]] and socket.size == 1
            await socket.close()

    run(main)


def test_a_caller_who_leaves_after_the_acknowledgement_was_recorded_sends_one_unsubscribe():
    from carbium.ws import _Handle

    class Sent:
        def __init__(self):
            self.frames = []

        async def send(self, text):
            self.frames.append(json.loads(text))

    async def main():
        socket = CarbiumSocket(KEY, _random=lambda: 0)
        socket._ws, socket.state = Sent(), "open"
        handle = socket._handles[1] = _Handle("accountSubscribe", [PUBKEY], lambda n: None, asyncio.get_running_loop().create_future())
        socket._handles[2] = _Handle("slotSubscribe", [], lambda n: None, asyncio.get_running_loop().create_future())  # keeps the socket open
        bind = asyncio.ensure_future(socket._bind(handle))
        await until(lambda: socket._ws.frames)
        leave = asyncio.ensure_future(socket._unsubscribe(1))  # runs before the bind resumes
        socket._on_message(json.dumps({"jsonrpc": "2.0", "id": socket._ws.frames[0]["id"], "result": 100}))
        await until(lambda: len(socket._ws.frames) >= 2)
        socket._on_message(json.dumps({"jsonrpc": "2.0", "id": socket._ws.frames[1]["id"], "result": True}))
        await leave
        await bind
        await asyncio.sleep(0.05)
        return [(frame["method"], frame.get("params")) for frame in socket._ws.frames]

    assert run(main) == [("accountSubscribe", [PUBKEY]), ("accountUnsubscribe", [100])]


def test_a_bind_waiting_out_a_rate_limit_across_a_reconnect_is_bound_on_the_new_connection_once():
    async def main():
        async with FakeSocketServer(refuse_binds=1) as f:
            socket = CarbiumSocket(KEY, url=f.url, _random=lambda: 0)
            sub = socket.slot_subscribe(lambda n: None)
            await until(lambda: f.refused == 1)  # refused with -32005 and an 800 ms hint: the bind waits it out
            await asyncio.sleep(0.1)
            await f.close_all(1012, "service restart")  # the connection drops and comes back during that wait
            server_id = await asyncio.wait_for(sub.ready, 4)
            await asyncio.sleep(0.3)
            assert (server_id, f.subscribes, f.connections) == (100, [(1, "slotSubscribe"), (2, "slotSubscribe")], 2)
            await socket.close()

    run(main)
