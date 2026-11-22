"""Carbon Stream against an in-process gRPC server: the request rules, the reconnect table, the ping, the unary calls
and the served count. The cases port the TypeScript SDK's stream tests. Skipped without the stream extra."""
from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

grpc = pytest.importorskip("grpc")

import carbium.stream  # noqa: E402
from carbium import CarbiumError  # noqa: E402
from carbium._geyser import geyser_pb2 as pb  # noqa: E402
from carbium._geyser import geyser_pb2_grpc as pb_grpc  # noqa: E402
from carbium.stream import CarbiumStream, _next_backoff, build_request, classify_stream_error  # noqa: E402

ROOT = Path(__file__).parent.parent
KEY = "c_serv_" + "Stream1234Key"  # a made-up key, joined here so no file holds one whole
SERVED = json.loads((ROOT / "spec/served.json").read_text())["stream"]
SLOTS = {"slots": {"s": {}}}


def slot(n: int):
    return pb.SubscribeUpdate(slot=pb.SubscribeUpdateSlot(slot=n))


def script(*updates, then=None):
    """One Subscribe's behaviour: send the updates, then end cleanly, abort with (status, details), or stay open."""
    async def play(fake, requests, context):
        for update in updates:
            yield update
        if then == "hold":
            await asyncio.sleep(30)
        elif then:
            await context.abort(getattr(grpc.StatusCode, then[0]), then[1])
    return play


class Fake(pb_grpc.GeyserServicer):
    def __init__(self, scripts=(), floor=0):
        self.scripts, self.floor = list(scripts), floor
        self.first, self.tokens, self.unary, self.opened = [], [], [], []

    async def Subscribe(self, requests, context):
        self.tokens.append(dict(context.invocation_metadata()).get("x-token"))
        self.opened.append(time.monotonic())
        self.first.append(await anext(requests))
        play = self.scripts.pop(0) if self.scripts else script(then="hold")
        async for update in play(self, requests, context):
            yield update

    def _seen(self, name, request, context):
        self.unary.append((name, request, dict(context.invocation_metadata()).get("x-token")))

    async def GetSlot(self, request, context):
        self._seen("GetSlot", request, context)
        return pb.GetSlotResponse(slot=7)

    async def GetBlockHeight(self, request, context):
        self._seen("GetBlockHeight", request, context)
        return pb.GetBlockHeightResponse(block_height=6)

    async def GetLatestBlockhash(self, request, context):
        self._seen("GetLatestBlockhash", request, context)
        return pb.GetLatestBlockhashResponse(slot=7, blockhash="h", last_valid_block_height=9)

    async def IsBlockhashValid(self, request, context):
        self._seen("IsBlockhashValid", request, context)
        return pb.IsBlockhashValidResponse(slot=7, valid=True)

    async def GetVersion(self, request, context):
        self._seen("GetVersion", request, context)
        return pb.GetVersionResponse(version="v")

    async def Ping(self, request, context):
        self._seen("Ping", request, context)
        return pb.PongResponse(count=request.count)

    async def SubscribeReplayInfo(self, request, context):
        self._seen("SubscribeReplayInfo", request, context)
        return pb.SubscribeReplayInfoResponse(first_available=self.floor)


class Served:
    def __init__(self, fake: Fake, client_type=CarbiumStream):
        self.fake, self.client_type = fake, client_type

    async def __aenter__(self):
        self.server = grpc.aio.server()
        pb_grpc.add_GeyserServicer_to_server(self.fake, self.server)
        port = self.server.add_insecure_port("127.0.0.1:0")
        await self.server.start()
        self.client = await self.client_type.connect(KEY, endpoint=f"http://127.0.0.1:{port}")
        return self.client

    async def __aexit__(self, *exc):
        await self.client.close()
        await self.server.stop(0)


def run(main, seconds: float = 30):
    return asyncio.run(asyncio.wait_for(main(), seconds))


async def until(condition, seconds: float = 10) -> None:
    deadline = time.monotonic() + seconds
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("timed out waiting")
        await asyncio.sleep(0.01)


@pytest.mark.parametrize("filters", [
    {"transactions": {"t": {"account_include": ["x"], "token_accounts": "ALL"}}},
    {"transactions_status": {"t": {"account_include": ["x"], "cuckoo_account_include": {}}}},
    {"accounts": {"a": {"account": ["x"], "cuckoo_accounts_filter": {}}}},
    {"blocks": {"b": {"cuckoo_account_include": {}}}},
    {"entry": {"e": {"include_update_parent": True}}},
    {"block_footer": {"f": {}}},
    {"transactions": {"bare": {}}},
    {"slots": {"s": {}}, "from_slot": 1, "commitment": "CONFIRMED"},
    {"slots": {"s": {}}, "not_a_field": 1},
])
def test_what_the_service_refuses_is_refused_before_anything_is_sent(filters):
    with pytest.raises(CarbiumError) as caught:
        build_request(filters)
    assert caught.value.kind == "request"


def test_the_whole_filter_set_goes_in_one_request_and_from_slot_is_kept_at_processed():
    request = build_request({"slots": {"s": {"filter_by_commitment": True}}, "transactions": {"t": {"vote": False}}, "from_slot": 42, "commitment": "PROCESSED"})
    assert (request.from_slot, request.slots["s"].filter_by_commitment, request.transactions["t"].HasField("vote")) == (42, True, True)


@pytest.mark.parametrize("status, details, action, wait", [
    ("UNAUTHENTICATED", "unknown api key", "stop", 2000),
    ("PERMISSION_DENIED", "streaming is not included in this tier; upgrade the plan", "stop", 2000),
    ("INVALID_ARGUMENT", "from_slot is only supported on the initial subscribe request", "stop", 2000),
    ("UNIMPLEMENTED", "", "stop", 2000),
    ("RESOURCE_EXHAUSTED", "credits exhausted", "stop", 2000),
    ("RESOURCE_EXHAUSTED", "consumer_credits_exhausted: stream egress budget spent; buy a plan or upgrade", "stop", 2000),
    ("RESOURCE_EXHAUSTED", "consumer_lagged: your stream fell behind the ring; reduce filters or read faster", "retry", 2000),
    ("RESOURCE_EXHAUSTED", "credits for this account are still loading on this node; retry after Retry-After", "retry", 1000),
    ("UNAVAILABLE", "credits for this account are still loading on this node; retry after Retry-After", "retry", 1000),
    ("UNAVAILABLE", "server shutting down, reconnect", "retry", 2000),
    ("OUT_OF_RANGE", "gap: re-seed required", "reseed", 2000),
])
def test_the_published_table_decides_stop_reseed_or_retry(status, details, action, wait):
    error = grpc.aio.AioRpcError(getattr(grpc.StatusCode, status), grpc.aio.Metadata(), grpc.aio.Metadata(), details)
    assert classify_stream_error(error)[1:] == (details or str(error), action, wait)
    assert classify_stream_error(None).action == "retry"


def test_updates_reach_on_update_pings_and_pongs_do_not_and_the_key_goes_as_x_token():
    async def main():
        fake = Fake([script(pb.SubscribeUpdate(ping=pb.SubscribeUpdatePing()), slot(10), pb.SubscribeUpdate(pong=pb.SubscribeUpdatePong(id=1)))])
        async with Served(fake) as stream:
            updates, events = [], []
            live = stream.subscribe_with_reconnect({"slots": {"s": {"filter_by_commitment": True}}}, updates.append, on_status=events.append, _random=lambda: 0)
            await until(lambda: any(e["type"] == "retry" for e in events))
            live.close()
            await live.done
        assert [u.slot.slot for u in updates] == [10] and fake.tokens[0] == KEY
        assert fake.first[0].slots["s"].filter_by_commitment and KEY not in str(fake.first[0])
        assert [e["type"] for e in events][:2] == ["connected", "retry"]  # a clean end is a retry

    run(main)


@pytest.mark.parametrize("status, details", [
    ("UNAUTHENTICATED", "unknown api key"), ("PERMISSION_DENIED", "filter not in v1 coverage; request provisioning"),
    ("INVALID_ARGUMENT", "empty subscribe request stream"), ("UNIMPLEMENTED", "method not served"),
    ("RESOURCE_EXHAUSTED", "credits exhausted"), ("RESOURCE_EXHAUSTED", "consumer_credits_exhausted: stream egress budget spent; buy a plan or upgrade"),
])
def test_a_refusal_the_table_says_to_stop_on_ends_the_stream_with_no_second_subscribe(status, details):
    async def main():
        fake = Fake([script(then=(status, details))])
        async with Served(fake) as stream:
            live = stream.subscribe_with_reconnect(SLOTS, lambda u: None, _random=lambda: 0)
            with pytest.raises(CarbiumError) as caught:
                await live.done
            await asyncio.sleep(0.2)
        return caught.value, len(fake.first)

    e, opened = run(main)
    assert (e.kind, e.reason, e.retryable, opened) == ("stream", status, False, 1) and details in e.message


def test_out_of_range_reseeds_with_the_last_slot_and_subscribes_again_live_at_once():
    async def main():
        fake = Fake([script(slot(500), then=("OUT_OF_RANGE", "gap: re-seed required"))])
        async with Served(fake) as stream:
            reseeds = []
            live = stream.subscribe_with_reconnect({**SLOTS, "from_slot": 400}, lambda u: None, on_reseed=reseeds.append, _random=lambda: 0)
            await until(lambda: len(fake.first) == 2)
            live.close()
            await live.done
        assert reseeds == [{"last_slot": 500}] and fake.opened[1] - fake.opened[0] < 1.0
        assert (fake.first[0].from_slot, fake.first[1].HasField("from_slot")) == (400, False)

    run(main)


def test_a_retry_waits_2_s_resumes_from_the_last_slot_above_the_floor_and_doubles_the_next_wait():
    async def main():
        fake = Fake([script(pb.SubscribeUpdate(transaction=pb.SubscribeUpdateTransaction(slot=900)), then=("UNAVAILABLE", "server shutting down, reconnect")),
                     script(then=("UNAVAILABLE", "cfan downstream unavailable"))], floor=850)
        async with Served(fake) as stream:
            live = stream.subscribe_with_reconnect(SLOTS, lambda u: None, _random=lambda: 0)
            await until(lambda: len(fake.first) == 3, 12)
            live.close()
            await live.done
        gaps = [b - a for a, b in zip(fake.opened, fake.opened[1:])]
        assert gaps[0] >= 2.0 and gaps[1] >= 4.0, gaps
        assert fake.first[1].from_slot == 900 and [name for name, _, _ in fake.unary] == ["SubscribeReplayInfo"] * 2  # one floor read per retry

    run(main)


def test_a_resume_below_the_retained_floor_reseeds_and_goes_live():
    async def main():
        fake = Fake([script(slot(100))], floor=200)
        async with Served(fake) as stream:
            reseeds = []
            live = stream.subscribe_with_reconnect(SLOTS, lambda u: None, on_reseed=reseeds.append, _random=lambda: 0)
            await until(lambda: len(fake.first) == 2, 6)
            live.close()
            await live.done
        assert reseeds == [{"last_slot": 100}] and not fake.first[1].HasField("from_slot")

    run(main)


def test_credits_still_loading_waits_at_least_its_retry_after():
    async def main():
        fake = Fake([script(then=("UNAVAILABLE", "credits for this account are still loading on this node; retry after Retry-After"))])
        async with Served(fake) as stream:
            live = stream.subscribe_with_reconnect(SLOTS, lambda u: None, _random=lambda: 0)
            await until(lambda: len(fake.first) == 2, 4)
            live.close()
            await live.done
        assert 1.0 <= fake.opened[1] - fake.opened[0] < 2.8  # the larger of its 1 s and the 2 s backoff, as stream.ts waits

    run(main)


def test_the_backoff_stops_at_30_s_however_many_tries():
    wait = 2.0
    for _ in range(5000):
        wait = _next_backoff(wait)
    assert wait == 30.0


def test_one_ping_at_a_time_on_the_stream_and_the_next_after_its_pong(monkeypatch):
    monkeypatch.setattr(carbium.stream, "PING_EVERY", 0.1)

    async def main():
        later, counts = [], []

        async def pings(fake, requests, context):
            async def read():
                async for request in requests:
                    later.append(request)

            reader = asyncio.ensure_future(read())
            await asyncio.sleep(0.65)  # six ping periods with the pong held back
            counts.append(len(later))
            yield pb.SubscribeUpdate(pong=pb.SubscribeUpdatePong(id=1))
            await asyncio.sleep(0.45)
            counts.append(len(later))
            await asyncio.sleep(30)
            reader.cancel()

        fake = Fake([pings])
        async with Served(fake) as stream:
            live = stream.subscribe_with_reconnect(SLOTS, lambda u: None, _random=lambda: 0)
            await until(lambda: len(counts) == 2, 5)
            live.close()
            await live.done
        return later, counts

    later, counts = run(main)
    assert counts == [1, 2] and [r.ping.id for r in later] == [1, 2] and all(not r.slots for r in later)


def test_close_ends_an_open_stream_and_done_settles():
    async def main():
        fake = Fake([script(slot(1), then="hold")])
        async with Served(fake) as stream:
            seen = []
            live = stream.subscribe_with_reconnect(SLOTS, seen.append, _random=lambda: 0)
            await until(lambda: len(seen) == 1)
            live.close()
            await asyncio.wait_for(live.done, 2)
        return len(fake.first)

    assert run(main) == 1


UNARY = {"get_slot": [], "get_block_height": [], "get_latest_blockhash": [], "is_blockhash_valid": ["h"], "get_version": [], "ping": [1], "subscribe_replay_info": []}
STREAMING = {"subscribe", "subscribe_with_reconnect", "close", "connect"}


async def unary_names(client_type) -> dict[str, str]:
    """Each typed unary call and the gRPC method it reached on the server, with the key it carried."""
    fake = Fake()
    out = {}
    async with Served(fake, client_type) as stream:
        for name, _ in inspect.getmembers(client_type, inspect.isfunction):
            if name.startswith("_") or name in STREAMING:
                continue
            before = len(fake.unary)
            try:
                await getattr(stream, name)(*UNARY.get(name, []))
            except Exception as e:
                out[name] = f"nothing ({getattr(e, 'reason', None) or type(e).__name__})"
                continue
            reached, _, token = fake.unary[before]
            out[name] = reached if token == KEY else f"{reached} without the key"
    return out


def name_problems(names: dict[str, str], served: list[str]) -> list[str]:
    sent = list(names.values())
    out = [f"served but no call reaches it: {n}" for n in sorted(set(served) - set(sent))]
    out += [f"{name} reaches {n}, which is not served" for name, n in names.items() if n not in served]
    return out + [f"{n} is reached by {sent.count(n)} calls" for n in sorted(set(sent)) if sent.count(n) > 1]


def test_every_served_unary_call_has_a_typed_call_and_nothing_else_does():
    deshred = lambda self: self._unary("SubscribeDeshred", pb.SubscribeDeshredRequest())
    assert name_problems(run(lambda: unary_names(type("Less", (CarbiumStream,), {"ping": None}))), SERVED["unary"]) == ["served but no call reaches it: Ping"]
    assert name_problems(run(lambda: unary_names(type("More", (CarbiumStream,), {"subscribe_deshred": deshred}))), SERVED["unary"]) == [
        "subscribe_deshred reaches nothing (AttributeError), which is not served"]  # the generated stub has no such call
    again = lambda self: self._unary("GetVersion", pb.GetVersionRequest())
    assert name_problems(run(lambda: unary_names(type("Crossed", (CarbiumStream,), {"get_slot": again}))), SERVED["unary"]) == [
        "served but no call reaches it: GetSlot", "GetVersion is reached by 2 calls"]
    names = run(lambda: unary_names(CarbiumStream))
    assert name_problems(names, SERVED["unary"]) == [] and len(names) == len(SERVED["unary"]) == 7
    calls = {m.name for m in pb.DESCRIPTOR.services_by_name["Geyser"].methods}
    assert calls == {"Subscribe", *SERVED["unary"]}  # the package's service offers nothing else


def test_every_served_filter_reaches_the_server_and_every_other_map_is_refused():
    maps = {f.name: f.json_name for f in pb.SubscribeRequest.DESCRIPTOR.fields if f.message_type and f.message_type.GetOptions().map_entry}
    probe = {"transactions": {"account_include": ["x"]}, "transactions_status": {"account_include": ["x"]}}

    async def main():
        fake = Fake([script(then="hold") for _ in maps])
        reached, refused = [], []
        async with Served(fake) as stream:
            for name, json_name in maps.items():
                try:
                    call = await stream.subscribe({name: {"probe": probe.get(name, {})}})
                except CarbiumError:
                    refused.append(json_name)
                    continue
                await until(lambda: len(fake.first) == len(reached) + 1)
                if "probe" in getattr(fake.first[-1], name):
                    reached.append(json_name)
                call.cancel()
        return reached, refused

    reached, refused = run(main)
    assert sorted(reached) == sorted(SERVED["filters"]) and refused == ["blockFooter"]


def test_a_key_of_the_wrong_kind_or_in_the_endpoint_is_refused_before_anything_loads():
    for key, endpoint, kind in (("c_acct_" + "Abc123", carbium.stream.DEFAULT_ENDPOINT, "auth"), (KEY + "\n", carbium.stream.DEFAULT_ENDPOINT, "auth"),
                                (KEY, "https://services.carbium.sh:9443/?api-key=" + KEY, "request"), (KEY, "services.carbium.sh:9443", "request")):
        with pytest.raises(CarbiumError) as caught:
            run(lambda: CarbiumStream.connect(key, endpoint=endpoint))
        assert caught.value.kind == kind and KEY not in caught.value.message
    assert KEY not in repr(run(lambda: CarbiumStream.connect(KEY)))


@pytest.mark.skipif(importlib.util.find_spec("grpc_tools") is None, reason="the generator needs grpcio-tools")
def test_the_committed_stream_modules_are_what_the_generator_makes(tmp_path):
    made = subprocess.run([sys.executable, str(ROOT / "scripts/generate_stream.py"), str(tmp_path)], capture_output=True, text=True)
    assert made.returncode == 0, made.stderr
    names = sorted(p.name for p in tmp_path.glob("*_pb2*.py*"))
    assert names == sorted(p.name for p in (ROOT / "src/carbium/_geyser").glob("*_pb2*.py*")) and len(names) == 6
    for name in names:
        assert (tmp_path / name).read_text() == (ROOT / "src/carbium/_geyser" / name).read_text(), name


def test_an_exception_from_on_update_stops_the_stream_cancels_the_call_and_settles_done_with_it():
    async def main():
        ended = []

        async def held(fake, requests, context):
            yield slot(1)
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                ended.append("cancelled")
                raise

        def broken(update):
            raise ValueError("the handler broke")

        fake = Fake([held])
        async with Served(fake) as stream:
            live = stream.subscribe_with_reconnect(SLOTS, broken, _random=lambda: 0)
            with pytest.raises(ValueError, match="the handler broke"):
                await asyncio.wait_for(live.done, 3)
            await until(lambda: ended == ["cancelled"], 3)
            await asyncio.sleep(0.3)
        return len(fake.first)

    assert run(main) == 1


def test_an_exception_from_on_status_at_connected_stops_the_stream_too():
    async def main():
        def status(event):
            if event["type"] == "connected":
                raise RuntimeError("the status handler broke")

        fake = Fake([script(slot(1), then="hold")])
        async with Served(fake) as stream:
            live = stream.subscribe_with_reconnect(SLOTS, lambda u: None, on_status=status, _random=lambda: 0)
            with pytest.raises(RuntimeError, match="the status handler broke"):
                await asyncio.wait_for(live.done, 3)
            await asyncio.sleep(0.3)
        return len(fake.tokens)  # calls opened: one, never a second

    assert run(main) == 1


def test_a_refused_shape_opens_no_call_on_the_server():
    async def main():
        fake = Fake()
        async with Served(fake) as stream:
            with pytest.raises(CarbiumError):
                await stream.subscribe({"transactions": {"bare": {}}})
            with pytest.raises(CarbiumError):
                stream.subscribe_with_reconnect({"block_footer": {"f": {}}}, lambda u: None)
            await asyncio.sleep(0.3)
        return fake.tokens

    assert run(main) == []


def test_a_transaction_status_stream_records_its_last_slot_and_resumes_from_it():
    async def main():
        status_update = pb.SubscribeUpdate(transaction_status=pb.SubscribeUpdateTransactionStatus(slot=900))
        fake = Fake([script(status_update, then=("UNAVAILABLE", "server shutting down, reconnect"))], floor=850)
        async with Served(fake) as stream:
            live = stream.subscribe_with_reconnect({"transactions_status": {"t": {"account_include": ["x"]}}}, lambda u: None, _random=lambda: 0)
            await until(lambda: len(fake.first) == 2, 5)
            live.close()
            await live.done
        return fake.first[1].from_slot

    assert run(main) == 900


def test_close_while_the_first_write_waits_settles_done_promptly():
    async def main():
        held = []  # each connection kept open, never answered: the first write stays pending
        silent = await asyncio.start_server(lambda reader, writer: held.append(writer), "127.0.0.1", 0)
        port = silent.sockets[0].getsockname()[1]
        stream = await CarbiumStream.connect(KEY, endpoint=f"http://127.0.0.1:{port}")
        live = stream.subscribe_with_reconnect(SLOTS, lambda u: None, _random=lambda: 0)
        await asyncio.sleep(0.3)
        live.close()
        await asyncio.wait_for(live.done, 2)
        await stream.close()
        silent.close()

    run(main, 10)


def test_a_coroutine_on_update_is_refused_before_anything_opens():
    async def main():
        async def slow(update):
            await asyncio.sleep(1)

        fake = Fake()
        async with Served(fake) as stream:
            with pytest.raises(CarbiumError) as caught:
                stream.subscribe_with_reconnect(SLOTS, slow)
            await asyncio.sleep(0.2)
        return caught.value, fake.tokens

    e, tokens = run(main)
    assert (e.kind, tokens) == ("request", []) and "coroutine" in e.message


def test_no_key_is_reachable_from_an_open_reconnecting_stream():
    from test_transport import attribute_walk, shown

    async def main():
        fake = Fake([script(slot(1), then="hold")])
        async with Served(fake) as stream:
            seen = []
            live = stream.subscribe_with_reconnect(SLOTS, seen.append, _random=lambda: 0)
            await until(lambda: len(seen) == 1)
            found = [type(item).__name__ for root in (live, stream) for item in attribute_walk(root) if KEY in shown(item) or (isinstance(item, str) and KEY in item)]
            live.close()
            await live.done
        return found

    assert run(main) == []


def test_a_protobuf_version_refusal_is_named_as_it_is_not_as_a_missing_extra():
    child = subprocess.run([sys.executable, "-c", """
import asyncio, google.protobuf.runtime_version as rv
def refuse(*args):
    raise rv.VersionError("Detected incompatible Protobuf Gencode/Runtime versions when loading geyser.proto: gencode 7.35.1 runtime 7.0.0")
rv.ValidateProtobufRuntimeVersion = refuse
from carbium import CarbiumError, CarbiumStream
try:
    asyncio.run(CarbiumStream.connect("c_serv_" + "Version1234"))
except CarbiumError as e:
    print(e.kind, e.message)
"""], capture_output=True, text=True)
    assert child.returncode == 0, child.stderr
    assert "incompatible Protobuf" in child.stdout and "needs the stream extra" not in child.stdout, child.stdout


def test_the_backoff_goes_back_to_2_s_after_a_stream_that_delivered():
    async def main():
        fail = ("UNAVAILABLE", "server shutting down, reconnect")
        fake = Fake([script(slot(1), then=fail), script(then=fail), script(slot(2), then=fail)])
        async with Served(fake) as stream:
            live = stream.subscribe_with_reconnect(SLOTS, lambda u: None, _random=lambda: 0)
            await until(lambda: len(fake.first) == 4, 14)
            live.close()
            await live.done
        return [b - a for a, b in zip(fake.opened, fake.opened[1:])]

    gaps = run(main)
    assert 2.0 <= gaps[0] < 2.9 and gaps[1] >= 4.0 and 2.0 <= gaps[2] < 2.9, gaps


def test_a_confirmed_stream_resumes_live_with_no_from_slot_and_no_floor_read():
    async def main():
        fake = Fake([script(slot(500), then=("UNAVAILABLE", "server shutting down, reconnect"))], floor=0)
        async with Served(fake) as stream:
            live = stream.subscribe_with_reconnect({**SLOTS, "commitment": "CONFIRMED"}, lambda u: None, _random=lambda: 0)
            await until(lambda: len(fake.first) == 2, 5)
            live.close()
            await live.done
        return fake.first[1].HasField("from_slot"), [name for name, _, _ in fake.unary]

    assert run(main) == (False, [])


def test_the_jitter_adds_up_to_one_second_on_top_of_the_wait():
    async def main():
        fake = Fake([script(then=("UNAVAILABLE", "server shutting down, reconnect")) for _ in range(2)])
        async with Served(fake) as stream:
            delays = []
            for jitter in (0.0, 0.999):
                events = []
                live = stream.subscribe_with_reconnect(SLOTS, lambda u: None, on_status=events.append, _random=lambda: jitter)
                await until(lambda: any(e["type"] == "retry" for e in events), 3)
                delays.append(next(e["delay"] for e in events if e["type"] == "retry"))
                live.close()
                await live.done
        return delays

    assert run(main) == [2.0, 2.999]
