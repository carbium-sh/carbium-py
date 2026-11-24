"""The request and retry rules, each against a planted server. The cases mirror the TypeScript SDK's tests."""
from __future__ import annotations

import asyncio
import copy
import email.utils
import json
import pickle
import sys
import threading
import time
import traceback

import pytest

from carbium import AsyncCarbium, Carbium, CarbiumAccount, CarbiumDca, CarbiumError, CarbiumSocket, CarbiumTokens

from conftest import err, ok

KEY = "c_serv_" + "Test1234Key"  # a made-up key, joined here so no file holds one whole


def client(planted, **options) -> Carbium:
    return Carbium(KEY, endpoint=planted.url, _jitter=lambda: 0, **options)


def failure(call) -> CarbiumError:
    with pytest.raises(CarbiumError) as caught:
        call()
    return caught.value


def test_a_429_waits_at_least_retry_after_then_sends_once_more(planted):
    planted.answer(429, err(-32005, "rate limit exceeded", "RATE_LIMITED"), {"Retry-After": "2"}).answer(200, ok(7))
    assert client(planted).get_slot() == 7
    assert len(planted.requests) == 2
    assert 2000 <= planted.gaps_ms[0] < 2600


def test_retry_after_ms_wins_over_retry_after(planted):
    planted.answer(429, err(-32005, "rate limit exceeded", "RATE_LIMITED"), {"Retry-After": "2", "retry-after-ms": "1200"}).answer(200, ok())
    client(planted).get_slot()
    assert 1200 <= planted.gaps_ms[0] < 1800


def test_credits_exhausted_is_never_sent_again(planted):
    planted.answer(402, err(-32002, "credits exhausted", "CREDITS_EXHAUSTED", retryable=False), {"Retry-After": "1"})
    e = failure(client(planted).get_slot)
    assert len(planted.requests) == 1
    assert (e.kind, e.retryable, e.retry_after_ms, e.request_id) == ("credits", False, 1000, "rid-1")
    assert e.message.endswith("credits exhausted (x-request-id: rid-1)")


def test_a_node_error_with_the_same_code_is_not_a_credits_error(planted):
    planted.answer(200, err(-32002, "Transaction simulation failed"))
    e = failure(lambda: client(planted).rpc("simulateTransaction", ["AA=="]))
    assert (len(planted.requests), e.kind, e.reason) == (1, "node", None)


def test_credits_pending_waits_the_hint(planted):
    planted.answer(429, err(-32005, "credits pending", "CREDITS_PENDING"), {"retry-after-ms": "300"}).answer(200, ok())
    client(planted).get_slot()
    assert 300 <= planted.gaps_ms[0] < 900


def test_a_503_is_tried_four_times(planted):
    planted.answer(503, err(-32005, "load shed", "LOAD_SHED"), times=4)
    e = failure(client(planted).get_slot)
    assert (len(planted.requests), e.kind, e.retryable) == (4, "upstream", True)


def test_send_transaction_is_never_sent_twice(planted):
    planted.answer(503, err(-32011, "upstream down"))
    e = failure(lambda: client(planted).send_transaction("AA=="))
    assert (len(planted.requests), e.kind, e.method) == (1, "upstream", "sendTransaction")


def test_retry_off_sends_once_and_reports_the_hint(planted):
    planted.answer(429, err(-32005, "rate limit exceeded", "RATE_LIMITED"), {"retry-after-ms": "50"})
    e = failure(client(planted, retry=False).get_slot)
    assert (len(planted.requests), e.kind, e.retry_after_ms) == (1, "rate_limit", 50)


def test_a_hint_longer_than_the_cap_is_handed_to_the_caller(planted):
    planted.answer(503, err(-32011, "upstream down"), {"Retry-After": "60"})
    e = failure(client(planted).get_slot)
    assert (len(planted.requests), e.retry_after_ms, e.retryable) == (1, 60000, True)


def test_a_hint_longer_than_the_cap_on_a_200_answer_comes_back_at_once(planted):
    planted.answer(200, err(-32004, "Block not available for slot 5", retryAfterMs=5000)).answer(200, ok())
    started = time.monotonic()
    e = failure(lambda: client(planted, max_hint_ms=1000).get_block(5))
    assert time.monotonic() - started < 1 and (len(planted.requests), e.kind, e.code, e.retry_after_ms, e.retryable) == (1, "node", -32004, 5000, True)


def test_the_request_carries_the_key_in_a_header_and_nothing_else(planted):
    planted.answer(200, ok())
    client(planted).get_slot()
    sent = planted.requests[0]
    assert sent["path"] == "/" and sent["headers"]["x-api-key"] == KEY and "authorization" not in sent["headers"]
    assert sent["headers"]["solana-client"].startswith("python/carbium@")
    assert list(sent["json"]) == ["jsonrpc", "id", "method", "params"] and sent["json"]["params"] == []
    assert KEY not in sent["path"]


def test_version_one_transactions_are_asked_for_unless_the_caller_chose(planted):
    planted.answer(200, ok(None), times=3)
    c = client(planted)
    c.get_transaction("sig")
    c.get_block(5, {"transactionDetails": "none"})
    c.get_transaction("sig", {"maxSupportedTransactionVersion": 0})
    sent = [r["json"]["params"] for r in planted.requests]
    assert sent[0] == ["sig", {"maxSupportedTransactionVersion": 1}]
    assert sent[1] == [5, {"transactionDetails": "none", "maxSupportedTransactionVersion": 1}]
    assert sent[2] == ["sig", {"maxSupportedTransactionVersion": 0}]


def test_a_busy_index_is_tried_six_times_and_the_caller_can_only_lower_that(planted):
    planted.answer(429, err(-32010, "index busy", "CHOT_INDEX_UNAVAILABLE"), times=8)
    failure(client(planted).get_slot)
    assert len(planted.requests) == 6
    planted.requests.clear()
    failure(client(planted, max_attempts=2).get_slot)
    assert len(planted.requests) == 2


def test_a_node_behind_the_asked_slot_waits_for_it_to_catch_up(planted):
    planted.answer(200, err(-32016, "Minimum context slot has not been reached", contextSlot=100)).answer(200, ok())
    client(planted).get_balance("So11111111111111111111111111111111111111112", {"minContextSlot": 105})
    assert 1100 <= planted.gaps_ms[0] < 1700


def test_a_timeout_is_tried_twice_and_keeps_its_cause(planted):
    planted.answer(200, ok(), delay=1.5, times=2)
    e = failure(client(planted, timeout=0.15).get_slot)
    assert (len(planted.requests), e.kind, e.http_status) == (2, "transport", 0)
    assert e.__cause__ is not None and "timeout" in e.__cause__.type_name.lower()


def test_an_answer_that_is_not_json_rpc_is_an_upstream_error(planted):
    planted.answer(200, raw="null").answer(200, raw="<html>bad gateway</html>")
    first = failure(client(planted).get_slot)
    second = failure(client(planted).get_slot)
    assert (first.kind, first.request_id) == ("upstream", "rid-1")
    assert second.kind == "upstream" and "bad gateway" in second.message
    assert len(planted.requests) == 2


def test_a_network_error_keeps_its_cause():
    e = failure(Carbium(KEY, endpoint="http://127.0.0.1:9", max_attempts=1, _jitter=lambda: 0).get_slot)
    assert (e.kind, e.http_status, e.retryable) == ("transport", 0, True) and e.__cause__ is not None


def test_a_batch_the_service_would_refuse_is_refused_before_it_is_sent(planted):
    c = client(planted)
    many_keys = ["So11111111111111111111111111111111111111112"] * 2000
    for calls, expect in [
        ([], "1 to 100"),
        ([("getSlot", [])] * 101, "1 to 100"),
        ([("getProgramAccounts", ["x"])], "cannot be sent in a batch"),
        ([("quote", {})], "cannot be sent in a batch"),
        ([("getAsset", {"id": "x"}), ("getSlot", [])], "only with other DAS"),
        ([("getAccountInfo", ["a"]), ("getAccountInfo", ["b"]), ("getAccountInfo", ["c"])], "at most 2"),
        ([("getMultipleAccounts", [many_keys])], "51200"),
        ([("notAMethod", [])], "not a method"),
    ]:
        e = failure(lambda: c.batch(calls))
        assert e.kind == "request" and expect in e.message, (calls[:1], e.message)
    assert planted.requests == []
    assert failure(lambda: c.batch([("notAMethod", [])])).code == -32601


def test_a_batch_comes_back_in_the_order_it_was_sent(planted):
    planted.answer(200, [err(-32602, "invalid params", id=2), ok("second", id=3), ok("first", id=1)])
    out = client(planted).batch([("getSlot", []), ("getBalance", [1]), ("getBlockHeight", [])])
    assert [entry["ok"] for entry in out] == [True, False, True]
    assert (out[0]["result"], out[2]["result"]) == ("first", "second")
    assert (out[1]["error"].kind, out[1]["error"].method) == ("request", "getBalance")


def test_a_batch_refused_as_a_whole_raises_once(planted):
    planted.answer(400, [err(-32600, "batch too large", "BATCH_TOO_LARGE", id=None)])
    planted.answer(200, [err(-32600, "scan not batchable", "SCAN_NOT_BATCHABLE", id=1), err(-32600, "scan not batchable", "SCAN_NOT_BATCHABLE", id=2)])
    c = client(planted)
    assert failure(lambda: c.batch([("getSlot", [])])).kind == "request"
    assert failure(lambda: c.batch([("getSlot", []), ("getBlockHeight", [])])).reason == "SCAN_NOT_BATCHABLE"


def test_no_key_or_secret_survives_in_anything_an_error_shows(planted):
    leaked = KEY + "Leaked"
    account, secret = "c_acct_" + "Abc123Def456", "whsec_" + "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
    text = f"bad key {leaked} and {account} and {secret} at https://x.example/?api-key={leaked}"
    planted.answer(401, err(-32001, text, "UNKNOWN_KEY", detail={"echo": [text]}))
    e = failure(client(planted).get_slot)
    shown = " ".join([str(e), repr(e), e.message, json.dumps(e.data), repr(e.args)])
    for value in (leaked, account, secret):
        assert value not in shown
    assert "c_serv_…" in shown and "api-key=…" in shown
    assert KEY not in repr(client(planted)) and KEY not in str(client(planted))


def test_a_key_of_the_wrong_kind_or_in_the_address_is_refused_at_once():
    assert failure(lambda: Carbium("c_acct_" + "Abc123")).kind == "auth"
    assert failure(lambda: Carbium("not a key")).kind == "auth"
    assert failure(lambda: Carbium(KEY, endpoint="https://services.carbium.sh/?api-key=" + KEY)).kind == "request"
    assert failure(lambda: Carbium(KEY, endpoint="https://services.carbium.sh/" + KEY)).kind == "request"
    assert KEY not in failure(lambda: Carbium(KEY + "!")).message
    Carbium("").close()  # an empty key means keyless


def test_a_name_the_services_do_not_serve_is_refused_before_it_is_sent(planted):
    e = failure(lambda: client(planted).rpc("notAMethod"))
    assert (e.kind, e.code, planted.requests) == ("request", -32601, [])


def test_the_async_client_follows_the_same_rules(planted):
    planted.answer(429, err(-32005, "rate limit exceeded", "RATE_LIMITED"), {"retry-after-ms": "300"}).answer(200, ok(9))
    planted.answer(402, err(-32002, "credits exhausted", "CREDITS_EXHAUSTED"))

    async def run():
        async with AsyncCarbium(KEY, endpoint=planted.url, _jitter=lambda: 0) as c:
            first = await c.get_slot()
            with pytest.raises(CarbiumError) as caught:
                await c.get_slot()
            return first, caught.value

    first, e = asyncio.run(run())
    assert first == 9 and 300 <= planted.gaps_ms[0] < 900
    assert (e.kind, len(planted.requests)) == ("credits", 3)


SOL, USDC, SYSTEM = "So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "11111111111111111111111111111111"
QUOTE = {"srcAmountIn": "100000000", "destAmountOut": "14250000", "destAmountOutMin": "14178750", "swapMode": "ExactIn", "slippage": "50",
         "routePlan": [{"swapInfo": {"ammKey": "a", "label": "l", "inputMint": SOL, "outputMint": USDC, "inAmount": "100000000", "outAmount": "14250000",
                                     "feeAmount": "0", "feeMint": SOL}, "percent": "100", "bps": "10000"}], "requestId": "q-1", "aNewField": "kept"}


def test_the_rest_quote_sends_its_four_values_in_the_query_and_the_key_in_a_header(planted):
    planted.answer(200, QUOTE, times=2)
    c = client(planted)
    assert c.cq1_quote(SOL, USDC, 100000000, 50) == QUOTE
    c.cq1_quote(SOL, USDC, "12345678901234567890", 0)
    first, second = planted.requests
    assert first["method"] == "GET" and first["path"] == f"/cq1/quote?src_mint={SOL}&dst_mint={USDC}&amount_in=100000000&slippage_bps=50"
    assert second["path"].endswith("&amount_in=12345678901234567890&slippage_bps=0")
    assert first["headers"]["x-api-key"] == KEY and KEY not in first["path"] and first["text"] == ""


def test_a_rest_quote_parameter_fault_is_plain_text_and_a_request_error(planted):
    text = "Failed to deserialize query string: missing field `dst_mint`"
    planted.answer(400, raw=text, content_type="text/plain").answer(400, raw=f"bad key {KEY}Leaked", content_type="text/plain")
    e = failure(lambda: client(planted).cq1_quote(SOL, "", 1, 50))
    assert (e.kind, e.http_status, e.code, e.method, e.message) == ("request", 400, None, "cq1_quote", f"{text} (x-request-id: rid-1)")
    leaked = failure(lambda: client(planted).cq1_quote(SOL, USDC, 1, 50))
    assert KEY not in str(leaked) and "c_serv_…" in str(leaked)
    assert len(planted.requests) == 2


def test_a_rest_quote_429_is_retried_as_the_quote_is_and_a_key_refusal_is_not(planted):
    planted.answer(429, err(-32005, "rate limit exceeded", "RATE_LIMITED"), {"retry-after-ms": "300"}).answer(200, QUOTE)
    planted.answer(401, err(-32001, "unknown api key", "UNKNOWN_KEY"))
    c = client(planted)
    assert c.cq1_quote(SOL, USDC, 5, 50) == QUOTE
    assert len(planted.requests) == 2 and 300 <= planted.gaps_ms[0] < 900
    e = failure(lambda: c.cq1_quote(SOL, USDC, 5, 50))
    assert (e.kind, e.http_status, e.code, e.reason, len(planted.requests)) == ("auth", 401, -32001, "UNKNOWN_KEY", 3)


def test_a_rest_quote_amount_or_slippage_of_the_wrong_kind_is_refused_before_it_is_sent(planted):
    c = client(planted)
    for amount, slippage in ((1.5, 50), ("1e3", 50), (-1, 50), (True, 50), (5, "50"), (5, 0.5)):
        assert failure(lambda: c.cq1_quote(SOL, USDC, amount, slippage)).kind == "request"
    assert planted.requests == []


def test_the_async_client_has_the_rest_quote(planted):
    planted.answer(200, QUOTE)

    async def run():
        async with AsyncCarbium(KEY, endpoint=planted.url + "/", _jitter=lambda: 0) as c:
            return await c.cq1_quote(SOL, USDC, 7, 50)

    assert asyncio.run(run()) == QUOTE and planted.routes == [f"GET /cq1/quote?src_mint={SOL}&dst_mint={USDC}&amount_in=7&slippage_bps=50"]


ACCT = "c_acct_" + "Test1234Acct"
ADDRESS = "So11111111111111111111111111111111111111112"


def reachable(value, depth: int = 4) -> list[str]:
    """The repr of everything reachable from a value: an exception's chain and args, and every attribute, a few levels down."""
    out, seen, todo = [], set(), [(value, 0)]
    while todo and len(out) < 5000:
        item, level = todo.pop()
        if id(item) in seen or item is None:
            continue
        seen.add(id(item))
        try:
            out.append(repr(item))
        except Exception:
            continue
        if level >= depth or isinstance(item, (str, bytes, int, float, bool)):
            continue
        children = list(item) if isinstance(item, (list, tuple, set)) else list(item.values()) + list(item) if isinstance(item, dict) else []
        for name in dir(item):
            if name.startswith("__") and name not in ("__cause__", "__context__"):
                continue
            try:
                child = getattr(item, name)
            except Exception:
                continue
            if not callable(child) or isinstance(child, BaseException):
                children.append(child)
        todo += [(child, level + 1) for child in children]
    return out


def test_a_transport_failures_whole_chain_holds_no_key():
    for call, held in ((Carbium(KEY, endpoint="http://127.0.0.1:9", max_attempts=1).get_slot, KEY),
                       (CarbiumAccount(ACCT, base_url="http://127.0.0.1:9").keys_list, ACCT)):
        e = failure(call)
        assert e.__cause__ is not None and "ConnectError" in str(e.__cause__)
        shown = "".join(traceback.format_exception(e)) + " ".join(reachable(e.__cause__)) + " ".join(reachable(e.__context__))
        assert held not in shown, held


def test_a_key_with_a_trailing_newline_is_refused_at_construction():
    for make, held in ((lambda: Carbium(KEY + "\n"), KEY), (lambda: AsyncCarbium(KEY + "\n"), KEY), (lambda: CarbiumSocket(KEY + "\n"), KEY),
                       (lambda: CarbiumAccount(ACCT + "\n"), ACCT)):
        e = failure(make)
        assert e.kind == "auth" and held not in e.message


def test_batch_errors_carry_retryable_the_wait_and_the_request_id(planted):
    planted.answer(200, [ok(1, id=1), err(-32005, "busy", id=2, retryAfterMs=700), err(-32602, "invalid params", id=3)])
    planted.answer(200, [err(-32600, "scan not batchable", "SCAN_NOT_BATCHABLE", id=4, retryable=False), err(-32600, "scan not batchable", "SCAN_NOT_BATCHABLE", id=5, retryable=False)])
    c = client(planted)
    out = c.batch([("getSlot", []), ("getBalance", ["x"]), ("getBlockHeight", [])])
    busy, bad = out[1]["error"], out[2]["error"]
    assert (busy.retryable, busy.retry_after_ms, busy.request_id) == (True, 700, "rid-1")
    assert (bad.retryable, bad.retry_after_ms, bad.request_id) == (False, None, "rid-1")
    whole = failure(lambda: c.batch([("getSlot", []), ("getBlockHeight", [])]))
    assert (whole.retryable, whole.request_id, whole.reason) == (False, "rid-2", "SCAN_NOT_BATCHABLE")


def test_an_error_survives_pickle_and_copy_with_every_field():
    e = CarbiumError("rate_limit", "slow down", http_status=429, code=-32005, reason="RATE_LIMITED", retryable=True, retry_after_ms=300,
                     request_id="rid-9", method="getSlot", data={"echo": [1, {"a": None}]})
    for twin in (pickle.loads(pickle.dumps(e)), copy.copy(e), copy.deepcopy(e)):
        assert type(twin) is CarbiumError and vars(twin) == vars(e) and (str(twin), twin.args) == (str(e), e.args)


def test_a_node_behind_with_no_context_slot_waits_the_floor_and_a_hint_wins_over_the_row(planted):
    planted.answer(200, err(-32016, "Minimum context slot has not been reached")).answer(200, ok())
    planted.answer(200, err(-32016, "Minimum context slot has not been reached", contextSlot=104, retryAfterMs=1500)).answer(200, ok())
    c = client(planted)
    c.get_balance(ADDRESS, {"minContextSlot": 105})
    c.get_balance(ADDRESS, {"minContextSlot": 105})
    assert planted.gaps_ms[0] < 1000, planted.gaps_ms
    assert 1500 <= planted.gaps_ms[2] < 2100, planted.gaps_ms


def test_bad_params_are_sent_once_but_a_walk_that_read_no_history_is_sent_again(planted):
    planted.answer(200, err(-32602, "invalid params: Invalid param: WrongSize"))
    planted.answer(200, err(-32602, "the walk read no history: the validator answered no page within the walk's time budget")).answer(200, ok({"data": []}))
    c = client(planted)
    e = failure(lambda: c.get_transactions_for_address(ADDRESS))
    assert (len(planted.requests), e.kind, e.code, e.retryable) == (1, "request", -32602, False)
    assert c.get_transactions_for_address(ADDRESS) == {"data": []}
    assert [r["json"]["method"] for r in planted.requests] == ["getTransactionsForAddress"] * 3 and 250 <= planted.gaps_ms[1] < 850, planted.gaps_ms


def test_an_endpoint_that_is_not_an_http_address_is_refused_at_construction():
    for bad in ("http://[::1", "ftp://x", "https://", "not a url", "http://exa\x00mple.com"):
        for make in (lambda: Carbium(KEY, endpoint=bad), lambda: CarbiumAccount(ACCT, base_url=bad), lambda: CarbiumDca(base_url=bad), lambda: CarbiumTokens(base_url=bad)):
            assert failure(make).kind == "request", bad


def test_threads_sharing_a_client_never_get_the_same_call_id():
    c, ids, lock = Carbium(KEY), [], threading.Lock()

    def work():
        mine = [c._transport._one("getSlot", None)["id"] for _ in range(20000)]
        with lock:
            ids.extend(mine)

    before = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        threads = [threading.Thread(target=work) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        sys.setswitchinterval(before)
    assert len(set(ids)) == len(ids) == 160000


def test_a_429_marked_not_retryable_is_sent_once(planted):
    planted.answer(429, err(-32005, "rate limit exceeded", "RATE_LIMITED", retryable=False), times=3)
    e = failure(client(planted).get_slot)
    assert (len(planted.requests), e.kind, e.retryable) == (1, "rate_limit", False)


def test_a_batch_holding_send_transaction_is_sent_once_on_a_429(planted):
    planted.answer(429, err(-32005, "rate limit exceeded", "RATE_LIMITED"), times=3)
    e = failure(lambda: client(planted).batch([("sendTransaction", ["AA=="]), ("getSlot", [])]))
    assert (len(planted.requests), e.kind) == (1, "rate_limit")


def test_the_hint_in_the_error_data_sets_the_wait_and_wins_over_retry_after(planted):
    planted.answer(429, err(-32005, "rate limit exceeded", "RATE_LIMITED", retryAfterMs=1200), {"Retry-After": "3"}).answer(200, ok())
    client(planted).get_slot()
    assert 1200 <= planted.gaps_ms[0] < 1800, planted.gaps_ms


def test_a_retry_after_date_sets_the_wait(planted):
    planted.answer(429, err(-32005, "rate limit exceeded", "RATE_LIMITED"), {"Retry-After": email.utils.formatdate(time.time() + 3, usegmt=True)}).answer(200, ok())
    client(planted).get_slot()
    assert 1900 <= planted.gaps_ms[0] < 3600, planted.gaps_ms


def test_a_rest_quote_422_is_a_route_error_and_neither_refusal_is_retried(planted):
    planted.answer(422, raw="no route between these mints", content_type="text/plain").answer(400, raw="Failed to deserialize query string", content_type="text/plain")
    c = client(planted)
    route = failure(lambda: c.cq1_quote(SOL, USDC, 1, 50))
    request = failure(lambda: c.cq1_quote(SOL, USDC, 1, 50))
    assert (route.kind, route.http_status, request.kind, len(planted.requests)) == ("route", 422, "request", 2)


def test_a_quote_with_no_route_under_the_account_cap_is_a_route_error_sent_once(planted):
    planted.answer(200, err(-32055, "no route fits the account cap", why="accounts_over_cap"), times=4)
    e = failure(lambda: client(planted).quote(src_mint=SOL, dst_mint=USDC, amount_in=1, slippage_bps=50, swapTag=1))
    assert (e.kind, e.code, e.http_status, e.retryable, len(planted.requests)) == ("route", -32055, 200, False, 1)


def test_a_quote_sends_the_account_cap_and_the_integrator_fee_under_their_long_names_and_leaves_unset_ones_out(planted):
    planted.answer(200, ok({"quoteId": "q"}), times=2)
    c = client(planted)
    c.quote(src_mint=SOL, dst_mint=USDC, amount_in=1, slippage_bps=50, maxAccounts=8, integrator_fee_bps=25, integrator_fee_account=SYSTEM)
    c.quote(src_mint=SOL, dst_mint=USDC, amount_in=1, slippage_bps=50)
    base = {"src_mint": SOL, "dst_mint": USDC, "amount_in": 1, "slippage_bps": 50}
    assert [r["json"]["params"] for r in planted.requests] == [{**base, "maxAccounts": 8, "integrator_fee_bps": 25, "integrator_fee_account": SYSTEM}, base]


def attribute_walk(root, depth: int = 4) -> list:
    """Every object reachable through attribute dicts, dict values, lists and tuples, the way an error reporter or a
    debug log walks an object's attributes."""
    out, seen, todo = [], set(), [(root, 0)]
    while todo:
        item, level = todo.pop()
        if id(item) in seen:
            continue
        seen.add(id(item))
        out.append(item)
        if level >= depth or isinstance(item, (str, bytes)):
            continue
        children = list(vars(item).values()) if isinstance(getattr(item, "__dict__", None), dict) else []
        children += list(item.values()) if isinstance(item, dict) else list(item) if isinstance(item, (list, tuple)) else []
        todo += [(child, level + 1) for child in children]
    return out


def shown(item) -> str:
    try:
        return repr(item) + " " + str(item)
    except Exception:
        return ""


def test_no_client_holds_its_key_where_printing_its_attributes_would_show_it():
    from carbium import AsyncCarbiumAccount, AsyncCarbiumDca

    session, dca_key = "st" + "Vars0Check0Session00", "dca-" + "vars-check-key-1"
    clients = [(Carbium(KEY), KEY), (AsyncCarbium(KEY), KEY), (CarbiumAccount(ACCT), ACCT), (AsyncCarbiumAccount(ACCT), ACCT),
               (CarbiumAccount(session_token=session), session), (AsyncCarbiumAccount(session_token=session), session),
               (CarbiumDca(dca_key), dca_key), (AsyncCarbiumDca(dca_key), dca_key), (CarbiumSocket(KEY), KEY)]
    for client, held in clients:
        walked = attribute_walk(client)
        assert len(walked) > 10, type(client).__name__  # the walk reaches into the HTTP client's own state
        for item in walked:
            assert not (isinstance(item, str) and held in item), type(client).__name__
            assert held not in shown(item), (type(client).__name__, type(item).__name__)


def test_the_key_wrapper_prints_a_placeholder_and_is_never_pickled():
    from carbium.errors import Secret

    secret = Secret(KEY)
    assert KEY not in repr(secret) + str(secret) + str(vars(secret) if hasattr(secret, "__dict__") else "") and secret.reveal() == KEY
    for copier in (pickle.dumps, copy.copy, copy.deepcopy):
        with pytest.raises(TypeError):
            copier(secret)
