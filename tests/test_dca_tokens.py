"""The DCA and token clients against a planted server. The first case of each ports the TypeScript SDK's client tests."""
from __future__ import annotations

import asyncio
import json
import types
import typing
from typing import Any

import pytest

from carbium import AsyncCarbiumDca, AsyncCarbiumTokens, CarbiumDca, CarbiumError, CarbiumTokens

DCA_KEY = "dca-" + "test-key-1"  # a made-up key, joined here so no file holds one whole
BUILT = {"ok": True, "txn_base64": "AQ==", "blockhash": "h", "last_valid_block_height": "5", "position": "p", "cost": {},
         "summary": {"kind": "open", "owner": "o", "feePayer": "o", "instructions": []}, "authority": "a", "escrowAccount": "e", "index": 0, "plan": {}}
SOL = "So11111111111111111111111111111111111111112"


def failure(call) -> CarbiumError:
    with pytest.raises(CarbiumError) as caught:
        call()
    return caught.value


def test_an_ok_false_answer_is_a_dca_error_big_amounts_travel_as_text_and_the_key_goes_in_a_header(planted):
    planted.answer(400, {"ok": False, "err": "same_mint", "detail": "inputMint and outputMint are the same"})
    planted.answer(200, BUILT).answer(429, {"ok": False, "err": "rate_limited", "detail": "slow down"}, {"Retry-After": "3"}).answer(200, {"ok": True, "keeper": "k"})
    d = CarbiumDca(DCA_KEY, base_url=planted.url)
    e = failure(lambda: d.open({"owner": "o", "inputMint": "m", "outputMint": "m", "cycles": 2, "amountPerCycle": 10, "frequency": "daily"}))
    assert (e.kind, e.code, e.http_status, e.retryable, e.message) == ("dca", "same_mint", 400, False, "inputMint and outputMint are the same")
    built = d.open({"owner": "o", "inputMint": "a", "outputMint": "b", "cycles": 2, "totalAmount": 12345678901234567890, "intervalSecs": 3600})
    assert built["txn_base64"] == "AQ=="
    sent = planted.requests[1]
    assert (sent["json"]["totalAmount"], sent["json"]["cycles"], sent["headers"]["x-api-key"]) == ("12345678901234567890", 2, DCA_KEY)
    e = failure(lambda: d.quote({"inputMint": "a", "outputMint": "b", "amount": "5"}))
    assert (e.code, e.retryable, e.retry_after_ms) == ("rate_limited", True, 3000)
    assert d.health()["keeper"] == "k"
    assert planted.routes == ["POST /open", "POST /open", "GET /quote?inputMint=a&outputMint=b&amount=5", "GET /health"]


def test_ok_false_at_200_is_an_error_and_door_busy_may_be_tried_again(planted):
    planted.answer(200, {"ok": False, "err": "door_busy", "detail": "busy, try again"}).answer(200, raw="<html>gateway</html>", content_type="text/html")
    d = CarbiumDca(base_url=planted.url)
    busy = failure(lambda: d.positions("o"))
    assert (busy.kind, busy.code, busy.http_status, busy.retryable) == ("dca", "door_busy", 200, True)
    page = failure(d.health)
    assert (page.kind, page.code, page.retryable) == ("dca", None, False) and "gateway" in page.message
    assert "x-api-key" not in planted.requests[0]["headers"]


def test_send_is_sent_once_whatever_comes_back(planted):
    planted.answer(503, {"ok": False, "err": "upstream", "detail": "upstream down"}, {"Retry-After": "1"})
    planted.answer(200, {"ok": True, "sent": True}, delay=0.6)
    d = CarbiumDca(base_url=planted.url, timeout=0.3)
    e = failure(lambda: d.send({"txn_base64": "AQ=="}))
    assert (len(planted.requests), e.retryable, e.retry_after_ms, e.method) == (1, True, 1000, "send")
    e = failure(lambda: d.send({"txn_base64": "AQ=="}))
    assert (len(planted.requests), e.message, e.retryable) == (2, "the call timed out", False)
    assert planted.routes == ["POST /send"] * 2


def test_send_carries_only_the_documented_fields(planted):
    planted.answer(200, {"ok": True, "kind": "open", "signature": "s", "sent": True, "position": "p"}, times=2)
    signed = {"txn_base64": "AQ==", "built_txn_base64": "AA=="}

    async def send_async():
        async with AsyncCarbiumDca(base_url=planted.url) as d:
            return await d.send({**signed, "extra": True})

    with CarbiumDca(base_url=planted.url) as d:
        answers = [d.send({**signed, "extra": True}), asyncio.run(send_async())]
    assert [r["json"] for r in planted.requests] == [signed, signed] and [a["sent"] for a in answers] == [True, True]


def test_dca_paths_and_queries_and_the_key_never_shows(planted):
    planted.answer(401, {"ok": False, "err": "bad_key", "detail": f"the key {DCA_KEY} is unknown", "echo": [DCA_KEY]}).answer(200, {"ok": True})
    d = CarbiumDca(DCA_KEY, base_url=planted.url + "/api/dca/")
    e = failure(lambda: d.history("o"))
    for shown in (str(e), repr(e), json.dumps(e.data), repr(e.args), repr(d)):
        assert DCA_KEY not in shown
    assert e.code == "bad_key" and repr(d) == f"CarbiumDca(base_url='{planted.url}/api/dca', keyed=True)"
    d.positions("o", after="n1")
    d.position("a/b", opened_slot=5)
    d.withdraw({"owner": "o", "position": "p", "amount": 0})
    assert planted.routes == ["GET /api/dca/history?owner=o", "GET /api/dca/positions?owner=o&after=n1", "GET /api/dca/position/a%2Fb?openedSlot=5", "POST /api/dca/withdraw"]
    bad = failure(lambda: CarbiumDca("dca key\n"))
    assert bad.kind == "auth" and "dca key" not in bad.message


def test_a_missing_token_is_none_a_429_carries_its_wait_and_batch_sizes_are_checked_first(planted):
    planted.answer(404, {"error": "not_found", "mint": "x"})
    planted.answer(429, {"error": "rate_limited", "message": "slow down", "retryAfterSec": 12}, {"Retry-After": "12"})
    planted.answer(200, {"items": [{"mint": "m", "symbol": "M", "decimals": 6}], "unknown": ["u"]})
    t = CarbiumTokens(base_url=planted.url)
    assert t.lookup("x") is None
    e = failure(lambda: t.search("sol", limit=5))
    assert (e.kind, e.code, e.retry_after_ms, e.retryable) == ("tokens", "rate_limited", 12000, True)
    assert t.batch(["m", "u"])["unknown"] == ["u"]
    for mints in ([], ["m"] * 501):
        refused = failure(lambda: t.batch(mints))
        assert refused.kind == "tokens" and f"takes 1 to 500 mints, not {len(mints)}" in refused.message
    assert planted.routes == ["GET /tokens/x", "GET /tokens?q=sol&limit=5", "POST /tokens/batch"]
    assert t.image_url(SOL) == f"{planted.url}/img/{SOL}" and t.image_url("a/b").endswith("/img/a%2Fb")
    assert len(planted.requests) == 3


def test_the_token_wait_falls_back_to_the_bodys_seconds(planted):
    planted.answer(503, {"error": "unavailable", "retryAfterSec": 2.5})
    t = CarbiumTokens(base_url=planted.url)
    e = failure(lambda: t.search("", include_token22=True, page=2))
    assert (e.retry_after_ms, e.retryable, e.message) == (2500, True, "unavailable")
    assert planted.routes == ["GET /tokens?q=&page=2&include_token22=true"]


def test_a_refusal_with_ok_false_at_http_200_may_be_tried_again_by_its_code_and_an_empty_body_names_its_status(planted):
    for code in ("rate_limited", "rate_limited_open", "rpc_unavailable", "quote_unavailable", "door_busy"):
        planted.answer(200, {"ok": False, "err": code, "detail": "try again"})
    planted.answer(200, {"ok": False, "err": "same_mint", "detail": "inputMint and outputMint are the same"}).answer(503, raw="", content_type="text/plain")
    d = CarbiumDca(base_url=planted.url)
    for code in ("rate_limited", "rate_limited_open", "rpc_unavailable", "quote_unavailable", "door_busy"):
        e = failure(lambda: d.quote({"inputMint": "a", "outputMint": "b", "amount": "5"}))
        assert (e.kind, e.code, e.http_status, e.retryable) == ("dca", code, 200, True), code
    e = failure(lambda: d.quote({"inputMint": "a", "outputMint": "a", "amount": "5"}))
    assert (e.code, e.retryable) == ("same_mint", False)
    e = failure(d.health)
    assert (e.message, e.retryable, e.code) == ("HTTP 503", True, None)


def conforms(value: Any, hint: Any) -> bool:
    """Whether a decoded answer has the shape its type names: TypedDict keys, unions, literals, lists and maps."""
    origin, args = typing.get_origin(hint), typing.get_args(hint)
    if hint is Any:
        return True
    if typing.is_typeddict(hint):
        fields = typing.get_type_hints(hint)  # required keys from the hints: __required_keys__ misses a NotRequired written as a string
        required = {k for k, t in typing.get_type_hints(hint, include_extras=True).items() if typing.get_origin(t) is not typing.NotRequired}
        return isinstance(value, dict) and required <= value.keys() and all(conforms(value[k], fields[k]) for k in value if k in fields)
    if origin in (typing.Union, types.UnionType):
        return any(conforms(value, arg) for arg in args)
    if origin is typing.Literal:
        return value in args
    if origin is list:
        return isinstance(value, list) and all(conforms(item, args[0]) for item in value)
    if origin is dict:
        return isinstance(value, dict) and all(conforms(item, args[1]) for item in value.values())
    if hint in (None, type(None)):
        return value is None
    if hint is float:
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if hint is int:
        return isinstance(value, int) and not isinstance(value, bool)
    return isinstance(value, hint)


TRY = {"at": 1, "cycle": 1, "result": "ok", "code": "filled", "detail": None, "signature": None}
POSITION = {"address": "p", "owner": "o", "inMint": "a", "outMint": "b", "crank": "c", "inProgram": "t", "outProgram": "t", "index": 0, "maxSlippageBps": 50,
            "cyclesTotal": 10, "cyclesDone": 1, "intervalSecs": 3600, "amountPerCycle": "1000000", "minOutE6": "0", "cumIn": "1000000", "cumOut": "6600000",
            "nextTs": "1700003600", "active": True, "escrow": None, "middleDebt": None, "status": "waiting", "statusCode": "waiting", "statusDetail": None,
            "lastTry": TRY, "openedSlot": 300, "openedSignature": None, "served": True, "notServed": None, "aNewField": 1}
BUY = {"signature": "s", "cycle": 1, "inAmount": "1000000", "outAmount": "6600000", "ts": "1700000000", "slot": 301, "openedSlot": 300, "verdict": "filled", "detail": None}
LIVE_SHAPES = {
    "quote": {"ok": True, "amount": "5000000", "out": "33000000", "minOut": "32835000", "rateE6": "6600000", "feeBps": 10, "inDecimals": 6, "outDecimals": 9,
              "valueUsdc": "5000000", "limits": {"minAmountPerCycle": None, "minValueUsdc": "1000000", "minIntervalSecs": 3600, "maxCycles": 1000, "maxActive": 10}},
    "open": BUILT,
    "withdraw": {**BUILT, "summary": {"kind": "withdraw", "owner": "o", "feePayer": "o", "instructions": []}},
    "positions": {"ok": True, "owner": "o", "positions": [POSITION], "next": None},
    "history": {"ok": True, "owner": "o", "next": None, "orders": [{"address": "p", "inMint": "a", "outMint": "b", "cyclesDone": 10, "cyclesTotal": 10,
                "amountPerCycle": "1000000", "intervalSecs": 3600, "cumIn": "10000000", "cumOut": "66000000", "minOutE6": "0", "openedSlot": 300,
                "openedSignature": None, "openedTs": None, "closedSlot": 900, "closedSignature": "s", "closedTs": "1700036000", "lastTry": None, "ended": "finished"}]},
    "position_detail": {**POSITION, "ownerIn": None, "ownerOut": "5", "tracked": True, "fills": [BUY]},
    "position_closed": {"address": "p", "closed": True, "closedSlot": 900, "openedSlot": 300, "openedSignature": None, "status": "closed", "statusCode": "closed",
                        "statusDetail": None, "lastTry": None, "fills": [BUY]},
}
TOKEN = {"mint": "m", "symbol": "", "name": "", "decimals": 0, "logo_url": None, "logo_status": "broken_perm", "is_token22": False, "daily_volume_usd": None,
         "first_seen_at": 1700000000000, "last_seen_at": 1700000000000, "supply": None, "mint_authority": None, "freeze_authority": None, "metaplex_uri": None,
         "update_authority": None, "seller_fee_basis_points": None, "token_standard": 2, "description": None, "external_url": None, "extensions": None,
         "tags": ["stable", {"name": "verified"}]}
TOKEN_SHAPES = {"lookup": TOKEN, "search": {"items": [TOKEN, {**TOKEN, "logo_status": "unchecked", "token_standard": None, "tags": None}], "page": 1, "limit": 50, "total": None},
                "batch": {"items": [TOKEN], "unknown": ["u"]}}


def test_each_typed_read_decodes_the_answer_shape_the_services_send(planted):
    d, t = CarbiumDca(base_url=planted.url), CarbiumTokens(base_url=planted.url)
    calls = [(d, "quote", ({"inputMint": "a", "outputMint": "b", "amount": "5"},), "quote"), (d, "open", ({"owner": "o"},), "open"),
             (d, "withdraw", ({"owner": "o", "position": "p"},), "withdraw"), (d, "positions", ("o",), "positions"), (d, "history", ("o",), "history"),
             (d, "position", ("p",), "position_detail"), (d, "position", ("p",), "position_closed"),
             (t, "lookup", ("m",), "lookup"), (t, "search", ("",), "search"), (t, "batch", (["m"],), "batch")]
    for client, name, args, shape in calls:
        planted.answer(200, {**LIVE_SHAPES, **TOKEN_SHAPES}[shape])
    wrong = []
    for client, name, args, shape in calls:
        answer = getattr(client, name)(*args)
        if not conforms(answer, typing.get_type_hints(getattr(type(client), name))["return"]):
            wrong.append(shape)
    assert wrong == []
    assert not conforms({**LIVE_SHAPES["quote"], "valueUsdc": 5.0}, typing.get_type_hints(CarbiumDca.quote)["return"])  # the checker can fail


def test_the_async_clients_follow_the_same_rules(planted):
    planted.answer(503, {"ok": False, "err": "upstream"}).answer(404, {"error": "not_found"}).answer(200, TOKEN_SHAPES["batch"])

    async def run():
        async with AsyncCarbiumDca(base_url=planted.url) as d, AsyncCarbiumTokens(base_url=planted.url) as t:
            with pytest.raises(CarbiumError) as caught:
                await d.send({"txn_base64": "AQ=="})
            missing = await t.lookup("x")
            found = await t.batch(["m"])
            return caught.value, missing, found

    e, missing, found = asyncio.run(run())
    assert (e.kind, e.retryable, missing, found["unknown"]) == ("dca", True, None, ["u"])
    assert planted.routes == ["POST /send", "GET /tokens/x", "POST /tokens/batch"]
