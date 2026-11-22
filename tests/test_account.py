"""The account client against a planted server: the credential each call takes, the error shape, and the generator's
refusals. The first cases port the TypeScript SDK's account and session tests."""
from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from pathlib import Path

import pytest

from carbium import AsyncCarbiumAccount, CarbiumAccount, CarbiumError

ROOT = Path(__file__).parent.parent
ACCT = "c_acct_" + "Test1234Acct"  # made-up credentials, joined here so no file holds one whole
SESSION = "st" + "Ab12Cd34Ef56Gh78"


def key_client(planted, **options) -> CarbiumAccount:
    return CarbiumAccount(ACCT, base_url=planted.url, **options)


def session_client(planted, **options) -> CarbiumAccount:
    return CarbiumAccount(session_token=SESSION, base_url=planted.url, **options)


def failure(call) -> CarbiumError:
    with pytest.raises(CarbiumError) as caught:
        call()
    return caught.value


def test_an_account_key_goes_as_a_bearer_and_an_error_carries_the_services_code(planted):
    planted.answer(200, [{"key_id": 1, "prefix": "c_serv_ab", "status": "active"}])
    planted.answer(429, {"error": "rate_limited", "message": "too many calls: wait Retry-After seconds"}, {"Retry-After": "37"})
    planted.answer(415, raw="Expected request with `Content-Type: application/json`", content_type="text/plain")
    planted.answer(200, {"event_id": "evt_test_1", "outcome": "delivered", "http_status": 200, "reason": "", "latency_ms": 90, "refusal": None, "ts": 1})
    a = key_client(planted)
    assert a.keys_list()[0]["key_id"] == 1
    assert planted.requests[0]["headers"]["authorization"] == f"Bearer {ACCT}" and planted.routes[0] == "GET /v1/keys"
    e = failure(a.account_balance)
    assert (e.kind, e.http_status, e.code, e.retry_after_ms, e.request_id, e.retryable) == ("account", 429, "rate_limited", 37000, "rid-2", False)
    assert e.message == "too many calls: wait Retry-After seconds (x-request-id: rid-2)"
    e = failure(lambda: a.keys_create({"name": "x"}))
    assert (e.kind, e.http_status, e.code) == ("account", 415, None) and "Content-Type" in e.message
    assert a.webhooks_test(7)["outcome"] == "delivered"
    assert planted.routes == ["GET /v1/keys", "GET /v1/balance", "POST /v1/keys", "POST /v1/webhooks/7/test"]
    assert planted.requests[2]["json"] == {"name": "x"} and planted.requests[2]["headers"]["content-type"] == "application/json"


def test_the_plain_text_refusals_are_read_as_the_message(planted):
    for status in (422, 400):
        planted.answer(status, raw=f"Json deserialize error: missing field `name` ({status})", content_type="text/plain")
    a = key_client(planted)
    for status in (422, 400):
        e = failure(lambda: a.keys_rename(3, {"name": "x"}))
        assert (e.kind, e.http_status, e.code, e.data) == ("account", status, None, None)
        assert e.message.startswith("Json deserialize error: missing field `name`")


def test_nothing_is_sent_twice(planted):
    planted.answer(503, {"error": "unavailable", "message": "the ledger is unreachable"}, {"Retry-After": "2"})
    planted.answer(429, {"error": "rate_limited", "message": "slow down"}, {"Retry-After": "3"})
    a = key_client(planted)
    first = failure(lambda: a.keys_create({"name": "x"}))
    second = failure(a.keys_list)
    assert len(planted.requests) == 2
    assert (first.retryable, first.retry_after_ms, second.retryable, second.retry_after_ms) == (True, 2000, False, 3000)


def test_a_call_the_held_credential_cannot_make_is_refused_before_it_is_sent(planted):
    a, s, none = key_client(planted), session_client(planted), CarbiumAccount(base_url=planted.url)
    for call in (lambda: a.create_account_api_key({"name": "cli", "access": "read"}), a.list_account_api_keys, lambda: a.reveal_key_secret(1),
                 lambda: a.billing_checkout({"tier": "dev"}), a.logout, lambda: a.account_delete({"confirm": "DELETE"})):
        e = failure(call)
        assert e.kind == "auth" and "takes a session token; this client holds an account key" in e.message
    assert "takes an account key; this client holds a session token" in failure(s.self).message
    assert "takes an account key or a session token; this client holds no credential" in failure(none.keys_list).message
    assert planted.requests == []


def test_a_public_call_sends_no_credential_and_plans_takes_a_session(planted):
    a, s = key_client(planted), session_client(planted)
    for client in (a, s):
        client.terms()
        client.signup_challenge()
        client.pin_start({"email": "a@b.c", "terms_version": "2026-10-01", "pow": {"challenge": "abc", "nonce": 7}})
        client.pin_verify({"pin_token": "pt", "code": "123456", "terms_version": "2026-10-01"})
        client.wallet_login({"step": "challenge", "wallet": "W"})
    a.plans()
    s.plans()
    CarbiumAccount(base_url=planted.url).terms()
    sent = [r["headers"].get("authorization") for r in planted.requests]
    assert sent == [None] * 11 + [f"Bearer {SESSION}", None]
    assert planted.routes[:5] == ["GET /v1/terms", "GET /v1/signup/challenge", "POST /v1/login/pin", "POST /v1/login/pin/verify", "POST /v1/auth/wallet"]
    assert planted.requests[2]["json"] == {"email": "a@b.c", "terms_version": "2026-10-01", "pow": {"challenge": "abc", "nonce": 7}}


def test_a_session_mints_and_revokes_account_keys_lists_keys_and_reveals_a_secret(planted):
    planted.answer(200, {"api_key_id": 9, "name": "cli", "prefix": "c_acct_AbCd", "access": "read", "raw_key": "c_acct_" + "RAW"})
    planted.answer(200, [{"key_id": 1}]).answer(200, {"available": True, "raw_key": "c_serv_" + "RAW"}).answer(200, {"api_key_id": 9, "revoked": True})
    s = session_client(planted)
    assert s.create_account_api_key({"name": "cli", "access": "read", "expires_in_days": 90})["api_key_id"] == 9
    assert s.keys_list()[0]["key_id"] == 1 and s.reveal_key_secret(1)["available"] is True and s.revoke_account_api_key(9)["revoked"] is True
    assert planted.routes == ["POST /v1/account/api-keys", "GET /v1/keys", "POST /v1/keys/1/secret", "DELETE /v1/account/api-keys/9"]
    assert all(r["headers"]["authorization"] == f"Bearer {SESSION}" for r in planted.requests)
    assert planted.requests[0]["json"] == {"name": "cli", "access": "read", "expires_in_days": 90}


def test_path_ids_are_integers_and_a_name_is_one_encoded_segment(planted):
    a = key_client(planted)
    for bad in ("5", "5/../keys", True, 5.0, None):
        assert failure(lambda: a.keys_rotate(bad)).kind == "request"
    assert failure(lambda: a.modules_enable_free("")).kind == "request"
    assert planted.requests == []
    a.modules_enable_free("quote/../x")
    a.modules_enable_free("quote", {"plan": "free"})
    assert planted.routes == ["POST /v1/modules/quote%2F..%2Fx", "POST /v1/modules/quote"]
    assert [r["json"] for r in planted.requests] == [{}, {"plan": "free"}]


def test_query_values_are_sent_by_their_wire_names_and_left_out_when_unset(planted):
    a = key_client(planted)
    a.usage_daily(from_="2026-11-01", to="2026-11-07", group="key")
    a.webhooks_deliveries(7)
    a.activity(4, limit=10, before=1700000000000)
    session_client(planted).billing_portal(module="quote")
    assert planted.routes == ["GET /v1/usage?from=2026-11-01&to=2026-11-07&group=key", "GET /v1/webhooks/7/deliveries",
                              "GET /v1/account/api-keys/4/activity?limit=10&before=1700000000000", "POST /v1/billing/portal?module=quote"]


def test_no_key_session_token_or_secret_survives_in_an_error_or_a_repr(planted):
    secret = "whsec_" + "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
    for client, held in ((key_client(planted), ACCT), (session_client(planted), SESSION)):
        text = f"refused {held} and {ACCT} and {SESSION} and {secret} at https://x.example/?token={SESSION}"
        planted.answer(401, {"error": "unauthorized", "message": text, "echo": [text, {"deep": text}]})
        e = failure(client.keys_list)
        shown = " ".join([str(e), repr(e), e.message, json.dumps(e.data), repr(e.args), repr(client), str(client)])
        for value in (held, ACCT, secret):  # a session token has no prefix: only the client that holds it can know it
            assert value not in shown, value
        assert "whsec_…" in shown and "token=…" in shown and e.code == "unauthorized"
    assert repr(key_client(planted)) == f"CarbiumAccount(base_url='{planted.url}', credential='key')"


def test_the_wrong_kind_of_credential_or_one_in_the_address_is_refused_at_once():
    for make in (lambda: CarbiumAccount("c_serv_" + "Abc123"), lambda: CarbiumAccount("not a key"), lambda: CarbiumAccount(ACCT, session_token=SESSION),
                 lambda: CarbiumAccount(session_token=ACCT), lambda: CarbiumAccount(session_token="whsec_" + "Abc123Def")):
        e = failure(make)
        assert e.kind == "auth" and ACCT not in e.message
    unsafe = failure(lambda: CarbiumAccount(session_token=SESSION + "\n"))
    assert unsafe.kind == "auth" and SESSION not in unsafe.message
    assert failure(lambda: CarbiumAccount(base_url="https://carbium.sh/?token=" + SESSION)).kind == "request"
    assert failure(lambda: CarbiumAccount(base_url="https://carbium.sh/" + ACCT)).kind == "request"


def test_a_204_is_none_and_a_network_error_keeps_its_cause(planted):
    planted.answer(204, raw="")
    assert session_client(planted).logout() is None
    gone = CarbiumAccount(ACCT, base_url="http://127.0.0.1:9")
    get, post = failure(gone.keys_list), failure(lambda: gone.keys_create())
    assert (get.kind, get.http_status, get.retryable, post.retryable) == ("account", 0, True, False)
    assert get.__cause__ is not None


def test_the_test_delivery_is_given_longer_than_other_calls(planted):
    planted.answer(200, {"outcome": "delivered"}, delay=0.6).answer(200, [], delay=0.6)
    a = key_client(planted, timeout=0.3)
    assert a.webhooks_test(7) == {"outcome": "delivered"}
    e = failure(a.keys_list)
    assert (e.kind, e.message) == ("account", "the call timed out")


def test_the_async_client_follows_the_same_rules(planted):
    planted.answer(200, [{"key_id": 1}]).answer(429, {"error": "rate_limited", "message": "slow down"}, {"Retry-After": "37"})

    async def run():
        async with AsyncCarbiumAccount(ACCT, base_url=planted.url) as a:
            keys = await a.keys_list()
            with pytest.raises(CarbiumError) as caught:
                await a.keys_create({"name": "x"})
            with pytest.raises(CarbiumError) as refused:
                await a.logout()
            return keys, caught.value, refused.value

    keys, e, refused = asyncio.run(run())
    assert keys == [{"key_id": 1}] and (e.code, e.retry_after_ms) == ("rate_limited", 37000) and refused.kind == "auth"
    assert planted.routes == ["GET /v1/keys", "POST /v1/keys"]


def generate(tmp_path: Path, description: dict) -> subprocess.CompletedProcess:
    source = tmp_path / "openapi.json"
    source.write_text(json.dumps(description))
    return subprocess.run([sys.executable, str(ROOT / "scripts/generate_account.py"), str(source), str(tmp_path / "out.py")], capture_output=True, text=True)


def test_the_committed_calls_are_what_the_generator_makes_and_an_unnamed_route_stops_it(tmp_path):
    described = json.loads((ROOT / "spec/account-openapi.json").read_text())
    made = generate(tmp_path, described)
    assert made.returncode == 0, made.stderr
    assert (tmp_path / "out.py").read_text() == (ROOT / "src/carbium/_account_gen.py").read_text()
    unnamed = json.loads(json.dumps(described))
    unnamed["paths"]["/v1/keys/{id}/peek"] = {"get": {**described["paths"]["/v1/keys"]["get"], "operationId": "get_v1_keys_id_peek"}}
    refused = generate(tmp_path, unnamed)
    assert refused.returncode != 0 and "GET /v1/keys/{id}/peek has no name in NAMES" in refused.stderr
    gone = json.loads(json.dumps(described))
    del gone["paths"]["/v1/logout"]
    refused = generate(tmp_path, gone)
    assert refused.returncode != 0 and "NAMES has POST /v1/logout, which the description does not serve" in refused.stderr
