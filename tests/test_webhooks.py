"""The webhook check against the Standard Webhooks specification's own example, an answer this package did not compute."""
from __future__ import annotations

import base64
import hashlib
import hmac

import pytest

from carbium import CarbiumError, verify_webhook, verify_webhook_signature

SECRET = "whsec_" + "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"  # the specification's example, joined here so no file holds it whole
ID, STAMP, BODY = "msg_p5jXN8AQM9LWM0D4loKWxJek", "1614265330", b'{"test": 2432232314}'
SIGNATURE = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
now = lambda: int(STAMP)


def headers(signature=SIGNATURE, stamp=STAMP, message_id=ID) -> dict:
    return {"webhook-id": message_id, "webhook-timestamp": stamp, "webhook-signature": signature}


def reason(call) -> str:
    try:
        call()
        return "verified"
    except CarbiumError as e:
        assert e.kind == "webhook"
        return e.reason


def check(body=BODY, hdrs=None, secret=SECRET, **options):
    return reason(lambda: verify_webhook_signature(body, hdrs if hdrs is not None else headers(), secret, now=now, **options))


def test_the_published_example_verifies_from_bytes_text_and_mixed_case_headers():
    assert verify_webhook_signature(BODY, headers(), SECRET, now=now) == BODY
    assert verify_webhook_signature(BODY.decode(), {k.title(): [v] for k, v in headers().items()}, SECRET, now=now) == BODY
    assert reason(lambda: verify_webhook(BODY, headers(), SECRET, now=now)) == "bad_json"  # a Carbium body is a list


def test_one_changed_byte_anywhere_fails():
    for position in (0, len(BODY) // 2, len(BODY) - 1):
        flipped = bytearray(BODY)
        flipped[position] ^= 1
        assert check(bytes(flipped)) == "no_match", position
    assert check(BODY + b" ") == "no_match"
    assert check(b'{"test":2432232314}') == "no_match"  # the same JSON, encoded again


def test_a_wrong_or_malformed_secret_and_missing_headers_are_named():
    assert check(secret="whsec_" + base64.b64encode(b"another secret").decode()) == "no_match"
    assert check(secret="MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw") == "bad_secret"
    assert check(secret="whsec_") == "bad_secret"
    assert check(secret="whsec_" + "not base64!") == "bad_secret"
    assert check(hdrs={"webhook-id": ID, "webhook-timestamp": STAMP}) == "missing_header"
    assert check(hdrs=headers(stamp="soon")) == "bad_timestamp"


def test_the_clock_window_is_300_seconds_either_way():
    for skew, expected in ((300, "verified"), (-300, "verified"), (301, "stale"), (-301, "stale")):
        assert reason(lambda: verify_webhook_signature(BODY, headers(), SECRET, now=lambda: int(STAMP) + skew)) == expected
    assert reason(lambda: verify_webhook_signature(BODY, headers(), SECRET, now=lambda: int(STAMP) + 400, tolerance_seconds=600)) == "verified"


def test_a_clock_that_is_missing_not_a_number_or_infinite_is_refused_as_stale():
    for clock in (float("nan"), float("inf"), float("-inf")):
        assert reason(lambda: verify_webhook_signature(BODY, headers(), SECRET, now=lambda: clock)) == "stale", clock
    assert check(tolerance_seconds=float("nan")) == "stale"
    for missing in (None, lambda: None, lambda: "1614265330"):
        assert reason(lambda: verify_webhook_signature(BODY, headers(), SECRET, now=missing)) == "stale", missing


def test_header_names_match_in_any_case_in_a_plain_mapping():
    for name in (str.title, str.upper):
        assert verify_webhook_signature(BODY, {name(k): v for k, v in headers().items()}, SECRET, now=now) == BODY, name


def test_either_entry_matches_while_a_secret_rotates_and_a_list_body_returns_its_events():
    assert check(hdrs=headers("v1,AAAA " + SIGNATURE)) == "verified"
    assert check(hdrs=headers(SIGNATURE + " v1,AAAA")) == "verified"
    assert check(hdrs=headers("v2," + SIGNATURE[3:])) == "no_match"
    body = b'[{"slot": 1, "signature": "abc"}]'
    key = base64.b64decode(SECRET[6:])
    signed = "v1," + base64.b64encode(hmac.new(key, f"{ID}.{STAMP}.".encode() + body, hashlib.sha256).digest()).decode()
    assert verify_webhook(body, headers(signed), SECRET, now=now) == [{"slot": 1, "signature": "abc"}]


def test_the_secret_never_shows_in_an_error():
    with pytest.raises(CarbiumError) as caught:
        verify_webhook_signature(BODY, headers("v1,AAAA"), SECRET, now=now)
    assert SECRET not in str(caught.value) and SECRET[6:] not in repr(caught.value)


def test_a_timestamp_of_hundreds_or_thousands_of_digits_is_refused():
    for digits in (400, 5000):
        assert check(hdrs=headers(stamp="9" * digits)) == "bad_timestamp", digits
