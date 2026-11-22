# carbium

Typed Python client for Carbium's Solana services on [services.carbium.sh](https://carbium.sh): JSON-RPC (every
served method, DAS included), quote and swap (`quote`, `swap`, `prices`, `health` and the REST quote), WebSocket
subscriptions, Carbon Stream (Yellowstone gRPC, an optional extra), the account API, webhook signature checks, the DCA
API on the dex and the token API. Blocking and asyncio clients. Python 3.11 and newer. Types ship with the package.

The package never holds a private key, signs or sends on its own: swap and DCA calls return one unsigned transaction
for your own wallet to sign, and `send_transaction` takes a transaction you signed, as a string: base58, or base64
with `encoding` set to `base64`. Every method it offers is served; what the services do not serve is not here.

```sh
pip install carbium
```

Carbon Stream needs the extra: `pip install 'carbium[stream]'`.

## Quick start

A service key (`c_serv_…`) comes from the dashboard at carbium.sh; put it in `CARBIUM_KEY`. Without one the call
reaches the door and is refused with 401, which the quick start prints.

```python
import os

from carbium import Carbium, CarbiumError

carbium = Carbium(os.environ.get("CARBIUM_KEY"))
try:
    answer = carbium.get_latest_blockhash()
    value = answer["value"]
    print("blockhash", value["blockhash"], "valid until block height", value["lastValidBlockHeight"], "at slot", answer["context"]["slot"])
except CarbiumError as error:
    print(error.kind, error.http_status, error.code, error.reason, error.message)
```

## RPC, DAS and the quote

```python
import os

from carbium import AsyncCarbium, Carbium

carbium = Carbium(os.environ["CARBIUM_KEY"])

# Standard Solana methods take positional params, as on the wire, in snake case.
balance = carbium.get_balance("So11111111111111111111111111111111111111112")
accounts = carbium.get_multiple_accounts([a, b], {"encoding": "base64"})

# DAS methods take their fields by name.
asset = carbium.get_asset(id=mint)

# Any served method by its wire name.
slot = carbium.rpc("getSlot", [{"commitment": "confirmed"}])

# A JSON-RPC batch: 1 to 100 calls, at most two permit methods, DAS only with DAS.
results = carbium.batch([("getSlot", []), ("getHealth", [])])

# A transaction your own wallet signed, sent as base64. It is never sent twice.
signature = carbium.send_transaction(signed_base64, {"encoding": "base64"})

# The REST quote, GET /cq1/quote: every value of the answer is a string.
quote = carbium.cq1_quote(SOL, USDC, 100_000_000, 50)

# The same calls on asyncio.
async def latest_slot() -> int:
    async with AsyncCarbium(os.environ["CARBIUM_KEY"]) as client:
        return await client.get_slot()
```

Token scans (`getTokenAccountsByOwner`, `getTokenAccountsByDelegate`) are billed by rows, up to 100,005 credits;
`getProgramAccounts` costs 505; a `getTransactionsForAddress` walk costs 55 plus 50 per upstream call after the first,
up to 51,255 at `limit` 1000. Name a mint, keep `limit` small, and read the method notes at
https://carbium.sh/skill.md before running one in a loop.

## Errors and retries

Every failure is a `CarbiumError`.

| Field | Meaning |
|---|---|
| `kind` | `auth`, `credits`, `rate_limit`, `request`, `upstream`, `node`, `route`, `transport`, `socket`, `stream`, `account`, `dca`, `tokens`, `webhook` |
| `http_status` | the HTTP status, 0 when nothing answered |
| `code` | the JSON-RPC code, an HTTP API's error string, or a close code |
| `reason` | the door's `error.data.reason`, a close reason, or a gRPC status name |
| `retryable` | whether waiting and sending the same request can change the answer |
| `retry_after_ms` | the server's wait hint |
| `request_id` | `x-request-id`, to quote to support |
| `data` | `error.data` as served |

The RPC client follows the retry policy published in skill.md: it waits the larger of the server's hint
(`retry-after-ms`, else `retryAfterMs` in the error's data, else `Retry-After`) and 250 ms doubling to 4 s, plus up to
250 ms of jitter, and tries at most 4 times unless the published row says otherwise. A hint over `max_hint_ms` (30 s
by default) is not waited for: the error comes back at once with it in `retry_after_ms`. `send_transaction`, 401,
402 (credits exhausted), 403, bad params (apart from `get_transactions_for_address`'s `-32602` "the walk read no
history", which is retried), unknown methods and every answer marked `retryable: false` are never retried.
`Carbium(retry=False)` turns retries off; `max_attempts` lowers the attempt count. The account, DCA and token clients
never retry: they hand back `retryable` and `retry_after_ms`. Keys, session tokens and webhook secrets never appear
in error messages, data, reprs or logs.

## WebSocket subscriptions (asyncio)

```python
import asyncio
import os

from carbium import CarbiumSocket


async def main() -> None:
    async with CarbiumSocket(os.environ["CARBIUM_KEY"], on_reseed=lambda info: print("re-read state", info)) as socket:
        sub = socket.account_subscribe(pubkey, lambda n: print(n["context"]["slot"], n["value"]["lamports"]), {"commitment": "confirmed"})
        await sub.ready
        ...
        await sub.unsubscribe()

asyncio.run(main())
```

All eight served subscriptions have a call (`account_subscribe`, `program_subscribe`, `logs_subscribe`,
`signature_subscribe`, `slot_subscribe`, `root_subscribe`, `slots_updates_subscribe`, `block_subscribe`). The key
travels in the `x-api-key` header, never in the address. The client follows the server's close codes: it reconnects
and binds every live subscription again after 1013, 1012, 1001 and network drops, reconnects once after 1008
`credits_exhausted`, and stops with `on_fatal` on a key refusal (close 1008 `key_…`, or a handshake refused with 401
or 403); a handshake refused with 429 waits its `Retry-After`. Nothing is replayed by the
server after a disconnect: `on_reseed` fires after every reconnect so you re-read what you may have missed
(`info["gap"]` is true after `stream_gap_reseed_required`). `signature_subscribe` fires once; after a reconnect, check
`getSignatureStatuses`.

## Carbon Stream (Yellowstone gRPC, asyncio)

Install the extra beside the package: `pip install 'carbium[stream]'` (grpcio and protobuf; nothing else needs them,
and `import carbium` never loads them). The key needs the grpc scope and a plan with streams.

```python
import os

from carbium import CarbiumStream


async def main() -> None:
    async with await CarbiumStream.connect(os.environ["CARBIUM_KEY"]) as stream:
        live = stream.subscribe_with_reconnect(
            {"transactions": {"pool": {"account_include": [program_id], "vote": False, "failed": False}}},
            on_update=lambda u: handle(u.transaction) if u.HasField("transaction") else None,
            on_reseed=lambda info: reload_over_rpc(),
        )
        ...
        live.close()
        await live.done
```

Filters use the proto field names (`accounts`, `slots`, `transactions`, `transactions_status`, `blocks`,
`blocks_meta`, `entry`, `commitment`, `accounts_data_slice`, `from_slot`), and updates are the Yellowstone
`SubscribeUpdate` messages. `subscribe_with_reconnect` puts the whole filter set in the first message (a filter change
needs a new stream), pings every 10 s, retries with the published waits, re-seeds through `on_reseed` on
`OUT_OF_RANGE`, and resumes from the last slot when the retained floor allows (a resume can replay a slot you already
saw: dedupe on `(slot, signature)`). It stops on a key, plan, argument or credits refusal and raises it from
`live.done`. Fields the service refuses (`token_accounts`, cuckoo filters, `include_update_parent`, `block_footer`)
and a bare transaction filter are caught before any call. The seven unary calls (`get_slot`, `get_block_height`,
`get_latest_blockhash`, `is_blockhash_valid`, `get_version`, `ping`, `subscribe_replay_info`) are on the same object.
`on_update` is called, never awaited, and must not block (a reader that takes no frame for 10 s is cut): a coroutine
function is refused, so hand slow work to your own task or queue. An exception from a handler stops the stream and is
raised from `live.done`. The key travels as `x-token` metadata; gRPC's
own `GRPC_TRACE` logging prints metadata, so leave it off while a key is in use. The generated messages come from the
upstream Yellowstone protos (Apache-2.0): see `carbium/_geyser/NOTICE`.

## Webhooks

```python
import os

from carbium import verify_webhook

# in your HTTPS handler, with the RAW body bytes (never a re-encoded JSON string)
events = verify_webhook(raw_body, request.headers, os.environ["CARBIUM_WEBHOOK_SECRET"])
```

Standard Webhooks: HMAC-SHA256 over `webhook-id.webhook-timestamp.body` with the secret's base64 after `whsec_`,
`v1,` entries in `webhook-signature` (two during a rotation), 300 s of clock tolerance, header names in any case.
Raises a `CarbiumError` of kind `webhook` with reason `missing_header`, `bad_timestamp`, `stale`, `bad_secret`,
`no_match` or `bad_json`. Dedupe on `webhook-id`: it stays the same across retries.

## Account API

An account API key (`c_acct_…`, made on the dashboard) runs the account by API: service keys, webhooks, usage and
records. Put it in `CARBIUM_ACCOUNT_KEY`.

```python
import os

from carbium import CarbiumAccount

account = CarbiumAccount(os.environ["CARBIUM_ACCOUNT_KEY"])
keys = account.keys_list()
created = account.webhooks_create({"key_id": keys[0]["key_id"], "url": "https://hooks.example.com/carbium", "addresses": [wallet]})
result = account.webhooks_test(created["webhook"]["webhook_id"])  # read result["outcome"]
```

Every operation of the account API has one call, generated from its description, 53 in all. The names are the
TypeScript SDK's in snake case: `keys_list`, `keys_create`, `keys_rotate`, `webhooks_test`, `usage_daily`,
`account_balance`, `self`, `activity`, and so on. Path ids are integers. A call that takes a session (made by a
sign-in) is refused before it is sent when the client holds only an account key: pass `session_token=` for those.
The public calls (`terms`, `plans`, `signup_challenge`, `pin_start`, `pin_verify`, `wallet_login`) send no
credential.

## DCA API

```python
from carbium import CarbiumDca

dca = CarbiumDca()
quote = dca.quote({"inputMint": USDC, "outputMint": SOL, "amount": "1000000"})
build = dca.open({"owner": owner, "inputMint": USDC, "outputMint": SOL, "cycles": 10, "amountPerCycle": "1000000", "frequency": "daily"})
# build["txn_base64"] is unsigned: sign it with the owner's wallet, then send it yourself or relay it:
sent = dca.send({"txn_base64": signed_base64})
```

Reads: `quote`, `positions`, `history`, `position`, `health`. Builders: `open`, `update`, `withdraw`, `close`. `send`
relays bytes the owner signed, once. An answer with `ok: false` is an error even at HTTP 200, and the codes
`rate_limited`, `rate_limited_open`, `rpc_unavailable`, `quote_unavailable` and `door_busy` may be tried again at any
status. Errors are kind `dca` with the API's `err` string as `code`.

## Token API

```python
from carbium import CarbiumTokens

tokens = CarbiumTokens()
sol = tokens.lookup("So11111111111111111111111111111111111111112")  # None when the registry does not hold it
top = tokens.search("", limit=20)
found = tokens.batch(mints)  # 1 to 500 mints
src = tokens.image_url(mint)
```

Keyless, 300 requests a minute per client. The four public routes: `search`, `lookup` (None until a mint is indexed),
`batch` (unknown mints are queued by the service) and `image_url` (always answers, with a fallback image).

## Limits checked before sending

Body 51,200 bytes; a batch holds 1 to 100 calls with at most two permit methods, DAS only with DAS; the scans,
`getSupply`, `getLargestAccounts`, the fee extras, `getTransactionsForAddress`, `prices`, `health`, `quote` and `swap`
never batch; a method the services do not serve is refused before it is sent; token `batch` takes up to 500 mints.

## Contributing

Enable the leak check once per clone: `git config core.hooksPath hooks`.

MIT. Docs: https://carbium.sh/docs and https://carbium.sh/skill.md.
