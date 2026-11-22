"""Every served method has a typed call, and nothing else has one. Each call is made through the client's own path
and the name it puts on the wire is read back, so a call that exists but sends the wrong name is caught too."""
from __future__ import annotations

import asyncio
import inspect
import json
import re
import subprocess
import sys
import typing
from pathlib import Path

from carbium import (METHODS, SUBSCRIPTIONS, AsyncCarbium, AsyncCarbiumAccount, AsyncCarbiumDca, AsyncCarbiumTokens, Carbium, CarbiumAccount, CarbiumDca,
                     CarbiumError, CarbiumSocket, CarbiumTokens)

from conftest import FakeSocketServer

SERVED = json.loads((Path(__file__).parent.parent / "spec/served.json").read_text())


def arguments(call) -> dict:
    """A placeholder for every required argument of a typed call."""
    wanted = [p for p in inspect.signature(call).parameters.values() if p.default is inspect.Parameter.empty]
    return {p.name: "x" for p in wanted}


def wire_names(methods: dict, client_type, run) -> list[tuple[str, str, str]]:
    """For each table entry: its wire name, its typed call, and the method name that call actually sends."""
    out: list[tuple[str, str, str]] = []
    client = client_type("")
    for wire, meta in methods.items():
        seen: list[str] = []

        def record(method, params, seen=seen):
            seen.append(method)
            return None

        async def record_async(method, params, seen=seen):
            seen.append(method)
            return None

        client._transport.call = record_async if client_type is AsyncCarbium else record
        call = getattr(client, meta.python)
        run(call(**arguments(call)))
        out.append((wire, meta.python, seen[0]))
    return out


def problems(methods: dict, served: list[str], client_type=Carbium, run=lambda value: value) -> list[str]:
    out = [f"served but has no typed call: {name}" for name in served if name not in methods]
    out += [f"has a typed call but is not served: {name}" for name in methods if name not in served]
    present = {name: meta for name, meta in methods.items() if hasattr(client_type, meta.python)}
    out += [f"listed but the call is missing: {name}" for name in methods if name not in present]
    out += [f"{wire}: its call {python} sends {sent}" for wire, python, sent in wire_names(present, client_type, run) if sent != wire]
    return out


def test_every_served_method_has_a_typed_call_and_nothing_else_does():
    less = {name: meta for name, meta in METHODS.items() if name != "getSlot"}
    assert problems(less, SERVED["rpc"]) == ["served but has no typed call: getSlot"]
    more = {**METHODS, "notAMethod": METHODS["getSlot"]._replace(python="get_slot")}
    assert "has a typed call but is not served: notAMethod" in problems(more, SERVED["rpc"])
    crossed = {**METHODS, "getSlot": METHODS["getSlot"]._replace(python="get_block_height")}
    assert problems(crossed, SERVED["rpc"]) == ["getSlot: its call get_block_height sends getBlockHeight"]
    assert problems(METHODS, SERVED["rpc"]) == []
    assert len(METHODS) == len(SERVED["rpc"])


def test_the_async_client_has_the_same_calls():
    assert problems(METHODS, SERVED["rpc"], AsyncCarbium, asyncio.run) == []


def test_the_subscription_names_are_the_served_ones():
    assert sorted(SUBSCRIPTIONS) == sorted(SERVED["ws"])


# The HTTP groups: each typed call is made through the client's real path against a planted server, and the verb and
# path it sent are read back and compared with the served list both ways.
DESCRIBED = json.loads((Path(__file__).parent.parent / "spec/account-openapi.json").read_text())
ACCOUNT_ROUTES = {f"{verb.upper()} {path}" for path, item in DESCRIBED["paths"].items() for verb in item if verb in ("get", "post", "put", "patch", "delete")}
PLACEHOLDERS = {"id": 7, "module": "mod1", "body": {}, "request": {}, "owner": "owner1", "address": "addr1", "mint": "mint1", "mints": ["m"], "q": "q",
                "src_mint": "mint1", "dst_mint": "mint2", "amount_in": 1, "slippage_bps": 50}
TEMPLATES = [(r"/7(?=/|$)", "/{id}"), (r"/mod1(?=/|$)", "/{module}"), (r"/position/addr1$", "/position/{address}"), (r"/(tokens|img)/mint1$", r"/\1/:mint")]


def calls_of(client_type, plumbing=("close", "aclose")) -> list[str]:
    return sorted(name for name, value in inspect.getmembers(client_type, callable) if not name.startswith("_") and name not in plumbing)


def template(route: str, prefix: str = "") -> str:
    verb, path = route.split("?")[0].split(" ", 1)
    path = path[len(prefix):] if path.startswith(prefix) else path
    for pattern, back in TEMPLATES:
        path = re.sub(pattern, back, path)
    return f"{verb} {path}"


async def made(call) -> None:
    """Makes one call to the end: awaits it, and reads a stream's first item."""
    result = call()
    if inspect.isawaitable(result):
        result = await result
    if hasattr(result, "__anext__"):
        async for _ in result:
            break
    elif hasattr(result, "__next__"):
        for _ in result:
            break
    return result


async def http_routes(planted, clients, plumbing=("close", "aclose"), prefix: str = "", names: list[str] | None = None) -> dict[str, str]:
    """For each typed call: the route it put on the wire, as "VERB template". A call the first client's credential
    cannot make is made with the next; a call that builds an address without sending it is read from the address."""
    out = {}
    for name in calls_of(type(clients[0]), plumbing) if names is None else names:
        for client in clients:
            before = len(planted.requests)
            call = getattr(client, name)
            wanted = {p.name: PLACEHOLDERS[p.name] for p in inspect.signature(call).parameters.values() if p.default is inspect.Parameter.empty}
            try:
                result = await made(lambda: call(**wanted))
            except CarbiumError as e:
                if e.kind == "auth" and len(planted.requests) == before:
                    continue  # this credential cannot make the call: the next one can
                raise
            if len(planted.requests) > before:
                out[name] = template(planted.routes[before], prefix)
            elif isinstance(result, str) and result.startswith(planted.url):
                out[name] = template("GET " + result[len(planted.url):], prefix)
            else:
                out[name] = "nothing sent"
            break
    return out


def route_problems(routes: dict[str, str], served) -> list[str]:
    sent = list(routes.values())
    out = [f"served but no call sends it: {route}" for route in sorted(set(served) - set(sent))]
    out += [f"{name} sends {route}, which is not served" for name, route in routes.items() if route not in served]
    out += [f"{route} is sent by {sent.count(route)} calls" for route in sorted(set(sent)) if sent.count(route) > 1]
    return out


def account_clients(planted, client_type) -> list:
    return [client_type("c_acct_" + "Count1234", base_url=planted.url), client_type(session_token="st" + "Count1234", base_url=planted.url)]


def test_every_account_operation_has_a_typed_call_and_nothing_else_does(planted):
    each = lambda client_type: route_problems(asyncio.run(http_routes(planted, account_clients(planted, client_type))), ACCOUNT_ROUTES)
    assert each(type("Less", (CarbiumAccount,), {"logout": None})) == ["served but no call sends it: POST /v1/logout"]
    peek = lambda self: self._request("keys_list", "/v1/keys/peek")
    assert each(type("More", (CarbiumAccount,), {"keys_peek": peek})) == ["keys_peek sends GET /v1/keys/peek, which is not served"]
    revoke = lambda self, id: self._request("keys_revoke", f"/v1/keys/{id}/rotate")
    assert each(type("Crossed", (CarbiumAccount,), {"keys_revoke": revoke})) == [
        "served but no call sends it: POST /v1/keys/{id}/revoke", "POST /v1/keys/{id}/rotate is sent by 2 calls"]
    routes = asyncio.run(http_routes(planted, account_clients(planted, CarbiumAccount)))
    assert route_problems(routes, ACCOUNT_ROUTES) == [] and len(routes) == len(ACCOUNT_ROUTES) == 53


def test_the_async_account_client_has_the_same_calls(planted):
    routes = asyncio.run(http_routes(planted, account_clients(planted, AsyncCarbiumAccount)))
    assert route_problems(routes, ACCOUNT_ROUTES) == [] and sorted(routes) == calls_of(CarbiumAccount)


def dca_problems(planted, client_type) -> list[str]:
    client = client_type(base_url=planted.url + "/api/dca")
    return route_problems(asyncio.run(http_routes(planted, [client], plumbing=("aclose",), prefix="/api/dca")), SERVED["dca"])


def tokens_problems(planted, client_type) -> list[str]:
    return route_problems(asyncio.run(http_routes(planted, [client_type(base_url=planted.url)])), SERVED["tokens"])


def test_every_served_dca_route_has_a_typed_call_and_nothing_else_does(planted):
    assert dca_problems(planted, type("Less", (CarbiumDca,), {"withdraw": None})) == ["served but no call sends it: POST /withdraw"]
    peek = lambda self: self._run(self._exchange("peek", "GET", "/peek"))
    assert dca_problems(planted, type("More", (CarbiumDca,), {"peek": peek})) == ["peek sends GET /peek, which is not served"]
    history = lambda self, owner: self._run(self._positions(owner, None))
    assert dca_problems(planted, type("Crossed", (CarbiumDca,), {"history": history})) == [
        "served but no call sends it: GET /history", "GET /positions is sent by 2 calls"]
    assert dca_problems(planted, CarbiumDca) == [] and len(calls_of(CarbiumDca, ("aclose",))) == len(SERVED["dca"]) == 10
    assert dca_problems(planted, AsyncCarbiumDca) == [] and calls_of(AsyncCarbiumDca, ("aclose",)) == calls_of(CarbiumDca, ("aclose",))


def test_every_served_token_route_has_a_typed_call_and_nothing_else_does(planted):
    assert tokens_problems(planted, type("Less", (CarbiumTokens,), {"search": None})) == ["served but no call sends it: GET /tokens"]
    peek = lambda self: self._json(*(lambda x: (x, self._send(x)))(self._exchange("peek", "GET", "/peek")))
    assert tokens_problems(planted, type("More", (CarbiumTokens,), {"peek": peek})) == ["peek sends GET /peek, which is not served"]
    lookup = lambda self, mint: self._json(*(lambda x: (x, self._send(x)))(self._exchange("lookup", "GET", "/tokens")))
    assert tokens_problems(planted, type("Crossed", (CarbiumTokens,), {"lookup": lookup})) == [
        "served but no call sends it: GET /tokens/:mint", "GET /tokens is sent by 2 calls"]
    assert tokens_problems(planted, CarbiumTokens) == [] and len(calls_of(CarbiumTokens)) == len(SERVED["tokens"]) == 4
    assert tokens_problems(planted, AsyncCarbiumTokens) == [] and calls_of(AsyncCarbiumTokens) == calls_of(CarbiumTokens)


async def socket_names(client_type) -> dict[str, str]:
    """For each typed subscribe call: the method name it put in its frame, read back from a planted socket server."""
    async with FakeSocketServer() as server:
        socket = client_type("c_serv_" + "Count1234", url=server.url, _random=lambda: 0)
        out = {}
        for name in calls_of(client_type, ("close", "subscribe")):
            call = getattr(socket, name)
            wanted = ["x" for p in inspect.signature(call).parameters.values() if p.default is inspect.Parameter.empty and p.name != "on_notification"]
            before = len(server.received)
            try:
                await call(*wanted, on_notification=lambda n: None).ready
            except CarbiumError as e:
                out[name] = f"nothing ({e.message})"
                continue
            out[name] = server.received[before]["method"]
        await socket.close()
    return out


def test_every_served_subscription_has_a_typed_call_and_nothing_else_does():
    each = lambda client_type: route_problems(asyncio.run(socket_names(client_type)), SERVED["ws"])
    assert each(type("Less", (CarbiumSocket,), {"root_subscribe": None})) == ["served but no call sends it: rootSubscribe"]
    vote = lambda self, on_notification: self.subscribe("voteSubscribe", [], on_notification)
    assert each(type("More", (CarbiumSocket,), {"vote_subscribe": vote})) == [
        "vote_subscribe sends nothing (voteSubscribe is not a served subscription), which is not served"]
    root = lambda self, on_notification: self.subscribe("slotSubscribe", [], on_notification)
    assert each(type("Crossed", (CarbiumSocket,), {"root_subscribe": root})) == ["served but no call sends it: rootSubscribe", "slotSubscribe is sent by 2 calls"]
    names = asyncio.run(socket_names(CarbiumSocket))
    assert route_problems(names, SERVED["ws"]) == [] and len(names) == len(SERVED["ws"]) == 8


def rest_problems(planted, client_type) -> list[str]:
    """The REST routes of the services endpoint: every public call of the RPC client that is not a JSON-RPC method."""
    rpc = {meta.python for meta in METHODS.values()}
    names = [name for name in calls_of(client_type, ("close", "aclose", "rpc", "batch")) if name not in rpc]
    routes = asyncio.run(http_routes(planted, [client_type("c_serv_" + "Count1234", endpoint=planted.url)], names=names))
    return route_problems(routes, SERVED["rest"])


def test_every_served_rest_route_has_a_typed_call_and_nothing_else_does(planted):
    assert rest_problems(planted, type("Less", (Carbium,), {"cq1_quote": None})) == ["served but no call sends it: GET /cq1/quote"]
    route = lambda self: self._transport.rest("/cq1/route", {}, "cq1_route")
    assert rest_problems(planted, type("More", (Carbium,), {"cq1_route": route})) == ["cq1_route sends GET /cq1/route, which is not served"]
    assert rest_problems(planted, Carbium) == [] and rest_problems(planted, AsyncCarbium) == [] and len(SERVED["rest"]) == 1


def test_every_typed_call_and_every_typeddict_resolves_its_hints():
    import carbium._account_gen, carbium._gen, carbium.client, carbium.dca, carbium.tokens

    failed = []
    for module in (carbium._gen, carbium._account_gen, carbium.client, carbium.dca, carbium.tokens):
        for name, value in vars(module).items():
            if typing.is_typeddict(value):
                try:
                    typing.get_type_hints(value)
                except Exception as e:
                    failed.append(f"{module.__name__}.{name}: {e}")
    for client_type in (Carbium, AsyncCarbium, CarbiumAccount, AsyncCarbiumAccount, CarbiumDca, AsyncCarbiumDca, CarbiumTokens, AsyncCarbiumTokens, CarbiumSocket):
        for name, function in inspect.getmembers(client_type, inspect.isfunction):
            if not name.startswith("_"):
                try:
                    typing.get_type_hints(function)
                except Exception as e:
                    failed.append(f"{client_type.__name__}.{name}: {e}")
    assert failed == []


def test_the_committed_rpc_calls_are_what_the_generator_makes_from_the_spec(tmp_path):
    root = Path(__file__).parent.parent
    made = subprocess.run([sys.executable, str(root / "scripts/generate.py"), str(root / "spec/openrpc.json"), str(tmp_path / "out.py")], capture_output=True, text=True)
    assert made.returncode == 0, made.stderr
    assert (tmp_path / "out.py").read_text() == (root / "src/carbium/_gen.py").read_text()
