#!/usr/bin/env python3
"""Writes src/carbium/_account_gen.py from the account API's OpenAPI description: a TypedDict per schema, the route
table, and one typed call per operation (sync and async). Every operation must have a name in NAMES below, and every
name an operation: a new route cannot slip in unnamed. Usage: python scripts/generate_account.py <openapi.json> [out.py]"""
from __future__ import annotations

import json
import keyword
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
doc = json.loads(Path(sys.argv[1]).read_text())
target = Path(sys.argv[2]) if len(sys.argv) > 2 else ROOT / "src/carbium/_account_gen.py"
schemas: dict[str, dict] = doc["components"]["schemas"]
classes: dict[str, str] = {}  # name -> source of a TypedDict, in the order first needed

# The call names, as the TypeScript SDK names them, in snake case. A route the TypeScript SDK does not name is named in
# the same style. POST /v1/auth/wallet is walletChallenge and walletVerify there: one route here, one call.
NAMES = {
    "GET /v1/keys": "keys_list",
    "POST /v1/keys": "keys_create",
    "POST /v1/keys/{id}/rotate": "keys_rotate",
    "POST /v1/keys/{id}/revoke": "keys_revoke",
    "DELETE /v1/keys/{id}": "keys_delete",
    "POST /v1/keys/{id}/name": "keys_rename",
    "PATCH /v1/keys/{id}/scope": "keys_set_scope",
    "PATCH /v1/keys/{id}/caps": "keys_set_caps",
    "PATCH /v1/keys/{id}/allowlist": "keys_set_allowlist",
    "PATCH /v1/keys/{id}/limits": "keys_set_limits",
    "POST /v1/keys/{id}/secret": "reveal_key_secret",
    "GET /v1/webhooks": "webhooks_list",
    "POST /v1/webhooks": "webhooks_create",
    "GET /v1/webhooks/{id}": "webhooks_get",
    "PATCH /v1/webhooks/{id}": "webhooks_update",
    "DELETE /v1/webhooks/{id}": "webhooks_delete",
    "GET /v1/webhooks/{id}/deliveries": "webhooks_deliveries",
    "POST /v1/webhooks/{id}/rotate": "webhooks_rotate_secret",
    "POST /v1/webhooks/{id}/test": "webhooks_test",
    "GET /v1/usage": "usage_daily",
    "GET /v1/keys/usage": "usage_per_key",
    "GET /v1/keys/load": "usage_load",
    "GET /v1/usage/hourly": "usage_hourly",
    "GET /v1/usage/latency": "usage_latency",
    "GET /v1/account": "account_get",
    "DELETE /v1/account": "account_delete",
    "GET /v1/account/deletion": "account_deletion",
    "GET /v1/balance": "account_balance",
    "GET /v1/invoices": "account_invoices",
    "GET /v1/payments": "account_payments",
    "GET /v1/payments/{id}/document": "account_payment_document",
    "GET /v1/modules": "account_modules",
    "GET /v1/billing/crypto-intent/{id}": "account_crypto_intent",
    "POST /v1/modules/{module}": "modules_enable_free",
    "DELETE /v1/modules/{module}": "modules_remove",
    "GET /v1/api-key": "self",
    "GET /v1/account/api-keys/{id}/activity": "activity",
    "POST /v1/account/api-keys": "create_account_api_key",
    "GET /v1/account/api-keys": "list_account_api_keys",
    "DELETE /v1/account/api-keys/{id}": "revoke_account_api_key",
    "GET /v1/terms": "terms",
    "GET /v1/plans": "plans",
    "GET /v1/signup/challenge": "signup_challenge",
    "POST /v1/login/pin": "pin_start",
    "POST /v1/login/pin/verify": "pin_verify",
    "POST /v1/auth/wallet": "wallet_login",
    "POST /v1/account/link-wallet": "wallet_link",
    "POST /v1/logout": "logout",
    "POST /v1/billing/checkout": "billing_checkout",
    "POST /v1/billing/crypto-intent": "billing_crypto_intent",
    "POST /v1/billing/portal": "billing_portal",
    "POST /v1/billing/renewal": "billing_cancel_renewal",
    "DELETE /v1/billing/renewal": "billing_resume_renewal",
}
CREDENTIALS = {"accountApiKey": "key", "session": "session"}  # a requirement of {} means no credential: "none"


def fail(message: str) -> None:
    sys.exit(f"generate_account: {message}")


def plain(name: str) -> bool:
    return name.isidentifier() and not keyword.iskeyword(name)


def pascal(name: str) -> str:
    return re.sub(r"(?:^|[^A-Za-z0-9])([A-Za-z0-9])", lambda m: m.group(1).upper(), name[:1].upper() + name[1:])


def union(parts: list[str]) -> str:
    seen = list(dict.fromkeys(parts))
    return " | ".join(seen) if seen else "Any"


def ref_name(ref: str) -> str:
    return pascal(ref.rsplit("/", 1)[1])


def flatten(schema: dict) -> dict:
    """An allOf as one object: the properties of every part, the later part winning, and every part's required."""
    if "allOf" not in schema:
        return schema
    properties: dict = {}
    required: list = []
    for part in schema["allOf"]:
        part = flatten(schemas[part["$ref"].rsplit("/", 1)[1]] if "$ref" in part else part)
        properties.update(part.get("properties", {}))
        required += part.get("required", [])
    return {"type": "object", "properties": properties, "required": list(dict.fromkeys(required))}


def typed_dict(name: str, schema: dict) -> str:
    """Records a TypedDict for an object schema and returns its name."""
    if name in classes:
        return name
    classes[name] = ""  # reserve the name: a type may refer to itself
    schema = flatten(schema)
    required = set(schema.get("required", []))
    fields = {key: py_type(sub, name + pascal(key)) for key, sub in schema["properties"].items()}
    if all(plain(key) for key in fields):
        body = "\n".join(f"    {key}: {t if key in required else f'NotRequired[{t}]'}" for key, t in fields.items())
        classes[name] = f"class {name}(TypedDict):\n{body or '    pass'}\n"
    else:  # a key Python cannot spell as a name: the functional form, with its types as strings
        body = ", ".join(f'"{key}": ' + (f'"{t}"' if key in required else f'NotRequired["{t}"]') for key, t in fields.items())
        classes[name] = f'{name} = TypedDict("{name}", {{{body}}})\n'
    return name


def py_type(schema: dict | None, hint: str) -> str:
    """The Python type for a JSON Schema; `hint` names a TypedDict if an inline object needs one."""
    if not schema:
        return "Any"
    if "$ref" in schema:
        return ref_name(schema["$ref"])
    if "enum" in schema:
        return "Literal[" + ", ".join(repr(v) for v in schema["enum"]) + "]"
    for key in ("anyOf", "oneOf"):
        if key in schema:
            return union([py_type(sub, f"{hint}{i}") for i, sub in enumerate(schema[key])])
    if "allOf" in schema:
        return typed_dict(hint, schema)
    kind = schema.get("type")
    if isinstance(kind, list):
        return union([py_type({**schema, "type": one}, hint) for one in kind])
    simple = {"string": "str", "integer": "int", "number": "Number", "boolean": "bool", "null": "None"}
    if kind in simple:
        return simple[kind]
    if kind == "array":
        return f"list[{py_type(schema.get('items'), hint + 'Item')}]"
    if kind == "object" or "properties" in schema:
        if schema.get("properties"):
            return typed_dict(hint, schema)
        extra = schema.get("additionalProperties")
        return f"dict[str, {py_type(extra, hint + 'Value')}]" if isinstance(extra, dict) else "dict[str, Any]"
    return "Any"


for name, schema in schemas.items():
    if pascal(name) in classes:
        fail(f"two schemas are both named {pascal(name)}")
    if flatten(schema).get("type") != "object":
        fail(f"schema {name} is not an object")
    typed_dict(pascal(name), schema)

operations = []  # (name, verb, path, operation)
for path, item in doc["paths"].items():
    for verb, op in item.items():
        if verb not in ("get", "post", "put", "patch", "delete"):
            continue
        route = f"{verb.upper()} {path}"
        if route not in NAMES:
            fail(f"{route} has no name in NAMES: name it before it can be generated")
        if "security" not in op:
            fail(f"{route} states no security: say which credentials it takes")
        operations.append((NAMES[route], verb.upper(), path, op))
named = {f"{verb} {path}" for _, verb, path, _ in operations}
for route in NAMES:
    if route not in named:
        fail(f"NAMES has {route}, which the description does not serve")
if len(set(NAMES.values())) != len(NAMES):
    fail("two routes share a name in NAMES")


def takes(op: dict) -> tuple[str, ...]:
    out = []
    for requirement in op["security"] or [{}]:
        if len(requirement) > 1:
            fail("a requirement that needs two credentials at once is not supported")
        scheme = next(iter(requirement), None)
        if scheme is not None and scheme not in CREDENTIALS:
            fail(f"unknown security scheme {scheme}")
        out.append(CREDENTIALS.get(scheme, "none"))
    return tuple(dict.fromkeys(out))


def result_type(name: str, op: dict) -> str:
    for status in ("200", "201"):
        content = op["responses"].get(status, {}).get("content", {})
        if "application/json" in content:
            return py_type(content["application/json"]["schema"], pascal(name) + "Result")
    return "None"


def docstring(verb: str, path: str, op: dict) -> str:
    text = ". ".join(t.rstrip(".") for t in (op.get("summary"), op.get("description")) if t) + "."
    if '"""' in text or "\\" in text:
        fail(f"{verb} {path}: its text cannot go in a docstring as it is")
    return f'"""{verb} {path}: {text}"""' if text else f'"""{verb} {path}"""'


def method_source(name: str, verb: str, path: str, op: dict, is_async: bool) -> str:
    params = op.get("parameters", [])
    args, query = [], []
    for p in params:
        if p["in"] == "path":
            args.append(f"{p['name']}: {py_type(p['schema'], pascal(name) + pascal(p['name']))}")
    url = path
    for p in params:
        if p["in"] == "path":
            check = "_int" if p["schema"].get("type") == "integer" else "_text"
            url = url.replace("{" + p["name"] + "}", "{" + f"{check}({p['name']})" + "}")
    url = f'f"{url}"' if url != path else f'"{url}"'
    body = None
    if "requestBody" in op:
        schema = op["requestBody"]["content"]["application/json"]["schema"]
        kind = py_type(schema, pascal(name) + "Body")
        if flatten(schemas[schema["$ref"].rsplit("/", 1)[1]] if "$ref" in schema else schema).get("required"):
            args.append(f"body: {kind}")
            body = "body"
        else:
            args.append(f"body: {kind} | None = None")
            body = "{} if body is None else body"
    keyword_only = []
    for p in params:
        if p["in"] == "query":
            arg = p["name"] if plain(p["name"]) else p["name"] + "_"
            keyword_only.append(f"{arg}: {py_type(p['schema'], pascal(name) + pascal(p['name']))} | None = None")
            query.append(f'"{p["name"]}": {arg}')
        elif p["in"] != "path":
            fail(f"{verb} {path}: a {p['in']} parameter is not supported")
    signature = ", ".join(["self", *args, *(["*", *keyword_only] if keyword_only else [])])
    extra = "".join([f", query={{{', '.join(query)}}}" if query else "", f", body={body}" if body else ""])
    head, call = ("async def", "await self._request") if is_async else ("def", "self._request")
    return (
        f"    {head} {name}({signature}) -> {result_type(name, op)}:\n"
        f"        {docstring(verb, path, op)}\n"
        f'        return {call}("{name}", {url}{extra})\n'
    )


sync_methods = "\n".join(method_source(n, v, p, op, False) for n, v, p, op in operations)
async_methods = "\n".join(method_source(n, v, p, op, True) for n, v, p, op in operations)
table = "\n".join(f'    "{n}": Operation("{v}", "{p}", {takes(op)!r}),' for n, v, p, op in operations)

out = f'''"""Generated by scripts/generate_account.py from the account API's OpenAPI description. Do not edit: an operation
the account API does not serve is not offered. Results are the JSON the service sends, typed; nothing is validated."""
from __future__ import annotations

from typing import Any, Literal, NamedTuple, NotRequired, TypeAlias, TypedDict
from urllib.parse import quote

from .errors import CarbiumError

Number: TypeAlias = int | float


{chr(10).join(classes.values())}

class Operation(NamedTuple):
    verb: str
    path: str
    takes: tuple[str, ...]  # the credentials it takes: "key" (an account key), "session", "none" (public)


OPERATIONS: dict[str, Operation] = {{
{table}
}}


def _int(value: int) -> str:
    """A path id: an integer, never text that could change the path."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise CarbiumError("request", f"a path id must be an integer, not {{type(value).__name__}}")
    return str(value)


def _text(value: str) -> str:
    """A path name such as a module: one path segment, encoded."""
    if not isinstance(value, str) or not value:
        raise CarbiumError("request", "a path name must be a non-empty string")
    return quote(value, safe="")


class AccountMethods:
    """One typed call per operation. The client that mixes this in supplies _request."""

    def _request(self, name: str, path: str, *, query: dict[str, Any] | None = None, body: Any = None) -> Any:
        raise NotImplementedError

{sync_methods}

class AsyncAccountMethods:
    """The same calls for the async client."""

    async def _request(self, name: str, path: str, *, query: dict[str, Any] | None = None, body: Any = None) -> Any:
        raise NotImplementedError

{async_methods}'''
target.write_text(out)
print(f"{target.name}: {len(operations)} operations, {len(classes)} TypedDicts")
