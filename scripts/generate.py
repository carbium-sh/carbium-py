#!/usr/bin/env python3
"""Writes src/carbium/_gen.py from the spec repo's openrpc.json: the result and config types, the method table,
and one typed call per served method (sync and async). Usage: python scripts/generate.py <openrpc.json> [out.py]"""
from __future__ import annotations

import json
import keyword
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
doc = json.loads(Path(sys.argv[1]).read_text())
target = Path(sys.argv[2]) if len(sys.argv) > 2 else ROOT / "src/carbium/_gen.py"
schemas: dict[str, dict] = doc["components"]["schemas"]
classes: dict[str, str] = {}  # name -> source of a TypedDict, in the order first needed


def plain(name: str) -> bool:
    return name.isidentifier() and not keyword.iskeyword(name)


def pascal(name: str) -> str:
    return re.sub(r"(?:^|[^A-Za-z0-9])([A-Za-z0-9])", lambda m: m.group(1).upper(), name[:1].upper() + name[1:])


def snake(name: str) -> str:
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name).lower()


def union(parts: list[str]) -> str:
    seen = list(dict.fromkeys(parts))
    return " | ".join(seen) if seen else "Any"


def typed_dict(name: str, schema: dict) -> str:
    """Records a TypedDict for an object schema and returns its name."""
    if name in classes:
        return name
    classes[name] = ""  # reserve the name: a type may refer to itself
    required = set(schema.get("required", []))
    fields = {key: py_type(sub, name + pascal(key)) for key, sub in schema["properties"].items()}
    wrap = lambda key, t: t if key in required else f"NotRequired[{t}]"
    if all(plain(key) for key in fields):
        body = "\n".join(f"    {key}: {wrap(key, t)}" for key, t in fields.items())
        classes[name] = f"class {name}(TypedDict):\n{body or '    pass'}\n"
    else:  # a key Python cannot spell as a name: the functional form, with its types as strings
        quote = lambda key, t: f'NotRequired["{t}"]' if key not in required else f'"{t}"'
        body = ", ".join(f'"{key}": {quote(key, t)}' for key, t in fields.items())
        classes[name] = f'{name} = TypedDict("{name}", {{{body}}})\n'
    return name


def py_type(schema: dict | None, hint: str) -> str:
    """The Python type for a JSON Schema; `hint` names a TypedDict if an inline object needs one."""
    if not schema:
        return "Any"
    if "$ref" in schema:
        return schema["$ref"].rsplit("/", 1)[1]
    if "const" in schema:
        return f"Literal[{schema['const']!r}]"
    if "enum" in schema:
        return "Literal[" + ", ".join(repr(v) for v in schema["enum"]) + "]"
    if "anyOf" in schema:
        return union([py_type(sub, f"{hint}{i}") for i, sub in enumerate(schema["anyOf"])])
    kind = schema.get("type")
    if isinstance(kind, list):
        return union([py_type({**schema, "type": one}, hint) for one in kind])
    if kind == "string":
        return "str"
    if kind == "integer":
        return "int"
    if kind == "number":
        return "Number"
    if kind == "boolean":
        return "bool"
    if kind == "null":
        return "None"
    if kind == "array":
        items = schema.get("items")
        if isinstance(items, list):  # a fixed-length array arrives as a JSON list, so it is typed as one
            return f"list[{union([py_type(sub, f'{hint}{i}') for i, sub in enumerate(items)])}]"
        return f"list[{py_type(items, hint + 'Item')}]"
    if kind == "object" or "properties" in schema:
        if schema.get("properties"):
            return typed_dict(hint, schema)
        extra = schema.get("additionalProperties")
        return f"dict[str, {py_type(extra, hint + 'Value')}]" if isinstance(extra, dict) else "dict[str, Any]"
    return "Any"


aliases: dict[str, str] = {}
for name, schema in schemas.items():
    if schema.get("type") == "object" and schema.get("properties"):
        typed_dict(name, schema)
    else:
        aliases[name] = py_type(schema, name + "Value")


def in_order(aliases: dict[str, str]) -> list[str]:
    """The aliases, each after the ones it names: they are real values, evaluated when the module loads."""
    out: list[str] = []

    def visit(name: str, path: tuple[str, ...] = ()) -> None:
        if name in out:
            return
        if name in path:
            sys.exit(f"generate: the alias {name} refers to itself")
        for ref in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", aliases[name]):
            if ref in aliases and ref != name:
                visit(ref, path + (name,))
        out.append(name)

    for name in aliases:
        visit(name)
    return out


def method_source(method: dict, is_async: bool) -> str:
    name = method["name"]
    result = py_type(method["result"]["schema"], pascal(name) + "Result")
    by_name = method["paramStructure"] == "by-name"
    args, passed = [], []
    for param in method["params"]:
        arg = param["name"] if by_name else snake(param["name"])
        if not plain(arg):
            arg += "_"
        kind = py_type(param["schema"], pascal(name) + pascal(param["name"]))
        args.append(f"{arg}: {kind}" if param["required"] else f"{arg}: {kind} | None = None")
        passed.append(f'"{param["name"]}": {arg}' if by_name else arg)
    signature = ", ".join(["self"] + (["*"] if by_name and args else []) + args)
    if by_name:
        params = f"_named({{{', '.join(passed)}}})" if passed else "None"
    else:
        params = f"_positional({', '.join(passed)})"
    head, call = ("async def", "await self._call") if is_async else ("def", "self._call")
    return f'    {head} {snake(name)}({signature}) -> {result}:\n        return {call}("{name}", {params})\n'


rpc = [m for m in doc["methods"] if "servers" not in m]
sync_methods = "\n".join(method_source(m, False) for m in rpc)
async_methods = "\n".join(method_source(m, True) for m in rpc)
table = "\n".join(
    f'    "{m["name"]}": Method("{m["tags"][0]["name"]}", "{m["paramStructure"]}", "{m["x-batch"]}", {m["x-permit"]}, "{snake(m["name"])}"),'
    for m in rpc
)
subscriptions = ", ".join(f'"{m["name"]}"' for m in doc["methods"] if "servers" in m and m["name"].endswith("Subscribe"))

out = f'''"""Generated by scripts/generate.py from the spec repo's openrpc.json. Do not edit: a method the services do
not serve is not offered. Results are the JSON the service sends, typed; nothing is validated or renamed."""
from __future__ import annotations

from typing import Any, Literal, NamedTuple, NotRequired, TypeAlias, TypedDict

Number: TypeAlias = int | float


{chr(10).join(classes.values())}
{chr(10).join(f"{name}: TypeAlias = {aliases[name]}" for name in in_order(aliases))}


class Method(NamedTuple):
    plane: str  # rpc, das or quote
    params: str  # by-position or by-name
    batch: str  # any, das (only with other DAS methods) or never
    permit: bool  # a batch holds at most two permit methods
    python: str  # the name of the typed call


METHODS: dict[str, Method] = {{
{table}
}}
SUBSCRIPTIONS: tuple[str, ...] = ({subscriptions},)


def _positional(*args: Any) -> list[Any]:
    """Positional params, without the trailing ones the caller left out."""
    kept = list(args)
    while kept and kept[-1] is None:
        kept.pop()
    return kept


def _named(args: dict[str, Any]) -> dict[str, Any]:
    """Object params, without the ones the caller left out."""
    return {{key: value for key, value in args.items() if value is not None}}


class RpcMethods:
    """One typed call per served method. The client that mixes this in supplies _call."""

    def _call(self, method: str, params: Any) -> Any:
        raise NotImplementedError

{sync_methods}

class AsyncRpcMethods:
    """The same calls for the async client."""

    async def _call(self, method: str, params: Any) -> Any:
        raise NotImplementedError

{async_methods}'''
target.write_text(out)
print(f"{target.name}: {len(rpc)} methods, {len(classes)} TypedDicts, {len(aliases)} aliases")
