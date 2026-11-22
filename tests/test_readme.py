"""The README's Python blocks, which become the PyPI page: each compiles as a script would, imports from carbium only
what it exports, binds a name before a top-level statement reads it, and reads only global names bound in the block,
builtins, or names the text around it assumes, declared here."""
from __future__ import annotations

import ast
import builtins
import re
import symtable
from pathlib import Path

import carbium

README = Path(__file__).resolve().parent.parent / "README.md"

# The names each block takes from the text around it, keyed by the block's first line after its plain imports.
DECLARES: dict[str, set[str]] = {
    "from carbium import Carbium, CarbiumError": set(),
    "from carbium import AsyncCarbium, Carbium": {"a", "b", "mint", "signed_base64", "SOL", "USDC"},
    "from carbium import CarbiumSocket": {"pubkey"},
    "from carbium import CarbiumStream": {"program_id", "handle", "reload_over_rpc"},
    "from carbium import verify_webhook": {"raw_body", "request"},
    "from carbium import CarbiumAccount": {"wallet"},
    "from carbium import CarbiumDca": {"USDC", "SOL", "owner", "signed_base64"},
    "from carbium import CarbiumTokens": {"mints", "mint"},
}


def unbound(code: str, where: str) -> set[str]:
    """The global names a block reads, in any scope, that it never binds and that are not builtins."""
    top = symtable.symtable(code, where, "exec")
    bound, read, todo = set(), set(), [top]
    while todo:
        table = todo.pop()
        todo += table.get_children()
        for symbol in table.get_symbols():
            if (table is top or symbol.is_declared_global()) and (symbol.is_assigned() or symbol.is_imported()):
                bound.add(symbol.get_name())
            if symbol.is_referenced() and (table is top or symbol.is_global()):
                read.add(symbol.get_name())
    return read - bound - set(dir(builtins))


def runs_now(node: ast.AST):
    """The node and what runs with it: everything under it but the bodies of functions and lambdas."""
    yield node
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
        for child in ast.iter_child_nodes(node):
            yield from runs_now(child)


def read_before_bound(tree: ast.Module) -> set[str]:
    """Names a top-level statement reads that only a later top-level statement binds."""
    bound: set[str] = set()
    early: set[str] = set()
    for statement in tree.body:
        nodes = list(runs_now(statement))
        binds = {n.id for n in nodes if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)}
        binds |= {a.asname or a.name.split(".")[0] for n in nodes if isinstance(n, (ast.Import, ast.ImportFrom)) for a in n.names}
        binds |= {n.name for n in nodes if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
        early |= {n.id for n in nodes if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)} - bound - binds
        bound |= binds
    return early & bound


def test_every_python_block_of_the_readme_compiles_and_reads_only_names_it_binds_or_declares():
    text = README.read_text()
    blocks = [(text.count("\n", 0, m.start()) + 2, m.group(1)) for m in re.finditer(r"^```python\n(.*?)^```", text, re.S | re.M)]
    assert blocks, "no python block in README.md"
    unused = set(DECLARES)
    for line, code in blocks:
        where = f"README.md:{line}"
        compile(code, where, "exec")  # a top-level await or async with fails here, as it would in a script
        tree = ast.parse(code)
        imported = {a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module == "carbium" for a in n.names}
        assert imported <= set(carbium.__all__), f"{where} imports what carbium does not export: {sorted(imported - set(carbium.__all__))}"
        early = read_before_bound(tree)
        assert not early, f"{where} reads before a later statement binds: {sorted(early)}"
        first = next((l for l in code.splitlines() if l.strip() and not l.startswith("import ")), "")
        assert first in DECLARES, f"{where} is a block with no DECLARES entry: add one keyed by its line {first!r}"
        unused.discard(first)
        assert unbound(code, where) == DECLARES[first], where
    assert not unused, f"DECLARES entries that no README block has: {sorted(unused)}"
