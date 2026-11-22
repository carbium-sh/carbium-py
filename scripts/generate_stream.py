#!/usr/bin/env python3
"""Carbon Stream's generated messages and stub, from the upstream Yellowstone protos (Apache-2.0).

  python scripts/generate_stream.py fetch      downloads the protos and their licence at the pinned commit into
                                               spec/yellowstone/ and drops the calls the service does not serve
  python scripts/generate_stream.py [out_dir]  writes the modules (default src/carbium/_geyser) with grpcio-tools
                                               1.85.3; the service must hold Subscribe and the served unary calls only
"""
from __future__ import annotations

import importlib.metadata
import importlib.resources
import json
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROTOS = ROOT / "spec/yellowstone"
COMMIT = "3c66a659b947e6df09136fc7570bf9918588e2fb"  # release v17.0.2+solana.4.4.1
UPSTREAM = f"https://raw.githubusercontent.com/rpcpool/yellowstone-grpc/{COMMIT}/yellowstone-grpc-proto"
UNSERVED = ("SubscribeDeshred", "SubscribeGossip")  # rpc lines removed from the service; every message stays as it is
TOOLS = "1.85.3"


def fail(message: str) -> None:
    sys.exit(f"generate_stream: {message}")


def fetch() -> None:
    PROTOS.mkdir(parents=True, exist_ok=True)
    for name, remote in (("geyser.proto", "proto/geyser.proto"), ("solana-storage.proto", "proto/solana-storage.proto"), ("LICENSE", "LICENSE_APACHE2")):
        text = urllib.request.urlopen(f"{UPSTREAM}/{remote}", timeout=30).read().decode()
        if name == "geyser.proto":
            lines = text.splitlines(keepends=True)
            removed = [line for line in lines if re.match(rf"\s*rpc ({'|'.join(UNSERVED)})\(", line)]
            if len(removed) != len(UNSERVED):
                fail("the upstream service no longer has the calls this script removes")
            header = [f"// Changed from yellowstone-grpc-proto/proto/geyser.proto at commit {COMMIT} (Apache-2.0, see LICENSE).\n",
                      "// Removed from service Geyser, as Carbon Stream does not serve them; nothing else is changed:\n",
                      *(f"//   {line.strip()}\n" for line in removed), "\n"]
            text = "".join(header + [line for line in lines if line not in removed])
        (PROTOS / name).write_text(text)
    print(f"fetched {COMMIT[:12]} into {PROTOS.relative_to(ROOT)}")


def generate(out: Path) -> None:
    if importlib.metadata.version("grpcio-tools") != TOOLS:
        fail(f"grpcio-tools {TOOLS} makes the committed files: install that version")
    from grpc_tools import protoc

    served = json.loads((ROOT / "spec/served.json").read_text())["stream"]["unary"]
    calls = re.findall(r"^\s*rpc (\w+)\(", (PROTOS / "geyser.proto").read_text(), re.M)
    if sorted(calls) != sorted(["Subscribe", *served]):
        fail(f"the service holds {sorted(calls)}, not Subscribe and the served unary calls")
    out.mkdir(parents=True, exist_ok=True)
    include = importlib.resources.files("grpc_tools") / "_proto"
    status = protoc.main(["protoc", f"-I{PROTOS}", f"-I{include}", f"--python_out={out}", f"--pyi_out={out}", f"--grpc_python_out={out}", "geyser.proto", "solana-storage.proto"])
    if status != 0:
        fail(f"protoc stopped with {status}")
    for path in sorted(out.glob("*_pb2*.py*")):  # the generated imports are top level; inside the package they are relative
        text = re.sub(r"^import (\w+_pb2) as", r"from . import \1 as", path.read_text(), flags=re.M)
        path.write_text(re.sub(r"^from (\w+_pb2) import", r"from .\1 import", text, flags=re.M))
    print(f"generated {', '.join(p.name for p in sorted(out.glob('*_pb2*.py*')))}")


if __name__ == "__main__":
    if sys.argv[1:] == ["fetch"]:
        fetch()
    else:
        generate(Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "src/carbium/_geyser")
