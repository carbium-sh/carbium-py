"""Carbon Stream is an extra: importing carbium never loads gRPC, and using the stream without the extra raises one
clear error naming it. Runs with and without the extra installed."""
from __future__ import annotations

import subprocess
import sys

CHILD = """
import sys
{block}
import asyncio, carbium
from carbium import CarbiumError, CarbiumStream
assert sys.modules.get("grpc") is None, "import carbium loaded grpc"
try:
    asyncio.run(CarbiumStream.connect("c_serv_" + "Extra1234"))
except CarbiumError as e:
    print(e.kind, e.message)
"""


def test_importing_carbium_never_loads_grpc():
    child = subprocess.run([sys.executable, "-c", "import sys, carbium; print(sorted(m for m in sys.modules if m.split('.')[0] in ('grpc', 'google')))"],
                           capture_output=True, text=True)
    assert (child.returncode, child.stdout.strip()) == (0, "[]"), child.stderr


def test_the_stream_without_the_extra_raises_one_error_that_names_it():
    child = subprocess.run([sys.executable, "-c", CHILD.format(block="sys.modules['grpc'] = None  # as if the extra were not installed")],
                           capture_output=True, text=True)
    assert child.returncode == 0, child.stderr
    assert child.stdout.startswith("stream Carbon Stream needs the stream extra: pip install 'carbium[stream]' ("), child.stdout
