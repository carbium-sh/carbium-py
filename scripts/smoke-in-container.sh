#!/bin/sh
# Runs inside a plain python image: installs the wheel (its dependencies from PyPI), lists it against a whitelist,
# imports it, and runs the README's first python block, the quick start.
set -eu
cd "$(mktemp -d)"
python --version
wheel=$(ls /pkg/carbium-*.whl)
python -m pip install --quiet --no-cache-dir --disable-pip-version-check --root-user-action=ignore "$wheel${SMOKE_EXTRA:+[$SMOKE_EXTRA]}"
python -m pip check
echo "--- wheel contents"
python -m zipfile -l /pkg/carbium-*.whl | awk 'NR > 1 {print $1}' | sort > files.txt
cat files.txt
grep -qx 'carbium/py.typed' files.txt || { echo "the wheel holds no py.typed"; exit 1; }
if grep -vE '^(carbium/([A-Za-z_]+\.py|py\.typed)?|carbium/_geyser/([A-Za-z0-9_]+\.pyi?|LICENSE|NOTICE)?|carbium-[0-9.]+\.dist-info/[A-Za-z/._-]*)$' files.txt; then echo "unexpected file in the wheel"; exit 1; fi
grep -qx 'carbium/_geyser/NOTICE' files.txt || { echo "the wheel holds no NOTICE for the generated stream modules"; exit 1; }
if grep -E '(^|/)(tests?/|scripts/|conftest\.py|test_[^/]*\.py)' files.txt; then echo "a test or script file in the wheel"; exit 1; fi
version=$(python -c "import carbium; print(carbium.__version__)")
echo "carbium $version"
[ "$version" = "0.1.0" ] || { echo "the version is not 0.1.0"; exit 1; }
echo "--- quick start from the installed README"
python -c "import importlib.metadata as m, re; text = m.distribution('carbium').read_text('METADATA'); print(re.search(r'\`\`\`python\n(.*?)\`\`\`', text, re.S).group(1))" > quickstart.py
test -s quickstart.py
cat quickstart.py
out=$(python quickstart.py 2>&1) || { echo "$out"; echo "the quick start exited non-zero"; exit 1; }
echo "$out"
if [ -n "${CARBIUM_KEY:-}" ]; then
  echo "$out" | grep -q '^blockhash ' || { echo "with a key the quick start must print the blockhash"; exit 1; }
  echo "$out" | grep -qF "$CARBIUM_KEY" && { echo "the key leaked into the output"; exit 1; }
else
  echo "$out" | grep -qE '^auth 401 -32001 ' || { echo "keyless, the quick start must print the 401 refusal"; exit 1; }
fi
echo "--- the stream extra"
if [ -n "${SMOKE_EXTRA:-}" ]; then
  python -c "import grpc, google.protobuf; from carbium._geyser import geyser_pb2_grpc; from carbium import CarbiumStream; print('stream extra loads: grpcio', grpc.__version__, 'protobuf', google.protobuf.__version__)"
else
  python -c "
import asyncio, sys, carbium
assert not any(m.split('.')[0] in ('grpc', 'google') for m in sys.modules), 'import carbium loaded grpc'
try:
    asyncio.run(carbium.CarbiumStream.connect('c_serv_' + 'Smoke1234'))
except carbium.CarbiumError as e:
    print(e.kind, e.message)
" | tee stream.txt
  grep -q "^stream Carbon Stream needs the stream extra: pip install 'carbium\[stream\]'" stream.txt || { echo "without the extra the stream must name it"; exit 1; }
fi
echo "smoke ok on $(python --version)${SMOKE_EXTRA:+ with the $SMOKE_EXTRA extra}"
