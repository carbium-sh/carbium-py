#!/usr/bin/env bash
# Builds the wheel in a container, then installs it in fresh python slim containers, checks what it holds, imports it
# and runs the README's quick start. CARBIUM_KEY may be unset: then the quick start must print the 401 refusal.
# SMOKE_EXTRA=stream installs the wheel with its stream extra and loads it.
# Usage: scripts/smoke.sh [image ...]   (default: python:3.11-slim python:3.13-slim python:3.15-slim)
set -euo pipefail
cd "$(dirname "$0")/.."
out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT
nice -n 19 docker run --rm --memory=1g --cpus=1 --pids-limit=256 -u "$(id -u):$(id -g)" -e HOME=/tmp -v "$PWD:/src:ro" -v "$out:/out" \
  python:3.13-slim sh -c 'cp -r /src/pyproject.toml /src/README.md /src/LICENSE /src/src /tmp/ && python -m pip wheel --quiet --no-deps --wheel-dir /out /tmp'
ls -l "$out"
images=("$@")
[ ${#images[@]} -gt 0 ] || images=(python:3.11-slim python:3.13-slim python:3.15-slim)
status=0
for image in "${images[@]}"; do
  echo "=== $image"
  if nice -n 19 docker run --rm --memory=1g --cpus=1 --pids-limit=256 ${CARBIUM_KEY:+-e CARBIUM_KEY} ${SMOKE_EXTRA:+-e SMOKE_EXTRA} -v "$out:/pkg:ro" -v "$PWD/scripts:/smoke:ro" \
    "$image" sh /smoke/smoke-in-container.sh; then
    echo "=== $image PASS"
  else
    echo "=== $image FAIL"; status=1
  fi
done
exit $status
