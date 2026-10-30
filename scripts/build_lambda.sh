#!/usr/bin/env bash
# Build dist/lambda/bronze/file_ingestion.zip for the three ingestion Lambdas (python3.12, x86_64).
#   * erl package (only the modules the handlers import + contracts/XSDs)
#   * pyarrow and lxml wheels for manylinux x86_64 / CPython 3.12, pinned to uv.lock.
#     pyarrow is bundled: the AWS-managed "AWS SDK for pandas" layer's PyArrow 24.0.0 could
#     not write the contract's Zstandard codec (AWS attempt of 2026-10-30). No layer is used.
#   * boto3 is NOT bundled: it comes from the Lambda runtime.
# The ZIP is above the 50 MB direct-upload limit; Terraform uploads it to the artifacts bucket.
# Smoke test of the built file: uv run python scripts/smoke_lambda_package.py
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=build/lambda
ZIP=dist/lambda/bronze/file_ingestion.zip
pin() { uv export --frozen --no-hashes --format requirements-txt | sed -n "s/^$1==\([0-9.]*\).*/\1/p"; }
LXML_VERSION=$(pin lxml)
PYARROW_VERSION=$(pin pyarrow)

rm -rf "$OUT" "$ZIP"
mkdir -p "$OUT/erl" "$(dirname "$ZIP")"
uv pip install --quiet --target "$OUT" --python-platform x86_64-manylinux_2_28 --python-version 3.12 \
  --only-binary :all: --no-deps "lxml==${LXML_VERSION}" "pyarrow==${PYARROW_VERSION}"
# headers, test suites and Cython sources are not needed at runtime
rm -rf "$OUT/pyarrow/include" "$OUT/pyarrow/tests" "$OUT/pyarrow/src"
find "$OUT/pyarrow" \( -name '*.pyx' -o -name '*.pxd' \) -delete

for f in __init__.py errors.py log.py storage.py; do cp "src/erl/$f" "$OUT/erl/"; done
mkdir -p "$OUT/erl/ingestion" "$OUT/erl/aws" "$OUT/erl/contracts"
cp src/erl/ingestion/{__init__,bronze,files}.py "$OUT/erl/ingestion/"
cp src/erl/aws/{__init__,lambda_handlers}.py "$OUT/erl/aws/"
cp src/erl/contracts/{__init__.py,*.json,*.xsd} "$OUT/erl/contracts/"

# deterministic archive: fixed timestamps and sorted entries
find "$OUT" -name '__pycache__' -prune -exec rm -rf {} +
python3 - "$OUT" "$ZIP" <<'PY'
import os, sys, zipfile
src, dst = sys.argv[1], sys.argv[2]
files = sorted(os.path.join(r, f) for r, _, fs in os.walk(src) for f in fs)
with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as z:
    for path in files:
        info = zipfile.ZipInfo(os.path.relpath(path, src), date_time=(2020, 1, 1, 0, 0, 0))
        info.external_attr = 0o644 << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        with open(path, "rb") as fh:
            z.writestr(info, fh.read())
PY
echo "built $ZIP ($(du -h "$ZIP" | cut -f1)), pyarrow ${PYARROW_VERSION}, lxml ${LXML_VERSION}, sha256 $(sha256sum "$ZIP" | cut -c1-16)"
